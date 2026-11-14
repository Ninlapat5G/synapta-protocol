//! Inbound side: accept connections, authenticate them, then serve the peer's requests.
//!
//! Defences, in order: connection-count limit → handshake within `handshake_ms` (nothing else is
//! accepted before it) → per-frame size limit → idle timeout → per-connection in-flight request
//! limit. A malformed or oversized frame gets an error frame and the connection is closed; the
//! node itself carries on.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWrite;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

use super::flooding;
use super::handshake::{self, HandshakeParams};
use crate::config::ms;
use crate::error::{ErrorBody, ErrorCode};
use crate::execution::EventSink;
use crate::node::service;
use crate::node::state::NodeState;
use crate::protocol::message::{Frame, Notification, ResponseFrame};
use crate::transport::framing::{read_frame, write_frame, FrameError};
use crate::transport::{BoxedConnection, Listener};

const OUTBOUND_QUEUE: usize = 256;
/// After a connection is closed, queued frames (such as the error explaining why) get this
/// long to be written.
const FLUSH_ON_CLOSE: Duration = Duration::from_millis(200);

pub(crate) async fn run_listener(state: Arc<NodeState>, mut listener: Box<dyn Listener>) {
    let permits = Arc::new(Semaphore::new(state.config.limits.max_inbound_connections));
    loop {
        let accepted = tokio::select! {
            _ = state.shutdown.cancelled() => return,
            accepted = listener.accept() => accepted,
        };
        match accepted {
            Ok((conn, remote)) => {
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    tracing::warn!(%remote, "inbound connection limit reached, refusing");
                    continue;
                };
                let conn_state = state.clone();
                state.tasks.spawn(async move {
                    serve_connection(conn_state, conn, remote).await;
                    drop(permit);
                });
            }
            Err(e) => {
                // e.g. out of file descriptors; pause instead of spinning on the error.
                tracing::warn!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn serve_connection(state: Arc<NodeState>, mut conn: BoxedConnection, remote: SocketAddr) {
    let limits = &state.config.limits;
    let timeouts = &state.config.timeouts;
    let params = HandshakeParams {
        identity: &state.identity,
        trust: &state.trust,
        max_frame_bytes: limits.max_frame_bytes,
    };
    let peer = match tokio::time::timeout(ms(timeouts.handshake_ms), handshake::accept(&mut conn, &params)).await {
        Ok(Ok(peer)) => peer,
        Ok(Err(e)) => {
            tracing::warn!(%remote, code = ?e.code, error = %e.message, "inbound handshake refused");
            return;
        }
        Err(_) => {
            tracing::warn!(%remote, "inbound handshake timed out");
            return;
        }
    };
    tracing::info!(peer = %peer, %remote, "inbound session authenticated");

    let (mut reader, writer) = tokio::io::split(conn);
    let (tx, rx) = mpsc::channel(OUTBOUND_QUEUE);
    let closed = state.shutdown.child_token();
    state
        .tasks
        .spawn(write_loop(writer, rx, closed.clone(), limits.max_frame_bytes));
    let inflight = Arc::new(Semaphore::new(limits.max_inflight_requests_per_connection));

    loop {
        let frame = tokio::select! {
            _ = closed.cancelled() => break,
            frame = tokio::time::timeout(ms(timeouts.idle_timeout_ms), read_frame(&mut reader, limits.max_frame_bytes)) => frame,
        };
        let frame = match frame {
            Err(_) => {
                tracing::info!(peer = %peer, "closing idle inbound session");
                break;
            }
            Ok(Err(FrameError::Closed)) => break,
            Ok(Err(FrameError::Io(e))) => {
                tracing::info!(peer = %peer, error = %e, "inbound connection lost");
                break;
            }
            Ok(Err(e)) => {
                tracing::warn!(peer = %peer, error = %e, "bad frame, closing session");
                if let Some(body) = e.to_error_body() {
                    let _ = tx.try_send(Frame::Error(body));
                }
                break;
            }
            Ok(Ok(frame)) => frame,
        };
        match frame {
            Frame::Request(request) => {
                let Ok(permit) = inflight.clone().try_acquire_owned() else {
                    let busy = ErrorBody::new(ErrorCode::Busy, "too many requests in flight on this connection");
                    let _ = tx.try_send(Frame::Response(ResponseFrame::from_result(request.id, Err(busy))));
                    continue;
                };
                let (handler_state, handler_tx, handler_peer) = (state.clone(), tx.clone(), peer.clone());
                state.tasks.spawn(async move {
                    let sink = event_sink(handler_tx.clone());
                    let outcome = service::handle_request(&handler_state, &handler_peer, request.request, sink).await;
                    let _ = handler_tx
                        .send(Frame::Response(ResponseFrame::from_result(request.id, outcome)))
                        .await;
                    drop(permit);
                });
            }
            Frame::Notify(Notification::Advert(signed)) => {
                if let Err(e) = flooding::accept_advert(&state, signed, Some(&peer)) {
                    tracing::warn!(peer = %peer, error = %e, "advert rejected");
                }
            }
            other => {
                tracing::warn!(peer = %peer, ?other, "unexpected frame on inbound session, closing");
                let body = ErrorBody::new(ErrorCode::BadRequest, "unexpected frame type after handshake");
                let _ = tx.try_send(Frame::Error(body));
                break;
            }
        }
    }
    closed.cancel();
    tracing::info!(peer = %peer, "inbound session closed");
}

/// Job events for jobs submitted on this connection are pushed back on it. Best effort:
/// if the queue is full the event is dropped; the final state stays queryable.
fn event_sink(tx: mpsc::Sender<Frame>) -> EventSink {
    Arc::new(move |event| {
        let _ = tx.try_send(Frame::Event(event));
    })
}

async fn write_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut rx: mpsc::Receiver<Frame>,
    closed: CancellationToken,
    max_frame: usize,
) {
    loop {
        let frame = tokio::select! {
            frame = rx.recv() => frame,
            _ = closed.cancelled() => {
                // Write what is already queued (e.g. the error frame saying why), then stop.
                let _ = tokio::time::timeout(FLUSH_ON_CLOSE, async {
                    while let Ok(frame) = rx.try_recv() {
                        if write_frame(&mut writer, &frame, max_frame).await.is_err() {
                            break;
                        }
                    }
                })
                .await;
                return;
            }
        };
        let Some(frame) = frame else { return };
        if write_frame(&mut writer, &frame, max_frame).await.is_err() {
            closed.cancel();
            return;
        }
    }
}

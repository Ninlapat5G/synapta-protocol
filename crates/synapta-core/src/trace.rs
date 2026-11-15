//! Optional report of what a node sends and receives, for tools that draw the network
//! (synapta-lab animates it). Off unless a [`TraceSink`] is given to [`crate::start_with`]; with
//! no sink nothing is built, so production nodes pay nothing.
//!
//! A frame is reported in plain form (kind, id, method, a short detail) by the node that wrote
//! it and again by the node that read it. To pair the two, match `(node, peer, channel, id)` on
//! one side with `(peer, node, opposite channel, id)` on the other. Inputs and outputs of jobs
//! are not included.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::discovery::advert;
use crate::error::ErrorCode;
use crate::protocol::message::{Envelope, Event, Frame, Notification, Request};
use crate::protocol::NodeId;

/// Receives every trace event. Called on the node's own tasks, so it must return quickly and
/// never block (copy the event into a queue and return).
pub type TraceSink = Arc<dyn Fn(TraceEvent) + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TraceEvent {
    /// Microseconds since the Unix epoch.
    pub at_us: u64,
    /// The node reporting.
    pub node: NodeId,
    /// The node at the other end of the connection.
    pub peer: NodeId,
    #[serde(flatten)]
    pub kind: TraceKind,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TraceKind {
    Sent {
        channel: Channel,
        frame: FrameSummary,
    },
    Received {
        channel: Channel,
        frame: FrameSummary,
    },
    /// This node's outbound link to `peer` became READY.
    LinkUp,
    /// This node's outbound link to `peer` closed.
    LinkDown {
        reason: String,
    },
}

/// Which of the pair's two connections a frame travelled on. Each node dials each peer, so
/// requests from `node` go out on its `Outbound` link and arrive on the peer's `Inbound` one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Outbound,
    Inbound,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FrameSummary {
    /// `request`, `response`, `event`, `notify`, `error` or `unknown`.
    pub frame: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    /// Request method, notification method or event name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<&'static str>,
    /// One useful fact: the capability called, the job asked about, the advert's origin, the
    /// error code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// For responses: whether it carries a result (true) or an error (false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
}

impl FrameSummary {
    pub fn of(frame: &Frame) -> Self {
        let summary = |frame| FrameSummary {
            frame,
            id: None,
            method: None,
            detail: None,
            ok: None,
        };
        match frame {
            Frame::Request(r) => FrameSummary {
                id: Some(r.id),
                method: Some(r.request.method()),
                detail: request_detail(&r.request),
                ..summary("request")
            },
            Frame::Response(r) => FrameSummary {
                id: Some(r.id),
                ok: Some(r.error.is_none()),
                detail: r.error.as_ref().map(|e| code_name(e.code)),
                ..summary("response")
            },
            Frame::Event(event) => {
                let (method, job) = match event {
                    Event::JobProgress(p) => ("job_progress", Some(p.job_id.clone())),
                    Event::JobFinished { job_id, .. } => ("job_finished", Some(job_id.clone())),
                    Event::Unknown => ("unknown", None),
                };
                FrameSummary {
                    method: Some(method),
                    detail: job,
                    ..summary("event")
                }
            }
            Frame::Notify(Notification::Advert(signed)) => FrameSummary {
                method: Some("advert"),
                detail: advert_origin(&signed.payload),
                ..summary("notify")
            },
            Frame::Notify(Notification::Unknown) => FrameSummary {
                method: Some("unknown"),
                ..summary("notify")
            },
            Frame::Error(error) => FrameSummary {
                detail: Some(code_name(error.code)),
                ..summary("error")
            },
            // Handshake frames are not traced (they happen before the peer is known); anything
            // else is a frame type this build does not know.
            _ => summary("unknown"),
        }
    }
}

/// The origin an advert payload names, read from its start: `node_id` is the first field
/// `NodeAdvert` serialises. Parsing the whole payload (kilobytes, with every neighbour) for every
/// traced advert stalled a lab of 100 nodes; the full parse is only the fallback.
fn advert_origin(payload: &str) -> Option<String> {
    let quick = payload
        .strip_prefix(r#"{"node_id":""#)
        .and_then(|rest| rest.split_once('"'))
        .map(|(id, _)| id.to_string());
    quick.or_else(|| advert::claimed_origin_of(payload).map(|id| id.to_string()))
}

/// The code as it appears on the wire (`busy`, `timeout`, ...).
fn code_name(code: ErrorCode) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

fn request_detail(request: &Request) -> Option<String> {
    match request {
        Request::Invoke { capability, .. } | Request::JobSubmit { capability, .. } => Some(capability.clone()),
        Request::JobStatus { job_id } | Request::JobResult { job_id, .. } | Request::JobCancel { job_id } => {
            Some(job_id.clone())
        }
        Request::SyncAdverts { known } => Some(format!("{} known", known.len())),
        Request::Relay(relay) => {
            let inner = serde_json::from_str::<Envelope>(&relay.envelope.payload)
                .map(|e| format!(" {} from {}", e.request.method(), e.from))
                .unwrap_or_default();
            Some(format!("to {}{inner}", relay.to))
        }
        Request::Ping | Request::Unknown => None,
    }
}

/// A node's handle on the sink: knows who is reporting, and does nothing without a sink.
#[derive(Clone)]
pub(crate) struct Tracer {
    node: NodeId,
    sink: Option<TraceSink>,
}

impl Tracer {
    pub fn new(node: NodeId, sink: Option<TraceSink>) -> Self {
        Self { node, sink }
    }

    pub fn sent(&self, peer: &NodeId, channel: Channel, frame: &Frame) {
        self.emit(peer, || TraceKind::Sent {
            channel,
            frame: FrameSummary::of(frame),
        });
    }

    pub fn received(&self, peer: &NodeId, channel: Channel, frame: &Frame) {
        self.emit(peer, || TraceKind::Received {
            channel,
            frame: FrameSummary::of(frame),
        });
    }

    pub fn link_up(&self, peer: &NodeId) {
        self.emit(peer, || TraceKind::LinkUp);
    }

    pub fn link_down(&self, peer: &NodeId, reason: &str) {
        self.emit(peer, || TraceKind::LinkDown { reason: reason.into() });
    }

    fn emit(&self, peer: &NodeId, kind: impl FnOnce() -> TraceKind) {
        let Some(sink) = &self.sink else { return };
        sink(TraceEvent {
            at_us: now_us(),
            node: self.node.clone(),
            peer: peer.clone(),
            kind: kind(),
        });
    }
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

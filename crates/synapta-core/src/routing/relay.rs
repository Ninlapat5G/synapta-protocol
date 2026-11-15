//! Calls to nodes this node has no link to — they sit in another ring — passed hop by hop
//! through the nodes that bridge the rings.
//!
//! ```text
//! node-a ──relay{to: z, ttl, envelope}──▶ node-x ──relay{to: z, ttl-1, envelope}──▶ node-z
//!        ◀──────────── signed reply ───────────── ◀──────────── signed reply ──────────────
//! ```
//!
//! - Each hop passes the request to its own next hop toward `to` (`routing::paths`), over its
//!   own encrypted link, and waits for the answer. A hop with no path answers `unreachable`;
//!   one that would have to pass it on with no hops left answers `ttl_exceeded`. Both mean
//!   nothing ran, so automatic routing may try another node.
//! - The origin signs the [`Envelope`] (who asks what of whom); `to` signs the [`RelayReply`].
//!   Without that, a hop could claim to be any node — and capability access lists check the
//!   caller — or make up a result. `to` remembers each envelope's nonce until it expires, so a
//!   recorded envelope cannot be run twice.
//! - Not end-to-end encrypted: the hops are trusted members (each link is encrypted), and any
//!   of them could have been picked to run the job anyway.
//! - Job progress events are not relayed; a relayed job's status and result are still there
//!   to ask for.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use super::paths;
use crate::config::ms;
use crate::discovery::link::LinkHandle;
use crate::error::{ErrorBody, ErrorCode};
use crate::execution::EventSink;
use crate::identity::{random_bytes, to_hex};
use crate::node::service;
use crate::node::state::NodeState;
use crate::protocol::message::{Envelope, RelayReply, RelayRequest, Request, SignedPayload};
use crate::protocol::NodeId;

const ENVELOPE_DOMAIN: &str = "synapta-relay-envelope\n";
const REPLY_DOMAIN: &str = "synapta-relay-reply\n";
/// Hops a relayed request may take. Paths are shortest paths, so this only stops a request
/// that would otherwise go round while the graph is changing.
pub(crate) const MAX_HOPS: u8 = 8;
/// Extra wait per hop, so each hop's answer (including a `timeout` from further on) reaches
/// the hop before it gives up on its own.
const HOP_GRACE: Duration = Duration::from_millis(1_000);
/// Envelopes remembered against replay. Past this, new relayed calls are refused as `busy`
/// until old entries expire.
const MAX_REMEMBERED_NONCES: usize = 65_536;

/// Envelope nonces seen by this node, with when each stops mattering (Unix ms).
#[derive(Default)]
pub(crate) struct SeenNonces {
    expiry: HashMap<(NodeId, String), u64>,
}

impl SeenNonces {
    /// Records the nonce. Refuses one already recorded (a replay), or any when full.
    fn insert(&mut self, from: &NodeId, nonce: &str, expires_at_ms: u64, now_ms: u64) -> Result<(), ErrorBody> {
        if self.expiry.len() >= MAX_REMEMBERED_NONCES {
            self.expiry.retain(|_, expiry| *expiry > now_ms);
        }
        if self.expiry.contains_key(&(from.clone(), nonce.to_string())) {
            return Err(ErrorBody::new(
                ErrorCode::BadRequest,
                "relayed request was already delivered",
            ));
        }
        if self.expiry.len() >= MAX_REMEMBERED_NONCES {
            return Err(ErrorBody::new(ErrorCode::Busy, "too many relayed requests in flight"));
        }
        self.expiry.insert((from.clone(), nonce.to_string()), expires_at_ms);
        Ok(())
    }
}

/// Origin side: sends `request` to `to` through `via` (the first hop) and returns what `to`
/// answered, after checking `to` signed it.
pub(crate) async fn call(
    state: &Arc<NodeState>,
    via: &LinkHandle,
    to: &NodeId,
    hops: u32,
    request: Request,
) -> Result<Value, ErrorBody> {
    let request_ms = state.config.timeouts.request_ms;
    let envelope = Envelope {
        from: state.node_id().clone(),
        to: to.clone(),
        nonce: to_hex(&random_bytes::<16>()),
        // Long enough for any hop count; only bounds how long `to` must remember the nonce.
        expires_at_ms: now_ms() + 2 * (request_ms + HOP_GRACE.as_millis() as u64 * u64::from(MAX_HOPS)),
        request,
    };
    let nonce = envelope.nonce.clone();
    let relay = Request::Relay(RelayRequest {
        to: to.clone(),
        ttl: MAX_HOPS - 1,
        envelope: sign(state, ENVELOPE_DOMAIN, &envelope),
    });
    let signed: SignedPayload = decode(via.call(relay, wait_for(state, hops)).await?)?;
    let reply: RelayReply = open(state, REPLY_DOMAIN, &signed, |r: &RelayReply| &r.from)?;
    if &reply.from != to || &reply.to != state.node_id() || reply.nonce != nonce {
        return Err(ErrorBody::new(
            ErrorCode::Internal,
            format!("relayed reply from {} does not answer this request", reply.from),
        ));
    }
    match (reply.result, reply.error) {
        (_, Some(error)) => Err(error),
        (Some(result), None) => Ok(result),
        (None, None) => Err(ErrorBody::new(ErrorCode::MalformedFrame, "relayed reply is empty")),
    }
}

/// Any hop: deliver the request if it is for this node, otherwise pass it on.
pub(crate) async fn handle(state: &Arc<NodeState>, relay: RelayRequest) -> Result<Value, ErrorBody> {
    if &relay.to == state.node_id() {
        return deliver(state, relay.envelope).await;
    }
    let routes = paths::current(state);
    let Some(route) = routes.get(&relay.to) else {
        return Err(ErrorBody::new(
            ErrorCode::Unreachable,
            format!("{} has no path to {}", state.node_id(), relay.to),
        ));
    };
    if relay.ttl == 0 {
        return Err(ErrorBody::new(
            ErrorCode::TtlExceeded,
            format!(
                "relayed request for {} ran out of hops at {}",
                relay.to,
                state.node_id()
            ),
        ));
    }
    let Some(next) = state.registry.read().unwrap().ready_link(&route.next_hop) else {
        return Err(ErrorBody::new(
            ErrorCode::Unreachable,
            format!("{}'s link to {} is down", state.node_id(), route.next_hop),
        ));
    };
    let hops = route.hops;
    let onward = Request::Relay(RelayRequest {
        ttl: relay.ttl - 1,
        ..relay
    });
    next.call(onward, wait_for(state, hops)).await
}

/// Destination: check the envelope, run the request as its origin, sign the answer.
async fn deliver(state: &Arc<NodeState>, signed: SignedPayload) -> Result<Value, ErrorBody> {
    let envelope: Envelope = open(state, ENVELOPE_DOMAIN, &signed, |e: &Envelope| &e.from)?;
    let now = now_ms();
    if &envelope.to != state.node_id() {
        return Err(ErrorBody::new(
            ErrorCode::BadRequest,
            format!("envelope is for {}", envelope.to),
        ));
    }
    if now >= envelope.expires_at_ms {
        return Err(ErrorBody::new(ErrorCode::BadRequest, "relayed request has expired"));
    }
    state
        .relay_nonces
        .lock()
        .unwrap()
        .insert(&envelope.from, &envelope.nonce, envelope.expires_at_ms, now)?;
    let outcome = match envelope.request {
        Request::Invoke { .. }
        | Request::JobSubmit { .. }
        | Request::JobStatus { .. }
        | Request::JobResult { .. }
        | Request::JobCancel { .. } => {
            // Boxed: handle_request can lead back here, so the future would contain itself.
            let no_events: EventSink = Arc::new(|_| {});
            Box::pin(service::handle_request(
                state,
                &envelope.from,
                envelope.request,
                no_events,
            ))
            .await
        }
        other => Err(ErrorBody::new(
            ErrorCode::UnsupportedRequest,
            format!("{} cannot be relayed", other.method()),
        )),
    };
    let (result, error) = match outcome {
        Ok(value) => (Some(value), None),
        Err(error) => (None, Some(error)),
    };
    let reply = RelayReply {
        from: state.node_id().clone(),
        to: envelope.from,
        nonce: envelope.nonce,
        result,
        error,
    };
    serde_json::to_value(sign(state, REPLY_DOMAIN, &reply))
        .map_err(|e| ErrorBody::new(ErrorCode::Internal, e.to_string()))
}

/// How long a hop `hops` away from the target waits: further away waits longer, so the
/// nearer hop's answer always arrives first.
fn wait_for(state: &NodeState, hops: u32) -> Duration {
    ms(state.config.timeouts.request_ms) + HOP_GRACE * hops.max(1)
}

fn sign<T: Serialize>(state: &NodeState, domain: &str, value: &T) -> SignedPayload {
    // Serialising a plain struct of owned fields cannot fail.
    let payload = serde_json::to_string(value).expect("relay message serialises");
    let signature = state.identity.sign(format!("{domain}{payload}").as_bytes());
    SignedPayload { payload, signature }
}

/// Parses a signed payload and checks the node it names as author (`author`) signed it.
fn open<T: DeserializeOwned>(
    state: &NodeState,
    domain: &str,
    signed: &SignedPayload,
    author: impl Fn(&T) -> &NodeId,
) -> Result<T, ErrorBody> {
    let value: T = serde_json::from_str(&signed.payload)
        .map_err(|e| ErrorBody::new(ErrorCode::BadRequest, format!("relay payload: {e}")))?;
    let author = author(&value);
    if !state.trust.verify(
        author,
        format!("{domain}{}", signed.payload).as_bytes(),
        &signed.signature,
    ) {
        return Err(ErrorBody::new(
            ErrorCode::UntrustedPeer,
            format!("relayed message is not signed by {author}"),
        ));
    }
    Ok(value)
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, ErrorBody> {
    serde_json::from_value(value).map_err(|e| ErrorBody::new(ErrorCode::MalformedFrame, format!("relay answer: {e}")))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

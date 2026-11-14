//! Structured errors that cross the wire.
//!
//! Every failure a peer, the router or an executor reports is an [`ErrorBody`] with a stable
//! machine-readable [`ErrorCode`]. Callers branch on the code, never on the message text.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The request was well-formed JSON but its content is invalid.
    BadRequest,
    /// A frame could not be decoded as a protocol message.
    MalformedFrame,
    /// A frame exceeded the configured size limit.
    FrameTooLarge,
    /// The handshake failed (bad signature, wrong peer, protocol violation).
    HandshakeFailed,
    /// The peer's node id is not in this node's trust configuration.
    UntrustedPeer,
    /// The peer speaks an incompatible protocol version.
    VersionMismatch,
    /// A request arrived before the handshake completed.
    NotReady,
    /// The caller is not allowed to use this capability on this node.
    Unauthorized,
    /// The explicit target node is not known to this node.
    TargetNotFound,
    /// The node (explicit target or executing node) does not provide the capability.
    CapabilityNotSupported,
    /// The explicit target is known but not reachable / not accepting work.
    TargetUnavailable,
    /// Automatic routing found no eligible node.
    NoEligibleNode,
    /// The executing node is at capacity. Guaranteed to be rejected before execution.
    Busy,
    /// The operation did not complete in time.
    Timeout,
    /// The connection to the peer was lost while the request was in flight.
    ConnectionLost,
    JobNotFound,
    Cancelled,
    ExecutionFailed,
    ShuttingDown,
    Internal,
}

impl ErrorCode {
    /// Codes that the executing node only returns *before* running anything.
    /// Automatic routing may safely try the next candidate after these; for every other
    /// failure the request may already have executed, so retrying could run it twice.
    pub fn is_pre_execution_rejection(self) -> bool {
        matches!(self, ErrorCode::Busy)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl ErrorBody {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }
}

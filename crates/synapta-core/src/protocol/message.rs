//! Every message that crosses a Synapta connection. `docs/protocol.md` documents this file;
//! change both together.
//!
//! Each [`Frame`] is UTF-8 JSON. `Hello` and `HelloAck` (and an `Error` refusing them) travel as
//! a 4-byte big-endian length plus the JSON (`transport::framing`); every later frame is
//! encrypted (`transport::secure`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{NodeId, NodeType};
use crate::error::ErrorBody;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    // --- handshake (dialer ⇄ acceptor), see discovery::handshake ---
    Hello(Hello),
    HelloAck(HelloAck),
    HelloProof(HelloProof),
    HelloFinish(HelloFinish),
    Welcome,
    // --- after the handshake ---
    /// Dialer → acceptor. Answered by exactly one `Response` with the same id.
    Request(RequestFrame),
    /// Acceptor → dialer.
    Response(ResponseFrame),
    /// Acceptor → dialer, unsolicited (job progress for jobs this dialer submitted).
    Event(Event),
    /// Dialer → acceptor, one-way (link-state advert flooding).
    Notify(Notification),
    /// Either side, sent right before closing because of a protocol violation.
    Error(ErrorBody),
}

/// Dialer → acceptor, in the clear.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hello {
    pub protocol_version: String,
    pub node_id: NodeId,
    /// First Noise NN handshake message (the dialer's ephemeral key), hex. Defaults to empty so a
    /// v0.0.1 hello still parses and is refused with `version_mismatch` rather than as malformed.
    #[serde(default)]
    pub noise: String,
}

/// Acceptor → dialer, in the clear.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelloAck {
    pub protocol_version: String,
    pub node_id: NodeId,
    /// Second Noise NN message (the acceptor's ephemeral key), hex. Default: see [`Hello::noise`].
    #[serde(default)]
    pub noise: String,
}

/// Acceptor → dialer, encrypted: proof the acceptor holds its node key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelloProof {
    /// Acceptor's signature over the handshake transcript, hex.
    pub signature: String,
}

/// Dialer → acceptor, encrypted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelloFinish {
    /// Dialer's signature over the handshake transcript, hex.
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestFrame {
    pub id: u64,
    pub request: Request,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum Request {
    #[serde(rename = "ping")]
    Ping,
    /// Exchange full link-state databases right after a link becomes READY.
    #[serde(rename = "adverts.sync")]
    SyncAdverts { adverts: Vec<SignedAdvert> },
    /// Execute a capability on the receiving node and wait for the result.
    /// The receiver never forwards: it executes locally or rejects (no multi-hop in v0.0.1).
    #[serde(rename = "invoke")]
    Invoke { capability: String, input: Value },
    #[serde(rename = "job.submit")]
    JobSubmit { capability: String, input: Value },
    #[serde(rename = "job.status")]
    JobStatus { job_id: String },
    /// Wait up to `wait_ms` for the job to finish, then return its current info.
    #[serde(rename = "job.result")]
    JobResult { job_id: String, wait_ms: u64 },
    #[serde(rename = "job.cancel")]
    JobCancel { job_id: String },
}

/// Exactly one of `result` / `error` is present.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseFrame {
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

impl ResponseFrame {
    pub fn from_result(id: u64, outcome: Result<Value, ErrorBody>) -> Self {
        match outcome {
            Ok(value) => Self {
                id,
                result: Some(value),
                error: None,
            },
            Err(error) => Self {
                id,
                result: None,
                error: Some(error),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    JobProgress(JobProgress),
    JobFinished { job_id: String, state: JobState },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
pub enum Notification {
    Advert(SignedAdvert),
}

// ---------------------------------------------------------------------------------------------
// Link-state adverts (the OSPF-inspired part). Signing / verification: discovery::advert.
// ---------------------------------------------------------------------------------------------

/// What a node says about itself. Flooded to every peer and relayed hop by hop.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeAdvert {
    pub node_id: NodeId,
    pub name: String,
    pub node_type: NodeType,
    pub protocol_version: String,
    /// Address other nodes dial to reach this node.
    pub address: String,
    /// Changes on every process start (start time in ms). Together with `seq` this orders
    /// adverts so a restarted node's fresh adverts beat the ones it sent before restarting.
    pub incarnation: u64,
    pub seq: u64,
    pub capabilities: Vec<CapabilityAdvert>,
    pub resources: ResourceState,
}

impl NodeAdvert {
    pub fn version(&self) -> (u64, u64) {
        (self.incarnation, self.seq)
    }

    pub fn capability(&self, name: &str) -> Option<&CapabilityAdvert> {
        self.capabilities.iter().find(|c| c.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapabilityAdvert {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Value,
    /// Node ids allowed to call this capability, or `"*"` for every trusted peer.
    /// Routing uses it to skip nodes that would refuse; the executing node enforces it.
    pub allow: Vec<String>,
}

impl CapabilityAdvert {
    pub fn allows(&self, caller: &NodeId) -> bool {
        self.allow.iter().any(|a| a == "*" || a == caller.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceState {
    pub cpu_percent: u8,
    pub memory_available_mb: u64,
    /// Real count of running jobs on the node.
    pub active_jobs: u32,
    pub max_concurrent_jobs: u32,
    pub availability: Availability,
    /// True when cpu/memory/availability come from configuration rather than the host.
    pub mocked: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    Available,
    /// Finishing current work, not accepting new jobs.
    Draining,
    Unavailable,
}

/// An advert plus the origin node's signature over the exact payload bytes.
/// Carrying the signed string (not a re-serialised struct) keeps verification independent of
/// JSON formatting differences between implementations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedAdvert {
    pub payload: String,
    pub signature: String,
}

// ---------------------------------------------------------------------------------------------
// Execution results
// ---------------------------------------------------------------------------------------------

/// Result of `invoke` as reported by the executing node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvokeResult {
    pub executed_by: NodeId,
    pub job_id: String,
    pub output: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSubmitted {
    pub executed_by: NodeId,
    pub job_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl JobState {
    pub fn is_finished(self) -> bool {
        !matches!(self, JobState::Running)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobProgress {
    pub job_id: String,
    pub percent: u8,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobInfo {
    pub job_id: String,
    pub executed_by: NodeId,
    pub capability: String,
    pub state: JobState,
    pub percent: u8,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
    pub elapsed_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_frame_wire_shape() {
        let frame = Frame::Request(RequestFrame {
            id: 7,
            request: Request::Invoke {
                capability: "math.add".into(),
                input: json!({"a": 1, "b": 2}),
            },
        });
        let wire = serde_json::to_value(&frame).unwrap();
        assert_eq!(
            wire,
            json!({"type": "request", "id": 7,
                   "request": {"method": "invoke", "params": {"capability": "math.add", "input": {"a": 1, "b": 2}}}})
        );
        let back: Frame = serde_json::from_value(wire).unwrap();
        assert_eq!(back, frame);
    }

    #[test]
    fn unit_request_and_welcome_round_trip() {
        for frame in [
            Frame::Welcome,
            Frame::Request(RequestFrame {
                id: 1,
                request: Request::Ping,
            }),
        ] {
            let text = serde_json::to_string(&frame).unwrap();
            assert_eq!(serde_json::from_str::<Frame>(&text).unwrap(), frame);
        }
    }

    #[test]
    fn unknown_frame_type_is_rejected() {
        assert!(serde_json::from_str::<Frame>(r#"{"type":"exec_shell","cmd":"rm -rf /"}"#).is_err());
    }
}

//! Every message that crosses a Synapta connection. `docs/protocol.md` documents this file;
//! change both together.
//!
//! Each [`Frame`] is UTF-8 JSON. `Hello` and `HelloAck` (and an `Error` refusing them) travel as
//! a 4-byte big-endian length plus the JSON (`transport::framing`); every later frame is
//! encrypted (`transport::secure`).
//!
//! Growing without a new wire version: unknown JSON fields are ignored everywhere, and the
//! `Unknown` variants below catch frame types, request methods, notifications, events and
//! error codes this build has not heard of — a newer peer may send them and the link stays up.
//! Anything else (a new value in another enum, a changed field) needs a new wire version.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use super::{NodeId, NodeType, WireRange};
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
    /// A frame type this build does not know. Ignored after the handshake.
    #[serde(other)]
    Unknown,
}

/// Dialer → acceptor, in the clear.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hello {
    /// Wire versions the dialer speaks. Defaults to none, so a hello from v0.0.1 / v0.0.2 (which
    /// sent a version string instead) still parses and is refused with `version_mismatch`.
    #[serde(default)]
    pub wire: WireRange,
    pub node_id: NodeId,
    /// First Noise NN handshake message (the dialer's ephemeral key), hex.
    #[serde(default)]
    pub noise: String,
    /// The dialer's membership card, for an acceptor that trusts its network's authority rather
    /// than listing the dialer by name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub card: Option<MemberCard>,
}

/// Acceptor → dialer, in the clear.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelloAck {
    /// Wire versions the acceptor speaks. Default: see [`Hello::wire`].
    #[serde(default)]
    pub wire: WireRange,
    /// The version this connection uses: the highest one in both ranges.
    #[serde(default)]
    pub wire_version: u32,
    pub node_id: NodeId,
    /// Second Noise NN message (the acceptor's ephemeral key), hex.
    #[serde(default)]
    pub noise: String,
    /// The acceptor's membership card. See [`Hello::card`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub card: Option<MemberCard>,
}

/// A network authority's signed statement that a node id holds a public key in its network
/// (`identity::CardClaims` in `payload`). Lets a node trust a peer nobody listed by name: it
/// only needs to trust the authority. Verified in `identity::TrustStore::admit`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberCard {
    pub payload: String,
    pub signature: String,
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

/// `{"method": ..., "params": ...}`. Serde is reached through [`with_unknown_methods`]: the
/// derived code alone cannot turn an unknown method that carries params into `Unknown`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(remote = "Self", tag = "method", content = "params")]
pub enum Request {
    #[serde(rename = "ping")]
    Ping,
    /// Exchange full link-state databases right after a link becomes READY.
    #[serde(rename = "adverts.sync")]
    SyncAdverts { adverts: Vec<SignedAdvert> },
    /// Execute a capability on the receiving node and wait for the result.
    /// The receiver executes it or refuses; it never passes it on (that is what `relay` is for).
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
    /// A request for a node the sender has no link to, passed on hop by hop (`routing::relay`).
    #[serde(rename = "relay")]
    Relay(RelayRequest),
    /// A method this build does not know. Answered with `unsupported_request`; never sent.
    #[serde(skip)]
    Unknown,
}

impl Request {
    /// Every method above. A method missing here would be read as `Unknown`, so
    /// `every_method_is_listed_as_known` checks the two stay in step.
    const METHODS: &'static [&'static str] = &[
        "ping",
        "adverts.sync",
        "invoke",
        "job.submit",
        "job.status",
        "job.result",
        "job.cancel",
        "relay",
    ];

    /// The wire name of this request's method; `"unknown"` for one this build does not know.
    pub fn method(&self) -> &'static str {
        match self {
            Request::Ping => "ping",
            Request::SyncAdverts { .. } => "adverts.sync",
            Request::Invoke { .. } => "invoke",
            Request::JobSubmit { .. } => "job.submit",
            Request::JobStatus { .. } => "job.status",
            Request::JobResult { .. } => "job.result",
            Request::JobCancel { .. } => "job.cancel",
            Request::Relay(_) => "relay",
            Request::Unknown => "unknown",
        }
    }
}

with_unknown_methods!(Request);

/// `relay` params. The answer to a relay request is a [`SignedPayload`] holding a
/// [`RelayReply`], signed by `to`, or an unsigned error from a hop that could not pass it on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelayRequest {
    /// The node that must execute it.
    pub to: NodeId,
    /// Hops left. A hop that would have to pass it on with none left answers `ttl_exceeded`.
    pub ttl: u8,
    /// An [`Envelope`], signed by the node that sent it first.
    pub envelope: SignedPayload,
}

/// JSON text and a signature over it by the node the JSON names as its author.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedPayload {
    pub payload: String,
    pub signature: String,
}

/// Who asks what of whom. Signed by `from`, so the hops in between can neither change it nor
/// pretend to be `from` (capability access lists check `from`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    /// Random, hex. `to` refuses an envelope it has already seen (until it expires).
    pub nonce: String,
    /// Unix time in ms after which `to` refuses it.
    pub expires_at_ms: u64,
    pub request: Request,
}

/// What `to` answered. Signed by `to`, so `from` knows no hop made it up.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelayReply {
    pub from: NodeId,
    pub to: NodeId,
    /// The envelope's nonce: ties this reply to that request.
    pub nonce: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
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
    JobFinished {
        job_id: String,
        state: JobState,
    },
    /// An event this build does not know. Ignored.
    #[serde(other)]
    Unknown,
}

/// Same shape and the same unknown-method handling as [`Request`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(remote = "Self", tag = "method", content = "params", rename_all = "snake_case")]
pub enum Notification {
    Advert(SignedAdvert),
    /// A notification this build does not know. Ignored; never sent.
    #[serde(skip)]
    Unknown,
}

impl Notification {
    const METHODS: &'static [&'static str] = &["advert"];
}

with_unknown_methods!(Notification);

/// Serde for a `{"method", "params"}` enum (derived with `remote = "Self"`) whose `METHODS`
/// lists the methods it knows: any other method becomes `Unknown`, whatever its params. A
/// known method with bad params is still an error, as before.
macro_rules! with_unknown_methods {
    ($enum:ident) => {
        impl Serialize for $enum {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                $enum::serialize(self, serializer)
            }
        }

        impl<'de> Deserialize<'de> for $enum {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = Value::deserialize(deserializer)?;
                let method = value.get("method").and_then(Value::as_str);
                if method.is_some_and(|m| !$enum::METHODS.contains(&m)) {
                    return Ok($enum::Unknown);
                }
                $enum::deserialize(value).map_err(serde::de::Error::custom)
            }
        }
    };
}
use with_unknown_methods;

// ---------------------------------------------------------------------------------------------
// Link-state adverts (the OSPF-inspired part). Signing / verification: discovery::advert.
// ---------------------------------------------------------------------------------------------

/// What a node says about itself. Flooded to every peer and relayed hop by hop.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeAdvert {
    pub node_id: NodeId,
    pub name: String,
    pub node_type: NodeType,
    /// Release the node runs. For people reading status output; compatibility is `wire`.
    pub software_version: String,
    /// Wire versions the node speaks. Nodes with no version in common are not dialled.
    pub wire: WireRange,
    /// Address other nodes dial to reach this node.
    pub address: String,
    /// Changes on every process start (start time in ms). Together with `seq` this orders
    /// adverts so a restarted node's fresh adverts beat the ones it sent before restarting.
    pub incarnation: u64,
    pub seq: u64,
    pub capabilities: Vec<CapabilityAdvert>,
    /// Nodes this node has a READY link to, sorted. Every node builds the whole graph from
    /// these and routes to nodes it cannot reach directly (`routing::paths`).
    #[serde(default)]
    pub neighbors: Vec<NodeId>,
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
    /// The origin's membership card, so nodes that never met the origin can still check its
    /// signature. Not part of the signed payload: the card carries its own signature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub card: Option<MemberCard>,
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

    /// What a newer peer might send. None of it may fail to parse (that would close the link),
    /// and none of it may turn into something this node acts on.
    #[test]
    fn messages_from_a_newer_version_parse_as_unknown() {
        let parse = |text: &str| serde_json::from_str::<Frame>(text).unwrap();
        assert_eq!(parse(r#"{"type":"exec_shell","cmd":"rm -rf /"}"#), Frame::Unknown);
        assert_eq!(
            parse(r#"{"type":"request","id":9,"request":{"method":"files.read","params":{"path":"x"}}}"#),
            Frame::Request(RequestFrame {
                id: 9,
                request: Request::Unknown
            })
        );
        assert_eq!(
            parse(r#"{"type":"notify","method":"topology_hint","params":{"hops":2}}"#),
            Frame::Notify(Notification::Unknown)
        );
        assert_eq!(
            parse(r#"{"type":"event","event":"job_log","job_id":"j1","line":"hi"}"#),
            Frame::Event(Event::Unknown)
        );
        let Frame::Response(response) =
            parse(r#"{"type":"response","id":1,"error":{"code":"quota_exceeded","message":"m"}}"#)
        else {
            panic!("not a response")
        };
        assert_eq!(response.error.unwrap().code, crate::error::ErrorCode::Unknown);
    }

    /// A sample of every known request and notification: each must come back as itself, which
    /// fails if its method is missing from `METHODS` (it would come back as `Unknown`).
    #[test]
    fn every_method_is_listed_as_known() {
        let advert = SignedAdvert {
            payload: "{}".into(),
            signature: "00".into(),
            card: None,
        };
        let requests = [
            Request::Ping,
            Request::SyncAdverts {
                adverts: vec![advert.clone()],
            },
            Request::Invoke {
                capability: "math.add".into(),
                input: json!({}),
            },
            Request::JobSubmit {
                capability: "math.add".into(),
                input: json!({}),
            },
            Request::JobStatus { job_id: "j".into() },
            Request::JobResult {
                job_id: "j".into(),
                wait_ms: 1,
            },
            Request::JobCancel { job_id: "j".into() },
            Request::Relay(RelayRequest {
                to: NodeId::new("node-z").unwrap(),
                ttl: 3,
                envelope: SignedPayload {
                    payload: "{}".into(),
                    signature: "00".into(),
                },
            }),
        ];
        assert_eq!(requests.len(), Request::METHODS.len(), "a sample per method");
        for request in requests {
            let text = serde_json::to_string(&request).unwrap();
            assert_eq!(serde_json::from_str::<Request>(&text).unwrap(), request, "{text}");
            assert!(text.contains(&format!("\"{}\"", request.method())), "{text}");
        }
        let notification = Notification::Advert(advert);
        let text = serde_json::to_string(&notification).unwrap();
        assert_eq!(serde_json::from_str::<Notification>(&text).unwrap(), notification);
    }

    /// A known method with bad params is malformed, not unknown.
    #[test]
    fn known_method_with_bad_params_is_still_an_error() {
        assert!(serde_json::from_str::<Request>(r#"{"method":"invoke","params":{"capability":7}}"#).is_err());
        assert!(serde_json::from_str::<Request>(r#"{"params":{}}"#).is_err());
    }

    /// Extra fields on a message this build knows are ignored, not an error.
    #[test]
    fn unknown_fields_are_ignored() {
        let frame: Frame =
            serde_json::from_str(r#"{"type":"request","id":1,"priority":5,"request":{"method":"ping"}}"#).unwrap();
        assert_eq!(
            frame,
            Frame::Request(RequestFrame {
                id: 1,
                request: Request::Ping
            })
        );
    }
}

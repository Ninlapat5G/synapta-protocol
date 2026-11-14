//! Synapta Protocol v0.0.1 — peer-to-peer node discovery, capability routing and remote
//! execution. See `docs/architecture.md` for how the modules fit together.
//!
//! | module        | job                                                         |
//! |---------------|-------------------------------------------------------------|
//! | `protocol`    | ids, versions and every wire message                        |
//! | `transport`   | byte streams (TCP today), framing, address policy           |
//! | `identity`    | Ed25519 keys, signatures, trust store                       |
//! | `discovery`   | handshake, outbound links, inbound server, advert flooding  |
//! | `registry`    | per-node view of peers (state + latest advert)              |
//! | `routing`     | deterministic policy (`decide`) and its execution (`dispatch`) |
//! | `execution`   | executor interface, built-in mocks, job lifecycle           |
//! | `node`        | wiring, public `NodeHandle` API, shutdown                   |

pub mod config;
pub mod discovery;
pub mod error;
pub mod execution;
pub mod identity;
pub mod node;
pub mod protocol;
pub mod registry;
pub mod routing;
pub mod transport;

use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub use config::NodeConfig;
pub use error::{ErrorBody, ErrorCode};
pub use node::{start, NodeHandle, NodeStatus};
pub use protocol::NodeId;

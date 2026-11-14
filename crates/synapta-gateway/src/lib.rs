//! Local HTTP API of a Synapta node: the UTCP adapter (manual + tool endpoint), control
//! endpoints for the CLI, and a matching minimal client.
//!
//! The core protocol never depends on this crate — it is one adapter on top of `NodeHandle`.

pub mod client;
pub mod http;
pub mod routes;
pub mod utcp;

use std::net::SocketAddr;
use std::sync::Arc;

use synapta_core::identity::{random_bytes, to_hex};
use synapta_core::transport::AddressPolicy;
use synapta_core::{BoxFuture, NodeHandle};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub struct Gateway {
    pub addr: SocketAddr,
    pub task: JoinHandle<()>,
}

/// A fresh random API token (32 bytes, hex).
pub fn generate_token() -> String {
    to_hex(&random_bytes::<32>())
}

/// Binds the API on a loopback address and serves until the node shuts down.
pub async fn start(node: NodeHandle, listen: SocketAddr, token: String) -> Result<Gateway, String> {
    AddressPolicy::LoopbackOnly
        .check(&listen)
        .map_err(|e| format!("api.listen: {e}"))?;
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|e| format!("cannot listen on {listen}: {e}"))?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let shutdown: BoxFuture<'static, ()> = Box::pin(node.shutdown_requested());
    let routes = Arc::new(routes::Routes { node, api_addr: addr });
    let task = tokio::spawn(http::serve(listener, token, routes, shutdown));
    tracing::info!(%addr, "local API listening");
    Ok(Gateway { addr, task })
}

//! The secure channel seen from the wire, with real nodes: node-a reaches node-b only through a
//! loopback relay that records (and, when armed, corrupts) the bytes a sends. Record-level
//! tampering, replay, reordering and the man-in-the-middle case are unit-tested in
//! `transport::secure` and `discovery::handshake`; this file checks that live links actually use
//! that channel.

mod common;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use common::{Cluster, NodeSpec, WAIT};
use serde_json::json;
use synapta_core::{ErrorCode, NodeHandle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A frame this large stands out from heartbeats (~70 bytes on the wire).
const LARGE_FRAME: usize = 1_000;

/// Loopback TCP relay to `target`. Keeps a copy of everything it carries, both directions.
#[derive(Clone)]
struct Relay {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<u8>>>,
    /// When set, flips the last byte of the next large chunk going toward `target`, then clears.
    corrupt_next_large: Arc<AtomicBool>,
}

impl Relay {
    async fn start(target: SocketAddr) -> Relay {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let relay = Relay {
            addr: listener.local_addr().unwrap(),
            seen: Arc::default(),
            corrupt_next_large: Arc::default(),
        };
        let r = relay.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect(target).await else {
                    continue;
                };
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                tokio::spawn(pump(client_read, server_write, r.clone(), true));
                tokio::spawn(pump(server_read, client_write, r.clone(), false));
            }
        });
        relay
    }

    fn contains(&self, needle: &str) -> bool {
        let seen = self.seen.lock().unwrap();
        seen.windows(needle.len()).any(|w| w == needle.as_bytes())
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    relay: Relay,
    toward_target: bool,
) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        relay.seen.lock().unwrap().extend_from_slice(&buf[..n]);
        if toward_target && n >= LARGE_FRAME && relay.corrupt_next_large.swap(false, Ordering::SeqCst) {
            buf[n - 1] ^= 0x01;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

/// node-b (echo provider) and node-a, whose only seed is a relay in front of node-b.
async fn relayed_pair() -> (Cluster, Relay) {
    let b_spec = NodeSpec {
        id: "node-b",
        node_type: "server",
        capabilities: vec![("system.echo", vec!["*"])],
        max_concurrent_jobs: 2,
        cpu_percent: 20,
        seeds: vec![],
        trusts: vec!["node-a"],
    };
    let mut cluster = Cluster::start(vec![b_spec]).await;
    let relay = Relay::start(cluster.node("node-b").listen_addr()).await;
    let a_spec = NodeSpec {
        id: "node-a",
        node_type: "client",
        capabilities: vec![],
        max_concurrent_jobs: 1,
        cpu_percent: 10,
        seeds: vec![relay.addr.to_string()],
        trusts: vec!["node-b"],
    };
    let config = cluster.config_for(&a_spec);
    cluster.nodes.push(synapta_core::start(config).await.unwrap());
    cluster.wait_full_mesh().await;
    (cluster, relay)
}

async fn echo(a: &NodeHandle, text: &str) -> Result<serde_json::Value, ErrorCode> {
    a.invoke("system.echo", json!({ "text": text }), None)
        .await
        .map(|routed| routed.value.output)
        .map_err(|failure| failure.error.code)
}

#[tokio::test]
async fn nothing_after_the_hello_is_readable_on_the_wire() {
    let (cluster, relay) = relayed_pair().await;
    let a = cluster.node("node-a");
    let marker = "synapta-wire-marker-7f3a";

    let output = echo(a, marker).await.unwrap();
    assert_eq!(output["echo"]["text"], marker, "the call really went through");

    // In the clear by design: the handshake's first two messages.
    assert!(relay.contains("\"type\":\"hello\""));
    assert!(relay.contains("node-a") && relay.contains("node-b"));
    // Everything else is not: request, response, adverts, heartbeats, the handshake proofs.
    for secret in [
        marker,
        "system.echo",
        "invoke",
        "ping",
        "advert",
        "hello_proof",
        "signature",
    ] {
        assert!(!relay.contains(secret), "{secret:?} visible on the wire");
    }
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_corrupted_frame_closes_the_link_and_runs_nothing() {
    let (cluster, relay) = relayed_pair().await;
    let (a, b) = (cluster.node("node-a"), cluster.node("node-b"));
    let large = "x".repeat(2 * LARGE_FRAME);

    relay.corrupt_next_large.store(true, Ordering::SeqCst);
    assert_eq!(echo(a, &large).await.unwrap_err(), ErrorCode::ConnectionLost);
    assert!(
        !relay.corrupt_next_large.load(Ordering::SeqCst),
        "the request was the frame corrupted"
    );
    assert!(
        b.status().executions.is_empty(),
        "node-b ran nothing from a corrupted frame"
    );

    // Only that connection is dropped: node-a reconnects (through the relay) and carries on.
    assert!(a.wait_until(WAIT, |s| s.is_ready("node-b")).await);
    assert_eq!(echo(a, "after").await.unwrap()["echo"]["text"], "after");
    cluster.shutdown().await;
}

//! Links that must stay up, or come back, on their own: a busy link keeps answering heartbeats,
//! and a seed that was away for a long time is found again when it returns.

mod common;

use std::time::Duration;

use common::{Cluster, NodeSpec, WAIT};
use serde_json::json;

fn provider(max_concurrent_jobs: u32) -> NodeSpec {
    NodeSpec {
        id: "node-b",
        node_type: "server",
        capabilities: vec![("test.execute", vec!["*"])],
        max_concurrent_jobs,
        cpu_percent: 20,
        seeds: vec![],
        trusts: vec!["node-a"],
    }
}

fn caller() -> NodeSpec {
    NodeSpec {
        id: "node-a",
        node_type: "client",
        capabilities: vec![],
        max_concurrent_jobs: 1,
        cpu_percent: 10,
        seeds: vec!["node-b".into()],
        trusts: vec!["node-b"],
    }
}

/// Regression: a ping took one of the connection's request slots (32 by default). With every
/// slot held by a slow job the ping came back `busy`, node-a closed the link as dead, and every
/// one of those jobs failed with `connection_lost`.
#[tokio::test]
async fn a_link_with_every_request_slot_busy_keeps_its_heartbeat() {
    let cluster = Cluster::start(vec![provider(64), caller()]).await;
    cluster.wait_full_mesh().await;
    let a = cluster.node("node-a").clone();
    let slots = cluster
        .node("node-b")
        .config()
        .limits
        .max_inflight_requests_per_connection;
    let heartbeat_timeout = Duration::from_millis(a.config().timeouts.heartbeat_timeout_ms);

    // Each job outlasts several heartbeat intervals and the heartbeat timeout.
    let slow = json!({ "suite": "smoke", "steps": 1, "step_ms": (heartbeat_timeout * 3).as_millis() as u64 });
    let calls: Vec<_> = (0..slots)
        .map(|_| {
            let (a, slow) = (a.clone(), slow.clone());
            tokio::spawn(async move { a.invoke("test.execute", slow, None).await })
        })
        .collect();
    for call in calls {
        let outcome = call.await.unwrap();
        assert!(outcome.is_ok(), "{:?}", outcome.err().map(|f| f.error));
    }
    let peer = a.status().peer("node-b").cloned().unwrap();
    assert!(a.status().is_ready("node-b"), "link dropped: {:?}", peer.last_error);
    cluster.shutdown().await;
}

/// Regression: a dialer stopped after `max_reconnect_attempts`, seeds included. A seed that was
/// down for longer and did not dial in by itself stayed cut off until this node restarted.
#[tokio::test]
async fn a_seed_that_was_away_for_a_long_time_is_found_again() {
    let seed_retry_ms = 300;
    let mut cluster = Cluster::start_with(vec![provider(2), caller()], |config| {
        config.timeouts.seed_retry_ms = seed_retry_ms;
        // Quick attempts, so the retry budget runs out fast (a refused connect can take seconds
        // on Windows, bounded by connect_ms).
        config.timeouts.connect_ms = 300;
        config.timeouts.max_reconnect_attempts = 2;
    })
    .await;
    cluster.wait_full_mesh().await;
    let a = cluster.node("node-a").clone();
    let b_addr = cluster.node("node-b").listen_addr();
    let t = a.config().timeouts.clone();

    assert!(cluster.nodes[0].shutdown().await);
    cluster.nodes.remove(0);
    // Long enough to use up every quick retry.
    let per_attempt = t.connect_ms + t.reconnect_max_ms;
    let budget = Duration::from_millis(per_attempt * u64::from(t.max_reconnect_attempts));
    tokio::time::sleep(budget * 2).await;
    assert!(!a.status().is_ready("node-b"));

    // node-b comes back on the same address and knows nothing about node-a (no seeds, fresh
    // state), so only node-a can bring the link back.
    let mut config = cluster.config_for(&provider(2));
    config.node.listen = b_addr;
    let b = synapta_core::start(config).await.unwrap();
    let back = a.wait_until(WAIT, |s| s.is_ready("node-b")).await;
    assert!(back, "node-a never redialled its seed: {:?}", a.status().peer("node-b"));
    b.shutdown().await;
    cluster.shutdown().await;
}

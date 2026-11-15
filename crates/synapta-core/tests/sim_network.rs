//! Real nodes on the simulated network (`transport::sim`): they reach each other only through
//! shared rings, notice a pulled cable through their own heartbeats, and report every frame to
//! a trace sink.

mod common;

use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{id, Cluster, NodeSpec, WAIT};
use serde_json::json;
use synapta_core::config::ms;
use synapta_core::trace::{Channel, TraceEvent, TraceKind, TraceSink};
use synapta_core::transport::sim::SimNetwork;
use synapta_core::{NodeConfig, StartOptions};

const ALL: [&str; 3] = ["node-a", "node-b", "node-c"];

fn spec(node: &'static str, seeds: &[&str]) -> NodeSpec {
    NodeSpec {
        id: node,
        node_type: "server",
        capabilities: vec![("math.add", vec!["*"])],
        max_concurrent_jobs: 2,
        cpu_percent: 20,
        seeds: seeds.iter().map(|s| s.to_string()).collect(),
        trusts: ALL.iter().copied().filter(|n| *n != node).collect(),
    }
}

/// Each node gets its own loopback address (127.0.0.10, .11, ...) and a transport on `network`.
fn on(network: &Arc<SimNetwork>, trace: Option<TraceSink>) -> impl Fn(&NodeConfig) -> StartOptions + '_ {
    move |config| StartOptions {
        transport: Some(network.transport(
            config.node.id.clone(),
            config.node.listen.ip(),
            ms(config.timeouts.connect_ms),
        )),
        trace: trace.clone(),
    }
}

fn own_address(config: &mut NodeConfig) {
    let n = ALL.iter().position(|n| *n == config.node.id.as_str()).unwrap() as u8;
    config.node.listen.set_ip(IpAddr::from([127, 0, 0, 10 + n]));
    // Unreachable dials wait this long, so keep it short.
    config.timeouts.connect_ms = 300;
    config.timeouts.seed_retry_ms = 300;
}

#[tokio::test]
async fn nodes_reach_each_other_only_through_a_shared_ring() {
    let network = SimNetwork::new();
    network.set_rings(&id("node-a"), ["one"]);
    network.set_rings(&id("node-b"), ["one", "two"]);
    network.set_rings(&id("node-c"), ["two"]);
    let specs = vec![
        spec("node-b", &[]),
        spec("node-a", &["node-b"]),
        spec("node-c", &["node-b"]),
    ];
    let cluster = Cluster::start_custom(specs, own_address, on(&network, None)).await;
    let (a, b, c) = (cluster.node("node-a"), cluster.node("node-b"), cluster.node("node-c"));

    assert!(
        b.wait_until(WAIT, |s| s.is_ready("node-a") && s.is_ready("node-c"))
            .await
    );
    // node-b passes node-c's advert on, so node-a knows node-c exists ...
    let knows_c = a
        .wait_until(WAIT, |s| s.peer("node-c").is_some_and(|p| p.advert.is_some()))
        .await;
    assert!(knows_c, "{:?}", a.status().peers);
    // ... but cannot connect to it.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!a.status().is_ready("node-c"));
    assert!(!c.status().is_ready("node-a"));
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_pulled_cable_is_noticed_and_plugging_it_back_restores_the_link() {
    let network = SimNetwork::new();
    network.set_rings(&id("node-a"), ["one"]);
    network.set_rings(&id("node-b"), ["one"]);
    let specs = vec![spec("node-b", &[]), spec("node-a", &["node-b"])];
    let cluster = Cluster::start_custom(specs, own_address, on(&network, None)).await;
    cluster.wait_full_mesh().await;
    let (a, b) = (cluster.node("node-a"), cluster.node("node-b"));

    network.set_rings(&id("node-a"), Vec::<String>::new());
    assert!(
        a.wait_until(WAIT, |s| !s.is_ready("node-b")).await,
        "node-a never noticed"
    );
    assert!(
        b.wait_until(WAIT, |s| !s.is_ready("node-a")).await,
        "node-b never noticed"
    );
    let reason = a
        .status()
        .peer("node-b")
        .unwrap()
        .last_error
        .clone()
        .unwrap_or_default();
    assert!(reason.contains("heartbeat"), "noticed through its heartbeat: {reason}");

    network.set_rings(&id("node-a"), ["one"]);
    cluster.wait_full_mesh().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn both_ends_of_a_call_are_traced() {
    let network = SimNetwork::new();
    network.set_rings(&id("node-a"), ["one"]);
    network.set_rings(&id("node-b"), ["one"]);
    let events: Arc<Mutex<Vec<TraceEvent>>> = Arc::default();
    let sink: TraceSink = {
        let events = events.clone();
        Arc::new(move |event| events.lock().unwrap().push(event))
    };
    let specs = vec![spec("node-b", &[]), spec("node-a", &["node-b"])];
    let cluster = Cluster::start_custom(specs, own_address, on(&network, Some(sink))).await;
    cluster.wait_full_mesh().await;
    let a = cluster.node("node-a");

    let sum = a
        .invoke("math.add", json!({ "a": 2, "b": 3 }), Some(&id("node-b")))
        .await
        .unwrap();
    assert_eq!(sum.value.output, json!({ "sum": 5 }));
    cluster.shutdown().await;

    let events = events.lock().unwrap();
    let find = |node: &str, peer: &str, want: &dyn Fn(&TraceKind) -> bool| {
        events
            .iter()
            .find(|e| e.node.as_str() == node && e.peer.as_str() == peer && want(&e.kind))
            .cloned()
    };
    // node-a sends the invoke on its own link ...
    let sent = find("node-a", "node-b", &|k| {
        matches!(k, TraceKind::Sent { channel: Channel::Outbound, frame }
            if frame.method == Some("invoke") && frame.detail.as_deref() == Some("math.add"))
    })
    .expect("node-a reports sending the invoke");
    let TraceKind::Sent { frame, .. } = &sent.kind else {
        unreachable!()
    };
    let call = frame.id;
    // ... node-b reads it, answers, and node-a reads the answer: same id all the way.
    let same_call = |channel: Channel, frame_kind: &'static str| {
        move |k: &TraceKind| match k {
            TraceKind::Sent { channel: c, frame } | TraceKind::Received { channel: c, frame } => {
                *c == channel && frame.frame == frame_kind && frame.id == call
            }
            _ => false,
        }
    };
    let received = find("node-b", "node-a", &same_call(Channel::Inbound, "request")).expect("node-b reads it");
    let answered = find("node-b", "node-a", &same_call(Channel::Inbound, "response")).expect("node-b answers");
    let back = find("node-a", "node-b", &same_call(Channel::Outbound, "response")).expect("node-a reads the answer");
    assert!(sent.at_us <= received.at_us && received.at_us <= answered.at_us && answered.at_us <= back.at_us);
    let TraceKind::Received { frame, .. } = &back.kind else {
        unreachable!()
    };
    assert_eq!(frame.ok, Some(true));

    // The link coming up and the advert exchange on it show up too.
    assert!(find("node-a", "node-b", &|k| *k == TraceKind::LinkUp).is_some());
    let sync = |k: &TraceKind| matches!(k, TraceKind::Sent { frame, .. } if frame.method == Some("adverts.sync"));
    assert!(find("node-a", "node-b", &sync).is_some());
}

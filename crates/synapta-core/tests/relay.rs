//! Work for nodes in other rings, relayed through the nodes that bridge them
//! (`routing::relay`), on real nodes over the simulated network.

mod common;

use std::net::IpAddr;
use std::sync::Arc;

use common::{id, identity, Cluster, NodeSpec, WAIT};
use serde_json::json;
use synapta_core::config::ms;
use synapta_core::discovery::handshake::{self, HandshakeParams, Session};
use synapta_core::identity::{parse_verifying_key, random_bytes, to_hex, TrustStore};
use synapta_core::protocol::message::{
    Envelope, Frame, JobState, RelayReply, RelayRequest, Request, RequestFrame, ResponseFrame, SignedPayload,
};
use synapta_core::transport::sim::SimNetwork;
use synapta_core::transport::BoxedConnection;
use synapta_core::{ErrorCode, NodeConfig, NodeHandle, StartOptions};

const IDS: [&str; 6] = ["node-a", "node-b", "node-c", "node-d", "node-e", "node-f"];

/// Trusts every other id; `math.add` / `test.execute` on node-d only, callable by `allow`.
fn spec(node: &'static str, seeds: &[&'static str], allow: &[&'static str]) -> NodeSpec {
    let capabilities = if node == "node-d" {
        vec![("math.add", allow.to_vec()), ("test.execute", allow.to_vec())]
    } else {
        vec![]
    };
    NodeSpec {
        id: node,
        node_type: "server",
        capabilities,
        max_concurrent_jobs: 4,
        cpu_percent: 20,
        seeds: seeds.iter().map(|s| s.to_string()).collect(),
        trusts: IDS.iter().copied().filter(|n| *n != node).collect(),
    }
}

fn own_address(config: &mut NodeConfig) {
    let n = IDS.iter().position(|n| *n == config.node.id.as_str()).unwrap() as u8;
    config.node.listen.set_ip(IpAddr::from([127, 0, 0, 20 + n]));
    config.timeouts.connect_ms = 300;
    config.timeouts.seed_retry_ms = 300;
}

async fn start(network: &Arc<SimNetwork>, specs: Vec<NodeSpec>) -> Cluster {
    Cluster::start_custom(specs, own_address, |config: &NodeConfig| StartOptions {
        transport: Some(network.transport(
            config.node.id.clone(),
            config.node.listen.ip(),
            ms(config.timeouts.connect_ms),
        )),
        trace: None,
    })
    .await
}

fn rings(network: &SimNetwork, layout: &[(&str, &[&str])]) {
    for (node, node_rings) in layout {
        network.set_rings(&id(node), node_rings.iter().copied());
    }
}

/// Waits until `from` has a way to `to` going through `via` first.
async fn wait_route(from: &NodeHandle, to: &str, via: &str) -> bool {
    let probe = from.clone();
    from.wait_until(WAIT, |_| {
        let decision = probe.explain("math.add", Some(&id(to)));
        decision.selected.is_some() && decision.candidates[0].via.as_ref().map(|v| v.as_str()) == Some(via)
    })
    .await
}

/// a — b — c — d, each neighbouring pair sharing a ring of its own.
fn chain(network: &SimNetwork) {
    rings(
        network,
        &[
            ("node-a", &["r1"]),
            ("node-b", &["r1", "r2"]),
            ("node-c", &["r2", "r3"]),
            ("node-d", &["r3"]),
        ],
    );
}

fn chain_specs(allow: &[&'static str]) -> Vec<NodeSpec> {
    vec![
        spec("node-d", &[], allow),
        spec("node-c", &["node-d"], allow),
        spec("node-b", &["node-c"], allow),
        spec("node-a", &["node-b"], allow),
    ]
}

#[tokio::test]
async fn work_crosses_two_bridges() {
    let network = SimNetwork::new();
    chain(&network);
    let cluster = start(&network, chain_specs(&["*"])).await;
    let a = cluster.node("node-a");
    assert!(
        wait_route(a, "node-d", "node-b").await,
        "{:?}",
        a.explain("math.add", None)
    );

    // Automatic routing finds node-d three hops away and says how it gets there.
    let sum = a.invoke("math.add", json!({ "a": 2, "b": 3 }), None).await.unwrap();
    assert_eq!(sum.value.output, json!({ "sum": 5 }));
    assert_eq!(sum.value.executed_by, id("node-d"));
    let chosen = &sum.routing.candidates[0];
    assert_eq!((chosen.hops, chosen.via.clone()), (3, Some(id("node-b"))));

    // A job goes the same way, and so do the calls that ask about it.
    let quick = json!({ "suite": "smoke", "steps": 2, "step_ms": 10 });
    let job = a.submit_job("test.execute", quick, Some(&id("node-d"))).await.unwrap();
    let done = a
        .job_result(&id("node-d"), &job.value.job_id, std::time::Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(done.state, JobState::Succeeded);
    assert!(!a.status().is_ready("node-d"), "never linked directly");
    cluster.shutdown().await;
}

#[tokio::test]
async fn access_lists_see_the_real_caller_not_the_bridge() {
    let network = SimNetwork::new();
    chain(&network);
    // node-d serves node-a only — not node-c, which is the hop that hands node-a's call over.
    let cluster = start(&network, chain_specs(&["node-a"])).await;
    let (a, c) = (cluster.node("node-a"), cluster.node("node-c"));
    assert!(wait_route(a, "node-d", "node-b").await);

    let sum = a
        .invoke("math.add", json!({ "a": 1, "b": 1 }), Some(&id("node-d")))
        .await;
    assert_eq!(sum.unwrap().value.output, json!({ "sum": 2 }));
    let refused = c
        .invoke("math.add", json!({ "a": 1, "b": 1 }), Some(&id("node-d")))
        .await
        .unwrap_err();
    assert_eq!(refused.error.code, ErrorCode::Unauthorized);
    cluster.shutdown().await;
}

#[tokio::test]
async fn when_a_bridge_goes_down_the_other_one_is_used() {
    let network = SimNetwork::new();
    // Two bridges between node-a's ring and node-d: node-b (ring r2) and node-c (ring r3).
    rings(
        &network,
        &[
            ("node-a", &["r1"]),
            ("node-b", &["r1", "r2"]),
            ("node-c", &["r1", "r3"]),
            ("node-d", &["r2", "r3"]),
        ],
    );
    let specs = vec![
        spec("node-d", &[], &["*"]),
        spec("node-b", &["node-d"], &["*"]),
        spec("node-c", &["node-d"], &["*"]),
        spec("node-a", &["node-b"], &["*"]),
    ];
    let cluster = start(&network, specs).await;
    let a = cluster.node("node-a");
    // Equal length: the lower id wins.
    assert!(wait_route(a, "node-d", "node-b").await);

    network.set_rings(&id("node-b"), Vec::<String>::new());
    assert!(
        wait_route(a, "node-d", "node-c").await,
        "{:?}",
        a.explain("math.add", None)
    );
    let sum = a.invoke("math.add", json!({ "a": 4, "b": 4 }), None).await.unwrap();
    assert_eq!(sum.value.executed_by, id("node-d"));

    // With both bridges gone there is no way, and routing says so before sending anything.
    network.set_rings(&id("node-c"), Vec::<String>::new());
    let cut_off = a
        .wait_until(WAIT, |_| a.explain("math.add", Some(&id("node-d"))).selected.is_none())
        .await;
    assert!(cut_off);
    let failed = a.invoke("math.add", json!({ "a": 1, "b": 1 }), None).await.unwrap_err();
    assert_eq!(failed.error.code, ErrorCode::NoEligibleNode);
    cluster.shutdown().await;
}

// --- envelopes that must not get through ---------------------------------------------------

/// A raw link to node-d as node-e, which node-d trusts, plugged into node-d's ring.
async fn link_as_node_e(network: &Arc<SimNetwork>, cluster: &Cluster) -> (BoxedConnection, Session) {
    let e = identity("node-e", cluster.key("node-e"));
    let mut trust = TrustStore::default();
    trust.insert(
        id("node-d"),
        parse_verifying_key(&cluster.key("node-d").public).unwrap(),
    );
    let params = HandshakeParams {
        identity: &e,
        trust: &trust,
        card: None,
        max_frame_bytes: 1 << 20,
    };
    network.set_rings(&id("node-e"), ["r1"]);
    let transport = network.transport(id("node-e"), "127.0.0.250".parse().unwrap(), ms(2_000));
    let mut stream = transport.dial(cluster.node("node-d").listen_addr()).await.unwrap();
    match handshake::dial(&mut stream, &params, Some(&id("node-d"))).await {
        Ok(session) => (stream, session),
        Err(e) => panic!("handshake with node-d failed: {e}"),
    }
}

/// An envelope for node-d claiming to come from `from`, signed with `signer`'s key.
fn envelope(cluster: &Cluster, from: &str, signer: &str) -> SignedPayload {
    let envelope = Envelope {
        from: id(from),
        to: id("node-d"),
        nonce: to_hex(&random_bytes::<16>()),
        expires_at_ms: u64::MAX,
        request: Request::Invoke {
            capability: "math.add".into(),
            input: json!({ "a": 20, "b": 22 }),
        },
    };
    let payload = serde_json::to_string(&envelope).unwrap();
    let signature = identity(signer, cluster.key(signer)).sign(format!("synapta-relay-envelope\n{payload}").as_bytes());
    SignedPayload { payload, signature }
}

async fn relay(
    stream: &mut BoxedConnection,
    session: &mut Session,
    request_id: u64,
    envelope: SignedPayload,
) -> ResponseFrame {
    let frame = Frame::Request(RequestFrame {
        id: request_id,
        request: Request::Relay(RelayRequest {
            to: id("node-d"),
            ttl: 3,
            envelope,
        }),
    });
    session.sealer.write_frame(stream, &frame).await.unwrap();
    match session.opener.read_frame(stream).await.unwrap() {
        Frame::Response(response) => response,
        other => panic!("expected a response, got {other:?}"),
    }
}

#[tokio::test]
async fn forged_and_replayed_envelopes_are_refused() {
    let network = SimNetwork::new();
    rings(&network, &[("node-d", &["r1"])]);
    let cluster = start(&network, vec![spec("node-d", &[], &["node-a", "node-e"])]).await;
    let (mut stream, mut session) = link_as_node_e(&network, &cluster).await;

    // node-e passes on an envelope claiming to be node-a's, signed by itself.
    let forged = relay(&mut stream, &mut session, 1, envelope(&cluster, "node-a", "node-e")).await;
    assert_eq!(forged.error.unwrap().code, ErrorCode::UntrustedPeer);

    // A genuine envelope runs once; the same envelope again is refused.
    let genuine = envelope(&cluster, "node-e", "node-e");
    let first = relay(&mut stream, &mut session, 2, genuine.clone()).await;
    let signed: SignedPayload = serde_json::from_value(first.result.unwrap()).unwrap();
    let reply: RelayReply = serde_json::from_str(&signed.payload).unwrap();
    assert_eq!(reply.result.unwrap()["output"], json!({ "sum": 42 }));
    let again = relay(&mut stream, &mut session, 3, genuine).await;
    let error = again.error.unwrap();
    assert_eq!(error.code, ErrorCode::BadRequest);
    assert!(error.message.contains("already delivered"), "{}", error.message);
    assert_eq!(cluster.node("node-d").status().executions.get("math.add"), Some(&1));
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_hop_without_a_path_says_unreachable() {
    let network = SimNetwork::new();
    rings(&network, &[("node-d", &["r1"])]);
    let cluster = start(&network, vec![spec("node-d", &[], &["*"])]).await;
    let (mut stream, mut session) = link_as_node_e(&network, &cluster).await;
    let frame = Frame::Request(RequestFrame {
        id: 1,
        request: Request::Relay(RelayRequest {
            to: id("node-f"),
            ttl: 3,
            envelope: envelope(&cluster, "node-e", "node-e"),
        }),
    });
    session.sealer.write_frame(&mut stream, &frame).await.unwrap();
    let Frame::Response(response) = session.opener.read_frame(&mut stream).await.unwrap() else {
        panic!("expected a response")
    };
    assert_eq!(response.error.unwrap().code, ErrorCode::Unreachable);
    cluster.shutdown().await;
}

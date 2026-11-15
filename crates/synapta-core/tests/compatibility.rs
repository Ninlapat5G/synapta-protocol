//! A live node meeting a peer from a newer release: messages it does not know must not cost the
//! link, and a node it shares no wire version with is known but never dialled.

mod common;

use std::time::Duration;

use common::{id, identity, Cluster, NodeSpec};
use serde_json::json;
use synapta_core::discovery::advert;
use synapta_core::discovery::handshake::{self, HandshakeParams, Session};
use synapta_core::identity::{parse_verifying_key, TrustStore};
use synapta_core::protocol::message::{
    Availability, Frame, NodeAdvert, Notification, Request, RequestFrame, ResourceState,
};
use synapta_core::protocol::{NodeType, WireRange};
use synapta_core::ErrorCode;
use tokio::net::{TcpListener, TcpStream};

/// node-b alone, trusting node-d — the identity the tests below play.
async fn node_b() -> Cluster {
    Cluster::start(vec![NodeSpec {
        id: "node-b",
        node_type: "server",
        capabilities: vec![("math.add", vec!["*"])],
        max_concurrent_jobs: 2,
        cpu_percent: 20,
        seeds: vec![],
        trusts: vec!["node-d"],
    }])
    .await
}

/// Dials node-b as node-d and completes the handshake.
async fn dial_as_node_d(cluster: &Cluster) -> (TcpStream, Session) {
    let d = identity("node-d", cluster.key("node-d"));
    let mut trust = TrustStore::default();
    trust.insert(
        id("node-b"),
        parse_verifying_key(&cluster.key("node-b").public).unwrap(),
    );
    let mut stream = TcpStream::connect(cluster.node("node-b").listen_addr()).await.unwrap();
    let params = HandshakeParams {
        identity: &d,
        trust: &trust,
        max_frame_bytes: 1 << 20,
    };
    match handshake::dial(&mut stream, &params, Some(&id("node-b"))).await {
        Ok(session) => (stream, session),
        Err(e) => panic!("handshake with node-b failed: {e}"),
    }
}

#[tokio::test]
async fn messages_from_a_newer_release_do_not_close_the_link() {
    let cluster = node_b().await;
    let (mut stream, mut session) = dial_as_node_d(&cluster).await;
    let newer = [
        // Frame type, notification and request this build has never heard of.
        r#"{"type":"stream_chunk","stream":1,"data":"aGk="}"#,
        r#"{"type":"notify","method":"topology_hint","params":{"hops":2}}"#,
        r#"{"type":"request","id":1,"request":{"method":"files.read","params":{"path":"/etc/passwd"}}}"#,
        // A known request with a field added by a newer release.
        r#"{"type":"request","id":2,"priority":"high","request":{"method":"ping"}}"#,
    ];
    for frame in newer {
        session.sealer.write_body(&mut stream, frame.as_bytes()).await.unwrap();
    }

    // Two answers, in either order (a ping is answered without queueing behind requests).
    let mut answers = Vec::new();
    for _ in 0..2 {
        let Frame::Response(response) = session.opener.read_frame(&mut stream).await.unwrap() else {
            panic!("expected a response")
        };
        answers.push(response);
    }
    answers.sort_by_key(|r| r.id);
    // The unknown request is refused, saying why; nothing ran.
    assert_eq!(answers[0].error.as_ref().unwrap().code, ErrorCode::UnsupportedRequest);
    assert!(cluster.node("node-b").status().executions.is_empty());
    // The ping with the extra field is answered normally.
    assert_eq!(answers[1].error, None);

    // Still connected: a real request works.
    let add = Frame::Request(RequestFrame {
        id: 3,
        request: Request::Invoke {
            capability: "math.add".into(),
            input: json!({ "a": 2, "b": 3 }),
        },
    });
    session.sealer.write_frame(&mut stream, &add).await.unwrap();
    let Frame::Response(sum) = session.opener.read_frame(&mut stream).await.unwrap() else {
        panic!("expected a response")
    };
    assert_eq!(sum.result.unwrap()["output"], json!({ "sum": 5 }));
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_node_on_another_wire_version_is_known_but_not_dialled() {
    let cluster = node_b().await;
    let b = cluster.node("node-b");
    // Where the newer node says it listens. Nothing may connect here.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let newer_wire = WireRange {
        min: WireRange::SUPPORTED.max + 1,
        max: WireRange::SUPPORTED.max + 2,
    };
    let newer_node = NodeAdvert {
        node_id: id("node-d"),
        name: "Node from the future".into(),
        node_type: NodeType::Server,
        software_version: "9.0.0".into(),
        wire: newer_wire,
        address: listener.local_addr().unwrap().to_string(),
        incarnation: 1,
        seq: 1,
        capabilities: vec![],
        resources: ResourceState {
            cpu_percent: 0,
            memory_available_mb: 0,
            active_jobs: 0,
            max_concurrent_jobs: 1,
            availability: Availability::Available,
            mocked: true,
        },
    };
    let signed = advert::sign(&newer_node, &identity("node-d", cluster.key("node-d")));

    // Relayed to node-b over a link from node-d (as a third node would pass it on).
    let (mut stream, mut session) = dial_as_node_d(&cluster).await;
    let notify = Frame::Notify(Notification::Advert(signed));
    session.sealer.write_frame(&mut stream, &notify).await.unwrap();

    let known = b
        .wait_until(common::WAIT, |s| {
            s.peer("node-d")
                .and_then(|p| p.advert.as_ref())
                .is_some_and(|a| a.wire == newer_wire)
        })
        .await;
    assert!(known, "node-b should keep the advert: {:?}", b.status().peer("node-d"));
    let dialled = tokio::time::timeout(Duration::from_secs(1), listener.accept()).await;
    assert!(dialled.is_err(), "node-b dialled a node it cannot talk to");
    cluster.shutdown().await;
}

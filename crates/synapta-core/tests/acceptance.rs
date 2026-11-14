//! Acceptance tests TEST-01 … TEST-10 (TEST-05, UTCP, lives in synapta-gateway).
//! Each test starts its own in-process cluster on 127.0.0.1 with OS-assigned ports.

mod common;

use std::time::Duration;

use common::{id, identity, wait_peer_load, Cluster, NodeSpec, WAIT};
use serde_json::json;
use synapta_core::discovery::handshake::{self, HandshakeParams};
use synapta_core::identity::{parse_verifying_key, TrustStore};
use synapta_core::protocol::message::{Event, Frame, JobState, Request, RequestFrame};
use synapta_core::registry::PeerState;
use synapta_core::routing::{Rejection, RoutingMode};
use synapta_core::transport::framing::{read_frame, write_frame};
use synapta_core::ErrorCode;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

async fn sandbox() -> Cluster {
    let cluster = Cluster::start(common::sandbox_specs()).await;
    cluster.wait_full_mesh().await;
    cluster
}

#[tokio::test]
async fn test_01_peer_discovery_and_handshake() {
    let cluster = sandbox().await;
    let a = cluster.node("node-a");

    // node-a was seeded with node-b only; it found node-c through node-b's adverts.
    assert_eq!(a.config().discovery.seeds.len(), 1);
    let status = a.status();
    for (peer, caps) in [
        ("node-b", vec!["math.add", "test.execute"]),
        ("node-c", vec!["system.info", "test.execute"]),
    ] {
        let view = status.peer(peer).unwrap();
        assert_eq!(view.state, PeerState::Ready);
        let advert = view.advert.as_ref().unwrap();
        let names: Vec<&str> = advert.capabilities.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, caps, "{peer} capabilities");
        assert!(advert.resources.mocked, "resources are declared as mocked");
        assert_eq!(advert.protocol_version, "0.0.1");
    }

    // A connection that skips the handshake gets NotReady and nothing runs.
    let b = cluster.node("node-b");
    let mut raw = TcpStream::connect(b.listen_addr()).await.unwrap();
    let invoke = Frame::Request(RequestFrame {
        id: 1,
        request: Request::Invoke {
            capability: "math.add".into(),
            input: json!({"a": 1, "b": 1}),
        },
    });
    write_frame(&mut raw, &invoke, 1 << 20).await.unwrap();
    match read_frame(&mut raw, 1 << 20).await.unwrap() {
        Frame::Error(e) => assert_eq!(e.code, ErrorCode::NotReady),
        other => panic!("expected NotReady, got {other:?}"),
    }
    assert!(
        read_frame(&mut raw, 1 << 20).await.is_err(),
        "connection closed after refusal"
    );
    assert!(b.status().executions.is_empty(), "nothing executed before handshake");
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_02_capability_based_routing() {
    let cluster = sandbox().await;
    let a = cluster.node("node-a");

    let outcome = a.invoke("math.add", json!({"a": 2, "b": 3}), None).await.unwrap();
    assert_eq!(outcome.value.output, json!({"sum": 5}));
    assert_eq!(outcome.value.executed_by, id("node-b"));

    let routing = &outcome.routing;
    assert_eq!(routing.mode, RoutingMode::Automatic);
    assert_eq!(routing.selected, Some(id("node-b")));
    assert_eq!(routing.candidates.len(), 1);
    assert_eq!(
        routing.nodes_without_capability, 2,
        "node-a and node-c do not offer math.add"
    );
    assert_eq!(routing.attempts.len(), 1);
    assert_eq!(
        a.recent_decisions().last().unwrap().selected,
        Some(id("node-b")),
        "decision is logged"
    );

    assert_eq!(cluster.node("node-b").status().executions.get("math.add"), Some(&1));
    assert!(!a.status().executions.contains_key("math.add"));
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_03_explicit_target() {
    let cluster = sandbox().await;
    let a = cluster.node("node-a");

    let input = json!({"suite": "smoke", "steps": 2, "step_ms": 10});
    let outcome = a.invoke("test.execute", input, Some(&id("node-c"))).await.unwrap();
    assert_eq!(outcome.value.executed_by, id("node-c"));
    assert_eq!(outcome.value.output["node"], "node-c");
    assert_eq!(outcome.routing.mode, RoutingMode::Explicit);
    assert_eq!(
        outcome.routing.candidates.len(),
        1,
        "explicit routing evaluates the target only"
    );

    assert_eq!(cluster.node("node-c").status().executions.get("test.execute"), Some(&1));
    assert!(
        cluster.node("node-b").status().executions.is_empty(),
        "node-b offers test.execute but was not used"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_04_resource_aware_routing() {
    let cluster = sandbox().await;
    let a = cluster.node("node-a");

    // Equal load: tie-break picks node-b.
    assert_eq!(a.explain("test.execute", None).selected, Some(id("node-b")));

    // Fill node-b (max 2 concurrent jobs) with real long-running jobs.
    let long = json!({"suite": "integration", "steps": 200, "step_ms": 50});
    let mut jobs = Vec::new();
    for _ in 0..2 {
        jobs.push(
            a.submit_job("test.execute", long.clone(), Some(&id("node-b")))
                .await
                .unwrap()
                .value
                .job_id,
        );
    }
    assert!(wait_peer_load(a, "node-b", 2).await, "node-a sees node-b at capacity");

    let outcome = a
        .invoke(
            "test.execute",
            json!({"suite": "smoke", "steps": 1, "step_ms": 0}),
            None,
        )
        .await
        .unwrap();
    assert_eq!(outcome.value.executed_by, id("node-c"));
    let b = outcome
        .routing
        .candidates
        .iter()
        .find(|c| c.node_id == id("node-b"))
        .unwrap();
    assert_eq!(
        b.rejection,
        Some(Rejection::AtCapacity {
            active_jobs: 2,
            max_concurrent_jobs: 2
        })
    );
    assert_eq!(b.load_permille, 1000);

    for job in jobs {
        assert_eq!(
            a.cancel_job(&id("node-b"), &job).await.unwrap().state,
            JobState::Cancelled
        );
    }
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_06_invalid_explicit_target() {
    let cluster = sandbox().await;
    let a = cluster.node("node-a");

    let missing = a
        .invoke("math.add", json!({"a": 1, "b": 2}), Some(&id("node-x")))
        .await
        .unwrap_err();
    assert_eq!(missing.error.code, ErrorCode::TargetNotFound);
    assert_eq!(missing.routing.selected, None);

    let wrong = a
        .invoke("math.add", json!({"a": 1, "b": 2}), Some(&id("node-c")))
        .await
        .unwrap_err();
    assert_eq!(wrong.error.code, ErrorCode::CapabilityNotSupported);
    assert!(wrong.routing.attempts.is_empty(), "nothing was sent anywhere");

    for node in ["node-a", "node-b", "node-c"] {
        assert!(
            cluster.node(node).status().executions.is_empty(),
            "{node} executed nothing (no silent fallback)"
        );
    }
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_07_peer_failure() {
    let cluster = sandbox().await;
    let a = cluster.node("node-a");
    assert_eq!(a.explain("test.execute", None).selected, Some(id("node-b")));

    assert!(cluster.node("node-b").shutdown().await);
    assert!(
        a.wait_until(WAIT, |s| s.peer_state("node-b") == Some(PeerState::Unavailable))
            .await
    );

    let auto = a
        .invoke(
            "test.execute",
            json!({"suite": "smoke", "steps": 1, "step_ms": 0}),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        auto.value.executed_by,
        id("node-c"),
        "automatic routing moves to the remaining provider"
    );

    let explicit = a
        .invoke("test.execute", json!({"suite": "smoke"}), Some(&id("node-b")))
        .await
        .unwrap_err();
    assert_eq!(explicit.error.code, ErrorCode::TargetUnavailable);

    let gone = a.invoke("math.add", json!({"a": 1, "b": 1}), None).await.unwrap_err();
    assert_eq!(gone.error.code, ErrorCode::NoEligibleNode);

    // The rest of the cluster keeps working.
    assert_eq!(
        a.invoke("system.echo", json!({"x": 1}), None)
            .await
            .unwrap()
            .value
            .executed_by,
        id("node-a")
    );
    for node in ["node-a", "node-c"] {
        assert!(cluster.node(node).shutdown().await);
    }
}

/// A peer that completes the handshake and then never answers: detected by heartbeat timeout,
/// not by a closed socket.
#[tokio::test]
async fn test_07b_silent_peer_detected_by_heartbeat_timeout() {
    let cluster = Cluster::start(vec![]).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a_spec = NodeSpec {
        id: "node-a",
        node_type: "client",
        capabilities: vec![("system.echo", vec!["*"])],
        max_concurrent_jobs: 1,
        cpu_percent: 0,
        seeds: vec![listener.local_addr().unwrap().to_string()],
        trusts: vec!["node-d"],
    };
    let config = cluster.config_for(&a_spec);

    // The silent peer is a real, trusted identity (node-d) that stops talking after the handshake.
    let silent_identity = identity("node-d", cluster.key("node-d"));
    let mut trust = TrustStore::default();
    trust.insert(
        id("node-a"),
        parse_verifying_key(&cluster.key("node-a").public).unwrap(),
    );
    let silent = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let params = HandshakeParams {
                identity: &silent_identity,
                trust: &trust,
                max_frame_bytes: 1 << 20,
            };
            if handshake::accept(&mut stream, &params).await.is_ok() {
                tokio::spawn(async move { while read_frame(&mut stream, 1 << 20).await.is_ok() {} });
            }
        }
    });

    let a = synapta_core::start(config).await.unwrap();
    assert!(
        a.wait_until(WAIT, |s| s.is_ready("node-d")).await,
        "handshake with silent peer succeeds"
    );
    let detected = a
        .wait_until(WAIT, |s| {
            s.peer("node-d").is_some_and(|p| {
                p.state == PeerState::Unavailable && p.last_error.as_deref().is_some_and(|e| e.contains("heartbeat"))
            })
        })
        .await;
    assert!(
        detected,
        "silent peer marked UNAVAILABLE by heartbeat timeout: {:#?}",
        a.status().peers
    );
    assert!(a.shutdown().await);
    silent.abort();
}

#[tokio::test]
async fn test_08_async_job_lifecycle() {
    let cluster = sandbox().await;
    let a = cluster.node("node-a");
    let b_id = id("node-b");

    // Run to completion with progress events.
    let submitted = a
        .submit_job(
            "test.execute",
            json!({"suite": "unit", "steps": 4, "step_ms": 50}),
            Some(&b_id),
        )
        .await
        .unwrap();
    let job = submitted.value.job_id.clone();
    assert_eq!(submitted.value.executed_by, b_id);
    assert_eq!(a.job_status(&b_id, &job).await.unwrap().state, JobState::Running);
    let done = a.job_result(&b_id, &job, Duration::from_secs(5)).await.unwrap();
    assert_eq!(done.state, JobState::Succeeded);
    assert_eq!(done.output.as_ref().unwrap()["passed"], true);
    assert_eq!(done.percent, 100);
    let ok = a
        .wait_until(WAIT, |_| {
            a.job_events(&b_id, &job)
                .iter()
                .any(|e| matches!(e, Event::JobFinished { .. }))
        })
        .await;
    assert!(
        ok || a
            .job_events(&b_id, &job)
            .iter()
            .any(|e| matches!(e, Event::JobFinished { .. }))
    );
    let events = a.job_events(&b_id, &job);
    let progress = events.iter().filter(|e| matches!(e, Event::JobProgress(_))).count();
    assert_eq!(progress, 4, "one progress event per step: {events:?}");

    // Cancel a long job; capacity is released.
    let long = a
        .submit_job(
            "test.execute",
            json!({"suite": "unit", "steps": 500, "step_ms": 50}),
            Some(&b_id),
        )
        .await
        .unwrap();
    let long_id = long.value.job_id;
    assert!(wait_peer_load(a, "node-b", 1).await);
    let cancelled = a.cancel_job(&b_id, &long_id).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert!(
        wait_peer_load(a, "node-b", 0).await,
        "cancelled job no longer counts as active"
    );

    // Finished jobs are cleaned up after the retention period (500 ms in tests).
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_eq!(
        a.job_status(&b_id, &long_id).await.unwrap_err().code,
        ErrorCode::JobNotFound
    );
    assert_eq!(cluster.node("node-b").status().retained_jobs, 0);
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_09_isolation_and_security() {
    // C allows system.info for node-b only.
    let mut specs = common::sandbox_specs();
    specs[0].capabilities[0].1 = vec!["node-b"];
    let cluster = Cluster::start(specs).await;
    cluster.wait_full_mesh().await;
    let (a, b, c) = (cluster.node("node-a"), cluster.node("node-b"), cluster.node("node-c"));

    // Loopback-only listeners; a wildcard address is refused at start-up.
    for node in &cluster.nodes {
        assert!(node.listen_addr().ip().is_loopback());
    }
    let mut wild = cluster.config_for(&common::sandbox_specs().remove(0));
    wild.node.listen = "0.0.0.0:0".parse().unwrap();
    let refused = synapta_core::start(wild)
        .await
        .err()
        .expect("wildcard listen address must be refused");
    assert!(refused.contains("not a loopback"), "{refused}");

    // Unauthorized capability: routing skips it, explicit target is refused…
    let auto = a.invoke("system.info", json!({}), None).await.unwrap_err();
    assert_eq!(auto.error.code, ErrorCode::NoEligibleNode);
    assert_eq!(auto.routing.candidates[0].rejection, Some(Rejection::NotAllowed));
    let explicit = a
        .invoke("system.info", json!({}), Some(&id("node-c")))
        .await
        .unwrap_err();
    assert_eq!(explicit.error.code, ErrorCode::Unauthorized);
    // …and node-c enforces it itself even if a caller ignores routing.
    let a_identity = identity("node-a", cluster.key("node-a"));
    let mut trust = TrustStore::default();
    trust.insert(
        id("node-c"),
        parse_verifying_key(&cluster.key("node-c").public).unwrap(),
    );
    let mut direct = TcpStream::connect(c.listen_addr()).await.unwrap();
    let params = HandshakeParams {
        identity: &a_identity,
        trust: &trust,
        max_frame_bytes: 1 << 20,
    };
    handshake::dial(&mut direct, &params, Some(&id("node-c")))
        .await
        .unwrap();
    let bypass = Frame::Request(RequestFrame {
        id: 9,
        request: Request::Invoke {
            capability: "system.info".into(),
            input: json!({}),
        },
    });
    write_frame(&mut direct, &bypass, 1 << 20).await.unwrap();
    match read_frame(&mut direct, 1 << 20).await.unwrap() {
        Frame::Response(r) => assert_eq!(r.error.unwrap().code, ErrorCode::Unauthorized),
        other => panic!("unexpected {other:?}"),
    }
    assert!(c.status().executions.is_empty());
    // node-b is allowed.
    assert_eq!(
        b.invoke("system.info", json!({}), None)
            .await
            .unwrap()
            .value
            .executed_by,
        id("node-c")
    );

    // Untrusted node cannot complete a handshake.
    let outsider_keys = common::keys();
    let outsider = identity("node-z", &outsider_keys);
    let mut trust_b = TrustStore::default();
    trust_b.insert(
        id("node-b"),
        parse_verifying_key(&cluster.key("node-b").public).unwrap(),
    );
    let mut stream = TcpStream::connect(b.listen_addr()).await.unwrap();
    let params = HandshakeParams {
        identity: &outsider,
        trust: &trust_b,
        max_frame_bytes: 1 << 20,
    };
    assert_eq!(
        handshake::dial(&mut stream, &params, None).await.unwrap_err().code,
        ErrorCode::UntrustedPeer
    );

    // Malformed JSON and an oversized length prefix: error frame, connection closed, node fine.
    let mut bad = TcpStream::connect(b.listen_addr()).await.unwrap();
    bad.write_all(&7u32.to_be_bytes()).await.unwrap();
    bad.write_all(b"{nope!}").await.unwrap();
    assert!(
        matches!(read_frame(&mut bad, 1 << 20).await.unwrap(), Frame::Error(e) if e.code == ErrorCode::MalformedFrame)
    );
    let mut huge = TcpStream::connect(b.listen_addr()).await.unwrap();
    huge.write_all(&(u32::MAX).to_be_bytes()).await.unwrap();
    assert!(
        matches!(read_frame(&mut huge, 1 << 20).await.unwrap(), Frame::Error(e) if e.code == ErrorCode::FrameTooLarge)
    );
    let mut garbage = TcpStream::connect(b.listen_addr()).await.unwrap();
    garbage.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    drop(garbage);

    assert_eq!(
        a.invoke("math.add", json!({"a": 20, "b": 22}), None)
            .await
            .unwrap()
            .value
            .output["sum"],
        42
    );
    assert!(!b.status().executions.contains_key("test.execute"));
    cluster.shutdown().await;
}

/// No code path in the core can start a host process: the only executors are the built-in mocks,
/// and this test fails if anyone adds process spawning to the library.
#[test]
fn test_09_no_host_command_execution_in_core() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut stack = vec![src];
    let mut checked = 0;
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).unwrap();
                for forbidden in ["std::process", "tokio::process", "Command::new"] {
                    assert!(!text.contains(forbidden), "{} uses {forbidden}", path.display());
                }
                checked += 1;
            }
        }
    }
    assert!(checked > 10);
}

#[tokio::test]
async fn test_10_deterministic_routing() {
    let cluster = sandbox().await;
    let a = cluster.node("node-a");

    // Same state → byte-identical decision.
    let reference = serde_json::to_string(&a.explain("test.execute", None)).unwrap();
    for _ in 0..50 {
        assert_eq!(
            serde_json::to_string(&a.explain("test.execute", None)).unwrap(),
            reference
        );
    }
    assert!(
        reference.contains("\"selected\":\"node-b\""),
        "tie between equal nodes breaks on node id"
    );

    // Real invocations under the same (settled) state go to the same node every time.
    for _ in 0..10 {
        let outcome = a
            .invoke(
                "test.execute",
                json!({"suite": "smoke", "steps": 1, "step_ms": 0}),
                None,
            )
            .await
            .unwrap();
        assert_eq!(outcome.value.executed_by, id("node-b"));
        assert!(wait_peer_load(a, "node-b", 0).await);
    }
    cluster.shutdown().await;
}

/// Restart safety for state exchange: a restarted node's adverts must outrank its old ones even
/// if the wall clock went backwards in between (incarnation is persisted in data_dir).
#[tokio::test]
async fn restart_after_clock_regression_still_outranks_old_adverts() {
    let mut specs = common::sandbox_specs();
    specs.truncate(1); // node-c alone is enough
    let cluster = Cluster::start(specs).await;
    let c = cluster.node("node-c");
    let first = c.status().node.incarnation;
    assert!(c.shutdown().await);

    // Pretend the previous run happened far in the future relative to the current clock.
    let future = first + 10_000_000_000;
    std::fs::write(c.config().node.data_dir.join("incarnation"), future.to_string()).unwrap();
    let restarted = synapta_core::start(c.config().clone()).await.unwrap();
    assert!(restarted.status().node.incarnation > future);
    assert!(restarted.shutdown().await);
}

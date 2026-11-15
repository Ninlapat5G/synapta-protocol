//! The lab end to end, without the web page: nodes join by card and ring, rewiring takes effect
//! live, crashes are noticed, and every frame lands in the recorder.

use std::time::Duration;

use serde_json::json;
use synapta_core::{NodeHandle, NodeId};
use synapta_lab::{CapabilitySetup, Lab, NodeSetup, PowerOff};

const WAIT: Duration = Duration::from_secs(15);

fn id(s: &str) -> NodeId {
    NodeId::new(s).unwrap()
}

fn node(name: &str, rings: &[&str], capabilities: &[&str]) -> NodeSetup {
    serde_json::from_value(json!({
        "id": name,
        "rings": rings,
        "capabilities": capabilities.iter().map(|c| json!({ "name": c })).collect::<Vec<_>>(),
    }))
    .unwrap()
}

async fn handle(lab: &Lab, name: &str) -> NodeHandle {
    lab.node(&id(name)).await.unwrap()
}

#[tokio::test]
async fn nodes_mesh_in_rings_and_reach_across_them() {
    let lab = Lab::new().unwrap();
    lab.add_node(node("node-a", &["home"], &[])).await.unwrap();
    lab.add_node(node("node-b", &["home", "office"], &[])).await.unwrap();
    lab.add_node(node("node-c", &["office"], &["math.add"])).await.unwrap();
    let a = handle(&lab, "node-a").await;

    // node-a reaches node-c only through node-b, the bridge.
    let probe = a.clone();
    let routed = a
        .wait_until(WAIT, |_| {
            probe.explain("math.add", Some(&id("node-c"))).selected.is_some()
        })
        .await;
    assert!(routed, "{:?}", a.explain("math.add", None));
    let sum = a.invoke("math.add", json!({ "a": 1, "b": 2 }), None).await.unwrap();
    assert_eq!(sum.value.executed_by, id("node-c"));
    assert_eq!(sum.routing.candidates[0].hops, 2);

    // Its frames are in the recorder, relay included.
    let page = lab.recorder().after(0, usize::MAX);
    let relayed = page
        .events
        .iter()
        .any(|e| serde_json::to_value(&e.event).unwrap()["frame"]["method"] == "relay");
    assert!(relayed, "no relay frame among {} events", page.events.len());
    lab.shutdown().await;
}

#[tokio::test]
async fn rewiring_is_live_and_a_crash_is_noticed() {
    let lab = Lab::new().unwrap();
    lab.add_node(node("node-a", &["one"], &[])).await.unwrap();
    lab.add_node(node("node-b", &["two"], &[])).await.unwrap();
    let a = handle(&lab, "node-a").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!a.status().is_ready("node-b"), "different rings");

    // Plug node-b into ring one: no restart, they find each other.
    lab.set_rings(&id("node-b"), vec!["one".into(), "two".into()])
        .await
        .unwrap();
    assert!(a.wait_until(WAIT, |s| s.is_ready("node-b")).await);

    lab.power_off(&id("node-b"), PowerOff::Crash).await.unwrap();
    assert!(a.wait_until(WAIT, |s| !s.is_ready("node-b")).await);
    let reason = a
        .status()
        .peer("node-b")
        .and_then(|p| p.last_error.clone())
        .unwrap_or_default();
    assert!(
        reason.contains("heartbeat"),
        "a crash is only noticed by heartbeat: {reason}"
    );

    lab.power_on(&id("node-b")).await.unwrap();
    assert!(a.wait_until(WAIT, |s| s.is_ready("node-b")).await);

    // Any other change restarts the node with it.
    let mut changed = node("node-b", &["one", "two"], &["math.add"]);
    changed.capabilities[0] = CapabilitySetup {
        name: "math.add".into(),
        allow: vec!["node-a".into()],
    };
    lab.update_node(changed).await.unwrap();
    let probe = a.clone();
    let offered = a
        .wait_until(WAIT, |_| {
            probe.explain("math.add", Some(&id("node-b"))).selected.is_some()
        })
        .await;
    assert!(offered);
    lab.remove_node(&id("node-b")).await.unwrap();
    assert_eq!(lab.nodes().await.len(), 1);
    lab.shutdown().await;
}

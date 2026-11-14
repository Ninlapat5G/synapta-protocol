//! Load spreading under traffic: does automatic routing keep every capable node busy without
//! pushing any node past its `max_concurrent_jobs`?
//!
//! Each scenario prints what it observed (`cargo test --test load_balancing -- --nocapture`)
//! and asserts what the routing policy promises. Loopback only, like every other test.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{wait_peer_load, Cluster, NodeSpec};
use serde_json::{json, Value};
use synapta_core::protocol::message::InvokeResult;
use synapta_core::routing::dispatch::{RouteFailure, Routed};
use synapta_core::{ErrorCode, NodeHandle};

type InvokeOutcome = Result<Routed<InvokeResult>, RouteFailure>;

/// node-a calls; node-b and node-c both provide `test.execute` with the given capacities.
async fn cluster(b_max: u32, c_max: u32) -> Cluster {
    let mut specs: Vec<NodeSpec> = common::sandbox_specs();
    for spec in &mut specs {
        match spec.id {
            "node-b" => spec.max_concurrent_jobs = b_max,
            "node-c" => spec.max_concurrent_jobs = c_max,
            _ => {}
        }
    }
    // Production heartbeat timeout (3 s) instead of the test default (600 ms, tuned for fast
    // failure detection): on a CPU-starved machine 600 ms drops links mid-run, and this file
    // measures routing, not liveness (that is test_07b).
    let cluster = Cluster::start_with(specs, |config| config.timeouts.heartbeat_timeout_ms = 3_000).await;
    cluster.wait_full_mesh().await;
    cluster
}

fn job(steps: u64, step_ms: u64) -> Value {
    json!({"suite": "smoke", "steps": steps, "step_ms": step_ms})
}

async fn wait_idle(a: &NodeHandle) {
    for peer in ["node-b", "node-c"] {
        assert!(wait_peer_load(a, peer, 0).await, "{peer} did not drain");
    }
}

/// What one scenario observed from the caller's side.
#[derive(Default)]
struct Tally {
    executed_by: BTreeMap<String, u32>,
    /// Calls that reached a node which then answered `busy`: a wasted round trip each.
    busy_bounces: u32,
    /// Calls refused at the caller because every capable node was full.
    cluster_full: u32,
    other_errors: Vec<String>,
}

impl Tally {
    fn record(&mut self, result: InvokeOutcome) {
        let attempts = match &result {
            Ok(routed) => &routed.routing.attempts,
            Err(failure) => &failure.routing.attempts,
        };
        self.busy_bounces += attempts.iter().filter(|a| a.outcome.contains("Busy")).count() as u32;
        match result {
            Ok(routed) => {
                *self
                    .executed_by
                    .entry(routed.value.executed_by.to_string())
                    .or_default() += 1
            }
            Err(failure) if failure.error.code == ErrorCode::NoEligibleNode => self.cluster_full += 1,
            Err(failure) => self.other_errors.push(failure.error.to_string()),
        }
    }

    fn merge(&mut self, other: Tally) {
        for (node, n) in other.executed_by {
            *self.executed_by.entry(node).or_default() += n;
        }
        self.busy_bounces += other.busy_bounces;
        self.cluster_full += other.cluster_full;
        self.other_errors.extend(other.other_errors);
    }

    fn count(&self, node: &str) -> u32 {
        self.executed_by.get(node).copied().unwrap_or(0)
    }

    fn completed(&self) -> u32 {
        self.executed_by.values().sum()
    }

    fn print(&self, scenario: &str) {
        println!(
            "[{scenario}] completed={} by_node={:?} busy_bounces={} cluster_full={} other_errors={}",
            self.completed(),
            self.executed_by,
            self.busy_bounces,
            self.cluster_full,
            self.other_errors.len()
        );
    }
}

/// Live job count of one node, sampled from the node itself (not from adverts).
#[derive(Debug, Default, Clone, Copy)]
struct Occupancy {
    peak: u32,
    /// Mean running jobs ÷ max_concurrent_jobs over the run.
    mean: f64,
}

/// Samples every node's own job count every 2 ms until stopped.
struct OccupancyWatch {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<BTreeMap<String, Occupancy>>,
}

impl OccupancyWatch {
    fn start(nodes: Vec<NodeHandle>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let task = tokio::spawn(async move {
            let mut peak = vec![0u32; nodes.len()];
            let mut sum = vec![0u64; nodes.len()];
            let mut samples = 0u64;
            while !stop_flag.load(Ordering::Relaxed) {
                for (i, node) in nodes.iter().enumerate() {
                    let active = node.status().node.resources.active_jobs;
                    peak[i] = peak[i].max(active);
                    sum[i] += u64::from(active);
                }
                samples += 1;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            nodes
                .iter()
                .enumerate()
                .map(|(i, node)| {
                    let max = f64::from(node.config().resources.max_concurrent_jobs);
                    let occupancy = Occupancy {
                        peak: peak[i],
                        mean: sum[i] as f64 / samples.max(1) as f64 / max,
                    };
                    (node.node_id().to_string(), occupancy)
                })
                .collect()
        });
        OccupancyWatch { stop, task }
    }

    async fn finish(self) -> BTreeMap<String, Occupancy> {
        self.stop.store(true, Ordering::Relaxed);
        self.task.await.unwrap()
    }
}

/// Short jobs sent one after another: every decision sees both nodes idle, so the tie-break
/// alone decides. Both nodes must share the work evenly instead of node-b taking it all.
#[tokio::test]
async fn sequential_short_jobs_alternate_between_idle_nodes() {
    let cluster = cluster(2, 2).await;
    let a = cluster.node("node-a");
    let mut tally = Tally::default();
    for _ in 0..100 {
        tally.record(a.invoke("test.execute", job(1, 0), None).await);
        wait_idle(a).await;
    }
    tally.print("sequential 100 short");
    assert!(tally.other_errors.is_empty(), "{:?}", tally.other_errors);
    assert_eq!(tally.count("node-b"), 50);
    assert_eq!(tally.count("node-c"), 50);
    cluster.shutdown().await;
}

/// Four jobs fired at the same instant at a cluster with exactly four free slots. Adverts cannot
/// have caught up, so only the caller's own bookkeeping can spread them: 2 + 2, no `busy`.
#[tokio::test]
async fn simultaneous_burst_fills_both_nodes_without_bounces() {
    let cluster = cluster(2, 2).await;
    let a = cluster.node("node-a");
    let mut total = Tally::default();
    for round in 0..10 {
        let calls: Vec<_> = (0..4)
            .map(|_| {
                let a = a.clone();
                tokio::spawn(async move { a.invoke("test.execute", job(4, 50), None).await })
            })
            .collect();
        let mut tally = Tally::default();
        for call in calls {
            tally.record(call.await.unwrap());
        }
        assert!(tally.other_errors.is_empty(), "{:?}", tally.other_errors);
        assert_eq!(
            (tally.count("node-b"), tally.count("node-c")),
            (2, 2),
            "round {round}: busy_bounces={}",
            tally.busy_bounces
        );
        total.merge(tally);
        wait_idle(a).await;
    }
    total.print("burst 10x4");
    // Normally 0. On a starved machine the calls are no longer simultaneous, and an advert issued
    // before a job started can arrive after it was sent (see `routing/ledger.rs`), costing one
    // `busy` round trip. The 2 + 2 split above is the hard requirement.
    assert!(total.busy_bounces <= 2, "busy_bounces={}", total.busy_bounces);
    assert_eq!(total.cluster_full, 0);
    cluster.shutdown().await;
}

/// Sustained overload: more concurrent callers than slots, jobs of mixed length (20–100 ms),
/// for a few seconds.
///
/// Callers back off 5 ms after `no_eligible_node`, as a real client should. A caller retrying
/// in a tight loop would measure how fast refusals are, not how well work is spread.
async fn sustained(b_max: u32, c_max: u32, workers: u64, run_for: Duration) -> (Tally, BTreeMap<String, Occupancy>) {
    let cluster = cluster(b_max, c_max).await;
    let a = cluster.node("node-a").clone();
    let watch = OccupancyWatch::start(vec![cluster.node("node-b").clone(), cluster.node("node-c").clone()]);

    let deadline = Instant::now() + run_for;
    let tasks: Vec<_> = (0..workers)
        .map(|worker| {
            let a = a.clone();
            tokio::spawn(async move {
                let mut tally = Tally::default();
                let mut i = 0u64;
                while Instant::now() < deadline {
                    let steps = 1 + (worker * 7 + i * 3) % 5;
                    let result = a.invoke("test.execute", job(steps, 20), None).await;
                    let full = matches!(&result, Err(f) if f.error.code == ErrorCode::NoEligibleNode);
                    tally.record(result);
                    if full {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    i += 1;
                }
                tally
            })
        })
        .collect();
    let mut tally = Tally::default();
    for task in tasks {
        tally.merge(task.await.unwrap());
    }
    let occupancy = watch.finish().await;
    if !tally.other_errors.is_empty() {
        for peer in a.status().peers {
            println!("    node-a -> {}: last_error={:?}", peer.node_id, peer.last_error);
        }
    }
    cluster.shutdown().await;

    tally.print(&format!("sustained {workers} workers, b={b_max} c={c_max}"));
    for (node, o) in &occupancy {
        println!(
            "    {node}: peak_active={} mean_occupancy={:.0}%",
            o.peak,
            o.mean * 100.0
        );
    }
    (tally, occupancy)
}

fn assert_sustained(tally: &Tally, occupancy: &BTreeMap<String, Occupancy>, caps: [(&str, u32); 2]) {
    assert!(tally.other_errors.is_empty(), "{:?}", tally.other_errors);
    for (node, max) in caps {
        let o = occupancy[node];
        assert!(o.peak <= max, "{node} ran {} jobs, capacity {max}", o.peak);
        // Measured 82–91% on an idle machine (the v1 policy managed 70–75%); 75% leaves room
        // for a busy CI host without letting a regression to v1 behaviour pass.
        assert!(o.mean >= 0.75, "{node} only {:.0}% occupied", o.mean * 100.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_overload_equal_capacity() {
    let (tally, occupancy) = sustained(2, 2, 6, Duration::from_secs(4)).await;
    assert_sustained(&tally, &occupancy, [("node-b", 2), ("node-c", 2)]);
    let share_b = f64::from(tally.count("node-b")) / f64::from(tally.completed());
    assert!((0.4..=0.6).contains(&share_b), "node-b share {share_b:.2}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_overload_follows_capacity() {
    let (tally, occupancy) = sustained(4, 2, 8, Duration::from_secs(4)).await;
    assert_sustained(&tally, &occupancy, [("node-b", 4), ("node-c", 2)]);
    // Capacity ratio is 4:2, so node-b should carry about two thirds.
    let share_b = f64::from(tally.count("node-b")) / f64::from(tally.completed());
    assert!((0.55..=0.78).contains(&share_b), "node-b share {share_b:.2}");
}

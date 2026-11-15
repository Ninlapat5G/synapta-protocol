//! Preset scenarios the page can run with one click. Each returns a JSON report; what happened
//! on the wire is in the trace like everything else.
//!
//! | name                | does                                                              |
//! |---------------------|-------------------------------------------------------------------|
//! | `rings`             | replaces the lab with `rings` rings of `per_ring` nodes in a chain, each pair joined by one bridge node |
//! | `burst`             | `count` calls at once from one node; who ran them and how long it took |
//! | `crash_during_jobs` | starts long jobs, crashes the node running the first, reports every job's end |
//! | `cut_bridge`        | unplugs a bridge from one of its rings                            |

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use synapta_core::NodeId;
use tokio::task::JoinSet;

use crate::lab::{CapabilitySetup, Lab, NodeSetup, PowerOff};

pub const NAMES: &[&str] = &["rings", "burst", "crash_during_jobs", "cut_bridge"];

/// Upper bounds so a typo cannot ask for a million nodes or calls.
const MAX_NODES: usize = 5_000;
const MAX_CALLS: usize = 10_000;

pub async fn run(lab: &Lab, name: &str, params: Value) -> Result<Value, String> {
    let params = if params.is_null() { json!({}) } else { params };
    match name {
        "rings" => rings(lab, parse(params)?).await,
        "burst" => burst(lab, parse(params)?).await,
        "crash_during_jobs" => crash_during_jobs(lab, parse(params)?).await,
        "cut_bridge" => cut_bridge(lab, parse(params)?).await,
        _ => Err(format!("no scenario {name:?}; there are {NAMES:?}")),
    }
}

fn parse<T: for<'de> Deserialize<'de>>(params: Value) -> Result<T, String> {
    serde_json::from_value(params).map_err(|e| format!("bad parameters: {e}"))
}

#[derive(Deserialize)]
struct RingsParams {
    #[serde(default = "three")]
    rings: usize,
    #[serde(default = "four")]
    per_ring: usize,
    /// Offered by every node.
    #[serde(default = "default_capabilities")]
    capabilities: Vec<String>,
}

fn three() -> usize {
    3
}
fn four() -> usize {
    4
}
fn default_capabilities() -> Vec<String> {
    vec!["math.add".into(), "test.execute".into(), "system.echo".into()]
}

/// Ring `r` holds nodes `r-1`, `r-2`, ...; the last node of ring r is also in ring r+1, so the
/// rings form a chain and work from the first ring must be relayed to reach the last.
async fn rings(lab: &Lab, p: RingsParams) -> Result<Value, String> {
    let total = p.rings * p.per_ring;
    if p.rings == 0 || p.per_ring == 0 || total > MAX_NODES {
        return Err(format!("rings x per_ring must be 1..={MAX_NODES}"));
    }
    let started = Instant::now();
    let mut setups = Vec::with_capacity(total);
    for r in 1..=p.rings {
        for n in 1..=p.per_ring {
            let mut rings = vec![format!("ring-{r}")];
            if n == p.per_ring && r < p.rings {
                rings.push(format!("ring-{}", r + 1));
            }
            setups.push(NodeSetup {
                capabilities: p
                    .capabilities
                    .iter()
                    .map(|name| CapabilitySetup {
                        name: name.clone(),
                        allow: vec!["*".into()],
                    })
                    .collect(),
                rings,
                ..NodeSetup::new(NodeId::new(format!("r{r}-n{n}"))?)
            });
        }
    }
    lab.load(setups).await?;
    Ok(json!({ "nodes": total, "started_in_ms": started.elapsed().as_millis() as u64 }))
}

#[derive(Deserialize)]
struct BurstParams {
    from: NodeId,
    #[serde(default = "math_add")]
    capability: String,
    #[serde(default = "sample_input")]
    input: Value,
    #[serde(default = "ten")]
    count: usize,
    #[serde(default)]
    target: Option<NodeId>,
}

fn math_add() -> String {
    "math.add".into()
}
fn sample_input() -> Value {
    json!({ "a": 1, "b": 2 })
}
fn ten() -> usize {
    10
}

async fn burst(lab: &Lab, p: BurstParams) -> Result<Value, String> {
    if p.count == 0 || p.count > MAX_CALLS {
        return Err(format!("count must be 1..={MAX_CALLS}"));
    }
    let node = lab.node(&p.from).await.ok_or_else(|| off(&p.from))?;
    let started = Instant::now();
    let mut calls = JoinSet::new();
    for _ in 0..p.count {
        let (node, capability, input, target) = (node.clone(), p.capability.clone(), p.input.clone(), p.target.clone());
        calls.spawn(async move {
            node.invoke(&capability, input, target.as_ref())
                .await
                .map(|r| r.value.executed_by)
                .map_err(|f| f.error.message)
        });
    }
    let mut ran_on: BTreeMap<String, u64> = BTreeMap::new();
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    while let Some(joined) = calls.join_next().await {
        match joined
            .map_err(|e| e.to_string())
            .and_then(|r| r.map_err(|e| e.to_string()))
        {
            Ok(executor) => *ran_on.entry(executor.to_string()).or_default() += 1,
            Err(message) => *errors.entry(message).or_default() += 1,
        }
    }
    Ok(json!({
        "calls": p.count,
        "elapsed_ms": started.elapsed().as_millis() as u64,
        "ran_on": ran_on,
        "errors": errors,
    }))
}

#[derive(Deserialize)]
struct CrashParams {
    from: NodeId,
    #[serde(default = "four")]
    jobs: usize,
}

/// Each job runs `test.execute` for about 4 s, so the crash lands mid-job.
async fn crash_during_jobs(lab: &Lab, p: CrashParams) -> Result<Value, String> {
    if p.jobs == 0 || p.jobs > MAX_CALLS {
        return Err(format!("jobs must be 1..={MAX_CALLS}"));
    }
    let node = lab.node(&p.from).await.ok_or_else(|| off(&p.from))?;
    let input = json!({ "suite": "smoke", "steps": 20, "step_ms": 200 });
    let mut submitted = Vec::new();
    for _ in 0..p.jobs {
        let job = node
            .submit_job("test.execute", input.clone(), None)
            .await
            .map_err(|f| format!("submit failed: {}", f.error.message))?;
        submitted.push(job.value);
    }
    let victim = submitted[0].executed_by.clone();
    if victim == p.from {
        return Err("the first job ran on the caller itself; pick a node without test.execute".into());
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    lab.power_off(&victim, PowerOff::Crash).await?;

    let mut outcomes = Vec::new();
    for job in &submitted {
        let outcome = match node
            .job_result(&job.executed_by, &job.job_id, Duration::from_secs(10))
            .await
        {
            Ok(info) => json!({ "state": info.state, "message": info.message }),
            Err(error) => json!({ "error": error.message }),
        };
        outcomes.push(json!({ "job": job.job_id, "executed_by": job.executed_by, "outcome": outcome }));
    }
    Ok(json!({ "crashed": victim, "jobs": outcomes }))
}

#[derive(Deserialize)]
struct CutParams {
    /// A node in two or more rings; the first such node by id when left out.
    #[serde(default)]
    bridge: Option<NodeId>,
}

async fn cut_bridge(lab: &Lab, p: CutParams) -> Result<Value, String> {
    let setups = lab.setups().await;
    let bridge = setups
        .iter()
        .filter(|s| s.rings.len() >= 2)
        .find(|s| p.bridge.as_ref().is_none_or(|b| b == &s.id))
        .ok_or("no node is in two or more rings")?;
    let mut rings = bridge.rings.clone();
    let unplugged = rings.pop().expect("at least two rings");
    lab.set_rings(&bridge.id, rings.clone()).await?;
    Ok(json!({ "bridge": bridge.id, "unplugged_from": unplugged, "still_in": rings }))
}

fn off(id: &NodeId) -> String {
    format!("{id} does not exist or is powered off")
}

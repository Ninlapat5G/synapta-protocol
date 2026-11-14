//! `synapta-sandbox` — one command for the whole end-to-end demo:
//! starts node-a/b/c as three separate `synapta-node` processes on 127.0.0.1 from the configs in
//! `examples/sandbox`, runs the acceptance scenario against them through their local APIs,
//! checks every step, kills node-b to show failure handling, then shuts the rest down.
//!
//! ```text
//! synapta-sandbox [--dir <empty dir>] [--auto-ports]
//! ```
//! `--auto-ports` lets the OS pick ports (used by the automated E2E test). Everything — keys,
//! configs, per-node logs and `transcript.json` (every request/response) — goes into `--dir`
//! (default: a new folder under the system temp dir). Exit code 0 only if every step passed.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use synapta_gateway::client::{ApiClient, RuntimeInfo, RUNTIME_FILE};

/// Start order matters: each node's seed must already be running (a→b→c).
const NODES: [(&str, &str); 3] = [
    ("node-c", include_str!("../../../../examples/sandbox/node-c.toml")),
    ("node-b", include_str!("../../../../examples/sandbox/node-b.toml")),
    ("node-a", include_str!("../../../../examples/sandbox/node-a.toml")),
];
const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(100);

fn main() -> ExitCode {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let auto_ports = take_switch(&mut args, "--auto-ports");
    let dir = match take_value(&mut args, "--dir") {
        Some(dir) => PathBuf::from(dir),
        None => std::env::temp_dir().join(format!("synapta-sandbox-{}", unix_millis())),
    };
    if !args.is_empty() {
        eprintln!("usage: synapta-sandbox [--dir <empty dir>] [--auto-ports]");
        return ExitCode::FAILURE;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(run(dir, auto_ports)) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("synapta-sandbox: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn run(dir: PathBuf, auto_ports: bool) -> Result<bool, String> {
    let mut sandbox = Sandbox::start(&dir, auto_ports).await?;
    println!("sandbox: {}", dir.display());

    let steps: Vec<(&str, StepResult)> = vec![
        ("TEST-01 discovery + handshake", sandbox.discovery().await),
        ("TEST-02 capability-based routing", sandbox.auto_routing().await),
        ("TEST-03 explicit target", sandbox.explicit_target().await),
        ("TEST-04 resource-aware routing", sandbox.resource_aware().await),
        ("TEST-05 UTCP cross-node invocation", sandbox.utcp().await),
        ("TEST-06 invalid explicit target", sandbox.invalid_target().await),
        ("TEST-08 async job lifecycle", sandbox.async_job().await),
        ("TEST-10 deterministic routing", sandbox.determinism().await),
        ("TEST-07 peer failure (node-b killed)", sandbox.peer_failure().await),
        ("graceful shutdown", sandbox.shutdown().await),
    ];

    sandbox.write_transcript()?;
    println!("\n==== summary ====");
    let mut all_passed = true;
    for (name, result) in &steps {
        match result {
            Ok(()) => println!("PASS  {name}"),
            Err(why) => {
                all_passed = false;
                println!("FAIL  {name}: {why}");
            }
        }
    }
    println!("transcript: {}", dir.join("transcript.json").display());
    println!("node logs:  {}", dir.join("data").display());
    Ok(all_passed)
}

type StepResult = Result<(), String>;

fn check(condition: bool, message: impl Into<String>) -> StepResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

/// A child node process; killed if still running when dropped (no orphans on early exit).
struct NodeProcess {
    child: Child,
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

struct Sandbox {
    dir: PathBuf,
    processes: BTreeMap<String, NodeProcess>,
    apis: BTreeMap<String, ApiClient>,
    transcript: Vec<Value>,
}

impl Sandbox {
    async fn start(dir: &Path, auto_ports: bool) -> Result<Sandbox, String> {
        if dir.exists() && std::fs::read_dir(dir).map_err(|e| e.to_string())?.next().is_some() {
            return Err(format!("{} is not empty", dir.display()));
        }
        std::fs::create_dir_all(dir.join("keys")).map_err(|e| e.to_string())?;
        for (id, _) in NODES {
            let (private, public) = synapta_core::identity::generate_keypair();
            std::fs::write(dir.join(format!("keys/{id}.key")), private).map_err(|e| e.to_string())?;
            std::fs::write(dir.join(format!("keys/{id}.pub")), public).map_err(|e| e.to_string())?;
        }

        // Seeds in the example files use the fixed example ports; map them to whatever address
        // each node actually got.
        let mut example_listen: BTreeMap<String, String> = BTreeMap::new();
        for (id, template) in NODES {
            let table: toml::Table = template.parse().map_err(|e| format!("{id} example: {e}"))?;
            example_listen.insert(
                table["node"]["listen"].as_str().unwrap_or_default().to_string(),
                id.to_string(),
            );
        }

        let node_bin = node_binary()?;
        let mut sandbox = Sandbox {
            dir: dir.to_path_buf(),
            processes: BTreeMap::new(),
            apis: BTreeMap::new(),
            transcript: Vec::new(),
        };
        let mut actual_listen: BTreeMap<String, SocketAddr> = BTreeMap::new();
        for (id, template) in NODES {
            let mut table: toml::Table = template.parse().map_err(|e| format!("{id} example: {e}"))?;
            if auto_ports {
                set(&mut table, &["node", "listen"], "127.0.0.1:0".into());
                set(&mut table, &["api", "listen"], "127.0.0.1:0".into());
            }
            let seeds: Vec<toml::Value> = table
                .get("discovery")
                .and_then(|d| d.get("seeds"))
                .and_then(|s| s.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|seed| {
                    let seed = seed.as_str().unwrap_or_default().to_string();
                    let addr = example_listen
                        .get(&seed)
                        .and_then(|node| actual_listen.get(node))
                        .map(|a| a.to_string());
                    toml::Value::String(addr.unwrap_or(seed))
                })
                .collect();
            if !seeds.is_empty() {
                set(&mut table, &["discovery", "seeds"], toml::Value::Array(seeds));
            }
            let config_path = dir.join(format!("{id}.toml"));
            std::fs::write(&config_path, toml::to_string(&table).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;

            let log = std::fs::File::create(dir.join(format!("{id}.stderr.log"))).map_err(|e| e.to_string())?;
            let child = Command::new(&node_bin)
                .args(["run", "--config"])
                .arg(&config_path)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .map_err(|e| format!("spawn {}: {e}", node_bin.display()))?;
            sandbox.processes.insert(id.to_string(), NodeProcess { child });

            let data_dir = dir.join("data").join(id);
            let runtime_file = data_dir.join(RUNTIME_FILE);
            if !poll_until(STARTUP_TIMEOUT, || async { runtime_file.exists() }).await {
                return Err(format!(
                    "{id} did not start; see {}",
                    dir.join(format!("{id}.stderr.log")).display()
                ));
            }
            let info: RuntimeInfo =
                serde_json::from_str(&std::fs::read_to_string(&runtime_file).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            println!(
                "started {id}: pid {} listen {} api {:?}",
                info.pid, info.listen, info.api
            );
            actual_listen.insert(id.to_string(), info.listen);
            sandbox
                .apis
                .insert(id.to_string(), ApiClient::from_data_dir(&data_dir)?);
        }
        Ok(sandbox)
    }

    /// Sends one request to a node's local API and records it in the transcript.
    async fn call(
        &mut self,
        node: &str,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), String> {
        let api = self.apis.get(node).ok_or(format!("no api for {node}"))?.clone();
        let (status, reply) = match &body {
            Some(body) => api.post(path, body).await?,
            None if method == "POST" => api.post(path, &json!({})).await?,
            None => api.get(path).await?,
        };
        println!(
            "\n{node} {method} {path}{}",
            body.as_ref().map(|b| format!(" {b}")).unwrap_or_default()
        );
        println!("  -> {status} {}", compact(&reply));
        self.transcript.push(
            json!({"node": node, "method": method, "path": path, "body": body, "status": status, "response": reply}),
        );
        Ok((status, reply))
    }

    async fn status(&self, node: &str) -> Result<Value, String> {
        let api = self.apis.get(node).ok_or(format!("no api for {node}"))?;
        Ok(api.get("/status").await?.1)
    }

    async fn wait_status(&self, node: &str, timeout: Duration, condition: impl Fn(&Value) -> bool) -> bool {
        let Some(api) = self.apis.get(node).cloned() else {
            return false;
        };
        poll_until(timeout, || {
            let api = api.clone();
            let condition = &condition;
            async move { matches!(api.get("/status").await, Ok((200, status)) if condition(&status)) }
        })
        .await
    }

    // ------------------------------------------------------------------ steps

    async fn discovery(&mut self) -> StepResult {
        println!("\n==== TEST-01 discovery + handshake ====");
        let ids: Vec<String> = self.apis.keys().cloned().collect();
        for node in &ids {
            let others: Vec<&String> = ids.iter().filter(|o| *o != node).collect();
            let meshed = self
                .wait_status(node, CONVERGE_TIMEOUT, |s| {
                    others
                        .iter()
                        .all(|o| peer_state(s, o) == "READY" && peer_advert(s, o).is_some())
                })
                .await;
            check(meshed, format!("{node} did not reach READY with {others:?}"))?;
        }
        let (_, a) = self.call("node-a", "GET", "/status", None).await?;
        let caps = |peer: &str| -> Vec<String> {
            peer_advert(&a, peer)
                .and_then(|ad| ad["capabilities"].as_array().cloned())
                .unwrap_or_default()
                .iter()
                .filter_map(|c| c["name"].as_str().map(String::from))
                .collect()
        };
        check(
            caps("node-b") == ["math.add", "test.execute"],
            "node-a registry: node-b capabilities",
        )?;
        check(
            caps("node-c") == ["system.info", "test.execute"],
            "node-a registry: node-c capabilities",
        )?;
        println!("node-a discovered node-c via node-b's adverts (node-a's only seed is node-b)");
        Ok(())
    }

    async fn auto_routing(&mut self) -> StepResult {
        println!("\n==== TEST-02 capability-based routing ====");
        let (status, reply) = self
            .call("node-a", "POST", "/tools/math.add", Some(json!({"a": 2, "b": 3})))
            .await?;
        check(status == 200 && reply["result"]["sum"] == 5, "math.add result")?;
        check(reply["executed_by"] == "node-b", "executed by node-b")?;
        print_decision(&reply["routing"]);
        let b = self.status("node-b").await?;
        let a = self.status("node-a").await?;
        check(
            b["executions"]["math.add"].as_u64() >= Some(1),
            "node-b executed math.add",
        )?;
        check(
            a["executions"].get("math.add").is_none(),
            "node-a did not execute math.add",
        )
    }

    async fn explicit_target(&mut self) -> StepResult {
        println!("\n==== TEST-03 explicit target ====");
        let before_b = self.status("node-b").await?["executions"]["test.execute"]
            .as_u64()
            .unwrap_or(0);
        let input = json!({"suite": "smoke", "steps": 2, "step_ms": 10});
        let (status, reply) = self
            .call("node-a", "POST", "/tools/test.execute?target=node-c", Some(input))
            .await?;
        check(status == 200 && reply["executed_by"] == "node-c", "executed by node-c")?;
        check(reply["routing"]["mode"] == "explicit", "explicit mode")?;
        let after_b = self.status("node-b").await?["executions"]["test.execute"]
            .as_u64()
            .unwrap_or(0);
        check(before_b == after_b, "node-b did not run test.execute")
    }

    async fn resource_aware(&mut self) -> StepResult {
        println!("\n==== TEST-04 resource-aware routing ====");
        let long = json!({"capability": "test.execute", "input": {"suite": "integration", "steps": 300, "step_ms": 50}, "target": "node-b"});
        let mut jobs = Vec::new();
        for _ in 0..2 {
            let (status, reply) = self.call("node-a", "POST", "/jobs", Some(long.clone())).await?;
            check(status == 202, "long job accepted")?;
            jobs.push(reply["job_id"].as_str().unwrap_or_default().to_string());
        }
        let full = self
            .wait_status("node-a", CONVERGE_TIMEOUT, |s| {
                advert_active_jobs(s, "node-b") == Some(2)
            })
            .await;
        check(full, "node-a sees node-b at 2/2 jobs")?;
        let quick = json!({"suite": "smoke", "steps": 1, "step_ms": 0});
        let (status, reply) = self.call("node-a", "POST", "/tools/test.execute", Some(quick)).await?;
        print_decision(&reply["routing"]);
        check(status == 200 && reply["executed_by"] == "node-c", "routed to node-c")?;
        let b = reply["routing"]["candidates"]
            .as_array()
            .and_then(|c| c.iter().find(|c| c["node_id"] == "node-b").cloned());
        check(
            b.is_some_and(|b| b["rejection"]["code"] == "at_capacity"),
            "node-b rejected as at_capacity",
        )?;
        for job in jobs {
            let (status, reply) = self
                .call("node-a", "POST", &format!("/jobs/node-b/{job}/cancel"), None)
                .await?;
            check(status == 200 && reply["state"] == "cancelled", "long job cancelled")?;
        }
        check(
            self.wait_status("node-a", CONVERGE_TIMEOUT, |s| {
                advert_active_jobs(s, "node-b") == Some(0)
            })
            .await,
            "node-b back to 0 jobs",
        )
    }

    async fn utcp(&mut self) -> StepResult {
        println!("\n==== TEST-05 UTCP cross-node invocation ====");
        let (status, manual) = self.call("node-a", "GET", "/utcp", None).await?;
        check(status == 200, "manual served")?;
        let tools = manual["tools"].as_array().cloned().unwrap_or_default();
        let add = tools
            .iter()
            .find(|t| t["name"] == "math.add")
            .ok_or("math.add tool missing")?;
        let url = add["tool_call_template"]["url"].as_str().unwrap_or_default();
        let path = url
            .split_once("//")
            .and_then(|(_, rest)| rest.find('/').map(|i| &rest[i..]))
            .unwrap_or_default()
            .to_string();
        let (status, reply) = self
            .call("node-a", "POST", &path, Some(json!({"a": 40, "b": 2})))
            .await?;
        check(
            status == 200 && reply["result"]["sum"] == 42 && reply["executed_by"] == "node-b",
            "UTCP tool call executed on node-b",
        )?;
        println!("(the official Python UTCP SDK run is a separate test: see docs/utcp-integration.md)");
        Ok(())
    }

    async fn invalid_target(&mut self) -> StepResult {
        println!("\n==== TEST-06 invalid explicit target ====");
        let before = self.status("node-b").await?["executions"].clone();
        let (status, reply) = self
            .call(
                "node-a",
                "POST",
                "/tools/math.add?target=node-x",
                Some(json!({"a": 1, "b": 1})),
            )
            .await?;
        check(
            status == 404 && reply["error"]["code"] == "target_not_found",
            "unknown node → target_not_found",
        )?;
        let (status, reply) = self
            .call(
                "node-a",
                "POST",
                "/tools/math.add?target=node-c",
                Some(json!({"a": 1, "b": 1})),
            )
            .await?;
        check(
            status == 404 && reply["error"]["code"] == "capability_not_supported",
            "wrong node → capability_not_supported",
        )?;
        check(
            self.status("node-b").await?["executions"] == before,
            "no silent fallback to node-b",
        )
    }

    async fn async_job(&mut self) -> StepResult {
        println!("\n==== TEST-08 async job lifecycle ====");
        let submit = json!({"capability": "test.execute", "input": {"suite": "unit", "steps": 5, "step_ms": 100}, "target": "node-b"});
        let (status, reply) = self.call("node-a", "POST", "/jobs", Some(submit)).await?;
        check(status == 202, "job accepted")?;
        let job = reply["job_id"].as_str().unwrap_or_default().to_string();
        let (_, running) = self.call("node-a", "GET", &format!("/jobs/node-b/{job}"), None).await?;
        check(running["state"] == "running", "job running")?;
        let (_, done) = self
            .call("node-a", "GET", &format!("/jobs/node-b/{job}?wait_ms=10000"), None)
            .await?;
        check(
            done["state"] == "succeeded" && done["output"]["passed"] == true,
            "job succeeded",
        )?;
        let (_, events) = self
            .call("node-a", "GET", &format!("/jobs/node-b/{job}/events"), None)
            .await?;
        let events = events["events"].as_array().cloned().unwrap_or_default();
        let progress = events.iter().filter(|e| e["event"] == "job_progress").count();
        check(progress == 5, format!("5 progress events received, got {progress}"))?;

        let long = json!({"capability": "test.execute", "input": {"suite": "unit", "steps": 500, "step_ms": 50}, "target": "node-b"});
        let (_, reply) = self.call("node-a", "POST", "/jobs", Some(long)).await?;
        let job = reply["job_id"].as_str().unwrap_or_default().to_string();
        let (_, cancelled) = self
            .call("node-a", "POST", &format!("/jobs/node-b/{job}/cancel"), None)
            .await?;
        check(cancelled["state"] == "cancelled", "job cancelled")?;
        check(
            self.wait_status("node-a", CONVERGE_TIMEOUT, |s| {
                advert_active_jobs(s, "node-b") == Some(0)
            })
            .await,
            "capacity released",
        )
    }

    async fn determinism(&mut self) -> StepResult {
        println!("\n==== TEST-10 deterministic routing ====");
        let (_, first) = self
            .call("node-a", "GET", "/routing/explain?capability=test.execute", None)
            .await?;
        let api = self.apis["node-a"].clone();
        for _ in 0..20 {
            let (_, again) = api.get("/routing/explain?capability=test.execute").await?;
            check(again == first, "same state produced a different decision")?;
        }
        println!("20 repeated decisions identical; selected {}", first["selected"]);
        check(
            first["selected"] == "node-b",
            "equal load ties break on node id (node-b)",
        )
    }

    async fn peer_failure(&mut self) -> StepResult {
        println!("\n==== TEST-07 peer failure ====");
        let mut b = self.processes.remove("node-b").ok_or("node-b not running")?;
        b.child.kill().map_err(|e| e.to_string())?;
        let _ = b.child.wait();
        println!("node-b process killed (hard kill, no goodbye)");
        let started = Instant::now();
        let detected = self
            .wait_status("node-a", CONVERGE_TIMEOUT, |s| peer_state(s, "node-b") == "UNAVAILABLE")
            .await;
        check(detected, "node-a marks node-b UNAVAILABLE")?;
        println!("detected after {} ms", started.elapsed().as_millis());
        let quick = json!({"suite": "smoke", "steps": 1, "step_ms": 0});
        let (status, reply) = self
            .call("node-a", "POST", "/tools/test.execute", Some(quick.clone()))
            .await?;
        check(
            status == 200 && reply["executed_by"] == "node-c",
            "automatic routing uses node-c",
        )?;
        let (status, reply) = self
            .call("node-a", "POST", "/tools/test.execute?target=node-b", Some(quick))
            .await?;
        check(
            status == 503 && reply["error"]["code"] == "target_unavailable",
            "explicit node-b → target_unavailable",
        )?;
        let (status, _) = self.call("node-a", "GET", "/status", None).await?;
        check(status == 200, "node-a still serving")
    }

    async fn shutdown(&mut self) -> StepResult {
        println!("\n==== graceful shutdown ====");
        for node in ["node-a", "node-c"] {
            let (status, _) = self.call(node, "POST", "/admin/shutdown", None).await?;
            check(status == 202, format!("{node} accepted shutdown"))?;
        }
        for node in ["node-a", "node-c"] {
            let process = self.processes.get_mut(node).ok_or(format!("{node} not running"))?;
            let exited = poll_until(Duration::from_secs(10), || {
                let done = matches!(process.child.try_wait(), Ok(Some(_)));
                async move { done }
            })
            .await;
            check(exited, format!("{node} exited"))?;
            let code = process.child.try_wait().ok().flatten().and_then(|s| s.code());
            check(code == Some(0), format!("{node} exit code {code:?}"))?;
            println!("{node} exited cleanly");
        }
        Ok(())
    }

    fn write_transcript(&self) -> Result<(), String> {
        let text = serde_json::to_string_pretty(&self.transcript).map_err(|e| e.to_string())?;
        std::fs::write(self.dir.join("transcript.json"), text).map_err(|e| e.to_string())
    }
}

fn peer<'a>(status: &'a Value, id: &str) -> Option<&'a Value> {
    status["peers"].as_array()?.iter().find(|p| p["node_id"] == id)
}

fn peer_state(status: &Value, id: &str) -> String {
    peer(status, id)
        .and_then(|p| p["state"].as_str())
        .unwrap_or("UNKNOWN")
        .to_string()
}

fn peer_advert<'a>(status: &'a Value, id: &str) -> Option<&'a Value> {
    peer(status, id).map(|p| &p["advert"]).filter(|a| !a.is_null())
}

fn advert_active_jobs(status: &Value, id: &str) -> Option<u64> {
    peer_advert(status, id)?["resources"]["active_jobs"].as_u64()
}

fn print_decision(decision: &Value) {
    println!(
        "  routing decision: {}",
        serde_json::to_string_pretty(decision)
            .unwrap_or_default()
            .replace('\n', "\n  ")
    );
}

fn compact(value: &Value) -> String {
    let text = value.to_string();
    if text.len() > 400 {
        format!(
            "{}… ({} bytes, full text in transcript.json)",
            &text[..text.floor_char_boundary(400)],
            text.len()
        )
    } else {
        text
    }
}

/// Re-checks `condition` every `POLL` until it holds or `timeout` passes. Used only for
/// cross-process state that has no push notification (another process's HTTP status).
async fn poll_until<F, Fut>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if condition().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(POLL).await;
    }
}

fn set(table: &mut toml::Table, path: &[&str], value: toml::Value) {
    let (last, parents) = path.split_last().expect("non-empty path");
    let mut current = table;
    for key in parents {
        current = current
            .entry(key.to_string())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()))
            .as_table_mut()
            .expect("config section is a table");
    }
    current.insert(last.to_string(), value);
}

fn node_binary() -> Result<PathBuf, String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let node = exe.with_file_name(format!("synapta-node{}", std::env::consts::EXE_SUFFIX));
    if node.exists() {
        Ok(node)
    } else {
        Err(format!(
            "{} not found next to synapta-sandbox; build the workspace first",
            node.display()
        ))
    }
}

fn take_switch(args: &mut Vec<String>, name: &str) -> bool {
    let found = args.iter().position(|a| a == name);
    if let Some(index) = found {
        args.remove(index);
    }
    found.is_some()
}

fn take_value(args: &mut Vec<String>, name: &str) -> Option<String> {
    let index = args.iter().position(|a| a == name)?;
    if index + 1 >= args.len() {
        return None;
    }
    let value = args.remove(index + 1);
    args.remove(index);
    Some(value)
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

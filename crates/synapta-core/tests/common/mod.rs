//! In-process sandbox cluster for integration tests: real nodes (own listener, registry,
//! config, keys, data dir) on 127.0.0.1 with OS-assigned ports, inside a temp directory.

#![allow(dead_code)] // each test binary uses a different subset

use std::path::{Path, PathBuf};
use std::time::Duration;

use synapta_core::identity::{generate_keypair, parse_signing_key, NodeIdentity};
use synapta_core::{NodeConfig, NodeHandle, NodeId};
use tempfile::TempDir;

pub const WAIT: Duration = Duration::from_secs(10);

pub struct Keys {
    pub private: String,
    pub public: String,
}

pub fn keys() -> Keys {
    let (private, public) = generate_keypair();
    Keys { private, public }
}

pub fn identity(id: &str, keys: &Keys) -> NodeIdentity {
    NodeIdentity::new(NodeId::new(id).unwrap(), parse_signing_key(&keys.private).unwrap())
}

pub struct NodeSpec {
    pub id: &'static str,
    pub node_type: &'static str,
    /// `(capability, allow list)`
    pub capabilities: Vec<(&'static str, Vec<&'static str>)>,
    pub max_concurrent_jobs: u32,
    pub cpu_percent: u8,
    pub seeds: Vec<String>,
    /// Ids whose keys this node trusts.
    pub trusts: Vec<&'static str>,
}

/// The v0.0.1 sandbox topology. Seeds form a chain A→B→C, so A must *discover* C.
pub fn sandbox_specs() -> Vec<NodeSpec> {
    let all = |caps: &[&'static str]| caps.iter().map(|c| (*c, vec!["*"])).collect::<Vec<_>>();
    vec![
        NodeSpec {
            id: "node-c",
            node_type: "edge",
            capabilities: all(&["system.info", "test.execute"]),
            max_concurrent_jobs: 2,
            cpu_percent: 20,
            seeds: vec![],
            trusts: vec!["node-a", "node-b"],
        },
        NodeSpec {
            id: "node-b",
            node_type: "server",
            capabilities: all(&["math.add", "test.execute"]),
            max_concurrent_jobs: 2,
            cpu_percent: 20,
            seeds: vec!["node-c".into()],
            trusts: vec!["node-a", "node-c"],
        },
        NodeSpec {
            id: "node-a",
            node_type: "client",
            capabilities: all(&["system.echo"]),
            max_concurrent_jobs: 4,
            cpu_percent: 10,
            seeds: vec!["node-b".into()],
            trusts: vec!["node-b", "node-c"],
        },
    ]
}

pub struct Cluster {
    pub dir: TempDir,
    pub keys: Vec<(String, Keys)>,
    pub nodes: Vec<NodeHandle>,
}

impl Cluster {
    /// Starts nodes in the given order. A seed written as a node id is replaced by that node's
    /// actual address, so it must refer to a node started earlier.
    pub async fn start(specs: Vec<NodeSpec>) -> Cluster {
        Self::start_with(specs, |_| {}).await
    }

    /// Like `start`, with a last change to every node's config before it starts.
    pub async fn start_with(specs: Vec<NodeSpec>, adjust: impl Fn(&mut NodeConfig)) -> Cluster {
        let dir = tempfile::tempdir().unwrap();
        // node-d is a spare identity for fake peers built inside tests.
        let ids = ["node-a", "node-b", "node-c", "node-d"];
        let keys: Vec<(String, Keys)> = ids.iter().map(|id| (id.to_string(), keys())).collect();
        let mut cluster = Cluster {
            dir,
            keys,
            nodes: Vec::new(),
        };
        for spec in specs {
            let mut config = cluster.config_for(&spec);
            adjust(&mut config);
            let node = synapta_core::start(config).await.unwrap();
            cluster.nodes.push(node);
        }
        cluster
    }

    pub fn key(&self, id: &str) -> &Keys {
        &self.keys.iter().find(|(k, _)| k == id).unwrap().1
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    pub fn config_for(&self, spec: &NodeSpec) -> NodeConfig {
        let key_file = self.path(&format!("{}.key", spec.id));
        std::fs::write(&key_file, &self.key(spec.id).private).unwrap();
        let seeds: Vec<String> = spec
            .seeds
            .iter()
            .map(|s| match self.try_node(s) {
                Some(node) => node.listen_addr().to_string(),
                None => s.clone(),
            })
            .map(|addr| format!("\"{addr}\""))
            .collect();
        let mut text = format!(
            r#"
[node]
id = "{id}"
name = "Node {id}"
type = "{node_type}"
listen = "127.0.0.1:0"
data_dir = "data/{id}"

[identity]
private_key_file = "{id}.key"

[discovery]
seeds = [{seeds}]

[timeouts]
request_ms = 5000
heartbeat_interval_ms = 200
heartbeat_timeout_ms = 600
idle_timeout_ms = 2000
reconnect_base_ms = 50
reconnect_max_ms = 300
max_reconnect_attempts = 5
advert_refresh_ms = 1000
advert_max_age_ms = 5000
advert_min_interval_ms = 20
job_retention_ms = 500
shutdown_grace_ms = 3000

[resources]
cpu_percent = {cpu}
memory_available_mb = 4096
max_concurrent_jobs = {max}
"#,
            id = spec.id,
            node_type = spec.node_type,
            seeds = seeds.join(", "),
            cpu = spec.cpu_percent,
            max = spec.max_concurrent_jobs,
        );
        for (name, allow) in &spec.capabilities {
            let allow: Vec<String> = allow.iter().map(|a| format!("\"{a}\"")).collect();
            text.push_str(&format!(
                "\n[[capabilities]]\nname = \"{name}\"\nallow = [{}]\n",
                allow.join(", ")
            ));
        }
        for trusted in &spec.trusts {
            text.push_str(&format!(
                "\n[[trust]]\nnode_id = \"{trusted}\"\npublic_key = \"{}\"\n",
                self.key(trusted).public
            ));
        }
        let path = self.path(&format!("{}.toml", spec.id));
        std::fs::write(&path, text).unwrap();
        NodeConfig::load(Path::new(&path)).unwrap()
    }

    pub fn try_node(&self, id: &str) -> Option<&NodeHandle> {
        self.nodes.iter().find(|n| n.node_id().as_str() == id)
    }

    pub fn node(&self, id: &str) -> &NodeHandle {
        self.try_node(id).unwrap_or_else(|| panic!("no node {id}"))
    }

    /// Waits until every node has a READY link to every other node.
    pub async fn wait_full_mesh(&self) {
        let ids: Vec<String> = self.nodes.iter().map(|n| n.node_id().to_string()).collect();
        for node in &self.nodes {
            let others: Vec<String> = ids.iter().filter(|i| *i != node.node_id().as_str()).cloned().collect();
            let ok = node
                .wait_until(WAIT, |s| {
                    others
                        .iter()
                        .all(|o| s.is_ready(o) && s.peer(o).unwrap().advert.is_some())
                })
                .await;
            assert!(
                ok,
                "{} did not reach all peers: {:#?}",
                node.node_id(),
                node.status().peers
            );
        }
    }

    pub async fn shutdown(&self) {
        for node in &self.nodes {
            assert!(node.shutdown().await, "{} did not shut down cleanly", node.node_id());
        }
    }
}

pub fn id(s: &str) -> NodeId {
    NodeId::new(s).unwrap()
}

/// Waits until `node` sees `peer` advertising exactly `active` running jobs.
pub async fn wait_peer_load(node: &NodeHandle, peer: &str, active: u32) -> bool {
    node.wait_until(WAIT, |s| {
        s.peer(peer)
            .and_then(|p| p.advert.as_ref())
            .is_some_and(|a| a.resources.active_jobs == active)
    })
    .await
}

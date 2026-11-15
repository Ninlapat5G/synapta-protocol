//! The running simulation: real Synapta nodes in this process, on a simulated network of rings
//! (`synapta_core::transport::sim`), all members of one network by card (`identity`).
//!
//! The lab is the network's authority: every node it creates gets a membership card, so nodes
//! added later are trusted without touching the others. Each node gets its own loopback address
//! (127.1.x.y) and a folder in the lab's temp dir for its key, card and data.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use synapta_core::config::ms;
use synapta_core::identity::{generate_keypair, issue_card, random_bytes, to_hex, unix_now, CardClaims};
use synapta_core::protocol::message::Availability;
use synapta_core::protocol::NodeType;
use synapta_core::transport::sim::SimNetwork;
use synapta_core::{NodeConfig, NodeHandle, NodeId, NodeStatus, StartOptions};
use tempfile::TempDir;
use tokio::sync::Mutex;

use crate::recorder::Recorder;

/// Name of the network every lab node belongs to (on its membership card).
pub const NETWORK: &str = "synapta-lab";
/// A new node is seeded with this many running ring-mates; the rest it learns from adverts.
const SEEDS_PER_NODE: usize = 3;
/// Trace events kept for live view and replay.
const TRACE_CAPACITY: usize = 200_000;
/// How long a card stays valid. The lab lives for one session; a year is "never" here.
const CARD_LIFETIME_S: u64 = 365 * 24 * 3600;

/// Everything the web page can set on a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeSetup {
    pub id: NodeId,
    #[serde(default)]
    pub name: String,
    #[serde(default = "server")]
    pub node_type: NodeType,
    #[serde(default)]
    pub capabilities: Vec<CapabilitySetup>,
    #[serde(default = "four")]
    pub max_concurrent_jobs: u32,
    #[serde(default)]
    pub cpu_percent: u8,
    #[serde(default = "memory")]
    pub memory_mb: u64,
    #[serde(default = "available")]
    pub availability: Availability,
    #[serde(default)]
    pub rings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapabilitySetup {
    pub name: String,
    #[serde(default = "everyone")]
    pub allow: Vec<String>,
}

fn server() -> NodeType {
    NodeType::Server
}
fn four() -> u32 {
    4
}
fn memory() -> u64 {
    4096
}
fn available() -> Availability {
    Availability::Available
}
fn everyone() -> Vec<String> {
    vec!["*".into()]
}

/// How a node goes down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PowerOff {
    /// Shuts down cleanly: peers see its connections close.
    Graceful,
    /// Like pulling the plug: it goes silent first, so peers only notice through heartbeats.
    Crash,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeView {
    pub setup: NodeSetup,
    pub address: Option<SocketAddr>,
    pub running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<NodeStatus>,
}

struct LabNode {
    setup: NodeSetup,
    ip: Ipv4Addr,
    dir: PathBuf,
    handle: Option<NodeHandle>,
}

pub struct Lab {
    dir: TempDir,
    authority: SigningKey,
    network: Arc<SimNetwork>,
    recorder: Arc<Recorder>,
    nodes: Mutex<BTreeMap<NodeId, LabNode>>,
    next_ip: AtomicU32,
}

impl Lab {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            dir: tempfile::Builder::new().prefix("synapta-lab-").tempdir()?,
            authority: SigningKey::from_bytes(&random_bytes::<32>()),
            network: SimNetwork::new(),
            recorder: Recorder::new(TRACE_CAPACITY),
            nodes: Mutex::new(BTreeMap::new()),
            next_ip: AtomicU32::new(0),
        })
    }

    pub fn recorder(&self) -> &Recorder {
        &self.recorder
    }

    pub fn network(&self) -> &SimNetwork {
        &self.network
    }

    /// Creates a node (key, card) and starts it in its rings.
    pub async fn add_node(&self, setup: NodeSetup) -> Result<(), String> {
        let mut nodes = self.nodes.lock().await;
        if nodes.contains_key(&setup.id) {
            return Err(format!("{} already exists", setup.id));
        }
        let dir = self.dir.path().join(setup.id.as_str());
        std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        self.write_identity(&setup.id, &dir)?;
        let mut node = LabNode {
            ip: self.allocate_ip()?,
            setup,
            dir,
            handle: None,
        };
        self.network.set_rings(&node.setup.id, node.setup.rings.clone());
        let handle = self.start(&node, &nodes).await?;
        node.handle = Some(handle);
        nodes.insert(node.setup.id.clone(), node);
        Ok(())
    }

    pub async fn remove_node(&self, id: &NodeId) -> Result<(), String> {
        let node = self.nodes.lock().await.remove(id).ok_or_else(|| missing(id))?;
        if let Some(handle) = node.handle {
            handle.shutdown().await;
        }
        self.network.remove(id);
        Ok(())
    }

    pub async fn power_off(&self, id: &NodeId, how: PowerOff) -> Result<(), String> {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(id).ok_or_else(|| missing(id))?;
        let Some(handle) = node.handle.take() else {
            return Ok(());
        };
        if how == PowerOff::Crash {
            // Silence its connections before they close, so peers get no goodbye.
            self.network.set_rings(id, Vec::<String>::new());
        }
        handle.shutdown().await;
        self.network.set_rings(id, node.setup.rings.clone());
        Ok(())
    }

    pub async fn power_on(&self, id: &NodeId) -> Result<(), String> {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get(id).ok_or_else(|| missing(id))?;
        if node.handle.is_some() {
            return Ok(());
        }
        let handle = self.start(node, &nodes).await?;
        nodes.get_mut(id).expect("checked above").handle = Some(handle);
        Ok(())
    }

    /// Applies a changed setup. Ring changes take effect live (cables move, nothing restarts);
    /// any other change restarts the node with the new configuration.
    pub async fn update_node(&self, setup: NodeSetup) -> Result<(), String> {
        let id = setup.id.clone();
        let (running, rings_only) = {
            let nodes = self.nodes.lock().await;
            let node = nodes.get(&id).ok_or_else(|| missing(&id))?;
            let same_apart_from_rings = NodeSetup {
                rings: setup.rings.clone(),
                ..node.setup.clone()
            } == setup;
            (node.handle.is_some(), same_apart_from_rings)
        };
        if rings_only {
            return self.set_rings(&id, setup.rings).await;
        }
        if running {
            self.power_off(&id, PowerOff::Graceful).await?;
        }
        {
            let mut nodes = self.nodes.lock().await;
            let node = nodes.get_mut(&id).ok_or_else(|| missing(&id))?;
            self.network.set_rings(&id, setup.rings.clone());
            node.setup = setup;
        }
        if running {
            self.power_on(&id).await?;
        }
        Ok(())
    }

    /// Moves a node's cables: it can reach exactly the nodes in these rings from now on.
    pub async fn set_rings(&self, id: &NodeId, rings: Vec<String>) -> Result<(), String> {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(id).ok_or_else(|| missing(id))?;
        node.setup.rings = rings.clone();
        let handle = node.handle.clone();
        self.network.set_rings(id, rings);
        // Introduce it to its new ring-mates; the rest follows from adverts.
        if let Some(handle) = handle {
            for seed in seeds(id, &nodes, &self.network) {
                handle.connect_to(seed);
            }
        }
        Ok(())
    }

    pub async fn node(&self, id: &NodeId) -> Option<NodeHandle> {
        self.nodes.lock().await.get(id).and_then(|n| n.handle.clone())
    }

    pub async fn nodes(&self) -> Vec<NodeView> {
        let nodes = self.nodes.lock().await;
        nodes
            .values()
            .map(|n| NodeView {
                setup: n.setup.clone(),
                address: n.handle.as_ref().map(NodeHandle::listen_addr),
                running: n.handle.is_some(),
                status: n.handle.as_ref().map(NodeHandle::status),
            })
            .collect()
    }

    /// Stops every node.
    pub async fn shutdown(&self) {
        let mut nodes = self.nodes.lock().await;
        for node in nodes.values_mut() {
            if let Some(handle) = node.handle.take() {
                handle.shutdown().await;
            }
        }
    }

    fn write_identity(&self, id: &NodeId, dir: &std::path::Path) -> Result<(), String> {
        let (private, public) = generate_keypair();
        let claims = CardClaims {
            network: NETWORK.into(),
            node_id: id.clone(),
            public_key: public,
            issued_at: unix_now(),
            expires_at: unix_now() + CARD_LIFETIME_S,
        };
        let card = serde_json::to_string(&issue_card(&self.authority, &claims)).map_err(|e| e.to_string())?;
        std::fs::write(dir.join("node.key"), private).map_err(|e| format!("write key: {e}"))?;
        std::fs::write(dir.join("node.card"), card).map_err(|e| format!("write card: {e}"))
    }

    /// 127.1.0.1, 127.1.0.2, ... skipping .0 and .255 in the last place.
    fn allocate_ip(&self) -> Result<Ipv4Addr, String> {
        loop {
            let n = self.next_ip.fetch_add(1, Ordering::Relaxed);
            if n >= 1 << 16 {
                return Err("the lab ran out of loopback addresses".into());
            }
            let ip = Ipv4Addr::new(127, 1, (n >> 8) as u8, n as u8);
            if !matches!(ip.octets()[3], 0 | 255) {
                return Ok(ip);
            }
        }
    }

    async fn start(&self, node: &LabNode, nodes: &BTreeMap<NodeId, LabNode>) -> Result<NodeHandle, String> {
        let seeds = seeds(&node.setup.id, nodes, &self.network);
        let config = self.config_for(node, &seeds)?;
        let options = StartOptions {
            transport: Some(self.network.transport(
                node.setup.id.clone(),
                IpAddr::V4(node.ip),
                ms(config.timeouts.connect_ms),
            )),
            trace: Some(self.recorder.sink()),
        };
        synapta_core::start_with(config, options).await
    }

    fn config_for(&self, node: &LabNode, seeds: &[SocketAddr]) -> Result<NodeConfig, String> {
        let s = &node.setup;
        let quoted = |items: &[String]| items.iter().map(|i| format!("{i:?}")).collect::<Vec<_>>().join(", ");
        let seeds: Vec<String> = seeds.iter().map(ToString::to_string).collect();
        let node_type = serde_json::to_value(s.node_type).map_err(|e| e.to_string())?;
        let availability = serde_json::to_value(s.availability).map_err(|e| e.to_string())?;
        let mut text = format!(
            r#"
[node]
id = "{id}"
name = {name:?}
type = {node_type}
listen = "{ip}:0"
data_dir = {data:?}

[identity]
private_key_file = {key:?}
card_file = {card:?}

[discovery]
seeds = [{seeds}]

[resources]
cpu_percent = {cpu}
memory_available_mb = {memory}
max_concurrent_jobs = {max}
availability = {availability}

[[trust_authority]]
network = "{NETWORK}"
public_key = "{authority}"
"#,
            id = s.id,
            name = if s.name.is_empty() {
                s.id.to_string()
            } else {
                s.name.clone()
            },
            ip = node.ip,
            data = node.dir.join("data"),
            key = node.dir.join("node.key"),
            card = node.dir.join("node.card"),
            seeds = quoted(&seeds),
            cpu = s.cpu_percent,
            memory = s.memory_mb,
            max = s.max_concurrent_jobs,
            authority = to_hex(self.authority.verifying_key().as_bytes()),
        );
        for cap in &s.capabilities {
            text.push_str(&format!(
                "\n[[capabilities]]\nname = {:?}\nallow = [{}]\n",
                cap.name,
                quoted(&cap.allow)
            ));
        }
        let config: NodeConfig = toml::from_str(&text).map_err(|e| format!("node {}: {e}", s.id))?;
        Ok(config)
    }
}

/// Listen addresses of up to `SEEDS_PER_NODE` running nodes `id` shares a ring with.
fn seeds(id: &NodeId, nodes: &BTreeMap<NodeId, LabNode>, network: &SimNetwork) -> Vec<SocketAddr> {
    nodes
        .values()
        .filter(|n| &n.setup.id != id && network.can_reach(id, &n.setup.id))
        .filter_map(|n| n.handle.as_ref().map(NodeHandle::listen_addr))
        .take(SEEDS_PER_NODE)
        .collect()
}

fn missing(id: &NodeId) -> String {
    format!("no node {id}")
}

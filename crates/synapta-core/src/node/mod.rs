//! A running node: start-up wiring, the public [`NodeHandle`] API, and graceful shutdown.

pub(crate) mod service;
pub(crate) mod state;

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::{ms, NodeConfig};
use crate::discovery::link::{ensure_dialing, DialTarget};
use crate::discovery::{flooding, server};
use crate::error::{ErrorBody, ErrorCode};
use crate::execution::builtin;
use crate::execution::jobs::{JobLimits, JobManager};
use crate::execution::ExecutorRegistry;
use crate::identity::NodeIdentity;
use crate::protocol::message::{CapabilityAdvert, Event, InvokeResult, JobInfo, JobSubmitted, NodeAdvert, Request};
use crate::protocol::{NodeId, WireRange, SOFTWARE_VERSION};
use crate::registry::{PeerState, PeerView};
use crate::routing::dispatch::{self, decode, RouteFailure, Routed};
use crate::routing::RoutingDecision;
use crate::trace::{TraceSink, Tracer};
use crate::transport::tcp::TcpTransport;
use crate::transport::{AddressPolicy, Transport};
use state::{NodeState, NodeStateParts};

/// Starts a node from its configuration. Returns once the listener is bound; peers connect in
/// the background (use [`NodeHandle::wait_until`] to wait for them).
pub async fn start(config: NodeConfig) -> Result<NodeHandle, String> {
    start_with(config, StartOptions::default()).await
}

/// What an embedding program (synapta-lab, tests) can plug into a node. The defaults are what
/// `synapta-node` runs with.
#[derive(Default, Clone)]
pub struct StartOptions {
    /// How the node listens and dials. Default: plain TCP under the loopback policy. The policy
    /// still applies to the config and to every address learned from adverts either way.
    pub transport: Option<Arc<dyn Transport>>,
    /// Receives a report of every frame sent and received. Default: none (nothing is built).
    pub trace: Option<TraceSink>,
}

/// [`start`] with a custom transport and/or trace sink.
pub async fn start_with(config: NodeConfig, options: StartOptions) -> Result<NodeHandle, String> {
    let policy = AddressPolicy::LoopbackOnly;
    config.validate(policy).map_err(|errors| errors.join("; "))?;
    let trust = config.load_trust_store()?;
    let identity = NodeIdentity::load(config.node.id.clone(), &config.identity.private_key_file)?;
    std::fs::create_dir_all(&config.node.data_dir)
        .map_err(|e| format!("cannot create data_dir {}: {e}", config.node.data_dir.display()))?;

    let (executors, capabilities) = offered_capabilities(&config)?;
    let transport = options
        .transport
        .unwrap_or_else(|| Arc::new(TcpTransport::new(policy, ms(config.timeouts.connect_ms))));
    let listener = transport
        .bind(config.node.listen)
        .await
        .map_err(|e| format!("cannot listen on {}: {e}", config.node.listen))?;
    let listen_addr = listener.local_addr();

    let shutdown = CancellationToken::new();
    let tasks = TaskTracker::new();
    let load_changed = Arc::new(Notify::new());
    let t = &config.timeouts;
    let jobs = JobManager::new(
        config.node.id.clone(),
        JobLimits {
            max_concurrent: config.resources.max_concurrent_jobs,
            max_duration: ms(t.max_job_duration_ms),
            retention: ms(t.job_retention_ms),
            max_retained: config.limits.max_retained_jobs,
        },
        load_changed.clone(),
        tasks.clone(),
        shutdown.clone(),
    );
    let seeds = config.discovery.seeds.clone();
    let tracer = Tracer::new(config.node.id.clone(), options.trace);
    let data_dir = config.node.data_dir.clone();
    let state = Arc::new(NodeState::new(NodeStateParts {
        config,
        identity,
        trust,
        policy,
        transport,
        listen_addr,
        incarnation: next_incarnation(&data_dir)?,
        executors,
        capabilities,
        jobs,
        load_changed,
        tracer,
        shutdown,
        tasks,
    }));

    flooding::publish_own_advert(&state);
    state.tasks.spawn(server::run_listener(state.clone(), listener));
    state.tasks.spawn(flooding::run_publisher(state.clone()));
    state.tasks.spawn(run_job_sweeper(state.clone()));
    for seed in seeds {
        ensure_dialing(&state, seed, DialTarget::Seed);
    }
    tracing::info!(node = %state.node_id(), listen = %listen_addr, "node started");
    Ok(NodeHandle { state })
}

/// Pairs each configured capability with its built-in executor; unknown names are an error.
fn offered_capabilities(config: &NodeConfig) -> Result<(ExecutorRegistry, Vec<CapabilityAdvert>), String> {
    let node_info = json!({
        "node_id": config.node.id,
        "name": config.node.name,
        "node_type": config.node.node_type,
        "software_version": SOFTWARE_VERSION,
        "wire": WireRange::SUPPORTED,
        "capabilities": config.capabilities.iter().map(|c| &c.name).collect::<Vec<_>>(),
        "resources": {
            "cpu_percent": config.resources.cpu_percent,
            "memory_available_mb": config.resources.memory_available_mb,
            "max_concurrent_jobs": config.resources.max_concurrent_jobs,
            "mocked": true,
        },
    });
    let catalog = builtin::catalog(node_info);
    let mut executors = ExecutorRegistry::default();
    let mut adverts = Vec::new();
    for cap in &config.capabilities {
        let executor = catalog
            .iter()
            .find(|e| e.spec().name == cap.name)
            .ok_or_else(|| format!("capability {:?} has no executor in this build", cap.name))?;
        let spec = executor.spec();
        adverts.push(CapabilityAdvert {
            name: spec.name.to_string(),
            description: spec.description.to_string(),
            input_schema: spec.input_schema,
            output_schema: spec.output_schema,
            allow: cap.allow.clone(),
        });
        executors.register(executor.clone());
    }
    Ok((executors, adverts))
}

async fn run_job_sweeper(state: Arc<NodeState>) {
    let period = ms(state.config.timeouts.job_retention_ms / 2).max(Duration::from_millis(50));
    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => return,
            _ = tokio::time::sleep(period) => {
                let removed = state.jobs.sweep(Instant::now());
                if removed > 0 {
                    tracing::debug!(removed, "finished jobs cleaned up");
                }
            }
        }
    }
}

/// Incarnation for this start: the clock in ms, but always above the previous start's value
/// (kept in `data_dir/incarnation`). Without this, a clock that moved backwards between restarts
/// would make every peer treat this node's fresh adverts as older than its pre-restart ones.
fn next_incarnation(data_dir: &Path) -> Result<u64, String> {
    let file = data_dir.join("incarnation");
    let previous = std::fs::read_to_string(&file)
        .ok()
        .and_then(|t| t.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let incarnation = now.max(previous + 1);
    std::fs::write(&file, incarnation.to_string()).map_err(|e| format!("write {}: {e}", file.display()))?;
    Ok(incarnation)
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeStatus {
    pub node: NodeAdvert,
    pub listen: SocketAddr,
    pub peers: Vec<PeerView>,
    /// Jobs started on this node per capability since start.
    pub executions: BTreeMap<String, u64>,
    pub retained_jobs: usize,
}

impl NodeStatus {
    pub fn peer(&self, id: &str) -> Option<&PeerView> {
        self.peers.iter().find(|p| p.node_id.as_str() == id)
    }

    pub fn peer_state(&self, id: &str) -> Option<PeerState> {
        self.peer(id).map(|p| p.state)
    }

    pub fn is_ready(&self, id: &str) -> bool {
        self.peer_state(id) == Some(PeerState::Ready)
    }
}

/// The node's public API, used by the local HTTP gateway, the CLI (through the gateway) and tests.
#[derive(Clone)]
pub struct NodeHandle {
    state: Arc<NodeState>,
}

impl NodeHandle {
    pub fn node_id(&self) -> &NodeId {
        self.state.node_id()
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.state.listen_addr
    }

    pub fn config(&self) -> &NodeConfig {
        &self.state.config
    }

    pub fn status(&self) -> NodeStatus {
        let registry = self.state.registry.read().unwrap();
        NodeStatus {
            node: self.state.advert_now(self.state.own_advert.lock().unwrap().seq),
            listen: self.state.listen_addr,
            peers: registry.views(Instant::now()),
            executions: self.state.jobs.executions(),
            retained_jobs: self.state.jobs.retained_count(),
        }
    }

    /// Dry run: what routing would decide right now, without executing anything.
    pub fn explain(&self, capability: &str, target: Option<&NodeId>) -> RoutingDecision {
        dispatch::current_decision(&self.state, capability, target)
    }

    pub async fn invoke(
        &self,
        capability: &str,
        input: Value,
        target: Option<&NodeId>,
    ) -> Result<Routed<InvokeResult>, RouteFailure> {
        dispatch::invoke(&self.state, capability, input, target).await
    }

    pub async fn submit_job(
        &self,
        capability: &str,
        input: Value,
        target: Option<&NodeId>,
    ) -> Result<Routed<JobSubmitted>, RouteFailure> {
        dispatch::submit(&self.state, capability, input, target).await
    }

    pub async fn job_status(&self, node: &NodeId, job_id: &str) -> Result<JobInfo, ErrorBody> {
        if node == self.node_id() {
            return self.state.jobs.status(node, job_id);
        }
        self.remote_job_call(node, Request::JobStatus { job_id: job_id.into() })
            .await
    }

    pub async fn job_result(&self, node: &NodeId, job_id: &str, wait: Duration) -> Result<JobInfo, ErrorBody> {
        if node == self.node_id() {
            return self.state.jobs.wait(node, job_id, wait).await;
        }
        let request = Request::JobResult {
            job_id: job_id.into(),
            wait_ms: wait.as_millis() as u64,
        };
        self.remote_job_call(node, request).await
    }

    pub async fn cancel_job(&self, node: &NodeId, job_id: &str) -> Result<JobInfo, ErrorBody> {
        if node == self.node_id() {
            return self.state.jobs.cancel(node, job_id).await;
        }
        self.remote_job_call(node, Request::JobCancel { job_id: job_id.into() })
            .await
    }

    /// Progress / finished events received for a job (local or remote), oldest first.
    pub fn job_events(&self, node: &NodeId, job_id: &str) -> Vec<Event> {
        self.state.job_events.events(node, job_id)
    }

    pub fn recent_decisions(&self) -> Vec<RoutingDecision> {
        self.state.routing_log.lock().unwrap().iter().cloned().collect()
    }

    async fn remote_job_call(&self, node: &NodeId, request: Request) -> Result<JobInfo, ErrorBody> {
        let link = self
            .state
            .registry
            .read()
            .unwrap()
            .ready_link(node)
            .ok_or_else(|| ErrorBody::new(ErrorCode::TargetUnavailable, format!("{node} is not reachable")))?;
        let timeout = ms(self.state.config.timeouts.request_ms);
        decode(link.call(request, timeout).await?)
    }

    /// Waits (without polling) until `condition` holds for the node status, or `timeout` passes.
    pub async fn wait_until(&self, timeout: Duration, condition: impl Fn(&NodeStatus) -> bool) -> bool {
        let mut changes = self.state.subscribe_registry();
        let wait = async {
            loop {
                if condition(&self.status()) {
                    return;
                }
                if changes.changed().await.is_err() {
                    return;
                }
            }
        };
        tokio::time::timeout(timeout, wait).await.is_ok() && condition(&self.status())
    }

    /// Resolves when shutdown has been requested (by [`Self::request_shutdown`] or a signal handler).
    pub fn shutdown_requested(&self) -> impl Future<Output = ()> + Send + 'static {
        self.state.shutdown.clone().cancelled_owned()
    }

    pub fn request_shutdown(&self) {
        self.state.shutdown.cancel();
    }

    /// Stops all background work: listener, links, dialers, jobs. Returns `true` if every task
    /// finished within `shutdown_grace_ms`.
    pub async fn shutdown(&self) -> bool {
        tracing::info!(node = %self.node_id(), "shutting down");
        self.state.shutdown.cancel();
        self.state.tasks.close();
        let grace = ms(self.state.config.timeouts.shutdown_grace_ms);
        let clean = tokio::time::timeout(grace, self.state.tasks.wait()).await.is_ok();
        if !clean {
            tracing::warn!(node = %self.node_id(), "some tasks did not stop within the grace period");
        }
        clean
    }
}

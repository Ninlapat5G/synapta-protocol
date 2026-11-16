//! Shared state of one running node. Each concern keeps its own lock, and no lock is held
//! across an `.await` (snapshots are copied out first).

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::NodeConfig;
use crate::discovery::link::GaveUp;
use crate::error::{ErrorBody, ErrorCode};
use crate::execution::job_events::JobEventLog;
use crate::execution::jobs::JobManager;
use crate::execution::{Executor, ExecutorRegistry};
use crate::identity::{NodeIdentity, TrustStore};
use crate::protocol::message::{CapabilityAdvert, MemberCard, NodeAdvert, ResourceState, SignedAdvert};
use crate::protocol::{NodeId, WireRange, SOFTWARE_VERSION};
use crate::registry::Registry;
use crate::routing::ledger::DispatchLedger;
use crate::routing::relay::SeenNonces;
use crate::routing::RoutingDecision;
use crate::trace::Tracer;
use crate::transport::{AddressPolicy, Transport};

pub(crate) struct NodeState {
    pub config: NodeConfig,
    pub identity: NodeIdentity,
    /// This node's membership card, if it has one.
    pub card: Option<MemberCard>,
    pub trust: TrustStore,
    pub policy: AddressPolicy,
    pub transport: Arc<dyn Transport>,
    /// The address actually bound (differs from config when it asked for port 0).
    pub listen_addr: SocketAddr,
    pub incarnation: u64,
    pub registry: RwLock<Registry>,
    registry_version: watch::Sender<u64>,
    pub own_advert: Mutex<OwnAdvert>,
    pub executors: ExecutorRegistry,
    /// What this node advertises, including access lists. Also the authority for authorising
    /// incoming calls, so advertised and enforced rules cannot drift apart.
    pub capabilities: Vec<CapabilityAdvert>,
    pub jobs: JobManager,
    pub load_changed: Arc<Notify>,
    pub job_events: JobEventLog,
    pub routing_log: Mutex<VecDeque<RoutingDecision>>,
    pub dispatch_ledger: DispatchLedger,
    /// Addresses with a running dialer task (at most one per address), each with the signal
    /// that cuts its current wait short.
    pub dialers: Mutex<HashMap<SocketAddr, Arc<Notify>>>,
    /// Learned addresses whose dialer gave up, and when it may try again (`link::GaveUp`).
    pub gave_up: Mutex<HashMap<SocketAddr, GaveUp>>,
    /// Relayed envelopes already delivered here (replay guard).
    pub relay_nonces: Mutex<SeenNonces>,
    pub tracer: Tracer,
    pub shutdown: CancellationToken,
    pub tasks: TaskTracker,
}

pub(crate) struct OwnAdvert {
    pub seq: u64,
    pub signed: Option<SignedAdvert>,
}

pub(crate) struct NodeStateParts {
    pub config: NodeConfig,
    pub identity: NodeIdentity,
    pub card: Option<MemberCard>,
    pub trust: TrustStore,
    pub policy: AddressPolicy,
    pub transport: Arc<dyn Transport>,
    pub listen_addr: SocketAddr,
    pub incarnation: u64,
    pub executors: ExecutorRegistry,
    pub capabilities: Vec<CapabilityAdvert>,
    pub jobs: JobManager,
    pub load_changed: Arc<Notify>,
    pub tracer: Tracer,
    pub shutdown: CancellationToken,
    pub tasks: TaskTracker,
}

impl NodeState {
    pub fn new(parts: NodeStateParts) -> Self {
        let limits = &parts.config.limits;
        Self {
            job_events: JobEventLog::new(limits.max_tracked_remote_jobs, limits.max_events_per_job),
            routing_log: Mutex::new(VecDeque::new()),
            dispatch_ledger: DispatchLedger::default(),
            registry: RwLock::new(Registry::default()),
            registry_version: watch::channel(0).0,
            own_advert: Mutex::new(OwnAdvert { seq: 0, signed: None }),
            dialers: Mutex::new(HashMap::new()),
            gave_up: Mutex::new(HashMap::new()),
            relay_nonces: Mutex::new(SeenNonces::default()),
            config: parts.config,
            identity: parts.identity,
            card: parts.card,
            trust: parts.trust,
            policy: parts.policy,
            transport: parts.transport,
            listen_addr: parts.listen_addr,
            incarnation: parts.incarnation,
            executors: parts.executors,
            capabilities: parts.capabilities,
            jobs: parts.jobs,
            load_changed: parts.load_changed,
            tracer: parts.tracer,
            shutdown: parts.shutdown,
            tasks: parts.tasks,
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.identity.node_id
    }

    /// Wakes everything waiting for registry changes (`NodeHandle::wait_until`).
    pub fn registry_changed(&self) {
        self.registry_version.send_modify(|v| *v += 1);
    }

    pub fn subscribe_registry(&self) -> watch::Receiver<u64> {
        self.registry_version.subscribe()
    }

    /// This node's advert as of now (live job count and links). `seq` is filled by the
    /// publisher. Takes the registry lock: never call it while holding that lock.
    pub fn advert_now(&self, seq: u64) -> NodeAdvert {
        let r = &self.config.resources;
        let neighbors = self
            .registry
            .read()
            .unwrap()
            .ready_links()
            .iter()
            .map(|link| link.peer().clone())
            .collect();
        NodeAdvert {
            node_id: self.node_id().clone(),
            name: self.config.node.name.clone(),
            node_type: self.config.node.node_type,
            software_version: SOFTWARE_VERSION.into(),
            wire: WireRange::SUPPORTED,
            address: self.listen_addr.to_string(),
            incarnation: self.incarnation,
            seq,
            capabilities: self.capabilities.clone(),
            neighbors,
            resources: ResourceState {
                cpu_percent: r.cpu_percent,
                memory_available_mb: r.memory_available_mb,
                active_jobs: self.jobs.active_count(),
                max_concurrent_jobs: r.max_concurrent_jobs,
                availability: r.availability,
                mocked: true,
            },
        }
    }

    /// Authoritative access check for a call arriving at this node.
    pub fn authorize(&self, caller: &NodeId, capability: &str) -> Result<Arc<dyn Executor>, ErrorBody> {
        let Some(cap) = self.capabilities.iter().find(|c| c.name == capability) else {
            return Err(ErrorBody::new(
                ErrorCode::CapabilityNotSupported,
                format!("{} does not provide {capability}", self.node_id()),
            ));
        };
        if caller != self.node_id() && !cap.allows(caller) {
            tracing::warn!(caller = %caller, capability, "unauthorized capability call refused");
            return Err(ErrorBody::new(
                ErrorCode::Unauthorized,
                format!("{caller} is not allowed to call {capability} on {}", self.node_id()),
            ));
        }
        self.executors
            .get(capability)
            .ok_or_else(|| ErrorBody::new(ErrorCode::Internal, format!("no executor for {capability}")))
    }

    pub fn record_decision(&self, decision: &RoutingDecision) {
        let json = serde_json::to_string(decision).unwrap_or_default();
        match &decision.error {
            None => tracing::info!(target: "synapta::routing", decision = %json, "routing decision"),
            Some(e) => tracing::warn!(target: "synapta::routing", decision = %json, code = ?e.code, "routing failed"),
        }
        let mut log = self.routing_log.lock().unwrap();
        if log.len() >= self.config.limits.routing_log_size.max(1) {
            log.pop_front();
        }
        log.push_back(decision.clone());
    }
}

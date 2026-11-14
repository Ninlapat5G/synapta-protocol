//! Execution layer: the [`Executor`] interface, the registry of executors a node offers,
//! and the job lifecycle (`jobs`).
//!
//! Extension point: a future `script.execute` (Syn-Script runtime) or a real agent runtime is
//! one more `Executor` registered here — routing, jobs and the wire protocol stay unchanged.
//! No executor in v0.0.1 starts a host process; peers can only trigger the built-in mocks.

pub mod builtin;
pub mod job_events;
pub mod jobs;

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::ErrorBody;
use crate::protocol::message::Event;
use crate::protocol::NodeId;
use crate::BoxFuture;

/// Static description of a capability, advertised to peers and turned into UTCP tool schemas.
pub struct CapabilitySpec {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
    pub output_schema: Value,
}

pub trait Executor: Send + Sync + 'static {
    fn spec(&self) -> CapabilitySpec;

    /// Runs one job. Must return promptly once `ctx.cancel` fires; the job manager also drops the
    /// future on cancel or when the job exceeds `max_job_duration`.
    fn execute(&self, ctx: JobContext, input: Value) -> BoxFuture<'static, Result<Value, ErrorBody>>;
}

/// Where job events go: the submitter's connection (remote) or the local event log.
pub type EventSink = Arc<dyn Fn(Event) + Send + Sync>;

pub struct JobContext {
    pub job_id: String,
    pub caller: NodeId,
    pub node_id: NodeId,
    pub cancel: CancellationToken,
    progress: jobs::ProgressReporter,
}

impl JobContext {
    pub fn progress(&self, percent: u8, message: impl Into<String>) {
        self.progress.report(percent, message.into());
    }
}

/// The executors this node offers, keyed by capability name.
#[derive(Clone, Default)]
pub struct ExecutorRegistry {
    executors: BTreeMap<String, Arc<dyn Executor>>,
}

impl ExecutorRegistry {
    pub fn register(&mut self, executor: Arc<dyn Executor>) {
        self.executors.insert(executor.spec().name.to_string(), executor);
    }

    pub fn get(&self, capability: &str) -> Option<Arc<dyn Executor>> {
        self.executors.get(capability).cloned()
    }
}

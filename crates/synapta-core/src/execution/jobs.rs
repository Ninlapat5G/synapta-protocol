//! Job lifecycle on the executing node: admission (capacity), run, progress, cancel,
//! timeout, result retention and cleanup.
//!
//! Every job ends: by finishing, by cancellation, by exceeding `max_job_duration`, or by node
//! shutdown (job tokens are children of the node's shutdown token). Finished records are
//! dropped after `retention`, and at most `max_retained` records are kept at any time.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::{EventSink, Executor, JobContext};
use crate::error::{ErrorBody, ErrorCode};
use crate::identity::{random_bytes, to_hex};
use crate::protocol::message::{Event, JobInfo, JobProgress, JobState};
use crate::protocol::NodeId;

pub struct JobLimits {
    pub max_concurrent: u32,
    pub max_duration: Duration,
    pub retention: Duration,
    pub max_retained: usize,
}

#[derive(Clone)]
pub struct JobManager {
    inner: Arc<Inner>,
}

struct Inner {
    node_id: NodeId,
    limits: JobLimits,
    jobs: Mutex<HashMap<String, JobRecord>>,
    /// Jobs admitted per capability since start — evidence of where work actually ran.
    executions: Mutex<BTreeMap<String, u64>>,
    /// Fired whenever the number of running jobs changes (drives advert updates).
    load_changed: Arc<Notify>,
    tracker: TaskTracker,
    shutdown: CancellationToken,
}

struct JobRecord {
    capability: String,
    owner: NodeId,
    state: JobState,
    percent: u8,
    message: String,
    output: Option<Value>,
    error: Option<ErrorBody>,
    started: Instant,
    finished: Option<Instant>,
    cancel: CancellationToken,
    done: watch::Sender<bool>,
    sink: Option<EventSink>,
}

impl JobManager {
    pub fn new(
        node_id: NodeId,
        limits: JobLimits,
        load_changed: Arc<Notify>,
        tracker: TaskTracker,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                node_id,
                limits,
                jobs: Mutex::new(HashMap::new()),
                executions: Mutex::new(BTreeMap::new()),
                load_changed,
                tracker,
                shutdown,
            }),
        }
    }

    pub fn active_count(&self) -> u32 {
        let jobs = self.inner.jobs.lock().unwrap();
        jobs.values().filter(|j| j.state == JobState::Running).count() as u32
    }

    /// Admits and starts a job, or rejects it with `Busy` *before* anything runs.
    pub fn submit(
        &self,
        owner: NodeId,
        executor: Arc<dyn Executor>,
        input: Value,
        sink: Option<EventSink>,
    ) -> Result<String, ErrorBody> {
        let inner = &self.inner;
        if inner.shutdown.is_cancelled() {
            return Err(ErrorBody::new(ErrorCode::ShuttingDown, "node is shutting down"));
        }
        let capability = executor.spec().name.to_string();
        let job_id = to_hex(&random_bytes::<8>());
        let cancel = inner.shutdown.child_token();
        {
            let mut jobs = inner.jobs.lock().unwrap();
            let running = jobs.values().filter(|j| j.state == JobState::Running).count() as u32;
            if running >= inner.limits.max_concurrent {
                return Err(ErrorBody::new(
                    ErrorCode::Busy,
                    format!(
                        "{} is at capacity ({running}/{} jobs)",
                        inner.node_id, inner.limits.max_concurrent
                    ),
                ));
            }
            evict_oldest_finished(&mut jobs, inner.limits.max_retained);
            jobs.insert(
                job_id.clone(),
                JobRecord {
                    capability,
                    owner: owner.clone(),
                    state: JobState::Running,
                    percent: 0,
                    message: String::new(),
                    output: None,
                    error: None,
                    started: Instant::now(),
                    finished: None,
                    cancel: cancel.clone(),
                    done: watch::channel(false).0,
                    sink,
                },
            );
        }
        *inner
            .executions
            .lock()
            .unwrap()
            .entry(capability_name(&executor))
            .or_default() += 1;
        inner.load_changed.notify_one();
        tracing::info!(job_id, owner = %owner, capability = %capability_name(&executor), "job started");

        let ctx = JobContext {
            job_id: job_id.clone(),
            caller: owner,
            node_id: inner.node_id.clone(),
            cancel: cancel.clone(),
            progress: ProgressReporter {
                inner: Arc::downgrade(inner),
                job_id: job_id.clone(),
            },
        };
        let task_inner = inner.clone();
        let task_id = job_id.clone();
        inner.tracker.spawn(async move {
            let max_duration = task_inner.limits.max_duration;
            let outcome = tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(ErrorBody::new(ErrorCode::Cancelled, "job cancelled")),
                _ = tokio::time::sleep(max_duration) => Err(ErrorBody::new(
                    ErrorCode::Timeout, format!("job exceeded max duration of {} ms", max_duration.as_millis()))),
                result = executor.execute(ctx, input) => result,
            };
            finish(&task_inner, &task_id, outcome);
        });
        Ok(job_id)
    }

    /// Synchronous execution: submit, wait up to `timeout`, cancel if it is still running.
    pub async fn run(
        &self,
        owner: NodeId,
        executor: Arc<dyn Executor>,
        input: Value,
        timeout: Duration,
    ) -> Result<(String, Value), ErrorBody> {
        let job_id = self.submit(owner.clone(), executor, input, None)?;
        let info = self.wait(&owner, &job_id, timeout).await?;
        match info.state {
            JobState::Succeeded => Ok((job_id, info.output.unwrap_or(Value::Null))),
            JobState::Running => {
                self.cancel(&owner, &job_id).await?;
                Err(ErrorBody::new(
                    ErrorCode::Timeout,
                    format!("no result within {} ms", timeout.as_millis()),
                ))
            }
            _ => Err(info
                .error
                .unwrap_or_else(|| ErrorBody::new(ErrorCode::ExecutionFailed, "job failed"))),
        }
    }

    /// Only the node that submitted a job can see it. Others get `JobNotFound`, which also
    /// avoids revealing which job ids exist.
    pub fn status(&self, owner: &NodeId, job_id: &str) -> Result<JobInfo, ErrorBody> {
        let jobs = self.inner.jobs.lock().unwrap();
        let record = owned(&jobs, owner, job_id)?;
        Ok(info(&self.inner.node_id, job_id, record))
    }

    /// Waits up to `wait` for the job to finish and returns its info either way.
    pub async fn wait(&self, owner: &NodeId, job_id: &str, wait: Duration) -> Result<JobInfo, ErrorBody> {
        let mut done = {
            let jobs = self.inner.jobs.lock().unwrap();
            owned(&jobs, owner, job_id)?.done.subscribe()
        };
        let _ = tokio::time::timeout(wait, done.wait_for(|finished| *finished)).await;
        self.status(owner, job_id)
    }

    /// Requests cancellation and waits briefly for the job to stop.
    pub async fn cancel(&self, owner: &NodeId, job_id: &str) -> Result<JobInfo, ErrorBody> {
        {
            let jobs = self.inner.jobs.lock().unwrap();
            owned(&jobs, owner, job_id)?.cancel.cancel();
        }
        self.wait(owner, job_id, Duration::from_secs(2)).await
    }

    /// Drops finished jobs older than the retention period. Returns how many were removed.
    pub fn sweep(&self, now: Instant) -> usize {
        let mut jobs = self.inner.jobs.lock().unwrap();
        let before = jobs.len();
        let retention = self.inner.limits.retention;
        jobs.retain(|_, j| j.finished.is_none_or(|t| now.duration_since(t) < retention));
        before - jobs.len()
    }

    pub fn retained_count(&self) -> usize {
        self.inner.jobs.lock().unwrap().len()
    }

    pub fn executions(&self) -> BTreeMap<String, u64> {
        self.inner.executions.lock().unwrap().clone()
    }
}

fn capability_name(executor: &Arc<dyn Executor>) -> String {
    executor.spec().name.to_string()
}

fn owned<'a>(jobs: &'a HashMap<String, JobRecord>, owner: &NodeId, job_id: &str) -> Result<&'a JobRecord, ErrorBody> {
    jobs.get(job_id)
        .filter(|j| &j.owner == owner)
        .ok_or_else(|| ErrorBody::new(ErrorCode::JobNotFound, format!("no job {job_id}")))
}

fn info(node_id: &NodeId, job_id: &str, record: &JobRecord) -> JobInfo {
    let end = record.finished.unwrap_or_else(Instant::now);
    JobInfo {
        job_id: job_id.to_string(),
        executed_by: node_id.clone(),
        capability: record.capability.clone(),
        state: record.state,
        percent: record.percent,
        message: record.message.clone(),
        output: record.output.clone(),
        error: record.error.clone(),
        elapsed_ms: end.duration_since(record.started).as_millis() as u64,
    }
}

fn finish(inner: &Inner, job_id: &str, outcome: Result<Value, ErrorBody>) {
    let (sink, state) = {
        let mut jobs = inner.jobs.lock().unwrap();
        let Some(record) = jobs.get_mut(job_id) else { return };
        match outcome {
            Ok(output) => {
                record.state = JobState::Succeeded;
                record.percent = 100;
                record.output = Some(output);
            }
            Err(error) => {
                record.state = match error.code {
                    ErrorCode::Cancelled => JobState::Cancelled,
                    ErrorCode::Timeout => JobState::TimedOut,
                    _ => JobState::Failed,
                };
                record.error = Some(error);
            }
        }
        record.finished = Some(Instant::now());
        record.done.send_replace(true);
        (record.sink.take(), record.state)
    };
    inner.load_changed.notify_one();
    tracing::info!(job_id, ?state, "job finished");
    if let Some(sink) = sink {
        sink(Event::JobFinished {
            job_id: job_id.to_string(),
            state,
        });
    }
}

fn evict_oldest_finished(jobs: &mut HashMap<String, JobRecord>, max_retained: usize) {
    while jobs.len() >= max_retained {
        let oldest = jobs
            .iter()
            .filter_map(|(id, j)| j.finished.map(|t| (t, id.clone())))
            .min()
            .map(|(_, id)| id);
        match oldest {
            Some(id) => jobs.remove(&id),
            // Everything retained is still running; capacity limits bound this case.
            None => break,
        };
    }
}

/// Handed to executors through `JobContext::progress`. Holds a weak reference so a job that
/// outlives its manager cannot keep it alive.
pub struct ProgressReporter {
    inner: Weak<Inner>,
    job_id: String,
}

impl ProgressReporter {
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Self {
            inner: Weak::new(),
            job_id: String::new(),
        }
    }

    pub(super) fn report(&self, percent: u8, message: String) {
        let Some(inner) = self.inner.upgrade() else { return };
        let percent = percent.min(100);
        let sink = {
            let mut jobs = inner.jobs.lock().unwrap();
            let Some(record) = jobs.get_mut(&self.job_id) else {
                return;
            };
            if record.state != JobState::Running {
                return;
            }
            record.percent = percent;
            record.message = message.clone();
            record.sink.clone()
        };
        if let Some(sink) = sink {
            sink(Event::JobProgress(JobProgress {
                job_id: self.job_id.clone(),
                percent,
                message,
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::builtin::catalog;
    use serde_json::json;

    fn manager(max_concurrent: u32, retention_ms: u64) -> JobManager {
        JobManager::new(
            NodeId::new("node-b").unwrap(),
            JobLimits {
                max_concurrent,
                max_duration: Duration::from_secs(30),
                retention: Duration::from_millis(retention_ms),
                max_retained: 100,
            },
            Arc::new(Notify::new()),
            TaskTracker::new(),
            CancellationToken::new(),
        )
    }

    fn executor(name: &str) -> Arc<dyn Executor> {
        catalog(json!({})).into_iter().find(|e| e.spec().name == name).unwrap()
    }

    fn owner() -> NodeId {
        NodeId::new("node-a").unwrap()
    }

    #[tokio::test]
    async fn capacity_is_enforced_before_execution() {
        let jobs = manager(1, 60_000);
        let slow = json!({"suite": "unit", "steps": 100, "step_ms": 100});
        let first = jobs
            .submit(owner(), executor("test.execute"), slow.clone(), None)
            .unwrap();
        let err = jobs.submit(owner(), executor("test.execute"), slow, None).unwrap_err();
        assert_eq!(err.code, ErrorCode::Busy);
        assert_eq!(jobs.active_count(), 1);
        jobs.cancel(&owner(), &first).await.unwrap();
        assert_eq!(jobs.active_count(), 0);
    }

    #[tokio::test]
    async fn other_nodes_cannot_see_or_cancel_a_job() {
        let jobs = manager(2, 60_000);
        let id = jobs
            .submit(
                owner(),
                executor("test.execute"),
                json!({"suite": "unit", "steps": 50}),
                None,
            )
            .unwrap();
        let stranger = NodeId::new("node-c").unwrap();
        assert_eq!(jobs.status(&stranger, &id).unwrap_err().code, ErrorCode::JobNotFound);
        assert_eq!(
            jobs.cancel(&stranger, &id).await.unwrap_err().code,
            ErrorCode::JobNotFound
        );
        assert_eq!(jobs.status(&owner(), &id).unwrap().state, JobState::Running);
        jobs.cancel(&owner(), &id).await.unwrap();
    }

    #[tokio::test]
    async fn sync_run_times_out_and_cancels() {
        let jobs = manager(2, 60_000);
        let err = jobs
            .run(
                owner(),
                executor("test.execute"),
                json!({"suite": "unit", "steps": 100, "step_ms": 50}),
                Duration::from_millis(100),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Timeout);
        assert_eq!(jobs.active_count(), 0, "timed-out sync job must not keep running");
    }

    #[tokio::test]
    async fn finished_jobs_are_swept_after_retention() {
        let jobs = manager(2, 10);
        let (id, out) = jobs
            .run(
                owner(),
                executor("math.add"),
                json!({"a": 1, "b": 2}),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(out, json!({"sum": 3}));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(jobs.sweep(Instant::now()), 1);
        assert_eq!(jobs.status(&owner(), &id).unwrap_err().code, ErrorCode::JobNotFound);
    }

    #[tokio::test]
    async fn progress_and_finish_events_reach_the_sink() {
        let jobs = manager(2, 60_000);
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = events.clone();
        let sink: EventSink = Arc::new(move |e| seen.lock().unwrap().push(e));
        let id = jobs
            .submit(
                owner(),
                executor("test.execute"),
                json!({"suite": "smoke", "steps": 3, "step_ms": 1}),
                Some(sink),
            )
            .unwrap();
        let info = jobs.wait(&owner(), &id, Duration::from_secs(2)).await.unwrap();
        assert_eq!(info.state, JobState::Succeeded);
        let events = events.lock().unwrap();
        assert_eq!(events.iter().filter(|e| matches!(e, Event::JobProgress(_))).count(), 3);
        assert!(matches!(
            events.last(),
            Some(Event::JobFinished {
                state: JobState::Succeeded,
                ..
            })
        ));
    }
}

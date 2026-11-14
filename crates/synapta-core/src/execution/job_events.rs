//! Caller-side log of job events (progress / finished): pushed by executing nodes for jobs this
//! node submitted elsewhere, and recorded directly for jobs this node runs for itself.
//!
//! Bounded on both axes: at most `max_jobs` jobs (oldest dropped) and `max_events` per job
//! (oldest dropped). Events only arrive on this node's own authenticated links, from the node
//! that runs the job, so the key (executing node, job id) cannot be spoofed by a third node.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use crate::protocol::message::Event;
use crate::protocol::NodeId;

type JobKey = (NodeId, String);

pub struct JobEventLog {
    max_jobs: usize,
    max_events: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    events: HashMap<JobKey, VecDeque<Event>>,
    /// Insertion order, for evicting the oldest job.
    order: VecDeque<JobKey>,
}

impl JobEventLog {
    pub fn new(max_jobs: usize, max_events: usize) -> Self {
        Self {
            max_jobs: max_jobs.max(1),
            max_events: max_events.max(1),
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn record(&self, node: &NodeId, event: Event) {
        let job_id = match &event {
            Event::JobProgress(p) => p.job_id.clone(),
            Event::JobFinished { job_id, .. } => job_id.clone(),
        };
        let key = (node.clone(), job_id);
        let mut inner = self.inner.lock().unwrap();
        if !inner.events.contains_key(&key) {
            while inner.order.len() >= self.max_jobs {
                if let Some(oldest) = inner.order.pop_front() {
                    inner.events.remove(&oldest);
                }
            }
            inner.order.push_back(key.clone());
        }
        let events = inner.events.entry(key).or_default();
        if events.len() >= self.max_events {
            events.pop_front();
        }
        events.push_back(event);
    }

    pub fn events(&self, node: &NodeId, job_id: &str) -> Vec<Event> {
        let inner = self.inner.lock().unwrap();
        inner
            .events
            .get(&(node.clone(), job_id.to_string()))
            .map(|e| e.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::message::JobProgress;

    fn progress(job: &str, percent: u8) -> Event {
        Event::JobProgress(JobProgress {
            job_id: job.into(),
            percent,
            message: String::new(),
        })
    }

    #[test]
    fn bounded_per_job_and_in_total() {
        let log = JobEventLog::new(2, 3);
        let b = NodeId::new("node-b").unwrap();
        for p in 0..5 {
            log.record(&b, progress("j1", p));
        }
        assert_eq!(log.events(&b, "j1").len(), 3);
        log.record(&b, progress("j2", 1));
        log.record(&b, progress("j3", 1));
        assert!(log.events(&b, "j1").is_empty(), "oldest job evicted");
        assert_eq!(log.events(&b, "j3").len(), 1);
    }
}

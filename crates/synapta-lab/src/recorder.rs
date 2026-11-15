//! Keeps the most recent trace events from every node, numbered in arrival order, so the web
//! page can follow them live (`after(last seen)`) or step back through them (replay).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use synapta_core::trace::{TraceEvent, TraceSink};

#[derive(Debug, Clone, Serialize)]
pub struct Recorded {
    /// 1, 2, 3, ... in the order events arrived. Never reused, also across `clear`.
    pub seq: u64,
    #[serde(flatten)]
    pub event: TraceEvent,
}

/// A page of events, for `GET /trace?after=<seq>`.
#[derive(Debug, Serialize)]
pub struct Page {
    pub events: Vec<Recorded>,
    /// Ask for `after = last` next time.
    pub last: u64,
    /// Events after the requested one were already dropped to make room: the page starts later.
    pub skipped: bool,
}

pub struct Recorder {
    capacity: usize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    last: u64,
    events: VecDeque<Recorded>,
}

impl Recorder {
    /// Keeps at most `capacity` events; older ones are dropped first.
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity: capacity.max(1),
            state: Mutex::new(State::default()),
        })
    }

    /// The sink to hand every node. Only appends under a short lock.
    pub fn sink(self: &Arc<Self>) -> TraceSink {
        let recorder = self.clone();
        Arc::new(move |event| recorder.push(event))
    }

    fn push(&self, event: TraceEvent) {
        let mut state = self.state.lock().unwrap();
        state.last += 1;
        let seq = state.last;
        if state.events.len() >= self.capacity {
            state.events.pop_front();
        }
        state.events.push_back(Recorded { seq, event });
    }

    /// Up to `limit` events with `seq > after`, oldest first.
    pub fn after(&self, after: u64, limit: usize) -> Page {
        let state = self.state.lock().unwrap();
        let first_kept = state.events.front().map_or(state.last + 1, |e| e.seq);
        let start = state.events.partition_point(|e| e.seq <= after);
        let events: Vec<Recorded> = state.events.iter().skip(start).take(limit).cloned().collect();
        Page {
            last: events.last().map_or(after.min(state.last), |e| e.seq),
            skipped: after + 1 < first_kept && !events.is_empty(),
            events,
        }
    }

    pub fn clear(&self) {
        self.state.lock().unwrap().events.clear();
    }
}

#[cfg(test)]
mod tests {
    use synapta_core::trace::TraceKind;
    use synapta_core::NodeId;

    use super::*;

    fn event(n: u64) -> TraceEvent {
        TraceEvent {
            at_us: n,
            node: NodeId::new("node-a").unwrap(),
            peer: NodeId::new("node-b").unwrap(),
            kind: TraceKind::LinkUp,
        }
    }

    #[test]
    fn pages_follow_on_and_report_what_was_dropped() {
        let recorder = Recorder::new(3);
        let sink = recorder.sink();
        for n in 1..=5 {
            sink(event(n));
        }
        // 1 and 2 were dropped to make room.
        let page = recorder.after(0, 10);
        assert_eq!(page.events.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3, 4, 5]);
        assert!(page.skipped);
        assert_eq!(page.last, 5);

        let page = recorder.after(3, 1);
        assert_eq!((page.events[0].seq, page.last, page.skipped), (4, 4, false));
        let page = recorder.after(5, 10);
        assert!(page.events.is_empty());
        assert_eq!(page.last, 5);
    }
}

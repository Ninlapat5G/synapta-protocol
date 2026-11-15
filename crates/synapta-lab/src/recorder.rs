//! Keeps the most recent trace events from every node, numbered in arrival order, so the web
//! page can follow them live (`after(last seen)`) or step back through them (replay).
//!
//! Heartbeats (ping and its reply) are left out unless asked for: every link pings every second,
//! so in a big ring they would push everything else out of the buffer within seconds.
//!
//! Nodes only drop events into a queue; one thread files them. Nodes used to take the buffer's
//! lock themselves on every frame: with 100 nodes starting (about a million frames a second)
//! that one lock stalled every runtime thread and the nodes missed their heartbeats.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use synapta_core::trace::{Channel, TraceEvent, TraceKind, TraceSink};
use synapta_core::NodeId;

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
    queue: SyncSender<TraceEvent>,
    filed: Arc<Filed>,
}

/// What the filing thread maintains and readers read.
struct Filed {
    capacity: usize,
    heartbeats: AtomicBool,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    last: u64,
    events: VecDeque<Recorded>,
    /// Pings on their way, to recognise their replies (a reply frame names no method).
    pings: HashSet<PingKey>,
}

/// One request on one connection: (requester, responder, requester's channel, frame id).
type PingKey = (NodeId, NodeId, bool, u64);

/// A ping wait list this long means replies were lost (nodes crashed): start over.
const MAX_PENDING_PINGS: usize = 200_000;
/// Events waiting to be filed. When full, new events are dropped rather than slowing nodes.
const QUEUE: usize = 1 << 20;
/// Events filed per lock of the buffer.
const BATCH: usize = 4096;
/// Pause when the queue is empty: how late, at most, an event shows up on the page.
const NAP: std::time::Duration = std::time::Duration::from_millis(5);

impl Recorder {
    /// Keeps at most `capacity` events; older ones are dropped first.
    pub fn new(capacity: usize) -> Arc<Self> {
        let (queue, incoming) = mpsc::sync_channel(QUEUE);
        let filed = Arc::new(Filed {
            capacity: capacity.max(1),
            heartbeats: AtomicBool::new(false),
            state: Mutex::new(State::default()),
        });
        let filer = filed.clone();
        // Ends when the recorder (the only sender) is dropped.
        std::thread::Builder::new()
            .name("lab-recorder".into())
            .spawn(move || filer.run(incoming))
            .expect("spawn the recorder thread");
        Arc::new(Self { queue, filed })
    }

    /// The sink to hand every node: queues the event and returns.
    pub fn sink(self: &Arc<Self>) -> TraceSink {
        let recorder = self.clone();
        Arc::new(move |event| {
            if let Err(TrySendError::Full(_)) = recorder.queue.try_send(event) {
                tracing::debug!("trace queue full, event dropped");
            }
        })
    }

    /// Whether pings and their replies are recorded too (off at first).
    pub fn set_heartbeats(&self, on: bool) {
        self.filed.heartbeats.store(on, Ordering::Relaxed);
    }

    pub fn heartbeats(&self) -> bool {
        self.filed.heartbeats.load(Ordering::Relaxed)
    }

    /// Up to `limit` events with `seq > after`, oldest first.
    pub fn after(&self, after: u64, limit: usize) -> Page {
        let state = self.filed.state.lock().unwrap();
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
        self.filed.state.lock().unwrap().events.clear();
    }
}

impl Filed {
    /// Drains the queue, then naps. Never blocks in `recv`: a waiting receiver makes every
    /// sender take the channel's wake-up lock, which brought back the stall this thread exists
    /// to avoid.
    fn run(&self, incoming: Receiver<TraceEvent>) {
        loop {
            let mut filed = 0;
            {
                let mut state = self.state.lock().unwrap();
                loop {
                    match incoming.try_recv() {
                        Ok(event) => self.file(&mut state, event),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => return,
                    }
                    filed += 1;
                    if filed == BATCH {
                        break;
                    }
                }
            }
            if filed < BATCH {
                std::thread::sleep(NAP);
            }
        }
    }

    fn file(&self, state: &mut State, event: TraceEvent) {
        if !self.heartbeats.load(Ordering::Relaxed) && is_heartbeat(&mut state.pings, &event) {
            return;
        }
        state.last += 1;
        let seq = state.last;
        if state.events.len() >= self.capacity {
            state.events.pop_front();
        }
        state.events.push_back(Recorded { seq, event });
    }
}

/// True for a ping or a reply to one, seen from either end. Remembers pings until the requester
/// has read the reply.
fn is_heartbeat(pings: &mut HashSet<PingKey>, event: &TraceEvent) -> bool {
    let (frame, channel, sent) = match &event.kind {
        TraceKind::Sent { frame, channel } => (frame, *channel, true),
        TraceKind::Received { frame, channel } => (frame, *channel, false),
        _ => return false,
    };
    let Some(id) = frame.id else { return false };
    let outbound = channel == Channel::Outbound;
    let (node, peer) = (event.node.clone(), event.peer.clone());
    match (frame.frame, sent) {
        ("request", true) if frame.method == Some("ping") => {
            if pings.len() >= MAX_PENDING_PINGS {
                pings.clear();
            }
            pings.insert((node, peer, outbound, id));
            true
        }
        ("request", false) => frame.method == Some("ping"),
        // The responder answers on the connection it received on: flip to the requester's view.
        ("response" | "error", true) => pings.contains(&(peer, node, !outbound, id)),
        ("response" | "error", false) => pings.remove(&(node, peer, outbound, id)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use synapta_core::trace::FrameSummary;

    use super::*;

    fn event(n: u64) -> TraceEvent {
        TraceEvent {
            at_us: n,
            node: NodeId::new("node-a").unwrap(),
            peer: NodeId::new("node-b").unwrap(),
            kind: TraceKind::LinkUp,
        }
    }

    fn frame(
        node: &str,
        peer: &str,
        sent: bool,
        channel: Channel,
        kind: &'static str,
        method: Option<&'static str>,
    ) -> TraceEvent {
        let frame = FrameSummary {
            frame: kind,
            id: Some(7),
            method,
            detail: None,
            ok: None,
        };
        TraceEvent {
            at_us: 0,
            node: NodeId::new(node).unwrap(),
            peer: NodeId::new(peer).unwrap(),
            kind: if sent {
                TraceKind::Sent { channel, frame }
            } else {
                TraceKind::Received { channel, frame }
            },
        }
    }

    #[test]
    fn heartbeats_are_left_out_from_both_ends_unless_asked_for() {
        use Channel::{Inbound, Outbound};
        let ping_and_reply = || {
            vec![
                frame("node-a", "node-b", true, Outbound, "request", Some("ping")),
                frame("node-b", "node-a", false, Inbound, "request", Some("ping")),
                frame("node-b", "node-a", true, Inbound, "response", None),
                frame("node-a", "node-b", false, Outbound, "response", None),
                // A reply to something else on the same connection is kept.
                frame("node-b", "node-a", true, Inbound, "response", None),
            ]
        };
        let recorder = Recorder::new(100);
        let sink = recorder.sink();
        ping_and_reply().into_iter().for_each(|e| sink(e));
        // Filed in order: once the last (kept) event is in, the ping before it was judged.
        while recorder.after(0, 100).last < 1 {
            std::thread::yield_now();
        }
        recorder.set_heartbeats(true);
        ping_and_reply().into_iter().for_each(|e| sink(e));
        while recorder.after(0, 100).last < 6 {
            std::thread::yield_now();
        }
        assert_eq!(recorder.after(0, 100).events.len(), 1 + 5);
    }

    #[test]
    fn pages_follow_on_and_report_what_was_dropped() {
        let recorder = Recorder::new(3);
        let sink = recorder.sink();
        for n in 1..=5 {
            sink(event(n));
        }
        // Filing happens on the recorder's thread.
        while recorder.after(0, 10).last < 5 {
            std::thread::yield_now();
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

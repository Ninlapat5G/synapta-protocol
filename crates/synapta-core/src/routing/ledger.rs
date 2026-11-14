//! What this node has sent to each peer that the peer's advert does not show yet.
//!
//! Adverts trail reality by up to `advert_min_interval_ms` plus transit. Without this ledger a
//! burst of calls all sees the same "idle" advert and lands on one node, and a slot freed by our
//! own finished call stays invisible until the next advert. The ledger corrects the advertised
//! job count with what this node knows for certain about its *own* calls:
//!
//! - **sent, no advert since** → `+1`: the advert we hold was issued before the call, so it
//!   cannot include it.
//! - **finished, and the advert we hold arrived between sending and finishing** → `-1`: that
//!   advert counted the job, which has since ended.
//!
//! "Arrived" stands in for "was issued". An advert issued before the job started but received
//! after we sent it gets credited with a job it doesn't count, so the estimate can only come out
//! *low*, never high. The worst case is a `busy` answer, after which routing moves on. Nothing
//! runs twice, and no node is skipped for being full when it isn't. Jobs from other callers are
//! not visible here. The executing node's own capacity check covers them.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use crate::protocol::NodeId;
use crate::registry::PeerSnapshot;

/// `(incarnation, seq)` of an advert, as held in the registry.
pub(crate) type AdvertVersion = (u64, u64);

/// Per-node input to the routing policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LedgerEntry {
    /// Correction to the advertised `active_jobs` (see module docs).
    pub unseen_jobs: i32,
    /// Ordinal of this node's most recent dispatch to that node; `None` = never sent there.
    pub last_dispatch: Option<u64>,
}

#[derive(Default)]
pub(crate) struct DispatchLedger {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    next_ticket: u64,
    dispatch_count: u64,
    last_dispatch: HashMap<NodeId, u64>,
    calls: HashMap<u64, Call>,
}

struct Call {
    node: NodeId,
    sent_at: AdvertVersion,
    state: CallState,
}

enum CallState {
    /// Request sent, no answer yet.
    InFlight,
    /// An async job was accepted. It runs for an unknown time, so it counts only until the
    /// next advert (which then reports it).
    Accepted,
    /// The job ended; `AdvertVersion` is what we held at that moment.
    Finished(AdvertVersion),
}

impl Call {
    /// This call's correction given the advert version now held, or `None` once no advert can
    /// be wrong about it any more (the call can be forgotten).
    fn correction(&self, now: AdvertVersion) -> Option<i32> {
        match self.state {
            CallState::InFlight => Some(if now == self.sent_at { 1 } else { 0 }),
            CallState::Accepted => (now == self.sent_at).then_some(1),
            CallState::Finished(at) if now == at => Some(if at == self.sent_at { 0 } else { -1 }),
            CallState::Finished(_) => None,
        }
    }
}

impl DispatchLedger {
    /// Corrections for every known node, dropping calls that newer adverts already reflect.
    pub fn view(&self, peers: &[PeerSnapshot]) -> BTreeMap<NodeId, LedgerEntry> {
        let versions: HashMap<&NodeId, AdvertVersion> = peers
            .iter()
            .filter_map(|p| p.advert.as_ref().map(|a| (&p.node_id, a.version())))
            .collect();
        let mut inner = self.inner.lock().unwrap();
        let mut view: BTreeMap<NodeId, LedgerEntry> = inner
            .last_dispatch
            .iter()
            .map(|(node, &ordinal)| {
                let entry = LedgerEntry {
                    unseen_jobs: 0,
                    last_dispatch: Some(ordinal),
                };
                (node.clone(), entry)
            })
            .collect();
        inner.calls.retain(|_, call| {
            // A node that left the registry has no advert to correct.
            let Some(&now) = versions.get(&call.node) else {
                return matches!(call.state, CallState::InFlight);
            };
            match call.correction(now) {
                Some(delta) => {
                    view.entry(call.node.clone()).or_default().unseen_jobs += delta;
                    true
                }
                None => false,
            }
        });
        view
    }

    /// Records a dispatch to `node`. `sent_at` is the advert version held when sending; `None`
    /// for the local node, whose own job count is always live.
    pub fn send(&self, node: &NodeId, sent_at: Option<AdvertVersion>) -> Ticket<'_> {
        let mut inner = self.inner.lock().unwrap();
        inner.dispatch_count += 1;
        let ordinal = inner.dispatch_count;
        inner.last_dispatch.insert(node.clone(), ordinal);
        let id = sent_at.map(|sent_at| {
            inner.next_ticket += 1;
            let id = inner.next_ticket;
            let call = Call {
                node: node.clone(),
                sent_at,
                state: CallState::InFlight,
            };
            inner.calls.insert(id, call);
            id
        });
        Ticket { ledger: self, id }
    }

    fn settle(&self, id: u64, state: Option<CallState>) {
        let mut inner = self.inner.lock().unwrap();
        match state {
            Some(state) => {
                if let Some(call) = inner.calls.get_mut(&id) {
                    call.state = state;
                }
            }
            None => {
                inner.calls.remove(&id);
            }
        }
    }
}

/// One outstanding call. Dropping it without settling forgets the call (no correction).
pub(crate) struct Ticket<'a> {
    ledger: &'a DispatchLedger,
    id: Option<u64>,
}

impl Ticket<'_> {
    /// The job ran to completion. `now` = advert version held at this moment.
    pub fn finished(mut self, now: Option<AdvertVersion>) {
        if let (Some(id), Some(now)) = (self.id.take(), now) {
            self.ledger.settle(id, Some(CallState::Finished(now)));
        }
    }

    /// The node accepted an async job that keeps running.
    pub fn accepted(mut self) {
        if let Some(id) = self.id.take() {
            self.ledger.settle(id, Some(CallState::Accepted));
        }
    }
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        // Refused, failed or abandoned: we cannot tell what the node counted, so claim nothing.
        if let Some(id) = self.id.take() {
            self.ledger.settle(id, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::message::{Availability, NodeAdvert, ResourceState};
    use crate::protocol::{NodeType, PROTOCOL_VERSION};
    use crate::registry::PeerState;

    fn b() -> NodeId {
        NodeId::new("node-b").unwrap()
    }

    fn peers_at(seq: u64) -> Vec<PeerSnapshot> {
        let advert = NodeAdvert {
            node_id: b(),
            name: "b".into(),
            node_type: NodeType::Server,
            protocol_version: PROTOCOL_VERSION.into(),
            address: "127.0.0.1:1".into(),
            incarnation: 1,
            seq,
            capabilities: vec![],
            resources: ResourceState {
                cpu_percent: 0,
                memory_available_mb: 0,
                active_jobs: 0,
                max_concurrent_jobs: 1,
                availability: Availability::Available,
                mocked: true,
            },
        };
        vec![PeerSnapshot {
            node_id: b(),
            state: PeerState::Ready,
            advert: Some(advert),
        }]
    }

    fn unseen(ledger: &DispatchLedger, seq: u64) -> i32 {
        ledger.view(&peers_at(seq)).get(&b()).map_or(0, |e| e.unseen_jobs)
    }

    #[test]
    fn in_flight_call_counts_until_an_advert_reflects_it() {
        let ledger = DispatchLedger::default();
        let _ticket = ledger.send(&b(), Some((1, 5)));
        assert_eq!(unseen(&ledger, 5), 1, "advert 5 predates the call");
        assert_eq!(unseen(&ledger, 6), 0, "advert 6 arrived after sending and includes it");
    }

    #[test]
    fn finished_call_frees_the_slot_the_advert_still_shows() {
        let ledger = DispatchLedger::default();
        let ticket = ledger.send(&b(), Some((1, 5)));
        ticket.finished(Some((1, 6)));
        assert_eq!(unseen(&ledger, 6), -1, "advert 6 counted the job that has ended");
        assert_eq!(unseen(&ledger, 7), 0, "advert 7 came after the end");
        assert_eq!(unseen(&ledger, 6), 0, "forgotten once reflected");
    }

    #[test]
    fn call_finished_before_any_new_advert_has_no_effect() {
        let ledger = DispatchLedger::default();
        ledger.send(&b(), Some((1, 5))).finished(Some((1, 5)));
        assert_eq!(unseen(&ledger, 5), 0);
    }

    #[test]
    fn accepted_job_counts_until_next_advert() {
        let ledger = DispatchLedger::default();
        ledger.send(&b(), Some((1, 5))).accepted();
        assert_eq!(unseen(&ledger, 5), 1);
        assert_eq!(unseen(&ledger, 6), 0);
        assert_eq!(unseen(&ledger, 5), 0, "forgotten after the advert caught up");
    }

    #[test]
    fn refused_call_is_forgotten() {
        let ledger = DispatchLedger::default();
        drop(ledger.send(&b(), Some((1, 5))));
        assert_eq!(unseen(&ledger, 5), 0);
    }

    #[test]
    fn dispatch_order_is_recorded_for_local_and_remote() {
        let ledger = DispatchLedger::default();
        let a = NodeId::new("node-a").unwrap();
        drop(ledger.send(&b(), Some((1, 5))));
        drop(ledger.send(&a, None));
        let view = ledger.view(&peers_at(5));
        assert_eq!(view[&b()].last_dispatch, Some(1));
        assert_eq!(view[&a].last_dispatch, Some(2));
    }
}

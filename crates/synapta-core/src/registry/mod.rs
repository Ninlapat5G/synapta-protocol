//! Per-node peer registry: what this node knows about every other node.
//!
//! Two independent facts are tracked per peer:
//! - the latest verified advert (capabilities + resources), learned directly or via relays;
//! - the state of *this node's own* outbound link to that peer.
//!
//! Routing only sends work over a READY link, so liveness always comes from a direct,
//! authenticated, heart-beaten connection — never from relayed (possibly old) adverts.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::discovery::link::LinkHandle;
use crate::protocol::message::{NodeAdvert, SignedAdvert};
use crate::protocol::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PeerState {
    /// Known from an advert, no link attempt yet.
    Discovered,
    /// Connecting / authenticating.
    Handshaking,
    /// Authenticated link up and heartbeats answered.
    Ready,
    /// Link failed, timed out or was closed.
    Unavailable,
}

pub struct PeerEntry {
    pub state: PeerState,
    pub state_since: Instant,
    pub last_error: Option<String>,
    pub advert: Option<NodeAdvert>,
    pub signed: Option<SignedAdvert>,
    pub advert_received_at: Option<Instant>,
    pub link: Option<LinkHandle>,
}

impl PeerEntry {
    fn new(now: Instant) -> Self {
        Self {
            state: PeerState::Discovered,
            state_since: now,
            last_error: None,
            advert: None,
            signed: None,
            advert_received_at: None,
            link: None,
        }
    }

    fn set_state(&mut self, state: PeerState, now: Instant) {
        if self.state != state {
            self.state = state;
            self.state_since = now;
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AdvertOutcome {
    /// Newer than what we had. `address_changed` is true for a new peer or a moved one.
    Accepted { address_changed: bool },
    /// Same or older version — ignore and do not re-flood (this is what stops flooding loops).
    Stale,
}

/// A copy of one peer's routing-relevant data, taken under the lock and used after it is released.
#[derive(Debug, Clone)]
pub struct PeerSnapshot {
    pub node_id: NodeId,
    pub state: PeerState,
    pub advert: Option<NodeAdvert>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PeerView {
    pub node_id: NodeId,
    pub state: PeerState,
    pub state_age_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advert: Option<NodeAdvert>,
}

#[derive(Default)]
pub struct Registry {
    // BTreeMap: iteration order is by node id, never by arrival order.
    peers: BTreeMap<NodeId, PeerEntry>,
}

impl Registry {
    pub fn apply_advert(&mut self, advert: NodeAdvert, signed: SignedAdvert, now: Instant) -> AdvertOutcome {
        let entry = self
            .peers
            .entry(advert.node_id.clone())
            .or_insert_with(|| PeerEntry::new(now));
        let address_changed = match &entry.advert {
            Some(current) if current.version() >= advert.version() => return AdvertOutcome::Stale,
            Some(current) => current.address != advert.address,
            None => true,
        };
        entry.advert = Some(advert);
        entry.signed = Some(signed);
        entry.advert_received_at = Some(now);
        AdvertOutcome::Accepted { address_changed }
    }

    pub fn set_state(&mut self, node_id: &NodeId, state: PeerState, error: Option<String>, now: Instant) {
        let entry = self.peers.entry(node_id.clone()).or_insert_with(|| PeerEntry::new(now));
        entry.set_state(state, now);
        if error.is_some() {
            entry.last_error = error;
        }
    }

    /// Registers a freshly authenticated link. Refuses if a live link to that peer exists
    /// (the caller then closes the duplicate connection).
    pub fn attach_link(&mut self, link: LinkHandle, now: Instant) -> bool {
        let entry = self
            .peers
            .entry(link.peer().clone())
            .or_insert_with(|| PeerEntry::new(now));
        if entry.link.as_ref().is_some_and(|l| !l.is_closed()) {
            return false;
        }
        entry.link = Some(link);
        entry.last_error = None;
        entry.set_state(PeerState::Ready, now);
        true
    }

    /// Removes `link_id` if it is still the peer's current link. An old link that dies late
    /// must not tear down a newer one.
    pub fn detach_link(&mut self, node_id: &NodeId, link_id: u64, error: String, now: Instant) {
        if let Some(entry) = self.peers.get_mut(node_id) {
            if entry.link.as_ref().is_some_and(|l| l.id() == link_id) {
                entry.link = None;
                entry.last_error = Some(error);
                entry.set_state(PeerState::Unavailable, now);
            }
        }
    }

    pub fn ready_link(&self, node_id: &NodeId) -> Option<LinkHandle> {
        let entry = self.peers.get(node_id)?;
        let link = entry.link.as_ref()?;
        (entry.state == PeerState::Ready && !link.is_closed()).then(|| link.clone())
    }

    pub fn ready_links(&self) -> Vec<LinkHandle> {
        self.peers.keys().filter_map(|id| self.ready_link(id)).collect()
    }

    pub fn is_ready(&self, node_id: &NodeId) -> bool {
        self.ready_link(node_id).is_some()
    }

    pub fn signed_adverts(&self) -> Vec<SignedAdvert> {
        self.peers.values().filter_map(|e| e.signed.clone()).collect()
    }

    /// Forgets peers whose advert was not refreshed within `max_age` and that have no link.
    /// Returns the forgotten ids.
    pub fn purge_expired(&mut self, max_age: Duration, now: Instant) -> Vec<NodeId> {
        let expired: Vec<NodeId> = self
            .peers
            .iter()
            .filter(|(_, e)| e.link.is_none() && e.advert_received_at.is_none_or(|t| now.duration_since(t) > max_age))
            .map(|(id, _)| id.clone())
            .collect();
        for id in &expired {
            self.peers.remove(id);
        }
        expired
    }

    pub fn snapshots(&self) -> Vec<PeerSnapshot> {
        self.peers
            .iter()
            .map(|(id, e)| PeerSnapshot {
                node_id: id.clone(),
                // A link that died but has not been detached yet is not READY for routing.
                state: if e.state == PeerState::Ready && e.link.as_ref().is_none_or(|l| l.is_closed()) {
                    PeerState::Unavailable
                } else {
                    e.state
                },
                advert: e.advert.clone(),
            })
            .collect()
    }

    pub fn views(&self, now: Instant) -> Vec<PeerView> {
        self.peers
            .iter()
            .map(|(id, e)| PeerView {
                node_id: id.clone(),
                state: e.state,
                state_age_ms: now.duration_since(e.state_since).as_millis() as u64,
                last_error: e.last_error.clone(),
                advert: e.advert.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::message::{Availability, ResourceState};
    use crate::protocol::{NodeType, PROTOCOL_VERSION};

    fn advert(incarnation: u64, seq: u64, address: &str) -> (NodeAdvert, SignedAdvert) {
        let advert = NodeAdvert {
            node_id: NodeId::new("node-b").unwrap(),
            name: "B".into(),
            node_type: NodeType::Server,
            protocol_version: PROTOCOL_VERSION.into(),
            address: address.into(),
            incarnation,
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
        let signed = SignedAdvert {
            payload: String::new(),
            signature: String::new(),
        };
        (advert, signed)
    }

    #[test]
    fn older_or_equal_adverts_never_overwrite_newer() {
        let mut registry = Registry::default();
        let now = Instant::now();
        let (a, s) = advert(1, 5, "127.0.0.1:1");
        assert_eq!(
            registry.apply_advert(a, s, now),
            AdvertOutcome::Accepted { address_changed: true }
        );
        let (a, s) = advert(1, 4, "127.0.0.1:1");
        assert_eq!(registry.apply_advert(a, s, now), AdvertOutcome::Stale);
        let (a, s) = advert(1, 5, "127.0.0.1:1");
        assert_eq!(registry.apply_advert(a, s, now), AdvertOutcome::Stale);
        let (a, s) = advert(1, 6, "127.0.0.1:1");
        assert_eq!(
            registry.apply_advert(a, s, now),
            AdvertOutcome::Accepted { address_changed: false }
        );
    }

    #[test]
    fn restarted_node_with_reset_seq_wins_through_incarnation() {
        let mut registry = Registry::default();
        let now = Instant::now();
        let (a, s) = advert(1_000, 900, "127.0.0.1:1");
        registry.apply_advert(a, s, now);
        // After a restart seq starts again at 1 but incarnation (start time) is larger.
        let (a, s) = advert(2_000, 1, "127.0.0.1:2");
        assert_eq!(
            registry.apply_advert(a, s, now),
            AdvertOutcome::Accepted { address_changed: true }
        );
    }

    #[test]
    fn expired_unlinked_peers_are_purged() {
        let mut registry = Registry::default();
        let start = Instant::now();
        let (a, s) = advert(1, 1, "127.0.0.1:1");
        registry.apply_advert(a, s, start);
        assert!(registry
            .purge_expired(Duration::from_secs(10), start + Duration::from_secs(5))
            .is_empty());
        assert_eq!(
            registry
                .purge_expired(Duration::from_secs(10), start + Duration::from_secs(11))
                .len(),
            1
        );
        assert!(registry.snapshots().is_empty());
    }
}

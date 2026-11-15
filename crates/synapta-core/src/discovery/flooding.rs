//! OSPF-inspired state exchange: each node originates a signed advert about itself and every
//! node floods adverts it has not seen before to all its peers.
//!
//! - Ordering: `(incarnation, seq)`. Only strictly newer adverts are stored and re-flooded,
//!   which is what terminates flooding (an advert already known is dropped silently).
//! - Not re-flooded to the origin's neighbours, nor to the neighbours of the peer that handed it
//!   over: each of those sent it to their own neighbours already. In a ring where everyone links
//!   to everyone that keeps one advert at about N messages instead of ~N², also for adverts that
//!   arrive over a bridge from another ring. Anything a race misses comes with the next refresh.
//! - New link: `adverts.sync` swaps versions, then only the missing adverts (see `answer_sync`).
//! - Freshness: a node re-originates its advert every `advert_refresh_ms` and whenever its load
//!   or links change, coalesced to at most one per `advert_min_interval_ms` per 10 links (see
//!   `coalescing_pause`). Adverts not refreshed within
//!   `advert_max_age_ms` are purged for peers without a link.
//! - Discovery: an accepted advert for a node we have no link to starts a dialer to its address
//!   (or, if the address changed, hurries the one already waiting), unless it shares no wire
//!   version with us.
//!
//! Not implemented: areas. Every node links to every node it shares a network with; nodes
//! further away are reached through relays (`routing::paths`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use super::advert;
use super::link::{ensure_dialing, DialTarget};
use crate::config::ms;
use crate::error::ErrorBody;
use crate::node::state::NodeState;
use crate::protocol::message::{AdvertVersion, Notification, SignedAdvert, SyncAdvertsResult};
use crate::protocol::{NodeId, WireRange};
use crate::registry::AdvertOutcome;

/// Verifies and stores an advert, floods it onward if new, and dials newly learned nodes.
/// `from` is the peer that handed it to us (not necessarily its origin). Returns whether it was new.
pub(crate) fn accept_advert(
    state: &Arc<NodeState>,
    signed: SignedAdvert,
    from: Option<&NodeId>,
) -> Result<bool, ErrorBody> {
    // Our own advert relayed back is dropped before verification: a node does not hold its own
    // key in its trust store, so verifying would raise a false "untrusted" alarm. Dropping on an
    // unverified claim is safe — the worst a forger achieves is that we ignore a fake of ourselves.
    let claim = advert::claimed(&signed);
    if claim.as_ref().is_some_and(|c| &c.node_id == state.node_id()) {
        return Ok(false);
    }
    // Not newer than what we hold: it would be dropped after verifying, so skip the (costly)
    // signature check. A forged "newer" one still goes through `verify` below and fails there.
    if let Some(c) = &claim {
        let held = state.registry.read().unwrap().advert_version(&c.node_id);
        if held.is_some_and(|held| held >= (c.incarnation, c.seq)) {
            return Ok(false);
        }
    }
    let advert = advert::verify(&signed, &state.trust, state.policy)?;
    let origin = advert.node_id.clone();
    let origin_neighbors = advert.neighbors.clone();
    let address = advert.address.parse().ok();
    let can_talk = WireRange::SUPPORTED.highest_common(advert.wire).is_some();
    let outcome = state
        .registry
        .write()
        .unwrap()
        .apply_advert(advert, signed.clone(), Instant::now());
    let AdvertOutcome::Accepted { address_changed } = outcome else {
        return Ok(false);
    };
    state.registry_changed();
    tracing::debug!(origin = %origin, from = ?from, address_changed, "advert accepted");

    let from_neighbors = from
        .map(|f| state.registry.read().unwrap().neighbors_of(f))
        .unwrap_or_default();
    let skip: Vec<&NodeId> = from
        .into_iter()
        .chain(std::iter::once(&origin))
        .chain(&origin_neighbors)
        .chain(&from_neighbors)
        .collect();
    flood(state, &signed, &skip);

    // A node on another wire version stays in the registry (so status can show it) but is not
    // dialled: every advert it refreshes would otherwise start a handshake that must fail.
    let linked = state.registry.read().unwrap().is_ready(&origin);
    if let (false, true, Some(address)) = (linked, can_talk, address) {
        ensure_dialing(state, address, DialTarget::Learned(origin), address_changed);
    }
    Ok(true)
}

/// Originates a new advert for this node (seq + 1) and floods it to every READY peer.
pub(crate) fn publish_own_advert(state: &Arc<NodeState>) {
    let signed = {
        let mut own = state.own_advert.lock().unwrap();
        own.seq += 1;
        let mut signed = advert::sign(&state.advert_now(own.seq), &state.identity);
        signed.card = state.card.clone();
        own.signed = Some(signed.clone());
        signed
    };
    flood(state, &signed, &[]);
}

fn flood(state: &NodeState, signed: &SignedAdvert, skip: &[&NodeId]) {
    let links = state.registry.read().unwrap().ready_links();
    for link in links.iter().filter(|l| !skip.contains(&l.peer())) {
        link.notify(Notification::Advert(signed.clone()));
    }
}

/// Versions of our own advert and every advert we hold: what `adverts.sync` sends.
pub(crate) fn known_versions(state: &NodeState) -> Vec<AdvertVersion> {
    let own = state.own_advert.lock().unwrap();
    let own = own.signed.as_ref().map(|_| AdvertVersion {
        node_id: state.node_id().clone(),
        incarnation: state.incarnation,
        seq: own.seq,
    });
    let learned = state.registry.read().unwrap().advert_versions();
    own.into_iter().chain(learned).collect()
}

/// Our signed advert for `node` (our own or one we hold).
pub(crate) fn signed_advert(state: &NodeState, node: &NodeId) -> Option<SignedAdvert> {
    if node == state.node_id() {
        return state.own_advert.lock().unwrap().signed.clone();
    }
    state.registry.read().unwrap().signed_advert(node)
}

/// Answers a peer's `adverts.sync`: the adverts it lacks or holds older, and the nodes whose
/// advert it holds newer than we do (or we do not know at all).
pub(crate) fn answer_sync(state: &NodeState, theirs: &[AdvertVersion]) -> SyncAdvertsResult {
    let version = |v: &AdvertVersion| (v.incarnation, v.seq);
    let theirs: HashMap<&NodeId, (u64, u64)> = theirs.iter().map(|v| (&v.node_id, version(v))).collect();
    let ours = known_versions(state);
    let adverts = ours
        .iter()
        .filter(|v| theirs.get(&v.node_id).is_none_or(|t| *t < version(v)))
        .filter_map(|v| signed_advert(state, &v.node_id))
        .collect();
    let ours: HashMap<&NodeId, (u64, u64)> = ours.iter().map(|v| (&v.node_id, version(v))).collect();
    let want = theirs
        .iter()
        .filter(|(id, t)| **id != state.node_id() && ours.get(*id).is_none_or(|o| o < *t))
        .map(|(id, _)| (*id).clone())
        .collect();
    SyncAdvertsResult { adverts, want }
}

/// How long to wait after an advert before the next one for a change. Each advert goes to
/// every peer, so a node with many links waits longer: one advert per `advert_min_interval_ms`
/// per 10 links. A fixed pause let a ring of 150 nodes produce ~200,000 adverts a second while
/// its links came up; the nodes spent their time verifying them and missed their heartbeats.
fn coalescing_pause(state: &NodeState) -> std::time::Duration {
    let links = state.registry.read().unwrap().ready_links().len() as u64;
    ms(state.config.timeouts.advert_min_interval_ms * (links / 10).max(1))
}

/// Background task: re-originate on load change (coalesced) and on refresh; purge stale peers.
pub(crate) async fn run_publisher(state: Arc<NodeState>) {
    let t = &state.config.timeouts;
    let mut refresh = tokio::time::interval(ms(t.advert_refresh_ms));
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => return,
            _ = state.load_changed.notified() => {
                publish_own_advert(&state);
                // Notify keeps at most one pending permit, so a burst of changes during this
                // pause results in a single advert afterwards.
                tokio::select! {
                    _ = state.shutdown.cancelled() => return,
                    _ = tokio::time::sleep(coalescing_pause(&state)) => {}
                }
            }
            _ = refresh.tick() => {
                publish_own_advert(&state);
                let purged = state.registry.write().unwrap().purge_expired(ms(t.advert_max_age_ms), Instant::now());
                if !purged.is_empty() {
                    tracing::info!(?purged, "forgot peers with expired adverts");
                    state.registry_changed();
                }
            }
        }
    }
}

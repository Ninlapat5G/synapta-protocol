//! OSPF-inspired state exchange: each node originates a signed advert about itself and every
//! node floods adverts it has not seen before to all its peers.
//!
//! - Ordering: `(incarnation, seq)`. Only strictly newer adverts are stored and re-flooded,
//!   which is what terminates flooding (an advert already known is dropped silently).
//! - Freshness: a node re-originates its advert every `advert_refresh_ms` and whenever its load
//!   changes (coalesced to at most one per `advert_min_interval_ms`). Adverts not refreshed within
//!   `advert_max_age_ms` are purged for peers without a link.
//! - Discovery: an accepted advert for a node we have no link to starts a dialer to its address
//!   (or hurries the one already waiting to retry), unless it shares no wire version with us.
//!
//! Not implemented (v0.0.1 scope): DB-description summaries (sync sends full adverts), areas,
//! multi-hop routing. Every node links to every node directly.

use std::sync::Arc;
use std::time::Instant;

use super::advert;
use super::link::{ensure_dialing, DialTarget};
use crate::config::ms;
use crate::error::ErrorBody;
use crate::node::state::NodeState;
use crate::protocol::message::{Notification, SignedAdvert};
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
    if advert::claimed_origin(&signed).as_ref() == Some(state.node_id()) {
        return Ok(false);
    }
    let advert = advert::verify(&signed, &state.trust, state.policy)?;
    let origin = advert.node_id.clone();
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

    let skip: Vec<&NodeId> = from.into_iter().chain(std::iter::once(&origin)).collect();
    flood(state, &signed, &skip);

    // A node on another wire version stays in the registry (so status can show it) but is not
    // dialled: every advert it refreshes would otherwise start a handshake that must fail.
    let linked = state.registry.read().unwrap().is_ready(&origin);
    if let (false, true, Some(address)) = (linked, can_talk, address) {
        ensure_dialing(state, address, DialTarget::Learned(origin));
    }
    Ok(true)
}

/// Originates a new advert for this node (seq + 1) and floods it to every READY peer.
pub(crate) fn publish_own_advert(state: &Arc<NodeState>) {
    let signed = {
        let mut own = state.own_advert.lock().unwrap();
        own.seq += 1;
        let signed = advert::sign(&state.advert_now(own.seq), &state.identity);
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

/// Our own advert plus every verified advert we hold — sent when a link comes up.
pub(crate) fn all_known_adverts(state: &NodeState) -> Vec<SignedAdvert> {
    let own = state.own_advert.lock().unwrap().signed.clone();
    let learned = state.registry.read().unwrap().signed_adverts();
    own.into_iter().chain(learned).collect()
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
                    _ = tokio::time::sleep(ms(t.advert_min_interval_ms)) => {}
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

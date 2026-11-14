//! Routing policy: decide *which node* runs a capability. Pure and deterministic — the same
//! cluster view always yields the same decision, independent of the order peers answered in.
//! `docs/routing.md` documents this file; change both together.
//!
//! Executing the decision (local call, remote call, safe fallback) lives in `dispatch`.

pub mod dispatch;

use serde::Serialize;

use crate::error::{ErrorBody, ErrorCode};
use crate::protocol::message::{Availability, NodeAdvert};
use crate::protocol::NodeId;
use crate::registry::{PeerSnapshot, PeerState};

pub const POLICY: &str = "least-loaded-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutingMode {
    Automatic,
    Explicit,
}

/// Everything the policy may look at. Built from the registry under its lock, then decided on
/// without holding any lock.
pub struct ClusterView {
    /// This node's own advert with its current load.
    pub local: NodeAdvert,
    pub peers: Vec<PeerSnapshot>,
}

/// Why a node cannot take the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum Rejection {
    CapabilityNotAdvertised,
    /// The node's access list for this capability does not include the caller.
    NotAllowed,
    /// No READY link from this node to the candidate.
    LinkNotReady {
        state: PeerState,
    },
    NotAvailable {
        availability: Availability,
    },
    AtCapacity {
        active_jobs: u32,
        max_concurrent_jobs: u32,
    },
}

impl Rejection {
    /// Error reported when this rejection hits an explicit target (no fallback allowed).
    fn explicit_error(&self, target: &NodeId, capability: &str) -> ErrorBody {
        let (code, message) = match self {
            Rejection::CapabilityNotAdvertised => (
                ErrorCode::CapabilityNotSupported,
                format!("{target} does not provide {capability}"),
            ),
            Rejection::NotAllowed => (
                ErrorCode::Unauthorized,
                format!("{target} does not allow this node to call {capability}"),
            ),
            Rejection::LinkNotReady { state } => (ErrorCode::TargetUnavailable, format!("{target} is {state:?}")),
            Rejection::NotAvailable { availability } => {
                (ErrorCode::TargetUnavailable, format!("{target} is {availability:?}"))
            }
            Rejection::AtCapacity {
                active_jobs,
                max_concurrent_jobs,
            } => (
                ErrorCode::Busy,
                format!("{target} is at capacity ({active_jobs}/{max_concurrent_jobs} jobs)"),
            ),
        };
        ErrorBody::new(code, message)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CandidateReport {
    pub node_id: NodeId,
    pub local: bool,
    pub eligible: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejection: Option<Rejection>,
    /// active_jobs * 1000 / max_concurrent_jobs. Integer so ordering is exact.
    pub load_permille: u32,
    pub cpu_percent: u8,
    /// 1 = preferred. Only for eligible candidates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank: Option<usize>,
}

/// One dispatch attempt, appended by `dispatch` after the policy decided.
#[derive(Debug, Clone, Serialize)]
pub struct Attempt {
    pub node_id: NodeId,
    pub outcome: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoutingDecision {
    pub capability: String,
    pub mode: RoutingMode,
    pub caller: NodeId,
    pub policy: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected: Option<NodeId>,
    pub candidates: Vec<CandidateReport>,
    /// Known nodes that do not advertise the capability (not listed as candidates).
    pub nodes_without_capability: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<Attempt>,
}

impl RoutingDecision {
    /// Eligible candidates, best first.
    pub fn ranked(&self) -> Vec<NodeId> {
        let mut eligible: Vec<&CandidateReport> = self.candidates.iter().filter(|c| c.rank.is_some()).collect();
        eligible.sort_by_key(|c| c.rank);
        eligible.into_iter().map(|c| c.node_id.clone()).collect()
    }
}

pub fn decide(view: &ClusterView, caller: &NodeId, capability: &str, target: Option<&NodeId>) -> RoutingDecision {
    let mut decision = RoutingDecision {
        capability: capability.to_string(),
        mode: if target.is_some() {
            RoutingMode::Explicit
        } else {
            RoutingMode::Automatic
        },
        caller: caller.clone(),
        policy: POLICY,
        selected: None,
        candidates: Vec::new(),
        nodes_without_capability: 0,
        error: None,
        attempts: Vec::new(),
    };

    match target {
        Some(target) => decide_explicit(view, caller, capability, target, &mut decision),
        None => decide_automatic(view, caller, capability, &mut decision),
    }
    decision
}

/// Explicit target: evaluate that node only. Never substitute another node.
fn decide_explicit(
    view: &ClusterView,
    caller: &NodeId,
    capability: &str,
    target: &NodeId,
    decision: &mut RoutingDecision,
) {
    let report = if &view.local.node_id == target {
        evaluate(&view.local, None, caller, capability)
    } else {
        match view.peers.iter().find(|p| &p.node_id == target) {
            Some(PeerSnapshot {
                advert: Some(advert),
                state,
                ..
            }) => evaluate(advert, Some(*state), caller, capability),
            _ => {
                decision.error = Some(ErrorBody::new(
                    ErrorCode::TargetNotFound,
                    format!("{target} is not a known node"),
                ));
                return;
            }
        }
    };
    match &report.rejection {
        Some(rejection) => decision.error = Some(rejection.explicit_error(target, capability)),
        None => decision.selected = Some(target.clone()),
    }
    decision.candidates.push(CandidateReport {
        rank: report.eligible.then_some(1),
        ..report
    });
}

fn decide_automatic(view: &ClusterView, caller: &NodeId, capability: &str, decision: &mut RoutingDecision) {
    let local = std::iter::once((&view.local, None));
    let peers = view
        .peers
        .iter()
        .filter_map(|p| p.advert.as_ref().map(|a| (a, Some(p.state))));
    for (advert, link_state) in local.chain(peers) {
        if advert.capability(capability).is_none() {
            decision.nodes_without_capability += 1;
            continue;
        }
        decision
            .candidates
            .push(evaluate(advert, link_state, caller, capability));
    }

    // The whole policy is this sort key. Ties end at node id, which is unique, so the order is total.
    let mut eligible: Vec<usize> = (0..decision.candidates.len())
        .filter(|&i| decision.candidates[i].eligible)
        .collect();
    eligible.sort_by(|&a, &b| {
        let (a, b) = (&decision.candidates[a], &decision.candidates[b]);
        (a.load_permille, a.cpu_percent, &a.node_id).cmp(&(b.load_permille, b.cpu_percent, &b.node_id))
    });
    for (rank, &index) in eligible.iter().enumerate() {
        decision.candidates[index].rank = Some(rank + 1);
    }
    // Report candidates in a stable order too, so two logs of the same decision are identical.
    decision.candidates.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    match eligible.first() {
        Some(_) => decision.selected = decision.ranked().into_iter().next(),
        None if decision.candidates.is_empty() => {
            decision.error = Some(ErrorBody::new(
                ErrorCode::NoEligibleNode,
                format!("no known node provides {capability}"),
            ));
        }
        None => {
            decision.error = Some(ErrorBody::new(
                ErrorCode::NoEligibleNode,
                format!(
                    "{} node(s) provide {capability} but none can take it now",
                    decision.candidates.len()
                ),
            ));
        }
    }
}

/// `link_state` is `None` for the local node (always reachable).
fn evaluate(advert: &NodeAdvert, link_state: Option<PeerState>, caller: &NodeId, capability: &str) -> CandidateReport {
    let r = &advert.resources;
    let load_permille = (u64::from(r.active_jobs) * 1000 / u64::from(r.max_concurrent_jobs.max(1))) as u32;
    let rejection = match advert.capability(capability) {
        None => Some(Rejection::CapabilityNotAdvertised),
        // A node may always call its own capabilities.
        Some(cap) if link_state.is_some() && !cap.allows(caller) => Some(Rejection::NotAllowed),
        Some(_) => match link_state {
            Some(state) if state != PeerState::Ready => Some(Rejection::LinkNotReady { state }),
            _ if r.availability != Availability::Available => Some(Rejection::NotAvailable {
                availability: r.availability,
            }),
            _ if r.active_jobs >= r.max_concurrent_jobs => Some(Rejection::AtCapacity {
                active_jobs: r.active_jobs,
                max_concurrent_jobs: r.max_concurrent_jobs,
            }),
            _ => None,
        },
    };
    CandidateReport {
        node_id: advert.node_id.clone(),
        local: link_state.is_none(),
        eligible: rejection.is_none(),
        rejection,
        load_permille,
        cpu_percent: r.cpu_percent,
        rank: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::message::{CapabilityAdvert, ResourceState};
    use crate::protocol::{NodeType, PROTOCOL_VERSION};
    use serde_json::json;

    fn id(s: &str) -> NodeId {
        NodeId::new(s).unwrap()
    }

    fn node(name: &str, caps: &[&str], active: u32, max: u32, cpu: u8) -> NodeAdvert {
        NodeAdvert {
            node_id: id(name),
            name: name.into(),
            node_type: NodeType::Server,
            protocol_version: PROTOCOL_VERSION.into(),
            address: "127.0.0.1:1".into(),
            incarnation: 1,
            seq: 1,
            capabilities: caps
                .iter()
                .map(|c| CapabilityAdvert {
                    name: c.to_string(),
                    description: String::new(),
                    input_schema: json!({}),
                    output_schema: json!({}),
                    allow: vec!["*".into()],
                })
                .collect(),
            resources: ResourceState {
                cpu_percent: cpu,
                memory_available_mb: 1024,
                active_jobs: active,
                max_concurrent_jobs: max,
                availability: Availability::Available,
                mocked: true,
            },
        }
    }

    fn peer(advert: NodeAdvert, state: PeerState) -> PeerSnapshot {
        PeerSnapshot {
            node_id: advert.node_id.clone(),
            state,
            advert: Some(advert),
        }
    }

    fn sandbox(b_active: u32) -> ClusterView {
        ClusterView {
            local: node("node-a", &["system.echo"], 0, 4, 10),
            peers: vec![
                peer(
                    node("node-b", &["math.add", "test.execute"], b_active, 2, 20),
                    PeerState::Ready,
                ),
                peer(
                    node("node-c", &["system.info", "test.execute"], 0, 2, 20),
                    PeerState::Ready,
                ),
            ],
        }
    }

    #[test]
    fn capability_routes_to_the_only_provider() {
        let d = decide(&sandbox(0), &id("node-a"), "math.add", None);
        assert_eq!(d.selected, Some(id("node-b")));
        assert_eq!(d.nodes_without_capability, 2);
    }

    #[test]
    fn equal_load_tie_breaks_on_node_id() {
        let d = decide(&sandbox(0), &id("node-a"), "test.execute", None);
        assert_eq!(d.selected, Some(id("node-b")));
        assert_eq!(d.ranked(), vec![id("node-b"), id("node-c")]);
    }

    #[test]
    fn full_node_is_skipped_with_reason() {
        let d = decide(&sandbox(2), &id("node-a"), "test.execute", None);
        assert_eq!(d.selected, Some(id("node-c")));
        let b = d.candidates.iter().find(|c| c.node_id == id("node-b")).unwrap();
        assert_eq!(
            b.rejection,
            Some(Rejection::AtCapacity {
                active_jobs: 2,
                max_concurrent_jobs: 2
            })
        );
    }

    #[test]
    fn lower_load_beats_node_id_order() {
        let d = decide(&sandbox(1), &id("node-a"), "test.execute", None);
        assert_eq!(d.selected, Some(id("node-c")), "node-b at 500 permille, node-c at 0");
    }

    #[test]
    fn decision_does_not_depend_on_peer_order() {
        let reference = serde_json::to_value(decide(&sandbox(0), &id("node-a"), "test.execute", None)).unwrap();
        for _ in 0..10 {
            let mut view = sandbox(0);
            view.peers.reverse();
            assert_eq!(
                serde_json::to_value(decide(&view, &id("node-a"), "test.execute", None)).unwrap(),
                reference
            );
            view.peers.rotate_left(1);
            assert_eq!(
                serde_json::to_value(decide(&view, &id("node-a"), "test.execute", None)).unwrap(),
                reference
            );
        }
    }

    #[test]
    fn explicit_target_never_falls_back() {
        let mut view = sandbox(0);
        view.peers[1].state = PeerState::Unavailable;
        let d = decide(&view, &id("node-a"), "test.execute", Some(&id("node-c")));
        assert_eq!(d.selected, None);
        assert_eq!(d.error.unwrap().code, ErrorCode::TargetUnavailable);
    }

    #[test]
    fn explicit_target_errors_are_specific() {
        let view = sandbox(2);
        let cases = [
            ("node-x", "test.execute", ErrorCode::TargetNotFound),
            ("node-c", "math.add", ErrorCode::CapabilityNotSupported),
            ("node-b", "test.execute", ErrorCode::Busy),
        ];
        for (target, cap, code) in cases {
            let d = decide(&view, &id("node-a"), cap, Some(&id(target)));
            assert_eq!(d.error.map(|e| e.code), Some(code), "{target} {cap}");
            assert_eq!(d.selected, None);
        }
    }

    #[test]
    fn access_list_excludes_caller() {
        let mut view = sandbox(0);
        for peer in &mut view.peers {
            for cap in &mut peer.advert.as_mut().unwrap().capabilities {
                cap.allow = vec!["node-z".into()];
            }
        }
        let d = decide(&view, &id("node-a"), "test.execute", None);
        assert_eq!(d.error.unwrap().code, ErrorCode::NoEligibleNode);
        assert!(d.candidates.iter().all(|c| c.rejection == Some(Rejection::NotAllowed)));
    }

    #[test]
    fn local_node_is_a_candidate() {
        let d = decide(&sandbox(0), &id("node-a"), "system.echo", None);
        assert_eq!(d.selected, Some(id("node-a")));
        assert!(d.candidates[0].local);
    }
}

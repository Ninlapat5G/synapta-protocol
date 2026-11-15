//! Shortest paths to every known node, for nodes this node has no direct link to (they sit in
//! another ring, reachable only through bridges).
//!
//! Link-state, like the advert flooding it builds on: every advert lists its node's READY
//! links (`NodeAdvert::neighbors`), so each node holds the whole graph and computes paths on
//! its own. A link between two other nodes counts only when both list each other — one stale
//! or lying advert cannot invent a path. This node's own links come from its registry (they are
//! fresher than any advert). Ties break by node id, so the result is the same on every run.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::Serialize;

use crate::node::state::NodeState;
use crate::protocol::message::NodeAdvert;
use crate::protocol::NodeId;
use crate::registry::{PeerSnapshot, PeerState};

/// How to reach a node: hand the request to `next_hop` (a node with a READY link from here).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Route {
    pub next_hop: NodeId,
    /// 1 = direct link.
    pub hops: u32,
}

/// Routes as of now, from this node's registry.
pub(crate) fn current(state: &NodeState) -> BTreeMap<NodeId, Route> {
    let peers = state.registry.read().unwrap().snapshots();
    from_snapshots(state.node_id(), &peers)
}

/// Routes from a registry snapshot: READY peers are this node's links.
pub fn from_snapshots(me: &NodeId, peers: &[PeerSnapshot]) -> BTreeMap<NodeId, Route> {
    let my_links = peers
        .iter()
        .filter(|p| p.state == PeerState::Ready)
        .map(|p| p.node_id.clone())
        .collect();
    shortest_routes(me, &my_links, peers.iter().filter_map(|p| p.advert.as_ref()))
}

/// Breadth-first search from `me` over `my_links` and the links the adverts confirm.
pub fn shortest_routes<'a>(
    me: &NodeId,
    my_links: &BTreeSet<NodeId>,
    adverts: impl IntoIterator<Item = &'a NodeAdvert>,
) -> BTreeMap<NodeId, Route> {
    let listed: BTreeMap<&NodeId, BTreeSet<&NodeId>> = adverts
        .into_iter()
        .map(|a| (&a.node_id, a.neighbors.iter().collect()))
        .collect();
    let confirmed = |a: &NodeId, b: &NodeId| {
        listed.get(a).is_some_and(|n| n.contains(b)) && listed.get(b).is_some_and(|n| n.contains(a))
    };

    let mut routes: BTreeMap<NodeId, Route> = BTreeMap::new();
    let mut queue: VecDeque<NodeId> = VecDeque::new();
    for link in my_links.iter().filter(|l| *l != me) {
        routes.insert(
            link.clone(),
            Route {
                next_hop: link.clone(),
                hops: 1,
            },
        );
        queue.push_back(link.clone());
    }
    while let Some(node) = queue.pop_front() {
        let Some(neighbors) = listed.get(&node) else { continue };
        let via = routes[&node].clone();
        for &next in neighbors {
            if next == me || routes.contains_key(next) || !confirmed(&node, next) {
                continue;
            }
            routes.insert(
                next.clone(),
                Route {
                    next_hop: via.next_hop.clone(),
                    hops: via.hops + 1,
                },
            );
            queue.push_back(next.clone());
        }
    }
    routes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::message::{Availability, ResourceState};
    use crate::protocol::{NodeType, WireRange, SOFTWARE_VERSION};

    fn id(s: &str) -> NodeId {
        NodeId::new(s).unwrap()
    }

    fn advert(node: &str, neighbors: &[&str]) -> NodeAdvert {
        NodeAdvert {
            node_id: id(node),
            name: node.into(),
            node_type: NodeType::Server,
            software_version: SOFTWARE_VERSION.into(),
            wire: WireRange::SUPPORTED,
            address: "127.0.0.1:1".into(),
            incarnation: 1,
            seq: 1,
            capabilities: vec![],
            neighbors: neighbors.iter().map(|n| id(n)).collect(),
            resources: ResourceState {
                cpu_percent: 0,
                memory_available_mb: 0,
                active_jobs: 0,
                max_concurrent_jobs: 1,
                availability: Availability::Available,
                mocked: true,
            },
        }
    }

    fn links(ids: &[&str]) -> BTreeSet<NodeId> {
        ids.iter().map(|n| id(n)).collect()
    }

    fn route(next_hop: &str, hops: u32) -> Route {
        Route {
            next_hop: id(next_hop),
            hops,
        }
    }

    #[test]
    fn a_chain_of_rings_is_walked_through_its_bridges() {
        // a — b — c — d, each pair in its own ring.
        let adverts = [
            advert("node-b", &["node-a", "node-c"]),
            advert("node-c", &["node-b", "node-d"]),
            advert("node-d", &["node-c"]),
        ];
        let routes = shortest_routes(&id("node-a"), &links(&["node-b"]), &adverts);
        assert_eq!(routes[&id("node-b")], route("node-b", 1));
        assert_eq!(routes[&id("node-c")], route("node-b", 2));
        assert_eq!(routes[&id("node-d")], route("node-b", 3));
    }

    #[test]
    fn a_link_only_one_side_lists_does_not_count() {
        // node-c claims a link to node-d; node-d does not list node-c.
        let adverts = [
            advert("node-b", &["node-a", "node-c"]),
            advert("node-c", &["node-b", "node-d"]),
            advert("node-d", &[]),
        ];
        let routes = shortest_routes(&id("node-a"), &links(&["node-b"]), &adverts);
        assert!(routes.contains_key(&id("node-c")));
        assert!(!routes.contains_key(&id("node-d")));
    }

    #[test]
    fn the_shortest_path_wins_and_ties_go_to_the_lower_id() {
        // node-a reaches node-e via node-b or node-c (both 2 hops), or via node-d → node-f (3).
        let adverts = [
            advert("node-b", &["node-a", "node-e"]),
            advert("node-c", &["node-a", "node-e"]),
            advert("node-d", &["node-a", "node-f"]),
            advert("node-f", &["node-d", "node-e"]),
            advert("node-e", &["node-b", "node-c", "node-f"]),
        ];
        let my_links = links(&["node-b", "node-c", "node-d"]);
        for _ in 0..3 {
            let routes = shortest_routes(&id("node-a"), &my_links, &adverts);
            assert_eq!(routes[&id("node-e")], route("node-b", 2));
        }
    }

    #[test]
    fn nodes_without_a_path_are_left_out() {
        let adverts = [advert("node-b", &["node-a"]), advert("node-z", &[])];
        let routes = shortest_routes(&id("node-a"), &links(&["node-b"]), &adverts);
        assert_eq!(routes.len(), 1);
    }
}

//! Membership cards on live nodes: nodes that list nobody by name, only their network's
//! authority, still form a full mesh; a node added later joins without anyone restarting; a
//! node without a valid card is kept out.

mod common;

use std::path::Path;

use common::{id, identity, Cluster, NodeSpec};
use ed25519_dalek::SigningKey;
use synapta_core::config::AuthorityEntry;
use synapta_core::discovery::handshake::{self, HandshakeParams};
use synapta_core::identity::{
    issue_card, parse_signing_key, parse_verifying_key, random_bytes, to_hex, unix_now, CardClaims, TrustStore,
};
use synapta_core::protocol::message::MemberCard;
use synapta_core::{ErrorBody, ErrorCode, NodeConfig};
use tokio::net::TcpStream;

const NETWORK: &str = "lab";

fn spec(node: &'static str, seeds: &[&str]) -> NodeSpec {
    NodeSpec {
        id: node,
        node_type: "server",
        capabilities: vec![("math.add", vec!["*"])],
        max_concurrent_jobs: 2,
        cpu_percent: 20,
        seeds: seeds.iter().map(|s| s.to_string()).collect(),
        trusts: vec![], // nobody by name
    }
}

/// Trust `network`'s authority, and carry a card from `issuer` that expires at `expires_at`.
fn member<'a>(network_key: &SigningKey, issuer: &'a SigningKey, expires_at: u64) -> impl Fn(&mut NodeConfig) + 'a {
    let network_key = network_key.verifying_key();
    move |config| {
        config.trust_authority = vec![AuthorityEntry {
            network: NETWORK.into(),
            public_key: to_hex(network_key.as_bytes()),
        }];
        let private = std::fs::read_to_string(&config.identity.private_key_file).unwrap();
        let public = parse_signing_key(private.trim()).unwrap().verifying_key();
        let claims = CardClaims {
            network: NETWORK.into(),
            node_id: config.node.id.clone(),
            public_key: to_hex(public.as_bytes()),
            issued_at: unix_now(),
            expires_at,
        };
        let card_file = Path::new(&config.identity.private_key_file).with_extension("card");
        std::fs::write(&card_file, serde_json::to_string(&issue_card(issuer, &claims)).unwrap()).unwrap();
        config.identity.card_file = Some(card_file);
    }
}

fn authority() -> SigningKey {
    SigningKey::from_bytes(&random_bytes::<32>())
}

/// Starts one more node into a running cluster, the way synapta-lab adds a node.
async fn add_node(cluster: &mut Cluster, spec: NodeSpec, adjust: impl Fn(&mut NodeConfig)) {
    let mut config = cluster.config_for(&spec);
    adjust(&mut config);
    cluster.nodes.push(synapta_core::start(config).await.unwrap());
}

#[tokio::test]
async fn card_holders_mesh_and_a_late_member_joins_without_restarts() {
    let lab = authority();
    let year = unix_now() + 365 * 24 * 3600;
    let specs = vec![
        spec("node-b", &[]),
        spec("node-a", &["node-b"]),
        spec("node-c", &["node-b"]),
    ];
    let mut cluster = Cluster::start_with(specs, member(&lab, &lab, year)).await;
    // node-a and node-c met only node-b, and list nobody: they trust each other by card.
    cluster.wait_full_mesh().await;

    add_node(&mut cluster, spec("node-d", &["node-a"]), member(&lab, &lab, year)).await;
    cluster.wait_full_mesh().await;
    cluster.shutdown().await;
}

/// Dials node-b as node-c showing `card`; returns node-b's refusal, if any.
async fn dial_with_card(cluster: &Cluster, card: MemberCard) -> Result<(), ErrorBody> {
    let c = identity("node-c", cluster.key("node-c"));
    let mut trust = TrustStore::default();
    trust.insert(
        id("node-b"),
        parse_verifying_key(&cluster.key("node-b").public).unwrap(),
    );
    let params = HandshakeParams {
        identity: &c,
        trust: &trust,
        card: Some(&card),
        max_frame_bytes: 1 << 20,
    };
    let mut stream = TcpStream::connect(cluster.node("node-b").listen_addr()).await.unwrap();
    handshake::dial(&mut stream, &params, Some(&id("node-b")))
        .await
        .map(|_| ())
}

fn card(issuer: &SigningKey, cluster: &Cluster, network: &str, expires_at: u64) -> MemberCard {
    let claims = CardClaims {
        network: network.into(),
        node_id: id("node-c"),
        public_key: cluster.key("node-c").public.clone(),
        issued_at: unix_now(),
        expires_at,
    };
    issue_card(issuer, &claims)
}

#[tokio::test]
async fn nodes_without_a_valid_card_are_kept_out() {
    let lab = authority();
    let year = unix_now() + 365 * 24 * 3600;
    let cluster = Cluster::start_with(vec![spec("node-b", &[])], member(&lab, &lab, year)).await;
    let refusal = |result: Result<(), ErrorBody>| {
        let error = result.expect_err("node-b let node-c in");
        assert_eq!(error.code, ErrorCode::UntrustedPeer, "{error}");
        error.message
    };

    let impostor = authority();
    assert!(refusal(dial_with_card(&cluster, card(&impostor, &cluster, NETWORK, year)).await).contains("not signed"));
    assert!(refusal(dial_with_card(&cluster, card(&lab, &cluster, "other", year)).await).contains("does not trust"));
    assert!(refusal(dial_with_card(&cluster, card(&lab, &cluster, NETWORK, unix_now() - 1)).await).contains("expired"));
    // The same node with a genuine card gets in.
    dial_with_card(&cluster, card(&lab, &cluster, NETWORK, year))
        .await
        .unwrap();
    cluster.shutdown().await;
}

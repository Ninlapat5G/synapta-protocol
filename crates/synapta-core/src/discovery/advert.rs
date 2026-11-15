//! Signing and verifying link-state adverts.
//!
//! Adverts are relayed hop by hop, so the node handing us an advert is usually not its author.
//! The origin's signature is what makes relaying safe: a relay can drop or delay an advert but
//! cannot forge one, change a capability, or point another node's address elsewhere.

use crate::error::{ErrorBody, ErrorCode};
use crate::identity::{NodeIdentity, TrustStore};
use crate::protocol::message::{NodeAdvert, SignedAdvert};
use crate::protocol::NodeId;
use crate::transport::AddressPolicy;

const DOMAIN: &str = "synapta-advert\n";

pub fn sign(advert: &NodeAdvert, identity: &NodeIdentity) -> SignedAdvert {
    // Serialising a plain struct of owned fields cannot fail.
    let payload = serde_json::to_string(advert).expect("advert serialises");
    let signature = identity.sign(&signed_bytes(&payload));
    SignedAdvert {
        payload,
        signature,
        card: None,
    }
}

/// Verifies the signature against the origin's trusted key and the advert's own sanity.
/// The claimed `node_id` inside the payload selects the key, so a payload that claims to be
/// node-b only verifies if node-b's key signed it.
pub fn verify(signed: &SignedAdvert, trust: &TrustStore, policy: AddressPolicy) -> Result<NodeAdvert, ErrorBody> {
    let advert: NodeAdvert = serde_json::from_str(&signed.payload)
        .map_err(|e| ErrorBody::new(ErrorCode::BadRequest, format!("advert payload: {e}")))?;
    // A node we never met (one in another ring) is trusted through the card it attached.
    trust
        .admit(&advert.node_id, signed.card.as_ref())
        .map_err(|e| ErrorBody::new(ErrorCode::UntrustedPeer, format!("advert: {e}")))?;
    if !trust.verify(&advert.node_id, &signed_bytes(&signed.payload), &signed.signature) {
        return Err(ErrorBody::new(
            ErrorCode::UntrustedPeer,
            format!("advert for {} is not signed by its trusted key", advert.node_id),
        ));
    }
    let address = advert.address.parse().map_err(|_| {
        ErrorBody::new(
            ErrorCode::BadRequest,
            format!("advert address {:?} is invalid", advert.address),
        )
    })?;
    policy
        .check(&address)
        .map_err(|e| ErrorBody::new(ErrorCode::BadRequest, format!("advert for {}: {e}", advert.node_id)))?;
    // The advert's wire versions are not checked here: a node we cannot talk to is still worth
    // knowing about (status shows why it is unusable). `flooding` just does not dial it.
    Ok(advert)
}

/// The node id an advert claims, without verifying it. Never use it to trust anything.
pub fn claimed_origin(signed: &SignedAdvert) -> Option<NodeId> {
    serde_json::from_str::<NodeAdvert>(&signed.payload)
        .ok()
        .map(|a| a.node_id)
}

fn signed_bytes(payload: &str) -> Vec<u8> {
    format!("{DOMAIN}{payload}").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{generate_keypair, parse_signing_key, parse_verifying_key};
    use crate::protocol::message::{Availability, ResourceState};
    use crate::protocol::{NodeType, WireRange, SOFTWARE_VERSION};

    fn advert(id: &str, address: &str) -> NodeAdvert {
        NodeAdvert {
            node_id: NodeId::new(id).unwrap(),
            name: id.into(),
            node_type: NodeType::Server,
            software_version: SOFTWARE_VERSION.into(),
            wire: WireRange::SUPPORTED,
            address: address.into(),
            incarnation: 1,
            seq: 1,
            capabilities: vec![],
            neighbors: vec![],
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

    fn keyed(id: &str) -> (NodeIdentity, TrustStore) {
        let (private, public) = generate_keypair();
        let identity = NodeIdentity::new(NodeId::new(id).unwrap(), parse_signing_key(&private).unwrap());
        let mut trust = TrustStore::default();
        trust.insert(identity.node_id.clone(), parse_verifying_key(&public).unwrap());
        (identity, trust)
    }

    #[test]
    fn valid_advert_verifies() {
        let (b, trust) = keyed("node-b");
        let signed = sign(&advert("node-b", "127.0.0.1:47322"), &b);
        assert_eq!(
            verify(&signed, &trust, AddressPolicy::LoopbackOnly).unwrap().node_id,
            b.node_id
        );
    }

    #[test]
    fn relay_cannot_tamper_with_payload() {
        let (b, trust) = keyed("node-b");
        let mut signed = sign(&advert("node-b", "127.0.0.1:47322"), &b);
        signed.payload = signed.payload.replace("47322", "47399");
        assert_eq!(
            verify(&signed, &trust, AddressPolicy::LoopbackOnly).unwrap_err().code,
            ErrorCode::UntrustedPeer
        );
    }

    #[test]
    fn node_cannot_sign_adverts_for_another_node() {
        let (c, _) = keyed("node-c");
        let (_, trust_b) = keyed("node-b");
        let forged = sign(&advert("node-b", "127.0.0.1:47322"), &c);
        assert!(verify(&forged, &trust_b, AddressPolicy::LoopbackOnly).is_err());
    }

    #[test]
    fn non_loopback_address_is_refused_even_if_signed() {
        let (b, trust) = keyed("node-b");
        let signed = sign(&advert("node-b", "203.0.113.9:47322"), &b);
        assert_eq!(
            verify(&signed, &trust, AddressPolicy::LoopbackOnly).unwrap_err().code,
            ErrorCode::BadRequest
        );
    }
}

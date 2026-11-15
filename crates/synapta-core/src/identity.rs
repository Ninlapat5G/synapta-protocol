//! Node identity: Ed25519 keys, signing, and who this node trusts.
//!
//! A node proves who it is by signing. A peer is trusted when either
//! - its node id and public key are listed in this node's configuration (`[[trust]]`), or
//! - it shows a membership card signed by a network authority this node trusts
//!   (`[[trust_authority]]`). Then adding a member does not mean editing every node's config.
//!
//! Claimed node ids are never trusted on their own.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::protocol::message::MemberCard;
use crate::protocol::NodeId;

const CARD_DOMAIN: &str = "synapta-card\n";

pub struct NodeIdentity {
    pub node_id: NodeId,
    signing_key: SigningKey,
}

impl NodeIdentity {
    pub fn new(node_id: NodeId, signing_key: SigningKey) -> Self {
        Self { node_id, signing_key }
    }

    /// Loads a private key file containing the 32-byte seed as hex.
    pub fn load(node_id: NodeId, key_file: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(key_file)
            .map_err(|e| format!("cannot read private key {}: {e}", key_file.display()))?;
        let signing_key =
            parse_signing_key(text.trim()).map_err(|e| format!("invalid private key {}: {e}", key_file.display()))?;
        Ok(Self::new(node_id, signing_key))
    }

    pub fn sign(&self, message: &[u8]) -> String {
        to_hex(&self.signing_key.sign(message).to_bytes())
    }
}

/// Generates a new key pair. Returns (private seed hex, public key hex).
pub fn generate_keypair() -> (String, String) {
    let signing_key = SigningKey::from_bytes(&random_bytes::<32>());
    (
        to_hex(signing_key.as_bytes()),
        to_hex(signing_key.verifying_key().as_bytes()),
    )
}

pub fn parse_signing_key(hex: &str) -> Result<SigningKey, String> {
    let bytes: [u8; 32] = from_hex(hex)?.try_into().map_err(|_| "expected 32 bytes".to_string())?;
    Ok(SigningKey::from_bytes(&bytes))
}

pub fn parse_verifying_key(hex: &str) -> Result<VerifyingKey, String> {
    let bytes: [u8; 32] = from_hex(hex)?.try_into().map_err(|_| "expected 32 bytes".to_string())?;
    VerifyingKey::from_bytes(&bytes).map_err(|e| e.to_string())
}

/// What a membership card says. `issued_at` / `expires_at` are Unix seconds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CardClaims {
    pub network: String,
    pub node_id: NodeId,
    /// The member's Ed25519 public key, hex.
    pub public_key: String,
    pub issued_at: u64,
    pub expires_at: u64,
}

/// Signs a membership card with a network authority's private key.
pub fn issue_card(authority: &SigningKey, claims: &CardClaims) -> MemberCard {
    // Serialising a plain struct of owned fields cannot fail.
    let payload = serde_json::to_string(claims).expect("card claims serialise");
    let signature = to_hex(&authority.sign(&card_bytes(&payload)).to_bytes());
    MemberCard { payload, signature }
}

fn card_bytes(payload: &str) -> Vec<u8> {
    format!("{CARD_DOMAIN}{payload}").into_bytes()
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A network authority this node trusts: any card it signed for `network` admits the holder.
#[derive(Clone)]
pub struct Authority {
    pub network: String,
    pub key: VerifyingKey,
}

/// Peers this node accepts, and the keys to check their signatures.
#[derive(Clone, Default)]
pub struct TrustStore {
    /// Listed by name in the config.
    keys: HashMap<NodeId, VerifyingKey>,
    authorities: Vec<Authority>,
    /// Learned from membership cards that checked out (shared between clones).
    carded: Arc<RwLock<HashMap<NodeId, CardedKey>>>,
}

#[derive(Clone, Copy)]
struct CardedKey {
    key: VerifyingKey,
    issued_at: u64,
    expires_at: u64,
}

impl TrustStore {
    pub fn insert(&mut self, node_id: NodeId, key: VerifyingKey) {
        self.keys.insert(node_id, key);
    }

    pub fn add_authority(&mut self, authority: Authority) {
        self.authorities.push(authority);
    }

    /// Decides whether `node_id` may connect: listed by name, already known from a valid card,
    /// or showing one now. A card that checks out is remembered, so the node's later signatures
    /// (adverts, relayed calls) verify even where they arrive without it.
    pub fn admit(&self, node_id: &NodeId, card: Option<&MemberCard>) -> Result<(), String> {
        if self.keys.contains_key(node_id) {
            return Ok(());
        }
        let Some(card) = card else {
            return match self.carded_key(node_id) {
                Some(_) => Ok(()),
                None => Err(format!(
                    "{node_id} is not in the trust configuration and shows no membership card"
                )),
            };
        };
        let (claims, key) = self.check_card(card)?;
        if &claims.node_id != node_id {
            return Err(format!("membership card is for {}, not {node_id}", claims.node_id));
        }
        let mut carded = self.carded.write().unwrap();
        // Keep the newest card: an older one (say, from before a key change) must not win back.
        let newer = carded
            .get(node_id)
            .is_none_or(|known| claims.issued_at >= known.issued_at);
        if newer {
            let entry = CardedKey {
                key,
                issued_at: claims.issued_at,
                expires_at: claims.expires_at,
            };
            carded.insert(node_id.clone(), entry);
        }
        Ok(())
    }

    /// Checks a card's signature, network and dates. Returns what it says and the member key.
    fn check_card(&self, card: &MemberCard) -> Result<(CardClaims, VerifyingKey), String> {
        let claims: CardClaims =
            serde_json::from_str(&card.payload).map_err(|e| format!("membership card is malformed: {e}"))?;
        let Some(authority) = self.authorities.iter().find(|a| a.network == claims.network) else {
            return Err(format!(
                "membership card is for network {:?}, which this node does not trust",
                claims.network
            ));
        };
        if !verify_with(&authority.key, &card_bytes(&card.payload), &card.signature) {
            return Err(format!(
                "membership card for {} is not signed by the {:?} authority",
                claims.node_id, claims.network
            ));
        }
        if unix_now() >= claims.expires_at {
            return Err(format!("membership card for {} has expired", claims.node_id));
        }
        let key = parse_verifying_key(&claims.public_key).map_err(|e| format!("membership card key: {e}"))?;
        Ok((claims, key))
    }

    fn carded_key(&self, node_id: &NodeId) -> Option<VerifyingKey> {
        let carded = self.carded.read().unwrap();
        let entry = carded.get(node_id)?;
        (unix_now() < entry.expires_at).then_some(entry.key)
    }

    fn key_for(&self, node_id: &NodeId) -> Option<VerifyingKey> {
        self.keys.get(node_id).copied().or_else(|| self.carded_key(node_id))
    }

    pub fn is_trusted(&self, node_id: &NodeId) -> bool {
        self.key_for(node_id).is_some()
    }

    /// Verifies that `node_id`'s trusted key signed `message`. Untrusted ids always fail.
    pub fn verify(&self, node_id: &NodeId, message: &[u8], signature_hex: &str) -> bool {
        self.key_for(node_id)
            .is_some_and(|key| verify_with(&key, message, signature_hex))
    }
}

fn verify_with(key: &VerifyingKey, message: &[u8], signature_hex: &str) -> bool {
    let Ok(bytes) = from_hex(signature_hex) else {
        return false;
    };
    let Ok(bytes) = <[u8; 64]>::try_from(bytes) else {
        return false;
    };
    // verify_strict rejects malleable / small-order signatures.
    key.verify_strict(message, &Signature::from_bytes(&bytes)).is_ok()
}

pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut buf = [0u8; N];
    // The OS RNG failing means the machine cannot produce keys or nonces at all; there is no
    // safe fallback, so stop rather than continue with predictable values.
    getrandom::getrandom(&mut buf).expect("OS random number generator unavailable");
    buf
}

pub fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    out
}

pub fn from_hex(hex: &str) -> Result<Vec<u8>, String> {
    if !hex.len().is_multiple_of(2) {
        return Err("hex string has odd length".into());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2).ok_or("invalid hex")?, 16).map_err(|_| "invalid hex".to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(id: &str) -> (NodeIdentity, VerifyingKey) {
        let (private, public) = generate_keypair();
        let identity = NodeIdentity::new(NodeId::new(id).unwrap(), parse_signing_key(&private).unwrap());
        (identity, parse_verifying_key(&public).unwrap())
    }

    #[test]
    fn hex_round_trip() {
        let bytes = random_bytes::<17>();
        assert_eq!(from_hex(&to_hex(&bytes)).unwrap(), bytes);
        assert!(from_hex("abc").is_err());
        assert!(from_hex("zz").is_err());
    }

    #[test]
    fn verify_requires_trusted_key_of_that_node() {
        let (a, a_key) = identity("node-a");
        let (b, _) = identity("node-b");
        let mut trust = TrustStore::default();
        trust.insert(a.node_id.clone(), a_key);

        let sig = a.sign(b"hello");
        assert!(trust.verify(&a.node_id, b"hello", &sig));
        assert!(!trust.verify(&a.node_id, b"hellO", &sig), "tampered message");
        // node-b is not trusted, and node-b cannot sign as node-a.
        assert!(!trust.verify(&b.node_id, b"hello", &b.sign(b"hello")));
        assert!(!trust.verify(&a.node_id, b"hello", &b.sign(b"hello")));
        assert!(!trust.verify(&a.node_id, b"hello", "not-hex"));
    }

    struct Network {
        authority: SigningKey,
        trust: TrustStore,
    }

    fn network(name: &str) -> Network {
        let authority = SigningKey::from_bytes(&random_bytes::<32>());
        let mut trust = TrustStore::default();
        trust.add_authority(Authority {
            network: name.into(),
            key: authority.verifying_key(),
        });
        Network { authority, trust }
    }

    fn card_for(authority: &SigningKey, network: &str, member: &VerifyingKey, id: &str, expires_at: u64) -> MemberCard {
        let claims = CardClaims {
            network: network.into(),
            node_id: NodeId::new(id).unwrap(),
            public_key: to_hex(member.as_bytes()),
            issued_at: unix_now(),
            expires_at,
        };
        issue_card(authority, &claims)
    }

    #[test]
    fn a_valid_card_admits_its_holder_and_teaches_its_key() {
        let lab = network("lab");
        let (z, z_key) = identity("node-z");
        let card = card_for(&lab.authority, "lab", &z_key, "node-z", unix_now() + 60);

        assert!(!lab.trust.is_trusted(&z.node_id));
        assert!(lab.trust.admit(&z.node_id, None).is_err(), "no card, not listed");
        lab.trust.admit(&z.node_id, Some(&card)).unwrap();
        // Later signatures verify without the card, also through a clone of the store.
        assert!(lab.trust.clone().verify(&z.node_id, b"m", &z.sign(b"m")));
        assert!(lab.trust.admit(&z.node_id, None).is_ok());
    }

    #[test]
    fn bad_cards_are_refused() {
        let lab = network("lab");
        let other = network("other");
        let (z, z_key) = identity("node-z");
        let later = unix_now() + 60;
        let refused = |card: MemberCard| lab.trust.admit(&z.node_id, Some(&card)).unwrap_err();

        // Another network's authority, or a card for another network name.
        assert!(refused(card_for(&other.authority, "lab", &z_key, "node-z", later)).contains("not signed"));
        assert!(refused(card_for(&lab.authority, "other", &z_key, "node-z", later)).contains("does not trust"));
        // Expired.
        assert!(refused(card_for(&lab.authority, "lab", &z_key, "node-z", unix_now() - 1)).contains("expired"));
        // Someone else's card.
        assert!(refused(card_for(&lab.authority, "lab", &z_key, "node-y", later)).contains("not node-z"));
        // Edited after signing.
        let mut edited = card_for(&lab.authority, "lab", &z_key, "node-z", later);
        edited.payload = edited.payload.replace("node-z", "node-q");
        assert!(lab.trust.admit(&NodeId::new("node-q").unwrap(), Some(&edited)).is_err());
        assert!(!lab.trust.is_trusted(&z.node_id));
    }

    #[test]
    fn an_older_card_does_not_replace_a_newer_one() {
        let lab = network("lab");
        let (z, z_key) = identity("node-z");
        let (_, old_key) = identity("node-z");
        let mut old = CardClaims {
            network: "lab".into(),
            node_id: z.node_id.clone(),
            public_key: to_hex(old_key.as_bytes()),
            issued_at: unix_now() - 100,
            expires_at: unix_now() + 60,
        };
        let new = card_for(&lab.authority, "lab", &z_key, "node-z", unix_now() + 60);
        lab.trust.admit(&z.node_id, Some(&new)).unwrap();
        lab.trust
            .admit(&z.node_id, Some(&issue_card(&lab.authority, &old)))
            .unwrap();
        assert!(lab.trust.verify(&z.node_id, b"m", &z.sign(b"m")), "still the newer key");
        old.issued_at = unix_now() + 1;
        lab.trust
            .admit(&z.node_id, Some(&issue_card(&lab.authority, &old)))
            .unwrap();
        assert!(!lab.trust.verify(&z.node_id, b"m", &z.sign(b"m")), "a newer card wins");
    }
}

//! Node identity: Ed25519 keys, signing and the trust store.
//!
//! A node proves who it is by signing; a peer is trusted only if its node id and public key are
//! listed in this node's configuration. Claimed node ids are never trusted on their own.

use std::collections::HashMap;
use std::path::Path;

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};

use crate::protocol::NodeId;

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

/// Public keys of the peers this node accepts, keyed by node id.
#[derive(Clone, Default)]
pub struct TrustStore {
    keys: HashMap<NodeId, VerifyingKey>,
}

impl TrustStore {
    pub fn insert(&mut self, node_id: NodeId, key: VerifyingKey) {
        self.keys.insert(node_id, key);
    }

    pub fn is_trusted(&self, node_id: &NodeId) -> bool {
        self.keys.contains_key(node_id)
    }

    /// Verifies that `node_id`'s trusted key signed `message`. Untrusted ids always fail.
    pub fn verify(&self, node_id: &NodeId, message: &[u8], signature_hex: &str) -> bool {
        let Some(key) = self.keys.get(node_id) else {
            return false;
        };
        let Ok(bytes) = from_hex(signature_hex) else {
            return false;
        };
        let Ok(bytes) = <[u8; 64]>::try_from(bytes) else {
            return false;
        };
        // verify_strict rejects malleable / small-order signatures.
        key.verify_strict(message, &Signature::from_bytes(&bytes)).is_ok()
    }
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
}

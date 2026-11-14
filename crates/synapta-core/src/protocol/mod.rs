//! Protocol constants, identifiers and the wire message schema.

pub mod message;

use std::fmt;

use serde::{Deserialize, Serialize};

/// Version string exchanged in the handshake. v0.0.x makes no compatibility promise,
/// so peers must match exactly (see `is_compatible_version`).
pub const PROTOCOL_VERSION: &str = "0.0.1";

pub fn is_compatible_version(version: &str) -> bool {
    version == PROTOCOL_VERSION
}

/// A node identifier: 1–63 chars of `[a-z0-9-]`, starting with a letter or digit.
/// Kept this narrow so it is safe in URLs, log fields, file names and UTCP tool names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Result<Self, String> {
        let id = id.into();
        let valid_len = (1..=63).contains(&id.len());
        let valid_chars = id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        let valid_start = id.bytes().next().is_some_and(|b| b != b'-');
        if valid_len && valid_chars && valid_start {
            Ok(Self(id))
        } else {
            Err(format!(
                "invalid node id {id:?}: expected 1-63 chars of [a-z0-9-], not starting with '-'"
            ))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for NodeId {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        NodeId::new(value)
    }
}

impl From<NodeId> for String {
    fn from(id: NodeId) -> String {
        id.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Capability names are dotted lowercase identifiers such as `math.add`.
pub fn is_valid_capability_name(name: &str) -> bool {
    let segments: Vec<&str> = name.split('.').collect();
    name.len() <= 128
        && segments.len() >= 2
        && segments.iter().all(|s| {
            s.bytes().next().is_some_and(|b| b.is_ascii_lowercase())
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    Server,
    Desktop,
    Mobile,
    Embedded,
    Edge,
    Client,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_validation() {
        assert!(NodeId::new("node-a").is_ok());
        assert!(NodeId::new("n1").is_ok());
        for bad in ["", "-a", "Node", "a/b", "a b", "a.b", &"x".repeat(64)] {
            assert!(NodeId::new(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn capability_name_validation() {
        assert!(is_valid_capability_name("math.add"));
        assert!(is_valid_capability_name("agent.execute_v2"));
        for bad in [
            "math",
            "Math.add",
            "math..add",
            ".add",
            "math.add.",
            "math.1x",
            "math-add.x",
        ] {
            assert!(!is_valid_capability_name(bad), "{bad:?} should be rejected");
        }
    }
}

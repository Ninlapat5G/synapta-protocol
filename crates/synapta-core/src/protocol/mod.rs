//! Protocol constants, identifiers and the wire message schema.

pub mod message;

use std::fmt;

use serde::{Deserialize, Serialize};

/// Release of this build. Shown in adverts and `system.info`; it never decides whether two
/// nodes can talk — [`WireRange`] does.
pub const SOFTWARE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// A span of wire versions. The wire version is one number that goes up only when what crosses
/// a connection changes incompatibly, so a release that leaves the wire alone still talks to
/// older nodes. 1 = v0.0.1 and 2 = v0.0.2 (neither announced a number: they read as 0),
/// 3 = v0.0.3 onward. `docs/protocol.md` lists what may change without a new number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WireRange {
    pub min: u32,
    pub max: u32,
}

impl WireRange {
    /// What this build speaks.
    pub const SUPPORTED: WireRange = WireRange { min: 4, max: 4 };

    /// The version two nodes use: the highest one both speak.
    pub fn highest_common(self, other: WireRange) -> Option<u32> {
        let version = self.max.min(other.max);
        (version >= self.min.max(other.min)).then_some(version)
    }
}

impl fmt::Display for WireRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.min, self.max) {
            (_, 0) => f.write_str("no numbered wire version (older than v0.0.3)"),
            (min, max) if min == max => write!(f, "wire version {max}"),
            (min, max) => write!(f, "wire versions {min}-{max}"),
        }
    }
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
    fn wire_ranges_meet_at_the_highest_shared_version() {
        let range = |min, max| WireRange { min, max };
        assert_eq!(range(3, 3).highest_common(range(3, 3)), Some(3));
        assert_eq!(range(3, 5).highest_common(range(4, 9)), Some(5));
        assert_eq!(range(3, 3).highest_common(range(4, 4)), None);
        // A v0.0.2 hello carries no range, and an inverted one matches nothing.
        assert_eq!(WireRange::SUPPORTED.highest_common(WireRange::default()), None);
        assert_eq!(range(3, 3).highest_common(range(5, 3)), None);
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

//! Node configuration (TOML) and its validation.
//!
//! Relative paths in a config file are resolved against the file's own directory, so a sandbox
//! folder can be moved as a whole. Unknown keys are rejected to catch typos early.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::identity::{parse_verifying_key, Authority, TrustStore};
use crate::protocol::message::{Availability, MemberCard};
use crate::protocol::{is_valid_capability_name, NodeId, NodeType};
use crate::transport::AddressPolicy;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub node: NodeSection,
    pub identity: IdentitySection,
    #[serde(default)]
    pub api: Option<ApiSection>,
    #[serde(default)]
    pub discovery: DiscoverySection,
    #[serde(default)]
    pub timeouts: Timeouts,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub resources: ResourceConfig,
    #[serde(default)]
    pub capabilities: Vec<CapabilityConfig>,
    #[serde(default)]
    pub trust: Vec<TrustEntry>,
    /// Network authorities whose membership cards this node accepts.
    #[serde(default)]
    pub trust_authority: Vec<AuthorityEntry>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSection {
    pub id: NodeId,
    pub name: String,
    #[serde(rename = "type")]
    pub node_type: NodeType,
    /// Port 0 picks a free port (used by automated tests).
    pub listen: SocketAddr,
    /// Logs, the local API token and runtime info are written here.
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentitySection {
    pub private_key_file: PathBuf,
    /// This node's membership card (JSON, as issued by its network authority).
    #[serde(default)]
    pub card_file: Option<PathBuf>,
}

/// Local HTTP API (UTCP tool endpoint + control). Optional per node.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiSection {
    pub listen: SocketAddr,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoverySection {
    /// Addresses dialled at start-up. Further peers are learned from their adverts.
    #[serde(default)]
    pub seeds: Vec<SocketAddr>,
}

/// All durations in milliseconds.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Timeouts {
    pub connect_ms: u64,
    pub handshake_ms: u64,
    pub request_ms: u64,
    pub heartbeat_interval_ms: u64,
    /// A ping not answered within this time marks the peer UNAVAILABLE.
    pub heartbeat_timeout_ms: u64,
    /// The accepting side closes a connection that sends nothing for this long.
    pub idle_timeout_ms: u64,
    pub reconnect_base_ms: u64,
    pub reconnect_max_ms: u64,
    /// Consecutive failed attempts before a dialer gives up until a newer advert arrives
    /// (learned addresses) or slows down to `seed_retry_ms` (seeds).
    pub max_reconnect_attempts: u32,
    /// How often a seed that keeps failing is tried again. Seeds are never given up on.
    pub seed_retry_ms: u64,
    pub advert_refresh_ms: u64,
    /// Adverts not refreshed for this long are dropped (peer forgotten unless linked).
    pub advert_max_age_ms: u64,
    /// Load and link changes are coalesced so a burst of jobs does not flood one advert per job.
    /// This is the pause per 10 links: a node with 150 links waits 15 times as long.
    pub advert_min_interval_ms: u64,
    pub job_retention_ms: u64,
    pub max_job_duration_ms: u64,
    pub shutdown_grace_ms: u64,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect_ms: 2_000,
            handshake_ms: 3_000,
            request_ms: 10_000,
            heartbeat_interval_ms: 1_000,
            heartbeat_timeout_ms: 3_000,
            idle_timeout_ms: 10_000,
            reconnect_base_ms: 100,
            reconnect_max_ms: 2_000,
            max_reconnect_attempts: 30,
            seed_retry_ms: 30_000,
            advert_refresh_ms: 5_000,
            advert_max_age_ms: 30_000,
            advert_min_interval_ms: 100,
            job_retention_ms: 60_000,
            max_job_duration_ms: 300_000,
            shutdown_grace_ms: 3_000,
        }
    }
}

pub fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_inbound_connections: usize,
    pub max_inflight_requests_per_connection: usize,
    pub max_retained_jobs: usize,
    pub max_tracked_remote_jobs: usize,
    pub max_events_per_job: usize,
    pub routing_log_size: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_inbound_connections: 64,
            max_inflight_requests_per_connection: 32,
            max_retained_jobs: 1_000,
            max_tracked_remote_jobs: 1_000,
            max_events_per_job: 100,
            routing_log_size: 100,
        }
    }
}

/// Resource figures advertised to peers. v0.0.1 does not read the host: cpu, memory and
/// availability are MOCK values from this section (adverts carry `mocked: true`).
/// `active_jobs` is always the real number of running jobs.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceConfig {
    pub cpu_percent: u8,
    pub memory_available_mb: u64,
    pub max_concurrent_jobs: u32,
    pub availability: Availability,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            cpu_percent: 0,
            memory_available_mb: 0,
            max_concurrent_jobs: 4,
            availability: Availability::Available,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityConfig {
    pub name: String,
    /// Node ids allowed to call it; `"*"` = any trusted peer. The node itself is always allowed.
    #[serde(default = "allow_all")]
    pub allow: Vec<String>,
}

fn allow_all() -> Vec<String> {
    vec!["*".into()]
}

/// A trusted peer. Give the key inline (`public_key`) or as a file (`public_key_file`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustEntry {
    pub node_id: NodeId,
    #[serde(default)]
    pub public_key: Option<String>,
    #[serde(default)]
    pub public_key_file: Option<PathBuf>,
}

/// A network authority: nodes showing a membership card it signed for `network` are trusted.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityEntry {
    pub network: String,
    pub public_key: String,
}

impl NodeConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("cannot read config {}: {e}", path.display()))?;
        let mut config: NodeConfig =
            toml::from_str(&text).map_err(|e| format!("invalid config {}: {e}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        config.resolve_paths(base);
        Ok(config)
    }

    fn resolve_paths(&mut self, base: &Path) {
        let resolve = |p: &PathBuf| if p.is_absolute() { p.clone() } else { base.join(p) };
        self.node.data_dir = resolve(&self.node.data_dir);
        self.identity.private_key_file = resolve(&self.identity.private_key_file);
        if let Some(card) = &self.identity.card_file {
            self.identity.card_file = Some(resolve(card));
        }
        for entry in &mut self.trust {
            if let Some(file) = &entry.public_key_file {
                entry.public_key_file = Some(resolve(file));
            }
        }
    }

    /// Checks everything that can be checked without starting the node.
    /// Returns every problem at once rather than stopping at the first.
    pub fn validate(&self, policy: AddressPolicy) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        let t = &self.timeouts;

        if let Err(e) = policy.check(&self.node.listen) {
            errors.push(format!("node.listen: {e}"));
        }
        if let Some(api) = &self.api {
            if let Err(e) = policy.check(&api.listen) {
                errors.push(format!("api.listen: {e}"));
            }
        }
        for seed in &self.discovery.seeds {
            if let Err(e) = policy.check(seed) {
                errors.push(format!("discovery.seeds: {e}"));
            }
        }

        let mut seen = std::collections::HashSet::new();
        for cap in &self.capabilities {
            if !is_valid_capability_name(&cap.name) {
                errors.push(format!("capabilities: invalid name {:?}", cap.name));
            }
            if !seen.insert(&cap.name) {
                errors.push(format!("capabilities: {:?} listed twice", cap.name));
            }
            for who in &cap.allow {
                if who != "*" && NodeId::new(who.clone()).is_err() {
                    errors.push(format!("capabilities.{}.allow: invalid node id {who:?}", cap.name));
                }
            }
        }

        if self.resources.max_concurrent_jobs == 0 {
            errors.push("resources.max_concurrent_jobs must be at least 1".into());
        }
        if self.resources.cpu_percent > 100 {
            errors.push("resources.cpu_percent must be 0-100".into());
        }

        let all = [
            ("connect_ms", t.connect_ms),
            ("handshake_ms", t.handshake_ms),
            ("request_ms", t.request_ms),
            ("heartbeat_interval_ms", t.heartbeat_interval_ms),
            ("heartbeat_timeout_ms", t.heartbeat_timeout_ms),
            ("idle_timeout_ms", t.idle_timeout_ms),
            ("reconnect_base_ms", t.reconnect_base_ms),
            ("reconnect_max_ms", t.reconnect_max_ms),
            ("seed_retry_ms", t.seed_retry_ms),
            ("advert_refresh_ms", t.advert_refresh_ms),
            ("advert_max_age_ms", t.advert_max_age_ms),
            ("job_retention_ms", t.job_retention_ms),
            ("max_job_duration_ms", t.max_job_duration_ms),
            ("shutdown_grace_ms", t.shutdown_grace_ms),
        ];
        for (name, value) in all {
            if value == 0 {
                errors.push(format!("timeouts.{name} must be greater than 0"));
            }
        }
        // A healthy dialer pings every heartbeat interval; the acceptor must not cut it off first.
        if t.idle_timeout_ms <= t.heartbeat_interval_ms {
            errors.push("timeouts.idle_timeout_ms must be greater than heartbeat_interval_ms".into());
        }
        if t.advert_max_age_ms <= t.advert_refresh_ms {
            errors.push("timeouts.advert_max_age_ms must be greater than advert_refresh_ms".into());
        }
        if self.limits.max_frame_bytes < 1024 {
            errors.push("limits.max_frame_bytes must be at least 1024".into());
        }

        for entry in &self.trust {
            if entry.public_key.is_some() == entry.public_key_file.is_some() {
                errors.push(format!(
                    "trust {}: set exactly one of public_key / public_key_file",
                    entry.node_id
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn load_trust_store(&self) -> Result<TrustStore, String> {
        let mut store = TrustStore::default();
        for entry in &self.trust {
            let hex = match (&entry.public_key, &entry.public_key_file) {
                (Some(key), None) => key.trim().to_string(),
                (None, Some(file)) => std::fs::read_to_string(file)
                    .map_err(|e| format!("trust {}: cannot read {}: {e}", entry.node_id, file.display()))?
                    .trim()
                    .to_string(),
                _ => {
                    return Err(format!(
                        "trust {}: set exactly one of public_key / public_key_file",
                        entry.node_id
                    ))
                }
            };
            let key =
                parse_verifying_key(&hex).map_err(|e| format!("trust {}: invalid public key: {e}", entry.node_id))?;
            store.insert(entry.node_id.clone(), key);
        }
        for entry in &self.trust_authority {
            let key = parse_verifying_key(entry.public_key.trim())
                .map_err(|e| format!("trust_authority {}: invalid public key: {e}", entry.network))?;
            store.add_authority(Authority {
                network: entry.network.clone(),
                key,
            });
        }
        Ok(store)
    }

    /// This node's membership card, if the config names one.
    pub fn load_card(&self) -> Result<Option<MemberCard>, String> {
        let Some(file) = &self.identity.card_file else {
            return Ok(None);
        };
        let text = std::fs::read_to_string(file).map_err(|e| format!("cannot read card {}: {e}", file.display()))?;
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| format!("invalid card {}: {e}", file.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
        [node]
        id = "node-b"
        name = "Node B"
        type = "server"
        listen = "127.0.0.1:47322"
        data_dir = "data/node-b"

        [identity]
        private_key_file = "keys/node-b.key"

        [[capabilities]]
        name = "math.add"
    "#;

    fn parse(text: &str) -> NodeConfig {
        toml::from_str(text).unwrap()
    }

    #[test]
    fn minimal_config_is_valid_with_defaults() {
        let config = parse(MINIMAL);
        config.validate(AddressPolicy::LoopbackOnly).unwrap();
        assert_eq!(config.capabilities[0].allow, vec!["*"]);
        assert_eq!(config.resources.max_concurrent_jobs, 4);
    }

    #[test]
    fn wildcard_listen_address_is_rejected() {
        let config = parse(&MINIMAL.replace("127.0.0.1:47322", "0.0.0.0:47322"));
        let errors = config.validate(AddressPolicy::LoopbackOnly).unwrap_err();
        assert!(errors.iter().any(|e| e.starts_with("node.listen")), "{errors:?}");
    }

    #[test]
    fn unknown_keys_are_rejected() {
        assert!(toml::from_str::<NodeConfig>(&format!("{MINIMAL}\n[resources]\ncpu = 5\n")).is_err());
    }

    #[test]
    fn inconsistent_timeouts_are_reported_together() {
        let config = parse(&format!(
            "{MINIMAL}\n[timeouts]\nidle_timeout_ms = 500\nheartbeat_interval_ms = 1000\nrequest_ms = 0\n"
        ));
        let errors = config.validate(AddressPolicy::LoopbackOnly).unwrap_err();
        assert_eq!(errors.len(), 2, "{errors:?}");
    }

    #[test]
    fn relative_paths_resolve_against_config_dir() {
        let mut config = parse(MINIMAL);
        config.resolve_paths(Path::new("/sandbox"));
        assert_eq!(config.node.data_dir, Path::new("/sandbox/data/node-b"));
        assert_eq!(config.identity.private_key_file, Path::new("/sandbox/keys/node-b.key"));
    }
}

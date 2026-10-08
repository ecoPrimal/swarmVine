// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Default TTL for gossip entries (max hops before drop).
pub const DEFAULT_TTL: u8 = 8;

/// Default entry lifetime before eviction.
const DEFAULT_EXPIRY: Duration = Duration::from_mins(10);

/// Gossip domain — which layer of the ecosystem this entry belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GossipTopic {
    /// Capability advertisements, topology, reachability.
    Tower,
    /// CAS availability, braid HEADs, depot manifests, content freshness.
    Data,
    /// Resource capacity, build queues, inference bandwidth.
    Compute,
    /// Fleet antibodies, quarantine broadcasts, immune system coordination.
    Defense,
}

impl std::fmt::Display for GossipTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tower => write!(f, "tower"),
            Self::Data => write!(f, "data"),
            Self::Compute => write!(f, "compute"),
            Self::Defense => write!(f, "defense"),
        }
    }
}

impl std::str::FromStr for GossipTopic {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "tower" => Ok(Self::Tower),
            "data" => Ok(Self::Data),
            "compute" => Ok(Self::Compute),
            "defense" => Ok(Self::Defense),
            _ => Err(()),
        }
    }
}

/// A single gossip entry propagated through the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipEntry {
    /// Which gossip domain this entry belongs to.
    pub topic: GossipTopic,
    /// Unique key within the topic (e.g. `capability.advertise:sporeGate:nestGate`).
    pub key: Arc<str>,
    /// Topic-specific JSON payload.
    pub payload: serde_json::Value,
    /// Gate that originally created this entry.
    pub origin_gate: Arc<str>,
    /// Deduplication nonce (16 bytes, hex-encoded for JSON).
    pub nonce: String,
    /// Remaining hops before this entry is dropped.
    pub ttl: u8,
    /// Monotonic version for conflict resolution (higher wins).
    pub version: u64,
    /// When the entry was created (epoch seconds).
    pub created_at_epoch: u64,
    /// When the entry should be evicted (epoch seconds).
    pub expires_at_epoch: u64,
}

impl GossipEntry {
    /// Create a new gossip entry originating from this gate.
    #[must_use]
    pub fn new(
        topic: GossipTopic,
        key: impl Into<Arc<str>>,
        payload: serde_json::Value,
        origin_gate: impl Into<Arc<str>>,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let nonce = generate_nonce();
        Self {
            topic,
            key: key.into(),
            payload,
            origin_gate: origin_gate.into(),
            nonce,
            ttl: DEFAULT_TTL,
            version: now,
            created_at_epoch: now,
            expires_at_epoch: now + DEFAULT_EXPIRY.as_secs(),
        }
    }

    /// Whether this entry has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= self.expires_at_epoch
    }

    /// Whether this entry has any hops remaining.
    #[must_use]
    pub const fn has_hops(&self) -> bool {
        self.ttl > 0
    }

    /// Create a forwarding copy with decremented TTL.
    #[must_use]
    pub fn for_forwarding(&self) -> Option<Self> {
        if self.ttl == 0 {
            return None;
        }
        let mut copy = self.clone();
        copy.ttl -= 1;
        Some(copy)
    }
}

/// Result of attempting to ingest a gossip entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestResult {
    /// Entry accepted and stored (new or updated).
    Accepted,
    /// Entry is a duplicate (nonce already seen).
    Duplicate,
    /// Entry has expired TTL.
    Expired,
    /// Entry failed validation.
    Rejected,
    /// Topic store is at capacity.
    AtCapacity,
}

/// Per-peer exchange statistics with gravitational distance metrics.
///
/// Every node in the mesh is its own nucleus. The `gravity` field
/// represents the relative "pull" of this peer — a function of latency,
/// exchange volume, and reliability. Higher gravity = closer peer in
/// the mesh topology. The epidemic loop can use gravity to prioritize
/// which peers receive gossip first (closer peers get critical entries
/// faster, cross-protecting against partition).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerStats {
    /// Total entries received from this peer.
    pub entries_received: u64,
    /// Total entries sent to this peer.
    pub entries_sent: u64,
    /// Last exchange timestamp (epoch seconds).
    pub last_exchange_epoch: u64,
    /// Entries rejected from this peer (duplicates, expired, invalid).
    pub entries_rejected: u64,
    /// Last measured round-trip latency in milliseconds.
    /// Updated on every successful gossip exchange.
    pub last_rtt_ms: u64,
    /// Exponentially weighted moving average of RTT (milliseconds).
    /// Smooths out jitter for routing decisions.
    pub ewma_rtt_ms: u64,
    /// Gravitational force — higher = closer/more reliable peer.
    /// Computed as: `1_000_000 / max(1, ewma_rtt_ms) * reliability_factor`
    /// where `reliability_factor = sent / max(1, sent + failed)`.
    pub gravity: u64,
    /// Count of failed exchange attempts (TCP + relay both failed).
    pub exchange_failures: u64,
    /// Whether this peer was reached via songBird relay (indirect path).
    pub via_relay: bool,
}

/// Gossip table statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipStats {
    /// Entries per topic.
    pub tower_entries: usize,
    /// Entries per topic.
    pub data_entries: usize,
    /// Entries per topic.
    pub compute_entries: usize,
    /// Entries per topic.
    pub defense_entries: usize,
    /// Total nonces in dedup history.
    pub nonce_history_size: usize,
    /// Known gossip peers.
    pub peer_count: usize,
    /// Total entries ingested since startup.
    pub total_ingested: u64,
    /// Total entries evicted since startup.
    pub total_evicted: u64,
}

/// Notification emitted to subscribers when a gossip entry is ingested.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipNotification {
    /// Which topic the entry belongs to.
    pub topic: GossipTopic,
    /// The entry key.
    pub key: Arc<str>,
    /// The originating gate.
    pub origin_gate: Arc<str>,
    /// Entry version (monotonic).
    pub version: u64,
}

/// Generate a unique nonce from timestamp, process id, and a global counter.
fn generate_nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = u64::from(std::process::id());
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{ts:032x}{pid:08x}{seq:08x}")
}

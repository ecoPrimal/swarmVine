//! Epidemic gossip engine — core types and in-memory store.
//!
//! Three gossip domains:
//! - **Tower**: capability advertisements, topology, reachability
//! - **Data**: CAS availability, braid HEADs, depot manifests, content freshness
//! - **Compute**: resource capacity, build queues, inference bandwidth

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Default TTL for gossip entries (max hops before drop).
const DEFAULT_TTL: u8 = 8;

/// Default entry lifetime before eviction.
const DEFAULT_EXPIRY: Duration = Duration::from_secs(600);

/// Maximum entries per topic to prevent memory exhaustion.
const MAX_ENTRIES_PER_TOPIC: usize = 1024;

/// Maximum nonce history for deduplication.
const MAX_NONCE_HISTORY: usize = 4096;

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
}

impl std::fmt::Display for GossipTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tower => write!(f, "tower"),
            Self::Data => write!(f, "data"),
            Self::Compute => write!(f, "compute"),
        }
    }
}

/// A single gossip entry propagated through the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipEntry {
    /// Which gossip domain this entry belongs to.
    pub topic: GossipTopic,
    /// Unique key within the topic (e.g. `capability.advertise:sporeGate:nestGate`).
    pub key: String,
    /// Topic-specific JSON payload.
    pub payload: serde_json::Value,
    /// Gate that originally created this entry.
    pub origin_gate: String,
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
    pub fn new(
        topic: GossipTopic,
        key: String,
        payload: serde_json::Value,
        origin_gate: String,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let nonce = generate_nonce();
        Self {
            topic,
            key,
            payload,
            origin_gate,
            nonce,
            ttl: DEFAULT_TTL,
            version: now,
            created_at_epoch: now,
            expires_at_epoch: now + DEFAULT_EXPIRY.as_secs(),
        }
    }

    /// Whether this entry has expired.
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= self.expires_at_epoch
    }

    /// Whether this entry has any hops remaining.
    pub fn has_hops(&self) -> bool {
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

/// Per-peer exchange statistics.
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
    /// Total nonces in dedup history.
    pub nonce_history_size: usize,
    /// Known gossip peers.
    pub peer_count: usize,
    /// Total entries ingested since startup.
    pub total_ingested: u64,
    /// Total entries evicted since startup.
    pub total_evicted: u64,
}

/// The gossip engine — stores entries, handles dedup, eviction, and forwarding.
pub struct GossipEngine {
    /// Our gate identity.
    node_id: String,
    /// Entries indexed by topic → key.
    entries: Arc<RwLock<HashMap<GossipTopic, HashMap<String, GossipEntry>>>>,
    /// Nonce deduplication set.
    seen_nonces: Arc<RwLock<Vec<String>>>,
    /// Per-peer exchange statistics.
    peer_stats: Arc<RwLock<HashMap<String, PeerStats>>>,
    /// Entries pending forwarding to peers.
    forward_queue: Arc<RwLock<Vec<GossipEntry>>>,
    /// Counters.
    total_ingested: Arc<std::sync::atomic::AtomicU64>,
    total_evicted: Arc<std::sync::atomic::AtomicU64>,
}

impl GossipEngine {
    /// Create a new gossip engine for the given gate.
    pub fn new(node_id: String) -> Self {
        let mut entries = HashMap::new();
        entries.insert(GossipTopic::Tower, HashMap::new());
        entries.insert(GossipTopic::Data, HashMap::new());
        entries.insert(GossipTopic::Compute, HashMap::new());

        Self {
            node_id,
            entries: Arc::new(RwLock::new(entries)),
            seen_nonces: Arc::new(RwLock::new(Vec::new())),
            peer_stats: Arc::new(RwLock::new(HashMap::new())),
            forward_queue: Arc::new(RwLock::new(Vec::new())),
            total_ingested: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            total_evicted: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Our node identity.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Ingest a gossip entry received from a peer (or locally injected).
    ///
    /// Returns the ingest result indicating whether the entry was accepted.
    pub async fn ingest(&self, entry: GossipEntry, from_peer: Option<&str>) -> IngestResult {
        if entry.is_expired() {
            if let Some(peer) = from_peer {
                self.record_rejection(peer).await;
            }
            return IngestResult::Expired;
        }

        if !entry.has_hops() && from_peer.is_some() {
            if let Some(peer) = from_peer {
                self.record_rejection(peer).await;
            }
            return IngestResult::Expired;
        }

        // Nonce dedup check
        {
            let nonces = self.seen_nonces.read().await;
            if nonces.iter().any(|n| n == &entry.nonce) {
                if let Some(peer) = from_peer {
                    self.record_rejection(peer).await;
                }
                return IngestResult::Duplicate;
            }
        }

        // Validate key format
        if entry.key.is_empty() || entry.key.len() > 256 || entry.key.chars().any(char::is_control)
        {
            if let Some(peer) = from_peer {
                self.record_rejection(peer).await;
            }
            return IngestResult::Rejected;
        }

        // Store the entry
        let should_forward = {
            let mut store = self.entries.write().await;
            let topic_store = store.entry(entry.topic).or_default();

            if topic_store.len() >= MAX_ENTRIES_PER_TOPIC
                && !topic_store.contains_key(&entry.key)
            {
                return IngestResult::AtCapacity;
            }

            let accepted = match topic_store.get(&entry.key) {
                Some(existing) if existing.version >= entry.version => false,
                _ => {
                    topic_store.insert(entry.key.clone(), entry.clone());
                    true
                }
            };

            accepted
        };

        if !should_forward {
            if let Some(peer) = from_peer {
                self.record_rejection(peer).await;
            }
            return IngestResult::Duplicate;
        }

        // Record nonce
        {
            let mut nonces = self.seen_nonces.write().await;
            nonces.push(entry.nonce.clone());
            if nonces.len() > MAX_NONCE_HISTORY {
                let drain_count = nonces.len() - MAX_NONCE_HISTORY;
                nonces.drain(..drain_count);
            }
        }

        // Queue for forwarding (if from a peer and has hops remaining)
        if from_peer.is_some() {
            if let Some(forward_entry) = entry.for_forwarding() {
                self.forward_queue.write().await.push(forward_entry);
            }
        }

        // Record peer stats
        if let Some(peer) = from_peer {
            self.record_receive(peer).await;
        }

        self.total_ingested
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        debug!(
            topic = %entry.topic,
            key = %entry.key,
            origin = %entry.origin_gate,
            ttl = entry.ttl,
            "Gossip entry ingested"
        );

        IngestResult::Accepted
    }

    /// Inject a locally-originated gossip entry.
    ///
    /// Creates a new entry with this gate as origin and ingests it.
    pub async fn inject(
        &self,
        topic: GossipTopic,
        key: String,
        payload: serde_json::Value,
    ) -> IngestResult {
        let entry = GossipEntry::new(topic, key, payload, self.node_id.clone());

        // Queue for forwarding to all peers (local origin = full TTL)
        if let Some(forward_entry) = entry.for_forwarding() {
            self.forward_queue.write().await.push(forward_entry);
        }

        self.ingest(entry, None).await
    }

    /// Query the gossip table by topic and optional key prefix.
    pub async fn query(
        &self,
        topic: GossipTopic,
        key_prefix: Option<&str>,
    ) -> Vec<GossipEntry> {
        let store = self.entries.read().await;
        let Some(topic_store) = store.get(&topic) else {
            return Vec::new();
        };

        topic_store
            .values()
            .filter(|e| {
                if e.is_expired() {
                    return false;
                }
                match key_prefix {
                    Some(prefix) => e.key.starts_with(prefix),
                    None => true,
                }
            })
            .cloned()
            .collect()
    }

    /// Drain the forward queue — returns entries that should be sent to peers.
    pub async fn drain_forward_queue(&self) -> Vec<GossipEntry> {
        let mut queue = self.forward_queue.write().await;
        std::mem::take(&mut *queue)
    }

    /// Evict expired entries from all topics.
    ///
    /// Returns the number of entries evicted.
    pub async fn evict_expired(&self) -> usize {
        let mut store = self.entries.write().await;
        let mut total_evicted = 0usize;

        for (topic, topic_store) in store.iter_mut() {
            let before = topic_store.len();
            topic_store.retain(|_, entry| !entry.is_expired());
            let evicted = before - topic_store.len();
            if evicted > 0 {
                info!(topic = %topic, evicted, remaining = topic_store.len(), "Evicted stale gossip entries");
                total_evicted += evicted;
            }
        }

        self.total_evicted
            .fetch_add(total_evicted as u64, std::sync::atomic::Ordering::Relaxed);

        total_evicted
    }

    /// Get current gossip table statistics.
    pub async fn stats(&self) -> GossipStats {
        let store = self.entries.read().await;
        let nonces = self.seen_nonces.read().await;
        let peers = self.peer_stats.read().await;

        GossipStats {
            tower_entries: store
                .get(&GossipTopic::Tower)
                .map_or(0, HashMap::len),
            data_entries: store.get(&GossipTopic::Data).map_or(0, HashMap::len),
            compute_entries: store
                .get(&GossipTopic::Compute)
                .map_or(0, HashMap::len),
            nonce_history_size: nonces.len(),
            peer_count: peers.len(),
            total_ingested: self
                .total_ingested
                .load(std::sync::atomic::Ordering::Relaxed),
            total_evicted: self
                .total_evicted
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Get per-peer exchange statistics.
    pub async fn peer_stats(&self) -> HashMap<String, PeerStats> {
        self.peer_stats.read().await.clone()
    }

    async fn record_receive(&self, peer: &str) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut stats = self.peer_stats.write().await;
        let entry = stats.entry(peer.to_owned()).or_default();
        entry.entries_received += 1;
        entry.last_exchange_epoch = now;
    }

    async fn record_rejection(&self, peer: &str) {
        let mut stats = self.peer_stats.write().await;
        let entry = stats.entry(peer.to_owned()).or_default();
        entry.entries_rejected += 1;
    }

    /// Record that we sent entries to a peer.
    pub async fn record_send(&self, peer: &str, count: u64) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut stats = self.peer_stats.write().await;
        let entry = stats.entry(peer.to_owned()).or_default();
        entry.entries_sent += count;
        entry.last_exchange_epoch = now;
    }
}

/// Generate a random 16-byte nonce as a hex string.
fn generate_nonce() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    SystemTime::now().hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    let h1 = hasher.finish();

    let mut hasher2 = DefaultHasher::new();
    h1.hash(&mut hasher2);
    std::thread::current().id().hash(&mut hasher2);
    let h2 = hasher2.finish();

    format!("{h1:016x}{h2:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_engine() -> GossipEngine {
        GossipEngine::new("testGate".to_owned())
    }

    #[tokio::test]
    async fn inject_and_query() {
        let engine = test_engine();
        let result = engine
            .inject(
                GossipTopic::Tower,
                "capability.advertise:testGate:nestGate".to_owned(),
                serde_json::json!({"capabilities": ["content.get", "content.put"]}),
            )
            .await;
        assert_eq!(result, IngestResult::Accepted);

        let entries = engine.query(GossipTopic::Tower, None).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].origin_gate, "testGate");
    }

    #[tokio::test]
    async fn query_with_prefix() {
        let engine = test_engine();
        engine
            .inject(
                GossipTopic::Tower,
                "capability.advertise:gate1:primal1".to_owned(),
                serde_json::json!({}),
            )
            .await;
        engine
            .inject(
                GossipTopic::Tower,
                "topology.reachability:gate1".to_owned(),
                serde_json::json!({}),
            )
            .await;

        let cap_entries = engine
            .query(GossipTopic::Tower, Some("capability."))
            .await;
        assert_eq!(cap_entries.len(), 1);

        let all_entries = engine.query(GossipTopic::Tower, None).await;
        assert_eq!(all_entries.len(), 2);
    }

    #[tokio::test]
    async fn duplicate_nonce_rejected() {
        let engine = test_engine();
        let entry = GossipEntry::new(
            GossipTopic::Data,
            "cas.have:testGate".to_owned(),
            serde_json::json!({}),
            "testGate".to_owned(),
        );

        let r1 = engine.ingest(entry.clone(), Some("peerA")).await;
        assert_eq!(r1, IngestResult::Accepted);

        let r2 = engine.ingest(entry, Some("peerB")).await;
        assert_eq!(r2, IngestResult::Duplicate);
    }

    #[tokio::test]
    async fn expired_entry_rejected() {
        let engine = test_engine();
        let mut entry = GossipEntry::new(
            GossipTopic::Compute,
            "compute.capacity:testGate".to_owned(),
            serde_json::json!({}),
            "testGate".to_owned(),
        );
        entry.expires_at_epoch = 0; // expired in the past

        let result = engine.ingest(entry, Some("peerA")).await;
        assert_eq!(result, IngestResult::Expired);
    }

    #[tokio::test]
    async fn version_conflict_resolution() {
        let engine = test_engine();

        let mut old = GossipEntry::new(
            GossipTopic::Tower,
            "capability.advertise:gate1:primal1".to_owned(),
            serde_json::json!({"version": "old"}),
            "gate1".to_owned(),
        );
        old.version = 1;
        engine.ingest(old, None).await;

        let mut new = GossipEntry::new(
            GossipTopic::Tower,
            "capability.advertise:gate1:primal1".to_owned(),
            serde_json::json!({"version": "new"}),
            "gate1".to_owned(),
        );
        new.version = 2;
        engine.ingest(new, None).await;

        let entries = engine.query(GossipTopic::Tower, None).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload["version"], "new");
    }

    #[tokio::test]
    async fn forward_queue_populated_on_peer_ingest() {
        let engine = test_engine();
        let entry = GossipEntry::new(
            GossipTopic::Data,
            "braid.head:gate1".to_owned(),
            serde_json::json!({"sha": "abc123"}),
            "gate1".to_owned(),
        );

        engine.ingest(entry, Some("gate1")).await;
        let queued = engine.drain_forward_queue().await;
        assert_eq!(queued.len(), 1);
        assert!(queued[0].ttl < DEFAULT_TTL);
    }

    #[tokio::test]
    async fn stats_reflect_state() {
        let engine = test_engine();
        engine
            .inject(
                GossipTopic::Tower,
                "t1".to_owned(),
                serde_json::json!({}),
            )
            .await;
        engine
            .inject(
                GossipTopic::Data,
                "d1".to_owned(),
                serde_json::json!({}),
            )
            .await;
        engine
            .inject(
                GossipTopic::Data,
                "d2".to_owned(),
                serde_json::json!({}),
            )
            .await;

        let stats = engine.stats().await;
        assert_eq!(stats.tower_entries, 1);
        assert_eq!(stats.data_entries, 2);
        assert_eq!(stats.compute_entries, 0);
        assert_eq!(stats.total_ingested, 3);
    }

    #[tokio::test]
    async fn evict_expired_removes_stale() {
        let engine = test_engine();
        let mut entry = GossipEntry::new(
            GossipTopic::Tower,
            "stale".to_owned(),
            serde_json::json!({}),
            "gate1".to_owned(),
        );
        entry.expires_at_epoch = 0;

        // Force insert bypassing the ingest expiry check
        {
            let mut store = engine.entries.write().await;
            store
                .entry(GossipTopic::Tower)
                .or_default()
                .insert("stale".to_owned(), entry);
        }

        let evicted = engine.evict_expired().await;
        assert_eq!(evicted, 1);

        let entries = engine.query(GossipTopic::Tower, None).await;
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn peer_stats_tracked() {
        let engine = test_engine();
        let entry = GossipEntry::new(
            GossipTopic::Tower,
            "test".to_owned(),
            serde_json::json!({}),
            "gate1".to_owned(),
        );

        engine.ingest(entry, Some("gate1")).await;

        let stats = engine.peer_stats().await;
        assert!(stats.contains_key("gate1"));
        assert_eq!(stats["gate1"].entries_received, 1);
    }
}

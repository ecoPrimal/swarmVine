//! Epidemic gossip engine — core types and in-memory store.
//!
//! Three gossip domains:
//! - **Tower**: capability advertisements, topology, reachability
//! - **Data**: CAS availability, braid HEADs, depot manifests, content freshness
//! - **Compute**: resource capacity, build queues, inference bandwidth

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, broadcast};
use tracing::{debug, info};

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
    #[must_use]
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

/// Notification emitted to subscribers when a gossip entry is ingested.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipNotification {
    /// Which topic the entry belongs to.
    pub topic: GossipTopic,
    /// The entry key.
    pub key: String,
    /// The originating gate.
    pub origin_gate: String,
    /// Entry version (monotonic).
    pub version: u64,
}

/// Broadcast channel capacity for gossip subscriptions.
const SUBSCRIPTION_CAPACITY: usize = 256;

/// The gossip engine — stores entries, handles dedup, eviction, and forwarding.
pub struct GossipEngine {
    /// Our gate identity.
    node_id: String,
    /// Entries indexed by topic → key.
    entries: Arc<RwLock<HashMap<GossipTopic, HashMap<String, GossipEntry>>>>,
    /// Nonce deduplication set (O(1) lookup).
    nonce_set: Arc<RwLock<HashSet<String>>>,
    /// FIFO order for nonce eviction.
    nonce_order: Arc<RwLock<VecDeque<String>>>,
    /// Per-peer exchange statistics.
    peer_stats: Arc<RwLock<HashMap<String, PeerStats>>>,
    /// Entries pending forwarding to peers.
    forward_queue: Arc<RwLock<Vec<GossipEntry>>>,
    /// Counters.
    total_ingested: Arc<std::sync::atomic::AtomicU64>,
    total_evicted: Arc<std::sync::atomic::AtomicU64>,
    /// Broadcast sender for subscription notifications.
    subscription_tx: broadcast::Sender<GossipNotification>,
}

impl GossipEngine {
    /// Create a new gossip engine for the given gate.
    #[must_use]
    pub fn new(node_id: String) -> Self {
        let mut entries = HashMap::new();
        entries.insert(GossipTopic::Tower, HashMap::new());
        entries.insert(GossipTopic::Data, HashMap::new());
        entries.insert(GossipTopic::Compute, HashMap::new());

        let (subscription_tx, _) = broadcast::channel(SUBSCRIPTION_CAPACITY);

        Self {
            node_id,
            entries: Arc::new(RwLock::new(entries)),
            nonce_set: Arc::new(RwLock::new(HashSet::new())),
            nonce_order: Arc::new(RwLock::new(VecDeque::new())),
            peer_stats: Arc::new(RwLock::new(HashMap::new())),
            forward_queue: Arc::new(RwLock::new(Vec::new())),
            total_ingested: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            total_evicted: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            subscription_tx,
        }
    }

    /// Our node identity.
    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Subscribe to gossip notifications.
    ///
    /// Returns a receiver that yields a [`GossipNotification`] for every
    /// successfully ingested entry. Subscribers that fall behind by more
    /// than [`SUBSCRIPTION_CAPACITY`] entries will receive a `Lagged` error
    /// and can resume from the current position.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<GossipNotification> {
        self.subscription_tx.subscribe()
    }

    /// Number of active subscribers on the broadcast channel.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.subscription_tx.receiver_count()
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
            let nonces = self.nonce_set.read().await;
            if nonces.contains(&entry.nonce) {
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

            if topic_store.len() >= MAX_ENTRIES_PER_TOPIC && !topic_store.contains_key(&entry.key) {
                return IngestResult::AtCapacity;
            }

            let result = match topic_store.get(&entry.key) {
                Some(existing) if existing.version >= entry.version => false,
                _ => {
                    topic_store.insert(entry.key.clone(), entry.clone());
                    true
                }
            };
            drop(store);
            result
        };

        if !should_forward {
            if let Some(peer) = from_peer {
                self.record_rejection(peer).await;
            }
            return IngestResult::Duplicate;
        }

        // Record nonce and evict oldest if over capacity.
        // Both locks are needed: set for O(1) contains, order for FIFO eviction.
        let evict_nonce = {
            let mut nonce_order = self.nonce_order.write().await;
            nonce_order.push_back(entry.nonce.clone());
            if nonce_order.len() > MAX_NONCE_HISTORY {
                nonce_order.pop_front()
            } else {
                None
            }
        };
        {
            let mut nonce_set = self.nonce_set.write().await;
            nonce_set.insert(entry.nonce.clone());
            if let Some(old) = evict_nonce {
                nonce_set.remove(&old);
            }
            drop(nonce_set);
        }

        // Queue for forwarding (if from a peer and has hops remaining)
        if from_peer.is_some()
            && let Some(forward_entry) = entry.for_forwarding()
        {
            self.forward_queue.write().await.push(forward_entry);
        }

        // Record peer stats
        if let Some(peer) = from_peer {
            self.record_receive(peer).await;
        }

        self.total_ingested
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Notify subscribers (best-effort: if no one is listening, this is a no-op).
        let _ = self.subscription_tx.send(GossipNotification {
            topic: entry.topic,
            key: entry.key.clone(),
            origin_gate: entry.origin_gate.clone(),
            version: entry.version,
        });

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
    pub async fn query(&self, topic: GossipTopic, key_prefix: Option<&str>) -> Vec<GossipEntry> {
        let store = self.entries.read().await;
        let Some(topic_store) = store.get(&topic) else {
            return Vec::new();
        };

        let entries = topic_store
            .values()
            .filter(|e| {
                !e.is_expired() && key_prefix.is_none_or(|prefix| e.key.starts_with(prefix))
            })
            .cloned()
            .collect();
        drop(store);
        entries
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

        drop(store);

        self.total_evicted
            .fetch_add(total_evicted as u64, std::sync::atomic::Ordering::Relaxed);

        total_evicted
    }

    /// Get current gossip table statistics.
    pub async fn stats(&self) -> GossipStats {
        let (tower_entries, data_entries, compute_entries) = {
            let store = self.entries.read().await;
            (
                store.get(&GossipTopic::Tower).map_or(0, HashMap::len),
                store.get(&GossipTopic::Data).map_or(0, HashMap::len),
                store.get(&GossipTopic::Compute).map_or(0, HashMap::len),
            )
        };
        let nonce_history_size = self.nonce_set.read().await.len();
        let peer_count = self.peer_stats.read().await.len();

        GossipStats {
            tower_entries,
            data_entries,
            compute_entries,
            nonce_history_size,
            peer_count,
            total_ingested: self
                .total_ingested
                .load(std::sync::atomic::Ordering::Relaxed),
            total_evicted: self
                .total_evicted
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Get per-peer exchange statistics.
    ///
    /// Clones the full map because callers need an owned snapshot and this is
    /// invoked infrequently (diagnostics / RPC); holding a read lock across
    /// serialization would be worse.
    pub async fn peer_stats(&self) -> HashMap<String, PeerStats> {
        self.peer_stats.read().await.clone()
    }

    async fn record_receive(&self, peer: &str) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut guard = self.peer_stats.write().await;
        let entry = guard.entry(peer.to_owned()).or_default();
        entry.entries_received += 1;
        entry.last_exchange_epoch = now;
        drop(guard);
    }

    async fn record_rejection(&self, peer: &str) {
        let mut guard = self.peer_stats.write().await;
        let entry = guard.entry(peer.to_owned()).or_default();
        entry.entries_rejected += 1;
        drop(guard);
    }

    /// Record that we sent entries to a peer.
    pub async fn record_send(&self, peer: &str, count: u64) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut guard = self.peer_stats.write().await;
        let entry = guard.entry(peer.to_owned()).or_default();
        entry.entries_sent += count;
        entry.last_exchange_epoch = now;
        drop(guard);
    }
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

// --- Phase 4: Domain-specific payload types ---

/// Bloom filter for compact set-membership queries (Data domain: `cas.have`).
///
/// Fixed-size bit array with `k` hash functions. Allows a gate to advertise
/// which CAS objects it holds without transmitting a full hash list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Bit array encoded as hex string for JSON transport.
    pub bits: Vec<u8>,
    /// Number of hash functions used.
    pub k: u8,
    /// Number of items inserted (for false-positive rate estimation).
    pub item_count: u32,
}

impl BloomFilter {
    /// Create a new bloom filter with the given bit size (in bytes) and hash count.
    #[must_use]
    pub fn new(byte_size: usize, k: u8) -> Self {
        Self {
            bits: vec![0u8; byte_size],
            k,
            item_count: 0,
        }
    }

    /// Insert a key into the filter using FNV-1a inspired multi-hashing.
    pub fn insert(&mut self, key: &[u8]) {
        let bit_count = self.bits.len() * 8;
        if bit_count == 0 {
            return;
        }
        let (h1, h2) = double_hash(key);
        for i in 0..u64::from(self.k) {
            let idx = (h1.wrapping_add(i.wrapping_mul(h2))) % (bit_count as u64);
            let byte_idx = (idx / 8) as usize;
            self.bits[byte_idx] |= 1 << (idx % 8);
        }
        self.item_count += 1;
    }

    /// Check whether a key might be in the filter (false positives possible).
    #[must_use]
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        let bit_count = self.bits.len() * 8;
        if bit_count == 0 {
            return false;
        }
        let (h1, h2) = double_hash(key);
        for i in 0..u64::from(self.k) {
            let idx = (h1.wrapping_add(i.wrapping_mul(h2))) % (bit_count as u64);
            let byte_idx = (idx / 8) as usize;
            if self.bits[byte_idx] & (1 << (idx % 8)) == 0 {
                return false;
            }
        }
        true
    }
}

/// Simple double-hash for bloom filter indexing (FNV-1a based, no external dep).
fn double_hash(key: &[u8]) -> (u64, u64) {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0100_0000_01b3;

    let mut h1 = FNV_OFFSET;
    for &b in key {
        h1 ^= u64::from(b);
        h1 = h1.wrapping_mul(FNV_PRIME);
    }
    let mut h2 = FNV_OFFSET ^ 0x5555_5555_5555_5555;
    for &b in key {
        h2 ^= u64::from(b);
        h2 = h2.wrapping_mul(FNV_PRIME);
    }
    (h1, h2)
}

/// Compute capacity announcement (Compute domain: `compute.capacity`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeCapacity {
    /// Available GPU slots (0 = no GPU).
    pub gpu_slots: u32,
    /// CPU cores available for scheduling.
    pub cpu_cores: u32,
    /// Free memory in megabytes.
    pub memory_mb: u64,
    /// Number of builds currently queued.
    pub build_queue_depth: u32,
    /// Estimated time to drain build queue (seconds).
    pub build_queue_eta_secs: u64,
    /// Inference throughput (tokens per second, 0 = no inference).
    pub inference_tokens_per_sec: u32,
}

/// Depot manifest entry for efficient binary distribution gossip (Data domain: `depot.manifest`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepotManifestEntry {
    /// Primal binary name (e.g. `swarmvine`).
    pub primal: String,
    /// Binary version string.
    pub version: String,
    /// Target triple (e.g. `x86_64-unknown-linux-musl`).
    pub target: String,
    /// BLAKE3 hash of the binary.
    pub blake3: String,
    /// Binary size in bytes.
    pub size_bytes: u64,
}

/// A gate's depot manifest — all binaries available at that gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepotManifest {
    /// Gate that owns this manifest.
    pub gate: String,
    /// Timestamp of last depot update (epoch seconds).
    pub updated_epoch: u64,
    /// Binaries available.
    pub entries: Vec<DepotManifestEntry>,
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

        let cap_entries = engine.query(GossipTopic::Tower, Some("capability.")).await;
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
            .inject(GossipTopic::Tower, "t1".to_owned(), serde_json::json!({}))
            .await;
        engine
            .inject(GossipTopic::Data, "d1".to_owned(), serde_json::json!({}))
            .await;
        engine
            .inject(GossipTopic::Data, "d2".to_owned(), serde_json::json!({}))
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

    #[tokio::test]
    async fn subscribe_receives_notification() {
        let engine = test_engine();
        let mut rx = engine.subscribe();
        assert_eq!(engine.subscriber_count(), 1);

        engine
            .inject(
                GossipTopic::Tower,
                "capability.advertise:testGate:primal".to_owned(),
                serde_json::json!({"cap": "test"}),
            )
            .await;

        let notification = rx.try_recv().unwrap();
        assert_eq!(notification.topic, GossipTopic::Tower);
        assert!(notification.key.starts_with("capability.advertise"));
        assert_eq!(notification.origin_gate, "testGate");
    }

    #[tokio::test]
    async fn subscribe_no_notification_on_duplicate() {
        let engine = test_engine();
        let entry = GossipEntry::new(
            GossipTopic::Data,
            "cas.have:gate1".to_owned(),
            serde_json::json!({}),
            "gate1".to_owned(),
        );

        engine.ingest(entry.clone(), None).await;
        let mut rx = engine.subscribe();
        // Second ingest with same nonce → duplicate, no notification
        engine.ingest(entry, None).await;
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn bloom_filter_insert_and_query() {
        let mut bf = BloomFilter::new(64, 3);
        bf.insert(b"hash_abc123");
        bf.insert(b"hash_def456");

        assert!(bf.maybe_contains(b"hash_abc123"));
        assert!(bf.maybe_contains(b"hash_def456"));
        assert!(!bf.maybe_contains(b"hash_never_inserted"));
        assert_eq!(bf.item_count, 2);
    }

    #[test]
    fn bloom_filter_empty_returns_false() {
        let bf = BloomFilter::new(0, 3);
        assert!(!bf.maybe_contains(b"anything"));
    }

    #[test]
    fn bloom_filter_serde_roundtrip() {
        let mut bf = BloomFilter::new(32, 4);
        bf.insert(b"test_hash");
        let json = serde_json::to_string(&bf).unwrap();
        let decoded: BloomFilter = serde_json::from_str(&json).unwrap();
        assert!(decoded.maybe_contains(b"test_hash"));
        assert_eq!(decoded.item_count, 1);
        assert_eq!(decoded.k, 4);
    }

    #[test]
    fn compute_capacity_serde() {
        let cap = ComputeCapacity {
            gpu_slots: 4,
            cpu_cores: 16,
            memory_mb: 32768,
            build_queue_depth: 2,
            build_queue_eta_secs: 300,
            inference_tokens_per_sec: 1500,
        };
        let json = serde_json::to_string(&cap).unwrap();
        let decoded: ComputeCapacity = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.gpu_slots, 4);
        assert_eq!(decoded.inference_tokens_per_sec, 1500);
    }

    #[test]
    fn depot_manifest_serde() {
        let manifest = DepotManifest {
            gate: "sporeGate".to_owned(),
            updated_epoch: 1_723_200_000,
            entries: vec![DepotManifestEntry {
                primal: "swarmvine".to_owned(),
                version: "0.1.0".to_owned(),
                target: "x86_64-unknown-linux-musl".to_owned(),
                blake3: "abc123def456".to_owned(),
                size_bytes: 2_400_000,
            }],
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let decoded: DepotManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.gate, "sporeGate");
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].primal, "swarmvine");
    }
}

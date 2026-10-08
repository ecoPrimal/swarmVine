// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! Epidemic gossip engine — core types and in-memory store.
//!
//! Three gossip domains:
//! - **Tower**: capability advertisements, topology, reachability
//! - **Data**: CAS availability, braid HEADs, depot manifests, content freshness
//! - **Compute**: resource capacity, build queues, inference bandwidth

pub use crate::gossip_types::*;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{RwLock, broadcast};
use tracing::{debug, info};

/// Maximum entries per topic to prevent memory exhaustion.
const MAX_ENTRIES_PER_TOPIC: usize = 1024;

/// Maximum nonce history for deduplication.
const MAX_NONCE_HISTORY: usize = 4096;

/// Broadcast channel capacity for gossip subscriptions.
const SUBSCRIPTION_CAPACITY: usize = 256;

type TopicStore = HashMap<Arc<str>, GossipEntry>;
type GossipTable = HashMap<GossipTopic, TopicStore>;

/// The gossip engine — stores entries, handles dedup, eviction, and forwarding.
pub struct GossipEngine {
    /// Our gate identity.
    node_id: String,
    /// Entries indexed by topic → key.
    entries: Arc<RwLock<GossipTable>>,
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
        entries.insert(GossipTopic::Defense, HashMap::new());

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
    /// than 256 entries will receive a `Lagged` error and can resume from
    /// the current position.
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

        if !entry.has_hops()
            && let Some(peer) = from_peer
        {
            self.record_rejection(peer).await;
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

        // Store the entry (defense-aware merge for opsonize tags)
        let should_forward = {
            let mut store = self.entries.write().await;
            let topic_store = store.entry(entry.topic).or_default();

            if topic_store.len() >= MAX_ENTRIES_PER_TOPIC && !topic_store.contains_key(&entry.key) {
                return IngestResult::AtCapacity;
            }

            let result = match topic_store.get(&entry.key) {
                Some(existing) if existing.version >= entry.version => {
                    // Defense opsonize tags: merge detectors + bump confidence
                    // even if version is not newer (corroboration from another gate)
                    if entry.topic == GossipTopic::Defense
                        && entry.key.starts_with("defense.opsonize:")
                        && existing.origin_gate != entry.origin_gate
                    {
                        let merged = merge_opsonize_payload(existing, &entry);
                        topic_store.insert(Arc::clone(&entry.key), merged);
                        true
                    } else {
                        false
                    }
                }
                _ => {
                    // Defense merge: also check if we're updating an existing tag
                    if entry.topic == GossipTopic::Defense
                        && entry.key.starts_with("defense.opsonize:")
                    {
                        if let Some(existing) = topic_store.get(&entry.key) {
                            let merged = merge_opsonize_payload(existing, &entry);
                            topic_store.insert(Arc::clone(&entry.key), merged);
                        } else {
                            topic_store.insert(Arc::clone(&entry.key), entry.clone());
                        }
                    } else {
                        topic_store.insert(Arc::clone(&entry.key), entry.clone());
                    }
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

        // Destructure entry to avoid clones — entry was already cloned into the store above.
        let GossipEntry {
            topic,
            key,
            origin_gate,
            nonce,
            ttl,
            version,
            ..
        } = entry;

        debug!(
            %topic, %key, origin = %origin_gate, ttl,
            "Gossip entry ingested"
        );

        // Notify subscribers (best-effort: if no one is listening, this is a no-op).
        let _ = self.subscription_tx.send(GossipNotification {
            topic,
            key,
            origin_gate,
            version,
        });

        // Record nonce and evict oldest if over capacity.
        let evict_nonce = {
            let mut nonce_order = self.nonce_order.write().await;
            nonce_order.push_back(nonce.clone());
            if nonce_order.len() > MAX_NONCE_HISTORY {
                nonce_order.pop_front()
            } else {
                None
            }
        };
        {
            let mut nonce_set = self.nonce_set.write().await;
            nonce_set.insert(nonce);
            if let Some(old) = evict_nonce {
                nonce_set.remove(&old);
            }
            drop(nonce_set);
        }

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

        self.total_evicted.fetch_add(
            u64::try_from(total_evicted).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );

        total_evicted
    }

    /// Get current gossip table statistics.
    pub async fn stats(&self) -> GossipStats {
        let (tower_entries, data_entries, compute_entries, defense_entries) = {
            let store = self.entries.read().await;
            (
                store.get(&GossipTopic::Tower).map_or(0, HashMap::len),
                store.get(&GossipTopic::Data).map_or(0, HashMap::len),
                store.get(&GossipTopic::Compute).map_or(0, HashMap::len),
                store.get(&GossipTopic::Defense).map_or(0, HashMap::len),
            )
        };
        let nonce_history_size = self.nonce_set.read().await.len();
        let peer_count = self.peer_stats.read().await.len();

        GossipStats {
            tower_entries,
            data_entries,
            compute_entries,
            defense_entries,
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

    /// Record round-trip latency from a gossip exchange.
    ///
    /// Updates EWMA (exponentially weighted moving average) with α=0.3
    /// and recomputes gravitational force. Every node is its own nucleus —
    /// gravity reflects how "close" this peer is in the mesh topology.
    pub async fn record_latency(&self, peer: &str, rtt_ms: u64, via_relay: bool) {
        let mut guard = self.peer_stats.write().await;
        let entry = guard.entry(peer.to_owned()).or_default();
        entry.last_rtt_ms = rtt_ms;
        entry.via_relay = via_relay;
        // EWMA with α=0.3 (new sample weight), integer approximation
        if entry.ewma_rtt_ms == 0 {
            entry.ewma_rtt_ms = rtt_ms;
        } else {
            // ewma = 0.7 * old + 0.3 * new (scaled by 1000 for integer math)
            entry.ewma_rtt_ms = (entry.ewma_rtt_ms * 700 + rtt_ms * 300) / 1000;
        }
        Self::recompute_gravity(entry);
        drop(guard);
    }

    /// Record a failed exchange attempt (both TCP and relay failed).
    pub async fn record_failure(&self, peer: &str) {
        let mut guard = self.peer_stats.write().await;
        let entry = guard.entry(peer.to_owned()).or_default();
        entry.exchange_failures += 1;
        Self::recompute_gravity(entry);
        drop(guard);
    }

    /// Recompute gravitational force for a peer.
    ///
    /// `gravity = (1_000_000 / max(1, ewma_rtt_ms)) * reliability`
    /// where `reliability = total_exchanges / max(1, total_exchanges + failures)`
    ///
    /// Higher gravity = closer, more reliable peer. Used by the epidemic loop
    /// to prioritize gossip delivery — critical entries reach high-gravity
    /// peers first, cross-protecting the mesh against partitions.
    fn recompute_gravity(stats: &mut PeerStats) {
        let rtt_factor = 1_000_000u64 / stats.ewma_rtt_ms.max(1);
        let total_exchanges = stats.entries_sent + stats.entries_received;
        let reliability = if total_exchanges == 0 {
            500 // 50% baseline for new peers
        } else {
            (total_exchanges * 1000) / (total_exchanges + stats.exchange_failures).max(1)
        };
        stats.gravity = (rtt_factor * reliability) / 1000;
    }

    /// Return peers sorted by gravitational force (highest first).
    ///
    /// Used by the epidemic loop to prioritize gossip delivery.
    /// Critical entries should be sent to high-gravity peers first.
    pub async fn peers_by_gravity(&self) -> Vec<(String, PeerStats)> {
        let guard = self.peer_stats.read().await;
        let mut peers: Vec<_> = guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        peers.sort_by(|a, b| b.1.gravity.cmp(&a.1.gravity));
        peers
    }
}

/// Merge two defense opsonize payloads: union detectors, max confidence, bump match_count,
/// and record all corroborating gates. The result uses the newer entry's envelope
/// but accumulates intelligence from both sightings.
fn merge_opsonize_payload(existing: &GossipEntry, incoming: &GossipEntry) -> GossipEntry {
    let mut merged = if incoming.version >= existing.version {
        incoming.clone()
    } else {
        existing.clone()
    };

    // Union the detectors arrays from both payloads
    let mut detectors: Vec<String> = Vec::new();
    for payload in [&existing.payload, &incoming.payload] {
        if let Some(arr) = payload.get("detectors").and_then(serde_json::Value::as_array) {
            for d in arr {
                if let Some(s) = d.as_str() {
                    if !detectors.iter().any(|existing| existing == s) {
                        detectors.push(s.to_owned());
                    }
                }
            }
        }
    }

    // Max confidence from both sightings
    let confidence = existing
        .payload
        .get("confidence")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0)
        .max(
            incoming
                .payload
                .get("confidence")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0),
        );

    // Sum match_count (corroboration)
    let match_count = existing
        .payload
        .get("match_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1)
        + incoming
            .payload
            .get("match_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(1);

    // Track corroborating gates
    let mut corroborating_gates: Vec<String> = Vec::new();
    for payload in [&existing.payload, &incoming.payload] {
        if let Some(gate) = payload.get("origin_gate").and_then(serde_json::Value::as_str) {
            if !corroborating_gates.contains(&gate.to_owned()) {
                corroborating_gates.push(gate.to_owned());
            }
        }
        if let Some(arr) = payload
            .get("corroborating_gates")
            .and_then(serde_json::Value::as_array)
        {
            for g in arr {
                if let Some(s) = g.as_str() {
                    if !corroborating_gates.contains(&s.to_owned()) {
                        corroborating_gates.push(s.to_owned());
                    }
                }
            }
        }
    }

    // Build merged payload by overlaying merged fields onto the winner
    if let serde_json::Value::Object(ref mut map) = merged.payload {
        map.insert("detectors".to_owned(), serde_json::json!(detectors));
        map.insert("confidence".to_owned(), serde_json::json!(confidence));
        map.insert("match_count".to_owned(), serde_json::json!(match_count));
        map.insert(
            "corroborating_gates".to_owned(),
            serde_json::json!(corroborating_gates),
        );
        map.insert(
            "last_confirmed_epoch".to_owned(),
            serde_json::json!(std::cmp::max(
                existing.payload.get("last_confirmed_epoch").and_then(serde_json::Value::as_u64).unwrap_or(0),
                incoming.payload.get("last_confirmed_epoch").and_then(serde_json::Value::as_u64).unwrap_or(0),
            )),
        );
    }

    // Use latest version
    merged.version = std::cmp::max(existing.version, incoming.version);
    // Extend expiry to whichever is later
    merged.expires_at_epoch =
        std::cmp::max(existing.expires_at_epoch, incoming.expires_at_epoch);

    info!(
        key = %merged.key,
        detectors = detectors.len(),
        match_count,
        gates = corroborating_gates.len(),
        "Defense opsonize tag merged (multi-gate corroboration)"
    );

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_engine() -> GossipEngine {
        GossipEngine::new("testGate".to_owned())
    }

    #[test]
    fn gossip_topic_from_str() {
        assert_eq!("tower".parse::<GossipTopic>(), Ok(GossipTopic::Tower));
        assert_eq!("data".parse::<GossipTopic>(), Ok(GossipTopic::Data));
        assert_eq!("compute".parse::<GossipTopic>(), Ok(GossipTopic::Compute));
        assert!("invalid".parse::<GossipTopic>().is_err());
        assert!("".parse::<GossipTopic>().is_err());
        assert!("Tower".parse::<GossipTopic>().is_err());
    }

    #[test]
    fn gossip_topic_display_roundtrips_from_str() {
        for topic in [GossipTopic::Tower, GossipTopic::Data, GossipTopic::Compute] {
            let display = topic.to_string();
            let parsed: GossipTopic = display.parse().unwrap();
            assert_eq!(parsed, topic);
        }
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
        assert_eq!(entries[0].origin_gate.as_ref(), "testGate");
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
            "cas.have:testGate",
            serde_json::json!({}),
            "testGate",
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
            "compute.capacity:testGate",
            serde_json::json!({}),
            "testGate",
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
            "capability.advertise:gate1:primal1",
            serde_json::json!({"version": "old"}),
            "gate1",
        );
        old.version = 1;
        engine.ingest(old, None).await;

        let mut new = GossipEntry::new(
            GossipTopic::Tower,
            "capability.advertise:gate1:primal1",
            serde_json::json!({"version": "new"}),
            "gate1",
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
            "braid.head:gate1",
            serde_json::json!({"sha": "abc123"}),
            "gate1",
        );

        engine.ingest(entry, Some("gate1")).await;
        let queued = engine.drain_forward_queue().await;
        assert_eq!(queued.len(), 1);
        assert!(queued[0].ttl < crate::gossip_types::DEFAULT_TTL);
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
        let mut entry =
            GossipEntry::new(GossipTopic::Tower, "stale", serde_json::json!({}), "gate1");
        entry.expires_at_epoch = 0;

        // Force insert bypassing the ingest expiry check
        {
            let mut store = engine.entries.write().await;
            store
                .entry(GossipTopic::Tower)
                .or_default()
                .insert("stale".into(), entry);
        }

        let evicted = engine.evict_expired().await;
        assert_eq!(evicted, 1);

        let entries = engine.query(GossipTopic::Tower, None).await;
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn peer_stats_tracked() {
        let engine = test_engine();
        let entry = GossipEntry::new(GossipTopic::Tower, "test", serde_json::json!({}), "gate1");

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
        assert_eq!(notification.origin_gate.as_ref(), "testGate");
    }

    #[tokio::test]
    async fn subscribe_no_notification_on_duplicate() {
        let engine = test_engine();
        let entry = GossipEntry::new(
            GossipTopic::Data,
            "cas.have:gate1",
            serde_json::json!({}),
            "gate1",
        );

        engine.ingest(entry.clone(), None).await;
        let mut rx = engine.subscribe();
        engine.ingest(entry, None).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn reject_empty_key() {
        let engine = test_engine();
        let mut entry =
            GossipEntry::new(GossipTopic::Tower, "valid", serde_json::json!({}), "gate1");
        entry.key = "".into();
        assert_eq!(
            engine.ingest(entry, Some("peer1")).await,
            IngestResult::Rejected
        );
    }

    #[tokio::test]
    async fn reject_control_char_in_key() {
        let engine = test_engine();
        let mut entry =
            GossipEntry::new(GossipTopic::Tower, "valid", serde_json::json!({}), "gate1");
        entry.key = "bad\u{0001}key".into();
        assert_eq!(
            engine.ingest(entry, Some("peer1")).await,
            IngestResult::Rejected
        );
    }

    #[tokio::test]
    async fn peer_ingest_zero_ttl_rejected() {
        let engine = test_engine();
        let mut entry = GossipEntry::new(
            GossipTopic::Data,
            "cas.have:gate1",
            serde_json::json!({}),
            "gate1",
        );
        entry.ttl = 0;
        assert_eq!(
            engine.ingest(entry, Some("peer1")).await,
            IngestResult::Expired
        );
    }

    #[tokio::test]
    async fn record_send_updates_peer_stats() {
        let engine = test_engine();
        engine.record_send("peer-remote", 5).await;
        let stats = engine.peer_stats().await;
        assert_eq!(stats["peer-remote"].entries_sent, 5);
    }

    #[tokio::test]
    async fn duplicate_from_peer_increments_rejection_count() {
        let engine = test_engine();
        let entry = GossipEntry::new(
            GossipTopic::Tower,
            "topology.reachability:gate1",
            serde_json::json!({}),
            "gate1",
        );
        engine.ingest(entry.clone(), Some("peer1")).await;
        engine.ingest(entry, Some("peer1")).await;
        let stats = engine.peer_stats().await;
        assert_eq!(stats["peer1"].entries_received, 1);
        assert_eq!(stats["peer1"].entries_rejected, 1);
    }

    #[tokio::test]
    async fn inject_queues_local_forward_entry() {
        let engine = test_engine();
        engine
            .inject(
                GossipTopic::Compute,
                "compute.capacity:local".to_owned(),
                serde_json::json!({}),
            )
            .await;
        let queued = engine.drain_forward_queue().await;
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].ttl, DEFAULT_TTL - 1);
    }

    #[tokio::test]
    async fn older_version_rejected_as_duplicate() {
        let engine = test_engine();
        let mut newer = GossipEntry::new(
            GossipTopic::Data,
            "braid.head:gate1",
            serde_json::json!({"v": "new"}),
            "gate1",
        );
        newer.version = 10;
        engine.ingest(newer, None).await;

        let mut older = GossipEntry::new(
            GossipTopic::Data,
            "braid.head:gate1",
            serde_json::json!({"v": "old"}),
            "gate1",
        );
        older.version = 5;
        assert_eq!(
            engine.ingest(older, Some("peer1")).await,
            IngestResult::Duplicate
        );
    }

    #[tokio::test]
    async fn defense_opsonize_merge_unions_detectors() {
        let engine = test_engine();

        // Gate 1 detects content_gate + ip_rotation
        let mut e1 = GossipEntry::new(
            GossipTopic::Defense,
            "defense.opsonize:49e77ea75aa7666e",
            serde_json::json!({
                "behavioral_hash": "49e77ea75aa7666e",
                "detectors": ["content_gate", "ip_rotation"],
                "confidence": 0.65,
                "origin_gate": "golgiBody",
                "match_count": 3,
            }),
            "golgiBody",
        );
        e1.version = 100;
        engine.ingest(e1, None).await;

        // Gate 2 detects content_gate + stealth_ua (corroboration)
        let mut e2 = GossipEntry::new(
            GossipTopic::Defense,
            "defense.opsonize:49e77ea75aa7666e",
            serde_json::json!({
                "behavioral_hash": "49e77ea75aa7666e",
                "detectors": ["content_gate", "stealth_ua"],
                "confidence": 0.80,
                "origin_gate": "sporeGate",
                "match_count": 2,
            }),
            "sporeGate",
        );
        e2.version = 99; // older version — should still merge for defense

        let result = engine.ingest(e2, Some("sporeGate")).await;
        assert_eq!(result, IngestResult::Accepted);

        let entries = engine
            .query(GossipTopic::Defense, Some("defense.opsonize:49e77ea"))
            .await;
        assert_eq!(entries.len(), 1);

        let payload = &entries[0].payload;
        let detectors = payload["detectors"].as_array().unwrap();
        // Should have union: content_gate, ip_rotation, stealth_ua
        assert_eq!(detectors.len(), 3, "detectors should be unioned: {detectors:?}");

        // Max confidence
        let confidence = payload["confidence"].as_f64().unwrap();
        assert!((confidence - 0.80).abs() < 0.001, "should take max confidence");

        // Sum match_count
        let match_count = payload["match_count"].as_u64().unwrap();
        assert_eq!(match_count, 5, "should sum match_count: 3 + 2");

        // Corroborating gates
        let gates = payload["corroborating_gates"].as_array().unwrap();
        assert_eq!(gates.len(), 2, "both gates should be recorded: {gates:?}");
    }
}

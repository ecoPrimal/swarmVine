//! Domain-specific payload types for gossip entries.
//!
//! Phase 4 types that give structure to the three gossip domains:
//! - **Data**: `BloomFilter` for CAS `have` sets, `DepotManifest` for binary distribution
//! - **Compute**: `ComputeCapacity` for scheduling hints

use serde::{Deserialize, Serialize};

/// Bloom filter for compact set-membership queries (Data domain: `cas.have`).
///
/// Fixed-size bit array with `k` hash functions. Allows a gate to advertise
/// which CAS objects it holds without transmitting a full hash list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Bit array for JSON transport.
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
        // Safe: bit_count derived from Vec::len (always fits u64 on 64-bit).
        let bit_count_u64 = u64::try_from(bit_count).unwrap_or(u64::MAX);
        let (h1, h2) = double_hash(key);
        for i in 0..u64::from(self.k) {
            let idx = (h1.wrapping_add(i.wrapping_mul(h2))) % bit_count_u64;
            let byte_idx = usize::try_from(idx / 8).unwrap_or(0);
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
        let bit_count_u64 = u64::try_from(bit_count).unwrap_or(u64::MAX);
        let (h1, h2) = double_hash(key);
        for i in 0..u64::from(self.k) {
            let idx = (h1.wrapping_add(i.wrapping_mul(h2))) % bit_count_u64;
            let byte_idx = usize::try_from(idx / 8).unwrap_or(0);
            if self.bits[byte_idx] & (1 << (idx % 8)) == 0 {
                return false;
            }
        }
        true
    }
}

/// FNV-1a double-hash for bloom filter indexing (zero external deps).
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

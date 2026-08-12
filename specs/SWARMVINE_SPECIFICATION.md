# swarmVine — Specification

**Version**: 0.1.0  
**Date**: August 12, 2026  
**Status**: ACTIVE — G65 default, zero-copy Arc<str>, 90% coverage, P2 bugs resolved  
**Wave**: 157k POST-PANDEMIC EVOLUTION  
**Tower Layer**: Cross-layer (Tower + Data + Compute)

---

## Identity

| Field | Value |
|-------|-------|
| Phase | 1–4 (Core + Cross-gate + skunkBat + Domain intelligence) |
| Domain | Epidemic gossip propagation across mesh |
| Position | skunkBat's companion — vine spreads, bat validates |
| Layers | Tower (capabilities), Data (CAS/braids), Compute (resources/tasks) |

## What It Is

swarmVine is the gossip engine. It propagates what exists, what changed, and who
has what across the Tower Atomic mesh using epidemic (store-and-forward) gossip.

The name: a vine grows along surfaces, branching and spreading organically. It
doesn't decide what grows — it carries the signal. swarmVine carries gossip
entries across gates like a vine carries nutrients through a canopy.

## Why It Exists

songBird handles mesh transport and peer discovery. But songBird grew to also
carry capability announcements, reachability gossip, topology inference, and
partition detection — all bundled into one primal. And that's only Tower-layer
gossip.

The ecosystem also needs gossip at other layers:
- **Data**: which gate has which CAS objects, HEAD SHAs, depot freshness, braid state
- **Compute**: which gate has GPU slots, build capacity, inference bandwidth

swarmVine extracts the gossip concern into its own primal, giving it a dedicated
evolution path, test surface, and composition boundary with skunkBat.

## The Three Gossip Domains

### Tower Gossip (capabilities + topology)

Propagates what primals exist and what they can do. Currently lives partially in
songBird's `mesh.capabilities_announce` — swarmVine absorbs and extends this.

Entries:
- `capability.advertise` — primal X at gate Y serves capabilities [A, B, C]
- `topology.reachability` — gate Y can reach gates [A, B, C] with latencies
- `endpoint.alive` — gate Y's songBird is listening at address Z

### Data Gossip (CAS + braids + freshness)

Propagates what data exists and where. New capability not in any primal today.

Entries:
- `cas.have` — gate Y has CAS objects [hash1, hash2, ...] (bloom filter)
- `braid.head` — gate Y's loamSpine HEAD is at commit SHA X
- `depot.manifest` — gate Y's depot has binaries [primal@version, ...]
- `content.fresh` — gate Y's nestGate content last updated at timestamp T

### Compute Gossip (resources + tasks)

Propagates available compute capacity. Enables distributed scheduling.

Entries:
- `compute.capacity` — gate Y has N GPU slots, M CPU cores, K MB free
- `build.queue` — gate Y has N builds queued, estimated time T
- `inference.bandwidth` — gate Y's squirrel can handle N tokens/sec

## Gossip Protocol

### Entry Structure

```rust
struct GossipEntry {
    topic: GossipTopic,         // tower | data | compute
    key: String,                // e.g. "capability.advertise:sporeGate:nestGate"
    payload: serde_json::Value, // topic-specific JSON payload
    origin_gate: String,        // gate that created this entry
    nonce: String,              // 48-char hex (timestamp + pid + sequence)
    ttl: u8,                    // remaining hops (default 8)
    version: u64,               // monotonic version for conflict resolution
    created_at_epoch: u64,      // epoch seconds
    expires_at_epoch: u64,      // epoch seconds (default: +600s)
}
```

### Epidemic Propagation

When a gossip entry arrives:
1. Check nonce for deduplication (reject if seen)
2. Validate TTL > 0 (drop if expired)
3. Optionally delegate to skunkBat for challenge-verify
4. Store in local gossip table (replace if version > existing)
5. Decrement TTL
6. Forward to all reachable mesh peers (except source)

### Transport

- **Same-gate**: UDS JSON-RPC with riboCipher `0xEC` (clear)
- **Cross-gate**: songBird mesh relay with riboCipher `0xED` (mito-obfuscated)
- **Wire method**: `gossip.spread` — single method for all gossip exchange

## Composition with skunkBat

swarmVine spreads. skunkBat validates.

| swarmVine | skunkBat |
|-----------|----------|
| Receives `gossip.spread` from peer | Validates origin lineage via bearDog |
| Stores entry in gossip table | Checks for gossip-bomb patterns (flood) |
| Forwards to mesh peers | Rate-limits suspicious origins |
| Serves `gossip.query` to local primals | Audits gossip entry provenance |
| Evicts stale entries by TTL | Detects topology-based attacks |

When swarmVine receives an entry, it can optionally call
`skunkBat.metadata.analyze` before accepting. This is the vine-bat loop:
vine carries, bat inspects, vine continues or drops.

## JSON-RPC Methods

| Method | Direction | Description |
|--------|-----------|-------------|
| `gossip.spread` | peer → swarmVine | Receive gossip entries from mesh peers |
| `gossip.inject` | local primal → swarmVine | Inject a new gossip entry (local origin) |
| `gossip.query` | local primal → swarmVine | Query gossip table by topic/key pattern |
| `gossip.subscribe` | local primal → swarmVine | Subscribe to topic changes (future: streaming) |
| `gossip.status` | any → swarmVine | Current gossip table stats |
| `gossip.peers` | any → swarmVine | Known gossip peers and exchange stats |

## Capability Registry

```toml
[capabilities.gossip]
owner = "swarmvine"
methods = [
    "gossip.spread",
    "gossip.inject",
    "gossip.query",
    "gossip.subscribe",
    "gossip.status",
    "gossip.peers",
]
```

## Evolution Path

### Phase 1 (COMPLETE): Core gossip engine
- In-memory gossip table with TTL eviction and O(1) nonce dedup
- `gossip.spread` / `gossip.inject` / `gossip.query` / `gossip.status` / `gossip.peers` methods
- Same-gate UDS transport with riboCipher signal detection
- Self-announce to biomeOS Neural API + songBird ipc.register
- Fully async dispatch (zero `block_in_place`)

### Phase 2 (COMPLETE): Cross-gate propagation
- songBird `mesh.peers` query for peer discovery
- Epidemic sweep loop — drains forward queue → pushes to mesh peers via TCP
- riboCipher clear (`0xEC 0x01`) on cross-gate wire
- Configurable sweep interval, eviction interval, gossip port
- Forward queue preserves entries when no peers available

### Phase 3 (COMPLETE): skunkBat integration
- Vine-bat pre-accept loop: `gossip.spread` → `metadata.analyze` → accept/reject
- Graceful degradation when skunkBat unreachable (default allow)
- riboCipher framing on vine-bat UDS communication

### Phase 4 (IN PROGRESS): Domain-specific intelligence + streaming
- `gossip.subscribe` implemented — broadcast channel for live notifications, snapshot-based polling for JSON-RPC callers
- `BloomFilter` type for compact CAS `have` set membership (FNV-1a double-hash, serde-friendly)
- `ComputeCapacity` type for compute scheduling hints (GPU/CPU/memory/queue/inference)
- `DepotManifest` + `DepotManifestEntry` types for efficient binary distribution gossip
- `endpoint.alive` self-injection on startup + periodic refresh (ant colony scout pattern)
- songBird `gossip.relay` fallback when TCP direct to peer fails — **upstream MeshRelay SHIPPED** (songBird `0dc82bc`/`9351230`: relay/inject/spread/subscribe)
- `primal.announce` payload includes explicit `protocol`, `tarpc_socket`, `gossip_port` for unambiguous biomeOS discovery
- Windows port: zero cross-compile warnings on linux-gnu, windows-gnu, apple-darwin (all UDS gated, `e5cfacd`)
- **Remaining**: songBird gossip delegation (`mesh.capabilities_announce` → swarmVine tower domain)
- **Remaining**: tarpc streaming (true push via channel, awaiting tarpc 0.38+ upstream)

---

**swarmVine — vine spreads, bat validates. Epidemic gossip for capability, data,
and compute propagation across the Tower Atomic mesh. Primal #16.**

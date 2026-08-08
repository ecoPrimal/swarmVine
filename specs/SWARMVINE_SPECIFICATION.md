# swarmVine — Specification

**Version**: 0.1.0  
**Date**: August 08, 2026  
**Status**: ACTIVE — budded from sourDough, core gossip engine  
**Wave**: 157a  
**Tower Layer**: Cross-layer (Tower + Data + Compute)

---

## Identity

| Field | Value |
|-------|-------|
| Phase | 1 (Budding — sourDough scaffold) |
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
    topic: GossipTopic,      // tower | data | compute
    key: String,             // e.g. "capability.advertise:sporeGate:nestGate"
    payload: Value,          // topic-specific JSON payload
    origin_gate: String,     // gate that created this entry
    nonce: [u8; 16],         // deduplication
    ttl: u8,                 // remaining hops (default 8)
    version: u64,            // monotonic version for conflict resolution
    created_at: SystemTime,  // when the entry was created
    expires_at: SystemTime,  // when the entry should be evicted
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

### Phase 1 (Now): Core gossip engine
- In-memory gossip table with TTL eviction
- `gossip.spread` / `gossip.inject` / `gossip.query` methods
- Same-gate UDS transport
- Self-announce to biomeOS + songBird

### Phase 2: Cross-gate propagation
- Connect to songBird mesh for peer list
- Forward gossip entries via songBird relay
- riboCipher `0xED` on cross-gate wire
- Periodic epidemic sweep (pull from peers on timer)

### Phase 3: skunkBat integration
- Pre-accept validation via `skunkBat.metadata.analyze`
- Gossip-bomb detection (rate limiting per origin)
- Lineage verification for origin gate identity

### Phase 4: Domain-specific intelligence
- Bloom filters for CAS `have` sets
- Depot manifest diffing for efficient binary distribution
- Compute scheduling hints for biomeOS graph execution

---

**swarmVine — vine spreads, bat validates. Epidemic gossip for capability, data,
and compute propagation across the Tower Atomic mesh. Primal #16.**

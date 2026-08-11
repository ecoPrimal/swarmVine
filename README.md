# swarmVine

**Status**: Active — Primal #16
**License**: AGPL-3.0-or-later (scyBorg Provenance Trio)
**Purpose**: Epidemic gossip engine for capability, data, and compute propagation across the Tower Atomic mesh
**Wave**: 157i POST-PANDEMIC CASCADE (G72 clean, MeshRelay shipped, Windows port done)

---

## Quick Start

```bash
cargo test --workspace          # 137 tests, <1s
cargo build --release           # LTO + strip → ~2.4 MB
./target/release/swarmvine      # local dev — production binary from plasmidBin
```

## Structure

```
swarmVine/
├── .github/workflows/     CI + release + plasmidBin notification
├── config/
│   └── capability_registry.toml   Machine-readable RPC surface declaration
├── crates/
│   ├── swarmvine-core/        Core library (gossip engine, transport, lifecycle, health)
│   └── swarmvine-server/      Dual-protocol server (JSON-RPC + tarpc)
├── specs/
│   └── SWARMVINE_SPECIFICATION.md
├── deny.toml              Supply chain auditing (cargo-deny)
└── LICENSE                AGPL-3.0-or-later
```

## Capability Wire (Dual-Protocol)

### JSON-RPC on `$XDG_RUNTIME_DIR/biomeos/swarmVine.sock`

| Method | Description |
|--------|-------------|
| `health.liveness` | Process liveness check |
| `health.readiness` | Readiness + capability list |
| `health.check` | Full diagnostic report with node ID |
| `capabilities.list` | Primal name, version, methods, gossip domains |
| `btsp.negotiate` | Bond Transport Security Protocol (Phase 3) |
| `primal.announce` | Self-announcement payload for biomeOS |
| `gossip.spread` | Receive gossip entries from mesh peers |
| `gossip.inject` | Inject a locally-originated gossip entry |
| `gossip.query` | Query gossip table by topic and key prefix |
| `gossip.subscribe` | Subscribe to topic changes (snapshot + broadcast channel) |
| `gossip.status` | Current gossip table statistics |
| `gossip.peers` | Per-peer exchange statistics |

### tarpc on `$XDG_RUNTIME_DIR/biomeos/swarmVine.tarpc.sock`

All gossip methods plus baseline health/capabilities — binary framing via bincode for sub-ms intra-gate composition (G64 Cephalization).

### Cross-gate TCP on `0.0.0.0:7800` (configurable via `GOSSIP_PORT`)

Remote swarmVine instances connect here for `gossip.spread` exchange using riboCipher clear signal (`0xEC 0x01`).

### Platform Support

| Platform | JSON-RPC | tarpc | Cross-gate TCP |
|----------|----------|-------|----------------|
| Linux (musl/gnu) | UDS | UDS | TCP |
| aarch64 | UDS | UDS | TCP |
| Windows (gnu) | TCP fallback | Not yet (graceful `Unsupported`) | TCP |

All UDS call sites use `swarmvine_core::transport` abstraction or `#[cfg(unix)]` gating.
Windows compilation unblocked (Wave 157d). Peer discovery and songBird query also
use transport abstraction (Wave 157e).

## Gossip Domains

| Domain | Layer | Example Entries |
|--------|-------|-----------------|
| **tower** | Capability | `capability.advertise`, `topology.reachability`, `endpoint.alive` |
| **data** | Content | `cas.have`, `braid.head`, `depot.manifest`, `content.fresh` |
| **compute** | Resource | `compute.capacity`, `build.queue`, `inference.bandwidth` |

## Composition

All peer primals discovered at runtime — zero hardcoded primal names or paths in production code.

- **skunkBat** — vine-bat loop: `gossip.spread` → `metadata.analyze` → accept/reject
- **songBird** — peer discovery via `mesh.peers`, gossip relay via `mesh.relay` when TCP peers unreachable (MeshRelay SHIPPED: relay/inject/spread/subscribe)
- **nestGate/loamSpine** — data gossip consumers (`cas.have`, `braid.head`)
- **toadStool/coralReef** — compute gossip consumers (`compute.capacity`)

Self-injects `endpoint.alive` gossip on startup and periodically — the ant colony's first scout.

## Deployment

Production binary is built and distributed via plasmidBin (triggered by
`notify-plasmidbin.yml` on push to main).

```bash
sourdough validate ecobin primals/x86_64-unknown-linux-musl/swarmvine
sourdough sign primals/x86_64-unknown-linux-musl/swarmvine --key signing.key
sourdough scaffold systemd swarmVine --role gate
```

## CI Gates

| Check | Status |
|-------|--------|
| `cargo fmt --check` | Enforced |
| `cargo clippy -D warnings` (pedantic + nursery) | Enforced |
| `cargo deny check` | Enforced |
| `cargo test --workspace` | 137 tests |
| `#![forbid(unsafe_code)]` | Enforced |
| `warn(missing_docs)` | Enforced |

---

*Vine spreads, bat validates. Primal #16 — epidemic gossip for the Tower Atomic mesh.*

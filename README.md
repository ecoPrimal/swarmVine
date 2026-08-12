# swarmVine

**Status**: Active — Primal #16
**License**: AGPL-3.0-or-later (scyBorg Provenance Trio: AGPL + ORC + CC-BY-SA-4.0)
**Purpose**: Epidemic gossip engine for capability, data, and compute propagation across the Tower Atomic mesh
**Wave**: 157k — P2 riboCipher compat fix, G65 default, zero-copy evolution, 90% coverage

---

## Quick Start

```bash
cargo test --workspace          # 186 tests, <1s
cargo build --release           # LTO + strip → ~2.5 MB
./target/release/swarmvine      # local dev — production binary from plasmidBin
```

## Structure

```
swarmVine/
├── .github/workflows/     CI + release + plasmidBin notification
├── config/
│   └── capability_registry.toml   Machine-readable RPC surface declaration
├── crates/
│   ├── swarmvine-core/        Core library (gossip engine, transport, types, lifecycle)
│   │   └── benches/           Criterion benchmarks (inject, query, nonce dedup)
│   └── swarmvine-server/      G65 server (JSON-RPC + tarpc, protocol negotiation)
│       └── tests/             Integration tests (UDS, TCP, gossip spread)
├── specs/
│   └── SWARMVINE_SPECIFICATION.md
├── deny.toml              Supply chain auditing (cargo-deny, ecoBin v3.0 ban list)
├── rustfmt.toml           Formatting configuration
├── LICENSE                AGPL-3.0-or-later (scyBorg preamble)
├── LICENSE-ORC            Open RPG Creative License (mechanics)
└── LICENSE-CC-BY-SA       CC-BY-SA-4.0 (creative/docs)
```

## Capability Wire

### G65 Protocol Negotiation (default)

Single socket with automatic protocol selection. Legacy dual-socket via `--legacy-dual-socket`.

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
| `gossip.subscribe` | Subscribe to topic changes (snapshot + broadcast) |
| `gossip.status` | Current gossip table statistics |
| `gossip.peers` | Per-peer exchange statistics |

### tarpc on `$XDG_RUNTIME_DIR/biomeos/swarmVine.tarpc.sock`

All gossip methods plus baseline health/capabilities — binary framing via bincode for sub-ms intra-gate composition (G64 Cephalization).

### Cross-gate TCP on `0.0.0.0:7800` (configurable via `GOSSIP_PORT`)

Remote swarmVine instances connect here for `gossip.spread` exchange. Accepts both riboCipher prefixed (`0xEC 0x01`) and legacy unsignalled JSON-RPC for rolling-deploy compatibility.

### Platform Support

| Platform | JSON-RPC | tarpc | Cross-gate TCP |
|----------|----------|-------|----------------|
| Linux (musl/gnu) | UDS | UDS | TCP |
| aarch64 | UDS | UDS | TCP |
| Windows (gnu) | TCP fallback | Graceful `Unsupported` | TCP |

## Gossip Domains

| Domain | Layer | Example Entries |
|--------|-------|-----------------|
| **tower** | Capability | `capability.advertise`, `topology.reachability`, `endpoint.alive` |
| **data** | Content | `cas.have`, `braid.head`, `depot.manifest`, `content.fresh` |
| **compute** | Resource | `compute.capacity`, `build.queue`, `inference.bandwidth` |

## Composition

All peer primals discovered at runtime — zero hardcoded primal names or paths.

- **skunkBat** — vine-bat loop: `gossip.spread` → `metadata.analyze` → accept/reject
- **songBird** — peer discovery via `mesh.peers`, gossip relay via `gossip.relay` when TCP unreachable
- **nestGate/loamSpine** — data gossip consumers (`cas.have`, `braid.head`)
- **toadStool/coralReef** — compute gossip consumers (`compute.capacity`)

## Deployment

Production binary via plasmidBin (`notify-plasmidbin.yml` on push to main).

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
| `cargo deny check` | Enforced (ecoBin v3.0 ban list) |
| `cargo test --workspace` | 186 tests |
| `cargo llvm-cov` | 90.8% line coverage |
| `#![forbid(unsafe_code)]` | Enforced |
| `warn(missing_docs)` | Enforced, 0 warnings |
| SPDX headers | All source files |

---

*Vine spreads, bat validates. Primal #16 — epidemic gossip for the Tower Atomic mesh.*

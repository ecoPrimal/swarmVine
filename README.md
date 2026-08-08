# swarmVine

**Status**: Draft
**License**: AGPL-3.0-or-later (scyBorg Provenance Trio)
**Purpose**: Epidemic gossip engine for capability, data, and compute propagation across the Tower Atomic mesh

---

## Quick Start

```bash
cargo test --workspace
cargo build --release
./target/release/swarmvine  # local dev — production binary from plasmidBin
```

## Structure

```
swarmVine/
├── .github/workflows/     CI + release + plasmidBin notification
├── crates/
│   ├── swarmvine-core/        Core traits (lifecycle, health, tarpc service)
│   └── swarmvine-server/      Dual-protocol server (JSON-RPC + tarpc)
├── deny.toml              Supply chain auditing
└── specs/
```

## Capability Wire (Dual-Protocol)

**JSON-RPC** on `$XDG_RUNTIME_DIR/biomeos/swarmvine.sock` (discovery, diagnostics, browser):

| Method | Description |
|--------|-------------|
| `health.liveness` | Process liveness check |
| `health.readiness` | Readiness + capabilities |
| `health.check` | Full diagnostic report |
| `capabilities.list` | Primal name, version, methods |

**tarpc** on `$XDG_RUNTIME_DIR/biomeos/swarmvine.tarpc.sock` (intra-gate composition, sub-ms):

## Deployment

Production binary is built and distributed via plasmidBin (triggered by
`notify-plasmidbin.yml` on push to main).

```bash
# Validate the plasmidBin binary
sourdough validate ecobin primals/x86_64-unknown-linux-musl/swarmvine

# Sign for distribution
sourdough sign primals/x86_64-unknown-linux-musl/swarmvine --key signing.key

# Generate systemd service
sourdough scaffold systemd swarmVine --role gate
```

---

*Scaffolded by sourDough — the nascent primal.*

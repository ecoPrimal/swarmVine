# Coding Conventions

This primal follows the ecoPrimals coding conventions (wateringHole standards).

## Quick Reference

- **Edition**: Rust 2024
- **License**: scyBorg Provenance Trio — AGPL-3.0 (code) + ORC (mechanics) + CC-BY-SA-4.0 (creative)
- **SPDX**: All source files carry `SPDX-License-Identifier: AGPL-3.0-or-later`
- **Linting**: Workspace-level `clippy::pedantic` + `clippy::nursery`, `forbid(unsafe_code)`
- **Docs**: `warn(missing_docs)` — all public items documented, 0 doc warnings
- **Max file size**: 800 LOC target (largest production: `spread.rs` at 404L excluding tests)
- **Test coverage**: 186 tests (84 core + 96 server + 6 integration), 90.8% line coverage
- **Benchmarks**: Criterion (gossip inject throughput, query, nonce dedup)
- **Zero-copy**: `Arc<str>` for GossipEntry key/origin_gate, `bytes` crate for wire frames
- **Domain types**: `gossip_types.rs` — GossipTopic, GossipEntry, PeerStats, GossipNotification
- **IPC**: JSON-RPC 2.0 (`domain.verb` naming) + tarpc binary (G64 dual-protocol)
- **G65**: Single-socket protocol negotiation is the **default** (--legacy-dual-socket for old)
- **Transport signals**: `swarmvine_core::ribocipher` — centralized constants
- **Environment keys**: `swarmvine_core::env_keys` — all env var reads, defaults, socket naming
- **Discovery**: Runtime via env/songBird/biomeOS — zero primal-name hardcoding
- **Platform paths**: `platform_paths::runtime_socket_dir()` — single source of truth
- **Socket naming**: `env_keys::{SWARMVINE_JSONRPC_SOCK, SWARMVINE_TARPC_SOCK, ...}`
- **Node identity**: `swarmvine_core::resolve_node_id()` — GATE_ID → hostname resolution
- **Self-audit**: `self_audit_rpc_surface_matches_registry` test vs `capability_registry.toml`
- **Supply chain**: `cargo-deny` — licenses, bans (ecoBin v3.0), sources enforced

## Wire Protocol

- **Same-gate**: UDS with riboCipher clear (`0xEC 0x01`) + JSON-RPC line protocol
- **Cross-gate**: TCP with riboCipher clear + `gossip.spread` batch. Accepts legacy unsignalled JSON-RPC for backward-compatible rolling deploys.
- **tarpc**: `.tarpc.sock` with bincode framing (G64)
- **G65**: Default single-socket protocol negotiation (100ms timeout → JSON-RPC fallback)
- **Relay**: songBird `gossip.relay` fallback when TCP direct fails
- **Windows**: All UDS via `transport.rs` abstraction or `#[cfg(unix)]`. Graceful `Unsupported` on non-Unix.
- **Zero deps**: Pure Rust (`hostname` → `read_system_hostname`). `futures-util` not umbrella.
- **Self-injection**: `endpoint.alive` gossip on startup + periodic refresh

## Architecture

```
swarmvine-core (library)
├── gossip_types.rs    GossipTopic, GossipEntry (Arc<str>), PeerStats, notifications
├── gossip.rs          GossipEngine — dedup, TTL, eviction, subscribe, forward queue
├── transport.rs       TransportEndpoint, bind/connect abstraction, bytes re-export
├── protocol_negotiation.rs  G65 single-socket selection
├── domain_types.rs    BloomFilter, ComputeCapacity, DepotManifest, CascadeNotification
├── platform_paths.rs  XDG/biomeos/fallback socket directory resolution
├── platform_substrate.rs    Symlinks, permissions, secure dirs
├── env_keys.rs        Named constants for all environment variables
├── ribocipher.rs      Wire signal constants (0xEC, 0xED, 0xEE)
├── tarpc_service.rs   tarpc trait definition + response types
├── health.rs          Health reports and status
├── lifecycle.rs       Primal state machine (Created→Running→Stopped)
├── error.rs           PrimalError + PrimalResult
└── platform_signal.rs Shutdown signal handling

swarmvine-server (binary)
├── lib.rs             CLI, startup, background tasks
├── server.rs          Connection handler (riboCipher dispatch, G65, legacy compat)
├── dispatch.rs        JSON-RPC method routing → handlers
├── rpc_types.rs       JSON-RPC wire types and helpers
├── spread.rs          Cross-gate epidemic propagation, peer discovery, relay
├── tarpc_server.rs    tarpc handler implementation
├── announce.rs        biomeOS/songBird self-announce
├── method_gate.rs     RPC access control (public/protected/enforcing)
└── test_support.rs    Integration test harness
```

## CI Gates

All enforced on push/PR via `.github/workflows/ci.yml`:
`cargo fmt --check` · `cargo clippy -D warnings` · `cargo deny check` · `cargo test --workspace`

*Consistency is the foundation of collaboration.*

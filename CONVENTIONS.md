# Coding Conventions

This primal follows the ecoPrimals coding conventions (wateringHole standards).

## Quick Reference

- **Edition**: Rust 2024
- **License**: AGPL-3.0-or-later (scyBorg triple license — `LICENSE` at root)
- **Linting**: Workspace-level `clippy::pedantic` + `clippy::nursery`, `forbid(unsafe_code)`
- **Docs**: `warn(missing_docs)` — all public items documented, 0 doc warnings
- **Max file size**: 1000 LOC (largest: `gossip.rs` at 755, `dispatch.rs` at 737)
- **Test coverage**: 82% line (137 tests, target 90%+)
- **Domain types**: `domain_types.rs` — BloomFilter, ComputeCapacity, DepotManifest extracted from gossip engine
- **IPC**: JSON-RPC 2.0 (`domain.verb` naming) + tarpc binary (G64 dual-protocol)
- **Transport signals**: `swarmvine_core::ribocipher` — centralized constants, no raw magic bytes
- **Environment keys**: `swarmvine_core::env_keys` — all env var reads via named constants
- **Discovery**: Runtime via env/songBird/biomeOS — zero primal-name hardcoding, zero `/tmp` hardcoding
- **Platform paths**: `platform_paths::runtime_socket_dir()` — single source of truth for all socket resolution. `/tmp` only in `platform_paths.rs`, zero duplicated fallbacks.
- **Node identity**: `swarmvine_core::resolve_node_id()` — shared `GATE_ID` → `HOSTNAME` → OS hostname resolution
- **Self-audit**: `self_audit_rpc_surface_matches_registry` test verifies dispatch matches `capability_registry.toml`
- **Supply chain**: `cargo-deny` — licenses, bans (ecoBin v3.0), sources enforced

## Wire Protocol

- **Same-gate**: UDS with riboCipher clear signal (`0xEC 0x01`) + JSON-RPC line protocol
- **Cross-gate**: TCP with riboCipher clear signal + `gossip.spread` batch
- **tarpc**: Dedicated `.tarpc.sock` with bincode framing (G64 Cephalization)
- **G65**: Single-socket protocol negotiation (documented tarpc 0.37 limitation)
- **Windows**: All UDS call sites use `transport.rs` abstraction or `#[cfg(unix)]` gating. tarpc UDS → graceful `Unsupported` on non-Unix. TCP gossip is platform-agnostic. songBird peer discovery uses `connect_transport` (no raw `UnixStream`).
- **Dependencies**: `hostname` only in core, `futures-util` not `futures` umbrella, zero dead deps (G72 Tier 1 clean).
- **Self-injection**: `endpoint.alive` gossip on startup + periodic refresh — ant colony scout pattern.
- **Announce payload**: Explicit `protocol`, `tarpc_socket`, `gossip_port` fields for unambiguous biomeOS discovery.
- **Gossip fallback**: TCP direct → songBird `mesh.relay` when peer unreachable.

## CI Gates

All enforced on push/PR via `.github/workflows/ci.yml`:
`cargo fmt --check` · `cargo clippy -D warnings` · `cargo deny check` · `cargo test --workspace`

*Consistency is the foundation of collaboration.*

//! Environment variable names and runtime constants used across swarmVine.
//!
//! Centralizes literal env var keys and default values so production code
//! reads them via named constants instead of scattered string literals.

/// Directory for `BiomeOS` Unix domain sockets (production deployments).
pub const BIOMEOS_SOCKET_DIR: &str = "BIOMEOS_SOCKET_DIR";

/// XDG Base Directory runtime path (typically `/run/user/{uid}`).
pub const XDG_RUNTIME_DIR: &str = "XDG_RUNTIME_DIR";

/// Explicit override path for the biomeOS neural-api socket.
pub const NEURAL_API_SOCKET: &str = "NEURAL_API_SOCKET";

/// Bind mode override for platforms with restricted socket access.
/// Values: "uds" (default), `tcp_only` (Android/SELinux), "both".
pub const PRIMAL_BIND_MODE: &str = "PRIMAL_BIND_MODE";

/// Override for the gossip TCP port.
pub const GOSSIP_PORT: &str = "GOSSIP_PORT";

/// Comma-separated list of peer addresses for gossip exchange.
pub const SWARMVINE_PEERS: &str = "SWARMVINE_PEERS";

/// Override for the `skunkBat` validation socket path.
pub const SKUNKBAT_SOCKET: &str = "SWARMVINE_SKUNKBAT_SOCK";

/// Override for the songBird socket path.
pub const SONGBIRD_SOCKET: &str = "SONGBIRD_SOCKET";

/// Protocol negotiation timeout in milliseconds.
pub const NEGOTIATE_TIMEOUT_MS: &str = "NEGOTIATE_TIMEOUT_MS";

// ── Runtime defaults ────────────────────────────────────────────────────

/// Default TCP port for cross-gate gossip exchange.
pub const DEFAULT_GOSSIP_PORT: u16 = 7800;

// ── Socket naming conventions ───────────────────────────────────────────

/// JSON-RPC socket filename for the `swarmVine` primal.
pub const SWARMVINE_JSONRPC_SOCK: &str = "swarmVine.sock";

/// tarpc binary RPC socket filename for `swarmVine`.
pub const SWARMVINE_TARPC_SOCK: &str = "swarmVine.tarpc.sock";

/// songBird IPC socket filename.
pub const SONGBIRD_SOCK: &str = "songbird.sock";

/// Neural API socket filename template prefix.
pub const NEURAL_API_SOCK_PREFIX: &str = "neural-api-";

/// Resolve the gossip TCP port from environment or use the default.
#[must_use]
pub fn resolve_gossip_port() -> u16 {
    std::env::var(GOSSIP_PORT)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_GOSSIP_PORT)
}

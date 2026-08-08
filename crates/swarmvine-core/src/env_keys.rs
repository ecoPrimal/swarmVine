//! Environment variable names used across the ecoPrimals ecosystem.
//!
//! Centralizes literal env var keys so production code reads them via named
//! constants instead of scattered string literals.

/// Directory for BiomeOS Unix domain sockets (production deployments).
pub const BIOMEOS_SOCKET_DIR: &str = "BIOMEOS_SOCKET_DIR";

/// XDG Base Directory runtime path (typically `/run/user/{uid}`).
pub const XDG_RUNTIME_DIR: &str = "XDG_RUNTIME_DIR";

/// Explicit override path for the biomeOS neural-api socket.
pub const NEURAL_API_SOCKET: &str = "NEURAL_API_SOCKET";

/// Bind mode override for platforms with restricted socket access.
/// Values: "uds" (default), "tcp_only" (Android/SELinux), "both".
pub const PRIMAL_BIND_MODE: &str = "PRIMAL_BIND_MODE";

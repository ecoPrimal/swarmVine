//! Neural API self-announcement (Wave 42 `primal.announce` standard).
//!
//! On startup, the primal announces itself to biomeOS's Neural API for
//! adaptive routing. This is fire-and-forget — if biomeOS is unavailable,
//! the primal operates normally without routing intelligence.

use std::path::{Path, PathBuf};
use tracing::{info, warn};

#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::UnixStream;

const PRIMAL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Announce this primal to biomeOS's Neural API socket.
///
/// On non-Unix platforms, this is a no-op (Neural API uses UDS).
#[cfg(not(unix))]
pub(crate) async fn announce_to_biomeos(_primal_name: &str, _socket: &Path, _family: &str) {
    info!("biomeOS announce not available on this platform (UDS-only)");
}

/// Announce this primal to biomeOS's Neural API socket.
///
/// Discovers the neural-api socket via tiered lookup, connects, and sends
/// a `primal.announce` JSON-RPC call. Logs a warning and returns gracefully
/// if biomeOS is unreachable.
#[cfg(unix)]
pub(crate) async fn announce_to_biomeos(primal_name: &str, socket: &Path, family: &str) {
    let Some(neural_socket) = discover_neural_api_socket(family) else {
        info!("biomeOS neural-api socket not found — skipping announce");
        return;
    };

    let socket_path = socket.display().to_string();
    let tarpc_socket_path = socket_path.replace(".sock", ".tarpc.sock");

    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "primal.announce",
        "params": {
            "primal": primal_name,
            "socket": socket_path,
            "protocol": "json-rpc",
            "tarpc_socket": tarpc_socket_path,
            "gossip_port": gossip_port(),
            "pid": std::process::id(),
            "capabilities": capabilities(),
            "methods": crate::dispatch::METHODS,
            "signal_tiers": signal_tiers(),
            "cost_hints": cost_hints(),
            "latency_estimates": latency_estimates(),
            "version": PRIMAL_VERSION,
        },
        "id": 1,
    });

    match UnixStream::connect(&neural_socket).await {
        Ok(stream) => {
            let mut reader = BufReader::new(stream);
            let writer = reader.get_mut();
            let msg = format!("{payload}\n");
            if let Err(e) = writer.write_all(msg.as_bytes()).await {
                warn!("Failed to send primal.announce: {e}");
                return;
            }
            // Read response (best-effort)
            let mut response = String::new();
            match reader.read_line(&mut response).await {
                Ok(0) | Err(_) => warn!("No response to primal.announce"),
                Ok(_) => info!("Announced to Neural API: {response}"),
            }
        }
        Err(e) => {
            info!("biomeOS neural-api not reachable ({e}) — primal operates standalone");
        }
    }
}

/// Discover biomeOS neural-api socket via tiered lookup.
#[cfg(unix)]
fn discover_neural_api_socket(family: &str) -> Option<PathBuf> {
    // Tier 1: explicit env override
    if let Ok(path) = std::env::var(swarmvine_core::env_keys::NEURAL_API_SOCKET) {
        let p = PathBuf::from(&path);
        if p.exists() {
            return Some(p);
        }
    }
    // Tier 2: platform runtime socket directory (consolidates XDG + platform fallback)
    let p = swarmvine_core::platform_paths::runtime_socket_dir().join(format!(
        "{}{family}.sock",
        swarmvine_core::env_keys::NEURAL_API_SOCK_PREFIX
    ));
    if p.exists() {
        return Some(p);
    }
    None
}

/// Register capabilities with Songbird for discovery by other primals.
///
/// Register with songBird for capability routing (non-Unix no-op).
#[cfg(not(unix))]
pub(crate) async fn register_with_songbird(_primal_name: &str, _endpoint_json: &serde_json::Value) {
    info!("songBird registration not available on this platform (UDS-only)");
}

/// Register with songBird for capability routing.
///
/// This enables `ipc.resolve` — when another primal needs to find us,
/// songbird will return our endpoint.
#[cfg(unix)]
pub(crate) async fn register_with_songbird(primal_name: &str, endpoint_json: &serde_json::Value) {
    let Some(songbird_socket) = discover_songbird_socket() else {
        info!("Songbird socket not found — skipping ipc.register");
        return;
    };

    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "ipc.register",
        "params": {
            "primal": primal_name,
            "capabilities": capabilities(),
            "endpoint": endpoint_json,
            "version": PRIMAL_VERSION,
        },
        "id": 1,
    });

    match UnixStream::connect(&songbird_socket).await {
        Ok(stream) => {
            let mut reader = BufReader::new(stream);
            let writer = reader.get_mut();
            let msg = format!("{payload}\n");
            if let Err(e) = writer.write_all(msg.as_bytes()).await {
                warn!("Failed to send ipc.register to songbird: {e}");
                return;
            }
            let mut response = String::new();
            match reader.read_line(&mut response).await {
                Ok(0) | Err(_) => warn!("No response from songbird ipc.register"),
                Ok(_) => info!("Registered with Songbird: {response}"),
            }
        }
        Err(e) => {
            info!("Songbird not reachable ({e}) — primal operates without discovery");
        }
    }
}

/// Discover songbird socket via tiered biomeOS resolution.
///
/// Platform-agnostic: on non-Unix, socket files won't exist so this returns `None`.
pub(crate) fn discover_songbird_socket() -> Option<PathBuf> {
    if let Ok(path) = std::env::var(swarmvine_core::env_keys::SONGBIRD_SOCKET) {
        let p = PathBuf::from(&path);
        if p.exists() {
            return Some(p);
        }
    }
    let p = swarmvine_core::platform_paths::runtime_socket_dir()
        .join(swarmvine_core::env_keys::SONGBIRD_SOCK);
    if p.exists() {
        return Some(p);
    }
    None
}

/// Inject an `endpoint.alive` Tower gossip entry for this gate.
///
/// Makes this swarmVine instance discoverable via gossip. The ant colony
/// needs scouts — this is the first one. Called on startup and periodically
/// by the eviction loop so the entry stays fresh.
pub(crate) async fn inject_endpoint_alive(primal: &swarmvine_core::SwarmVinePrimal, port: u16) {
    let node_id = primal.engine().node_id().to_owned();
    let key = format!("endpoint.alive:{node_id}");
    let payload = serde_json::json!({
        "gate": node_id,
        "gossip_port": port,
        "protocol": "json-rpc",
        "version": PRIMAL_VERSION,
        "pid": std::process::id(),
        "capabilities": capabilities(),
    });

    let result = primal
        .engine()
        .inject(swarmvine_core::GossipTopic::Tower, key, payload)
        .await;

    match result {
        swarmvine_core::IngestResult::Accepted => {
            info!(gate = %node_id, "Injected endpoint.alive gossip entry");
        }
        swarmvine_core::IngestResult::Duplicate => {
            // Expected on periodic re-inject; nonce seen before
        }
        other => {
            warn!(gate = %node_id, result = ?other, "endpoint.alive injection unexpected result");
        }
    }
}

/// Cross-gate gossip port resolved from env or default.
#[cfg(unix)]
fn gossip_port() -> u16 {
    swarmvine_core::env_keys::resolve_gossip_port()
}

/// Capability domains this primal serves.
pub(crate) const fn capabilities() -> &'static [&'static str] {
    &["gossip"]
}

/// Composition tiers this primal participates in.
///
/// swarmVine is cross-layer: tower (capabilities), node (data), meta (compute).
pub(crate) const fn signal_tiers() -> &'static [&'static str] {
    &["tower", "node", "meta"]
}

/// Cost hints per capability domain (arbitrary units, lower = cheaper).
#[cfg(unix)]
fn cost_hints() -> serde_json::Value {
    serde_json::json!({ "gossip": 1 })
}

/// Latency estimates per capability domain (milliseconds).
#[cfg(unix)]
fn latency_estimates() -> serde_json::Value {
    serde_json::json!({ "gossip": 5 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_returns_gossip_domain() {
        assert_eq!(capabilities(), &["gossip"]);
    }

    #[test]
    fn signal_tiers_returns_cross_layer_tiers() {
        assert_eq!(signal_tiers(), &["tower", "node", "meta"]);
    }

    #[test]
    fn discover_songbird_socket_none_when_missing() {
        let empty_runtime =
            std::env::temp_dir().join(format!("swarmvine-test-runtime-{}", std::process::id()));
        std::fs::create_dir_all(&empty_runtime).unwrap();
        let runtime_dir = empty_runtime.display().to_string();

        let result = temp_env::with_vars(
            [
                (
                    swarmvine_core::env_keys::SONGBIRD_SOCKET,
                    Some("/nonexistent/swarmvine-test-songbird.sock"),
                ),
                (
                    swarmvine_core::env_keys::XDG_RUNTIME_DIR,
                    Some(runtime_dir.as_str()),
                ),
                (
                    swarmvine_core::platform_paths::env_overrides::RUNTIME_DIR,
                    Some(empty_runtime.to_str().unwrap()),
                ),
            ],
            discover_songbird_socket,
        );

        let _ = std::fs::remove_dir_all(&empty_runtime);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn inject_endpoint_alive_creates_tower_entry() {
        let primal = swarmvine_core::SwarmVinePrimal::with_node_id("aliveTestGate".to_owned());
        inject_endpoint_alive(&primal, 7800).await;

        let entries = primal
            .engine()
            .query(swarmvine_core::GossipTopic::Tower, Some("endpoint.alive:"))
            .await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "endpoint.alive:aliveTestGate");
        assert_eq!(entries[0].payload["gossip_port"], 7800);
        assert_eq!(entries[0].payload["protocol"], "json-rpc");
    }

    #[test]
    fn gossip_port_defaults_to_7800() {
        let port = temp_env::with_var_unset(swarmvine_core::env_keys::GOSSIP_PORT, gossip_port);
        assert_eq!(port, 7800);
    }
}

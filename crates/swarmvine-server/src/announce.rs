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

const PRIMAL_NAME: &str = "swarmvine";
const PRIMAL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Announce this primal to biomeOS's Neural API socket.
///
/// On non-Unix platforms, this is a no-op (Neural API uses UDS).
#[cfg(not(unix))]
pub async fn announce_to_biomeos(_primal_name: &str, _socket: &Path, _family: &str) {
    info!("biomeOS announce not available on this platform (UDS-only)");
}

/// Announce this primal to biomeOS's Neural API socket.
///
/// Discovers the neural-api socket via tiered lookup, connects, and sends
/// a `primal.announce` JSON-RPC call. Logs a warning and returns gracefully
/// if biomeOS is unreachable.
#[cfg(unix)]
pub async fn announce_to_biomeos(primal_name: &str, socket: &Path, family: &str) {
    let Some(neural_socket) = discover_neural_api_socket(family) else {
        info!("biomeOS neural-api socket not found — skipping announce");
        return;
    };

    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "primal.announce",
        "params": {
            "primal": primal_name,
            "socket": socket.display().to_string(),
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
            let msg = format!("{}\n", payload);
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
    if let Ok(path) = std::env::var("NEURAL_API_SOCKET") {
        let p = PathBuf::from(&path);
        if p.exists() {
            return Some(p);
        }
    }
    // Tier 2: XDG_RUNTIME_DIR
    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        let p = PathBuf::from(format!("{runtime_dir}/biomeos/neural-api-{family}.sock"));
        if p.exists() {
            return Some(p);
        }
    }
    // Tier 3: /tmp fallback
    let p = PathBuf::from(format!("/tmp/biomeos/neural-api-{family}.sock"));
    if p.exists() {
        return Some(p);
    }
    None
}

/// Register capabilities with Songbird for discovery by other primals.
///
/// Register with songBird for capability routing (non-Unix no-op).
#[cfg(not(unix))]
pub async fn register_with_songbird(_primal_name: &str, _endpoint_json: &serde_json::Value) {
    info!("songBird registration not available on this platform (UDS-only)");
}

/// Register with songBird for capability routing.
///
/// This enables `ipc.resolve` — when another primal needs to find us,
/// songbird will return our endpoint.
#[cfg(unix)]
pub async fn register_with_songbird(primal_name: &str, endpoint_json: &serde_json::Value) {
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
            let msg = format!("{}\n", payload);
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

/// Discover songbird socket via standard biomeOS resolution.
#[cfg(unix)]
fn discover_songbird_socket() -> Option<PathBuf> {
    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        let p = PathBuf::from(format!("{runtime_dir}/biomeos/songbird.sock"));
        if p.exists() {
            return Some(p);
        }
    }
    let p = PathBuf::from("/tmp/biomeos/songbird.sock");
    if p.exists() {
        return Some(p);
    }
    None
}

/// Capability domains this primal serves.
pub fn capabilities() -> &'static [&'static str] {
    &["gossip"]
}

/// Composition tiers this primal participates in.
///
/// swarmVine is cross-layer: tower (capabilities), node (data), meta (compute).
pub fn signal_tiers() -> &'static [&'static str] {
    &["tower", "node", "meta"]
}

/// Cost hints per capability domain (arbitrary units, lower = cheaper).
fn cost_hints() -> serde_json::Value {
    serde_json::json!({ "gossip": 1 })
}

/// Latency estimates per capability domain (milliseconds).
fn latency_estimates() -> serde_json::Value {
    serde_json::json!({ "gossip": 5 })
}

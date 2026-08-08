//! Epidemic spread — drain forward queue and push to mesh peers.
//!
//! Peer discovery is tiered:
//! 1. `SWARMVINE_PEERS` env var — explicit `host:port` list (like `SONGBIRD_LOCAL_PEERS`)
//! 2. songBird `mesh.peers` UDS query — extracts peer IPs from the live mesh
//!
//! Cross-gate gossip uses TCP with riboCipher clear signal (0xEC 0x01).
//! Each peer exchange sends `gossip.spread` with a batch of entries.

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, info, warn};

/// riboCipher clear signal for JSON-RPC.
const RIBO_CLEAR: [u8; 2] = [0xEC, 0x01];

/// Default swarmVine gossip port (peers listen on this for cross-gate traffic).
const DEFAULT_GOSSIP_PORT: u16 = 7800;

/// Connection timeout for peer gossip exchange.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Read timeout for peer response.
const READ_TIMEOUT: Duration = Duration::from_secs(3);

/// Run a periodic epidemic sweep: drain forward queue → spread to peers.
///
/// This is the core epidemic loop. Runs every `interval` seconds. Each cycle:
/// 1. Drains entries queued for forwarding
/// 2. Discovers mesh peers (env + songBird)
/// 3. Sends `gossip.spread` batch to each peer's swarmVine
pub async fn epidemic_loop(
    primal: Arc<swarmvine_core::SwarmVinePrimal>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;

        let entries = primal.engine().drain_forward_queue().await;
        if entries.is_empty() {
            continue;
        }

        let peers = discover_peers().await;
        if peers.is_empty() {
            debug!(
                queued = entries.len(),
                "No peers discovered — gossip entries will be re-queued on next inject"
            );
            continue;
        }

        let node_id = primal.engine().node_id().to_owned();
        let batch_payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "gossip.spread",
            "params": {
                "from": node_id,
                "entries": entries,
            },
            "id": null,
        });

        let batch_json = match serde_json::to_string(&batch_payload) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "Failed to serialize gossip batch");
                continue;
            }
        };

        info!(
            entries = entries.len(),
            peers = peers.len(),
            "Epidemic sweep: spreading gossip to peers"
        );

        for peer_addr in &peers {
            let addr = peer_addr.clone();
            let payload = batch_json.clone();
            let engine = Arc::clone(&primal);
            tokio::spawn(async move {
                match spread_to_peer(&addr, &payload).await {
                    Ok(accepted) => {
                        debug!(peer = %addr, accepted, "Gossip spread delivered");
                        engine
                            .engine()
                            .record_send(&addr, accepted as u64)
                            .await;
                    }
                    Err(e) => {
                        warn!(peer = %addr, error = %e, "Gossip spread failed");
                    }
                }
            });
        }
    }
}

/// Discover peers for gossip exchange.
///
/// Tier 1: `SWARMVINE_PEERS` env — comma-separated `host:port` list.
/// Tier 2: songBird `mesh.peers` via local UDS — extracts peer IPs and
///         assumes swarmVine on DEFAULT_GOSSIP_PORT.
async fn discover_peers() -> Vec<String> {
    let mut peers = Vec::new();

    // Tier 1: explicit peers from environment
    if let Ok(env_peers) = std::env::var("SWARMVINE_PEERS") {
        for peer in env_peers.split(',') {
            let trimmed = peer.trim();
            if !trimmed.is_empty() {
                peers.push(trimmed.to_owned());
            }
        }
    }

    // Tier 2: songBird mesh.peers query
    if let Some(songbird_peers) = query_songbird_peers().await {
        for addr in songbird_peers {
            if !peers.iter().any(|p| p == &addr) {
                peers.push(addr);
            }
        }
    }

    peers
}

/// Query local songBird for mesh peers via UDS.
///
/// Returns `host:port` addresses for remote swarmVine instances.
#[cfg(unix)]
async fn query_songbird_peers() -> Option<Vec<String>> {
    let songbird_socket = discover_songbird_socket()?;

    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "mesh.peers",
        "params": {},
        "id": 1,
    });

    let stream = match tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio::net::UnixStream::connect(&songbird_socket),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            debug!(error = %e, "Cannot connect to songBird for peer discovery");
            return None;
        }
        Err(_) => {
            debug!("Timeout connecting to songBird");
            return None;
        }
    };

    let mut reader = BufReader::new(stream);
    let writer = reader.get_mut();

    // Send riboCipher clear signal + request
    if writer.write_all(&RIBO_CLEAR).await.is_err() {
        return None;
    }
    let msg = format!("{}\n", payload);
    if writer.write_all(msg.as_bytes()).await.is_err() {
        return None;
    }

    let mut response = String::new();
    match tokio::time::timeout(READ_TIMEOUT, reader.read_line(&mut response)).await {
        Ok(Ok(n)) if n > 0 => {}
        _ => return None,
    }

    let resp: serde_json::Value = serde_json::from_str(&response).ok()?;
    let result = resp.get("result")?;

    // Extract peer IPs from mesh.peers response
    // songBird returns peers as array of objects with node_id, path info, etc.
    let mut addrs = Vec::new();
    if let Some(peers) = result.get("peers").and_then(|p| p.as_array()) {
        for peer in peers {
            if let Some(addr) = extract_peer_address(peer) {
                addrs.push(addr);
            }
        }
    }
    // Some songBird versions return a flat object keyed by node_id
    if addrs.is_empty() {
        if let Some(obj) = result.as_object() {
            for (_node_id, info) in obj {
                if let Some(addr) = extract_peer_address(info) {
                    addrs.push(addr);
                }
            }
        }
    }

    if addrs.is_empty() {
        None
    } else {
        Some(addrs)
    }
}

#[cfg(not(unix))]
async fn query_songbird_peers() -> Option<Vec<String>> {
    None
}

/// Extract a swarmVine gossip address from a songBird peer entry.
///
/// Looks for `addr`, `address`, or `endpoint` fields containing an IP,
/// then appends the default gossip port.
fn extract_peer_address(peer: &serde_json::Value) -> Option<String> {
    // Try direct addr field first
    let addr_str = peer
        .get("addr")
        .or_else(|| peer.get("address"))
        .or_else(|| peer.get("endpoint"))
        .and_then(|v| v.as_str());

    if let Some(addr) = addr_str {
        // If it already has a port, extract IP and use gossip port
        let ip = if let Some(colon_pos) = addr.rfind(':') {
            &addr[..colon_pos]
        } else {
            addr
        };
        // Skip localhost (that's us)
        if ip == "127.0.0.1" || ip == "::1" || ip == "localhost" {
            return None;
        }
        return Some(format!("{ip}:{DEFAULT_GOSSIP_PORT}"));
    }

    // Try extracting from nested path info
    if let Some(paths) = peer.get("paths").and_then(|p| p.as_array()) {
        for path in paths {
            if let Some(ep) = path.get("endpoint").and_then(|e| e.as_str()) {
                let ip = if let Some(colon_pos) = ep.rfind(':') {
                    &ep[..colon_pos]
                } else {
                    ep
                };
                if ip != "127.0.0.1" && ip != "::1" && ip != "localhost" {
                    return Some(format!("{ip}:{DEFAULT_GOSSIP_PORT}"));
                }
            }
        }
    }

    None
}

/// Discover songBird socket via standard biomeOS resolution.
#[cfg(unix)]
fn discover_songbird_socket() -> Option<std::path::PathBuf> {
    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        let p = std::path::PathBuf::from(format!("{runtime_dir}/biomeos/songbird.sock"));
        if p.exists() {
            return Some(p);
        }
    }
    let p = std::path::PathBuf::from("/tmp/biomeos/songbird.sock");
    if p.exists() {
        return Some(p);
    }
    None
}

/// Send a gossip batch to a remote swarmVine peer via TCP.
///
/// Uses riboCipher clear signal (0xEC 0x01) + JSON-RPC line protocol.
/// Returns the number of entries accepted by the remote peer.
async fn spread_to_peer(addr: &str, payload: &str) -> Result<u32, String> {
    let stream = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio::net::TcpStream::connect(addr),
    )
    .await
    .map_err(|_| format!("Timeout connecting to {addr}"))?
    .map_err(|e| format!("TCP connect to {addr}: {e}"))?;

    let mut reader = BufReader::new(stream);
    let writer = reader.get_mut();

    // riboCipher clear signal
    writer
        .write_all(&RIBO_CLEAR)
        .await
        .map_err(|e| format!("Write signal to {addr}: {e}"))?;

    // JSON-RPC request line
    writer
        .write_all(payload.as_bytes())
        .await
        .map_err(|e| format!("Write payload to {addr}: {e}"))?;
    writer
        .write_all(b"\n")
        .await
        .map_err(|e| format!("Write newline to {addr}: {e}"))?;

    // Read response (best-effort, with timeout)
    let mut response = String::new();
    match tokio::time::timeout(READ_TIMEOUT, reader.read_line(&mut response)).await {
        Ok(Ok(n)) if n > 0 => {
            if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&response) {
                let accepted = resp
                    .get("result")
                    .and_then(|r| r.get("accepted"))
                    .and_then(|a| a.as_u64())
                    .unwrap_or(0);
                Ok(accepted as u32)
            } else {
                Ok(0)
            }
        }
        _ => Ok(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_peer_address_from_addr_field() {
        let peer = serde_json::json!({"addr": "192.168.4.2:7700"});
        let addr = extract_peer_address(&peer);
        assert_eq!(addr, Some(format!("192.168.4.2:{DEFAULT_GOSSIP_PORT}")));
    }

    #[test]
    fn extract_peer_address_from_endpoint_field() {
        let peer = serde_json::json!({"endpoint": "10.13.37.11:7700"});
        let addr = extract_peer_address(&peer);
        assert_eq!(addr, Some(format!("10.13.37.11:{DEFAULT_GOSSIP_PORT}")));
    }

    #[test]
    fn skip_localhost_address() {
        let peer = serde_json::json!({"addr": "127.0.0.1:7700"});
        assert_eq!(extract_peer_address(&peer), None);
    }

    #[test]
    fn extract_from_paths_array() {
        let peer = serde_json::json!({
            "node_id": "westGate",
            "paths": [
                {"endpoint": "192.168.4.149:7700", "type": "lan"}
            ]
        });
        let addr = extract_peer_address(&peer);
        assert_eq!(addr, Some(format!("192.168.4.149:{DEFAULT_GOSSIP_PORT}")));
    }

    #[test]
    fn no_address_returns_none() {
        let peer = serde_json::json!({"node_id": "westGate"});
        assert_eq!(extract_peer_address(&peer), None);
    }

    #[test]
    fn parse_env_peers() {
        let input = "10.13.37.2:7800,10.13.37.11:7800, 192.168.4.2:7800 ";
        let peers: Vec<String> = input
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        assert_eq!(peers.len(), 3);
        assert!(peers.contains(&"10.13.37.2:7800".to_owned()));
        assert!(peers.contains(&"10.13.37.11:7800".to_owned()));
        assert!(peers.contains(&"192.168.4.2:7800".to_owned()));
    }
}

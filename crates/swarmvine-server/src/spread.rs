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
use swarmvine_core::ribocipher;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, info, warn};

/// Resolve the gossip port from environment or use the default (7800).
fn gossip_port() -> u16 {
    std::env::var(swarmvine_core::env_keys::GOSSIP_PORT)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7800)
}

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
pub(crate) async fn epidemic_loop(
    primal: Arc<swarmvine_core::SwarmVinePrimal>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;

        let peers = discover_peers().await;
        if peers.is_empty() {
            continue;
        }

        let entries = primal.engine().drain_forward_queue().await;
        if entries.is_empty() {
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

        let batch_json: Arc<str> = match serde_json::to_string(&batch_payload) {
            Ok(s) => Arc::from(s),
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
            let payload = Arc::clone(&batch_json);
            let engine = Arc::clone(&primal);
            tokio::spawn(async move {
                match spread_to_peer(&addr, &payload).await {
                    Ok(accepted) => {
                        debug!(peer = %addr, accepted, "Gossip spread delivered");
                        engine
                            .engine()
                            .record_send(&addr, u64::from(accepted))
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
///         assumes swarmVine on the gossip port from `GOSSIP_PORT` (default 7800).
async fn discover_peers() -> Vec<String> {
    let mut peers = Vec::new();

    // Tier 1: explicit peers from environment
    if let Ok(env_peers) = std::env::var(swarmvine_core::env_keys::SWARMVINE_PEERS) {
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

/// Query local songBird for mesh peers via transport abstraction.
///
/// Returns `host:port` addresses for remote swarmVine instances.
/// Uses `connect_transport` so the same code compiles on all platforms
/// (UDS on Unix, `Unsupported` error on Windows → returns `None`).
async fn query_songbird_peers() -> Option<Vec<String>> {
    let songbird_socket = discover_songbird_socket()?;

    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "mesh.peers",
        "params": {},
        "id": 1,
    });

    let endpoint = swarmvine_core::transport::TransportEndpoint::Uds {
        path: songbird_socket.to_string_lossy().into_owned(),
    };
    let stream = match tokio::time::timeout(
        CONNECT_TIMEOUT,
        swarmvine_core::transport::connect_transport(&endpoint),
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

    let (rd, mut wr) = tokio::io::split(stream);
    let mut reader = BufReader::new(rd);

    if wr.write_all(&ribocipher::CLEAR_JSONRPC).await.is_err() {
        return None;
    }
    let msg = format!("{payload}\n");
    if wr.write_all(msg.as_bytes()).await.is_err() {
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
    let port = gossip_port();
    let mut addrs = Vec::new();
    if let Some(peers) = result.get("peers").and_then(|p| p.as_array()) {
        for peer in peers {
            if let Some(addr) = extract_peer_address(peer, port) {
                addrs.push(addr);
            }
        }
    }
    // Some songBird versions return a flat object keyed by node_id
    if addrs.is_empty()
        && let Some(obj) = result.as_object()
    {
        for (_node_id, info) in obj {
            if let Some(addr) = extract_peer_address(info, port) {
                addrs.push(addr);
            }
        }
    }

    if addrs.is_empty() { None } else { Some(addrs) }
}

/// Delegate to announce module's discovery logic.
fn discover_songbird_socket() -> Option<std::path::PathBuf> {
    crate::announce::discover_songbird_socket()
}

/// Extract a swarmVine gossip address from a songBird peer entry.
///
/// Looks for `addr`, `address`, or `endpoint` fields containing an IP,
/// then appends the default gossip port. Filters out loopback addresses
/// (including bracketed IPv6 `[::1]`).
fn extract_peer_address(peer: &serde_json::Value, port: u16) -> Option<String> {
    let addr_str = peer
        .get("addr")
        .or_else(|| peer.get("address"))
        .or_else(|| peer.get("endpoint"))
        .and_then(|v| v.as_str());

    if let Some(addr) = addr_str {
        let ip = strip_port(addr);
        if ip.is_empty() || is_loopback(ip) {
            return None;
        }
        return Some(format!("{ip}:{port}"));
    }

    if let Some(paths) = peer.get("paths").and_then(|p| p.as_array()) {
        for path in paths {
            if let Some(ep) = path.get("endpoint").and_then(|e| e.as_str()) {
                let ip = strip_port(ep);
                if !ip.is_empty() && !is_loopback(ip) {
                    return Some(format!("{ip}:{port}"));
                }
            }
        }
    }

    None
}

/// Strip trailing `:port` from an address string, handling bracketed IPv6.
fn strip_port(addr: &str) -> &str {
    addr.rfind(']').map_or_else(
        || addr.rfind(':').map_or(addr, |pos| &addr[..pos]),
        |bracket_end| &addr[..=bracket_end],
    )
}

/// Whether the address is a loopback (including bracketed IPv6).
fn is_loopback(ip: &str) -> bool {
    matches!(ip, "127.0.0.1" | "::1" | "[::1]" | "localhost")
}

/// Send a gossip batch to a remote swarmVine peer via TCP.
///
/// Uses riboCipher clear signal (0xEC 0x01) + JSON-RPC line protocol.
/// Returns the number of entries accepted by the remote peer.
async fn spread_to_peer(addr: &str, payload: &str) -> Result<u32, String> {
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| format!("Timeout connecting to {addr}"))?
        .map_err(|e| format!("TCP connect to {addr}: {e}"))?;

    let mut reader = BufReader::new(stream);
    let writer = reader.get_mut();

    // riboCipher clear signal
    writer
        .write_all(&ribocipher::CLEAR_JSONRPC)
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
            serde_json::from_str::<serde_json::Value>(&response).map_or(Ok(0), |resp| {
                let accepted = resp
                    .get("result")
                    .and_then(|r| r.get("accepted"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                Ok(u32::try_from(accepted).unwrap_or(0))
            })
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
        let addr = extract_peer_address(&peer, 7800);
        assert_eq!(addr, Some("192.168.4.2:7800".to_owned()));
    }

    #[test]
    fn extract_peer_address_from_endpoint_field() {
        let peer = serde_json::json!({"endpoint": "10.13.37.11:7700"});
        let addr = extract_peer_address(&peer, 7800);
        assert_eq!(addr, Some("10.13.37.11:7800".to_owned()));
    }

    #[test]
    fn skip_localhost_address() {
        let peer = serde_json::json!({"addr": "127.0.0.1:7700"});
        assert_eq!(extract_peer_address(&peer, 7800), None);
    }

    #[test]
    fn extract_from_paths_array() {
        let peer = serde_json::json!({
            "node_id": "westGate",
            "paths": [
                {"endpoint": "192.168.4.149:7700", "type": "lan"}
            ]
        });
        let addr = extract_peer_address(&peer, 7800);
        assert_eq!(addr, Some("192.168.4.149:7800".to_owned()));
    }

    #[test]
    fn no_address_returns_none() {
        let peer = serde_json::json!({"node_id": "westGate"});
        assert_eq!(extract_peer_address(&peer, 7800), None);
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

    #[test]
    fn discover_peers_returns_env_peers() {
        temp_env::with_var(
            swarmvine_core::env_keys::SWARMVINE_PEERS,
            Some("10.0.0.1:7800, 10.0.0.2:7800"),
            || {
                let peers = tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(discover_peers());
                assert!(peers.contains(&"10.0.0.1:7800".to_owned()));
                assert!(peers.contains(&"10.0.0.2:7800".to_owned()));
            },
        );
    }

    #[test]
    fn extract_peer_address_ipv6_bracketed() {
        let peer = serde_json::json!({"addr": "[2001:db8::2]:7700"});
        let addr = extract_peer_address(&peer, 7800);
        assert_eq!(addr, Some("[2001:db8::2]:7800".to_owned()));
    }

    #[test]
    fn extract_peer_address_bracketed_ipv6_loopback_filtered() {
        let peer = serde_json::json!({"addr": "[::1]:7700"});
        assert_eq!(extract_peer_address(&peer, 7800), None);
    }

    #[test]
    fn extract_peer_address_malformed_non_string_addr() {
        let peer = serde_json::json!({"addr": 12345});
        assert_eq!(extract_peer_address(&peer, 7800), None);
    }

    #[test]
    fn extract_peer_address_malformed_empty_string() {
        let peer = serde_json::json!({"addr": ""});
        assert_eq!(extract_peer_address(&peer, 7800), None);
    }

    #[test]
    fn extract_peer_address_from_address_field_ipv6() {
        let peer = serde_json::json!({"address": "fe80::1:7700"});
        let addr = extract_peer_address(&peer, 7800);
        assert_eq!(addr, Some("fe80::1:7800".to_owned()));
    }

    #[tokio::test]
    async fn spread_to_peer_sends_and_receives() {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut prefix = [0u8; 2];
            stream.read_exact(&mut prefix).await.unwrap();
            assert_eq!(prefix, ribocipher::CLEAR_JSONRPC);
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert!(line.contains("test"));
            let writer = reader.get_mut();
            writer
                .write_all(br#"{"jsonrpc":"2.0","result":{"accepted":3},"id":null}"#)
                .await
                .unwrap();
            writer.write_all(b"\n").await.unwrap();
        });

        let result = spread_to_peer(&format!("127.0.0.1:{port}"), r#"{"test":"payload"}"#).await;
        server.await.unwrap();
        assert_eq!(result.unwrap(), 3);
    }

    #[tokio::test]
    async fn spread_to_peer_read_timeout_returns_zero() {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut prefix = [0u8; 2];
            stream.read_exact(&mut prefix).await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            // Accept connection and read request but never respond.
        });

        let result = spread_to_peer(&format!("127.0.0.1:{port}"), r#"{"test":"payload"}"#).await;
        server.await.unwrap();
        assert_eq!(result.unwrap(), 0);
    }
}

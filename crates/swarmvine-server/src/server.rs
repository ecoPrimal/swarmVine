// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! Transport-injected server with first-byte protocol detection.
//!
//! Supports two modes:
//! - **Phase 2 (C2)**: First-byte riboCipher detection on JSON-RPC socket
//! - **Phase 3 (G65)**: Protocol negotiation on single socket (tarpc or JSON-RPC)

use anyhow::Result;
use swarmvine_core::protocol_negotiation::{IpcProtocol, negotiate_server};
use swarmvine_core::ribocipher;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{info, warn};

pub(crate) use swarmvine_core::transport::TransportEndpoint;

/// Resolve the default listen endpoint from ecosystem socket conventions.
///
/// Respects `PRIMAL_BIND_MODE` for platforms where UDS is unavailable (Android/SELinux).
fn default_endpoint(primal_name: &str, family_id: Option<&str>) -> TransportEndpoint {
    let bind_mode = std::env::var(swarmvine_core::env_keys::PRIMAL_BIND_MODE).unwrap_or_default();
    if bind_mode == "tcp_only" {
        return TransportEndpoint::Tcp {
            host: "127.0.0.1".to_owned(),
            port: 0,
        };
    }

    TransportEndpoint::platform_default(primal_name, family_id)
}

/// Run the server on the given (or default) transport endpoint.
///
/// When `negotiate` is true (the default), uses G65 protocol negotiation to multiplex
/// tarpc and JSON-RPC on a single socket. When false (legacy dual-socket mode), runs
/// Phase 2 JSON-RPC only on the primary socket.
pub(crate) async fn run(
    primal_name: &str,
    family_id: Option<&str>,
    injected_endpoint: Option<&TransportEndpoint>,
    primal: &swarmvine_core::SwarmVinePrimal,
    negotiate: bool,
) -> Result<()> {
    let endpoint = injected_endpoint
        .cloned()
        .unwrap_or_else(|| default_endpoint(primal_name, family_id));

    let gate = crate::method_gate::MethodGate::permissive();

    match &endpoint {
        TransportEndpoint::Uds { path } => {
            let listener = swarmvine_core::transport::bind_transport(&endpoint).await?;
            if negotiate {
                info!("Listening on unix://{path} [G65 protocol negotiation]");
            } else {
                info!("Listening on unix://{path}");
            }

            let announce_socket = std::path::PathBuf::from(path);
            let announce_family = family_id.unwrap_or("ecoPrimal").to_owned();
            let announce_name = primal_name.to_owned();
            let register_name = primal_name.to_owned();
            let register_endpoint = serde_json::json!(&endpoint);
            tokio::spawn(async move {
                crate::announce::announce_to_biomeos(
                    &announce_name,
                    &announce_socket,
                    &announce_family,
                )
                .await;
                crate::announce::register_with_songbird(&register_name, &register_endpoint).await;
            });

            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        let stream = accept?;
                        if negotiate {
                            handle_negotiated_connection(stream, primal, &gate).await;
                        } else {
                            handle_connection(stream, primal, &gate).await;
                        }
                    }
                    () = swarmvine_core::platform_signal::shutdown_signal() => {
                        info!("graceful shutdown initiated");
                        return Ok(());
                    }
                }
            }
        }
        TransportEndpoint::Tcp { host, port } => {
            let addr = format!("{host}:{port}");
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            info!("Listening on tcp://{addr}");

            // Phase 2: spawn per-connection handlers with Arc<SwarmVinePrimal> + Arc<MethodGate>.
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        let (stream, _) = accept?;
                        if negotiate {
                            handle_negotiated_connection(stream, primal, &gate).await;
                        } else {
                            handle_connection(stream, primal, &gate).await;
                        }
                    }
                    () = swarmvine_core::platform_signal::shutdown_signal() => {
                        info!("graceful shutdown initiated");
                        return Ok(());
                    }
                }
            }
        }
        TransportEndpoint::MeshRelay { .. } => {
            anyhow::bail!(
                "MeshRelay endpoints are not directly bindable — \
                 register capabilities with Songbird and let it route traffic"
            );
        }
    }
}

/// Handle a cross-gate TCP connection (public entry point for gossip listener).
pub(crate) async fn handle_tcp_connection(
    stream: tokio::net::TcpStream,
    primal: &swarmvine_core::SwarmVinePrimal,
    gate: &crate::method_gate::MethodGate,
) {
    handle_connection(stream, primal, gate).await;
}

async fn handle_connection<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut stream: S,
    primal: &swarmvine_core::SwarmVinePrimal,
    gate: &crate::method_gate::MethodGate,
) {
    use tokio::io::AsyncReadExt;

    let mut first = [0u8; 1];
    match stream.read(&mut first).await {
        Ok(0) => return,
        Err(e) => {
            warn!("Connection read error: {e}");
            return;
        }
        Ok(_) => {}
    }

    match first[0] {
        ribocipher::SIGNAL_CLEAR => {
            // riboCipher clear signal — read protocol type
            let mut proto = [0u8; 1];
            if stream.read_exact(&mut proto).await.is_err() {
                return;
            }
            match proto[0] {
                ribocipher::PROTOCOL_JSONRPC => {
                    handle_jsonrpc(BufReader::new(stream), primal, gate).await;
                }
                ribocipher::PROTOCOL_HEALTH_PROBE => {
                    // Health probe — respond with empty JSON-RPC success
                    if let Err(e) = tokio::io::AsyncWriteExt::write_all(
                        &mut stream,
                        b"{\"jsonrpc\":\"2.0\",\"result\":\"ok\",\"id\":0}\n",
                    )
                    .await
                    {
                        warn!("Probe write error: {e}");
                    }
                }
                other => warn!("Unsupported riboCipher protocol type: 0x{other:02X}"),
            }
        }
        ribocipher::SIGNAL_MITO => {
            // Mito-obfuscated signal — consume 4-byte HMAC tag, route as JSON-RPC
            let mut tag = [0u8; 4];
            if stream.read_exact(&mut tag).await.is_err() {
                return;
            }
            handle_jsonrpc(BufReader::new(stream), primal, gate).await;
        }
        ribocipher::SIGNAL_NUCLEAR => {
            // Nuclear-sealed signal — consume 6-byte payload, route as JSON-RPC
            let mut payload = [0u8; 6];
            if stream.read_exact(&mut payload).await.is_err() {
                return;
            }
            handle_jsonrpc(BufReader::new(stream), primal, gate).await;
        }
        first if first.is_ascii_graphic() || first == b' ' => {
            // Legacy unsignalled JSON-RPC — backward-compatible during rolling deploys.
            // Peers running pre-riboCipher binaries connect without [0xEC, 0x01] prefix.
            // Accept gracefully so gossip remains bidirectional during upgrade windows.
            warn!(
                "Legacy unsignalled connection (first byte 0x{first:02X}) — \
                 accepting as JSON-RPC for rolling-deploy compatibility"
            );
            handle_jsonrpc_legacy(stream, primal, gate, first).await;
        }
        other => {
            warn!("Unknown first byte 0x{other:02X} — dropping connection");
        }
    }
}

async fn handle_jsonrpc<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut reader: BufReader<S>,
    primal: &swarmvine_core::SwarmVinePrimal,
    gate: &crate::method_gate::MethodGate,
) {
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => return,
            Err(e) => {
                warn!("Read error: {e}");
                return;
            }
            Ok(_) => {}
        }

        let response = crate::dispatch::handle_request(line.trim(), primal, gate).await;
        let writer = reader.get_mut();
        if let Err(e) = writer.write_all(response.as_bytes()).await {
            warn!("Write error: {e}");
            return;
        }
        if let Err(e) = writer.write_all(b"\n").await {
            warn!("Write error: {e}");
            return;
        }
    }
}

/// Legacy fallback: first byte was already consumed, prepend it to the line buffer.
async fn handle_jsonrpc_legacy<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: S,
    primal: &swarmvine_core::SwarmVinePrimal,
    gate: &crate::method_gate::MethodGate,
    first_byte: u8,
) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    // First iteration: prepend the consumed byte
    line.push(char::from(first_byte));
    match reader.read_line(&mut line).await {
        Ok(0) => return,
        Err(e) => {
            warn!("Read error: {e}");
            return;
        }
        Ok(_) => {}
    }
    let response = crate::dispatch::handle_request(line.trim(), primal, gate).await;
    let writer = reader.get_mut();
    if let Err(e) = writer.write_all(response.as_bytes()).await {
        warn!("Write error: {e}");
        return;
    }
    if let Err(e) = writer.write_all(b"\n").await {
        warn!("Write error: {e}");
        return;
    }

    // Subsequent lines: normal loop
    handle_jsonrpc(reader, primal, gate).await;
}

/// G65 Protocol Negotiation handler.
///
/// Reads the client's `PROTOCOLS:` line, selects the best match,
/// then routes the connection to the appropriate handler.
/// If no negotiation header arrives (legacy client), falls back to JSON-RPC.
async fn handle_negotiated_connection<
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
>(
    mut stream: S,
    primal: &swarmvine_core::SwarmVinePrimal,
    gate: &crate::method_gate::MethodGate,
) {
    let server_supported = IpcProtocol::all_supported();
    let timeout: u64 = std::env::var(swarmvine_core::env_keys::NEGOTIATE_TIMEOUT_MS)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    match negotiate_server(&mut stream, server_supported, timeout).await {
        Ok(Some(IpcProtocol::Tarpc)) => {
            info!("G65: tarpc selected — handing off to tarpc handler");
            crate::tarpc_server::handle_tarpc_stream(stream);
        }
        Ok(Some(IpcProtocol::JsonRpc) | None) => {
            handle_jsonrpc(BufReader::new(stream), primal, gate).await;
        }
        Err(e) => {
            warn!("Protocol negotiation error: {e} — falling back to JSON-RPC");
            handle_jsonrpc(BufReader::new(stream), primal, gate).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::method_gate::MethodGate;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    fn make_primal() -> swarmvine_core::SwarmVinePrimal {
        swarmvine_core::SwarmVinePrimal::with_node_id("connTest".to_owned())
    }

    fn make_gate() -> MethodGate {
        MethodGate::permissive()
    }

    /// Drive `handle_connection` over an in-memory duplex and return the first response line.
    async fn connection_exchange(prefix: &[u8], request_line: Option<&str>) -> String {
        let (client, server) = tokio::io::duplex(8192);
        let primal = make_primal();
        let gate = make_gate();

        let server_task = tokio::spawn(async move {
            handle_connection(server, &primal, &gate).await;
        });

        let mut client = BufReader::new(client);
        client.write_all(prefix).await.unwrap();
        if let Some(line) = request_line {
            client.write_all(line.as_bytes()).await.unwrap();
        }
        client.get_mut().shutdown().await.unwrap();

        let mut response = String::new();
        client.read_line(&mut response).await.unwrap();

        server_task.await.unwrap();
        response
    }

    #[tokio::test]
    async fn ribocipher_clear_jsonrpc_responds() {
        let response = connection_exchange(
            &ribocipher::CLEAR_JSONRPC,
            Some("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"health.liveness\"}\n"),
        )
        .await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["result"]["alive"], true);
    }

    #[tokio::test]
    async fn legacy_unsignalled_json_responds() {
        let response = connection_exchange(
            b"{",
            Some("\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"health.liveness\"}\n"),
        )
        .await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["result"]["alive"], true);
    }

    #[tokio::test]
    async fn legacy_unsignalled_gossip_spread_accepted() {
        let spread_req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "gossip.spread",
            "params": {
                "entries": [{
                    "topic": "tower",
                    "key": "endpoint.alive:peerGate",
                    "payload": {"port": 7800},
                    "origin_gate": "peerGate",
                    "nonce": "aabbccdd11223344aabbccdd11223344aabbccdd11223344",
                    "ttl": 5,
                    "version": 1,
                    "created_at_epoch": 1_723_456_789,
                    "expires_at_epoch": 1_823_456_789
                }]
            }
        });
        let line = format!("{spread_req}\n");
        let response = connection_exchange(b"{", Some(&line[1..])).await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert!(
            parsed["result"]["accepted"].as_u64().unwrap_or(0) > 0
                || parsed["result"]["rejected"].is_u64(),
            "gossip.spread should process without riboCipher prefix: {parsed}"
        );
    }

    #[tokio::test]
    async fn ribocipher_health_probe_returns_ok() {
        let response = connection_exchange(
            &[ribocipher::SIGNAL_CLEAR, ribocipher::PROTOCOL_HEALTH_PROBE],
            None,
        )
        .await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["result"], "ok");
        assert_eq!(parsed["id"], 0);
    }

    #[test]
    fn default_endpoint_tcp_only_from_env() {
        temp_env::with_var(
            swarmvine_core::env_keys::PRIMAL_BIND_MODE,
            Some("tcp_only"),
            || {
                let endpoint = default_endpoint("swarmVine", None);
                match endpoint {
                    TransportEndpoint::Tcp { host, port } => {
                        assert_eq!(host, "127.0.0.1");
                        assert_eq!(port, 0);
                    }
                    other => panic!("expected Tcp endpoint, got {other:?}"),
                }
            },
        );
    }

    fn mito_prefix() -> Vec<u8> {
        let mut prefix = vec![ribocipher::SIGNAL_MITO];
        prefix.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        prefix
    }

    fn nuclear_prefix() -> Vec<u8> {
        let mut prefix = vec![ribocipher::SIGNAL_NUCLEAR];
        prefix.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
        prefix
    }

    #[tokio::test]
    async fn ribocipher_mito_jsonrpc_responds() {
        let response = connection_exchange(
            &mito_prefix(),
            Some("{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"health.liveness\"}\n"),
        )
        .await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["result"]["alive"], true);
    }

    #[tokio::test]
    async fn ribocipher_nuclear_jsonrpc_responds() {
        let response = connection_exchange(
            &nuclear_prefix(),
            Some("{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"health.liveness\"}\n"),
        )
        .await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["result"]["alive"], true);
    }

    /// Drive `handle_negotiated_connection` over duplex; returns protocol line + first JSON-RPC line.
    async fn negotiated_exchange(
        client_prefix: &str,
        request_line: Option<&str>,
    ) -> (String, String) {
        let (client, server) = tokio::io::duplex(8192);
        let primal = make_primal();
        let gate = make_gate();

        let server_task = tokio::spawn(async move {
            handle_negotiated_connection(server, &primal, &gate).await;
        });

        let mut client = BufReader::new(client);
        client.write_all(client_prefix.as_bytes()).await.unwrap();
        client.flush().await.unwrap();

        let mut protocol_line = String::new();
        if client_prefix.starts_with("PROTOCOLS:") {
            client.read_line(&mut protocol_line).await.unwrap();
        }

        if let Some(line) = request_line {
            client.write_all(line.as_bytes()).await.unwrap();
            client.flush().await.unwrap();
        }
        client.get_mut().shutdown().await.unwrap();

        let mut response = String::new();
        client.read_line(&mut response).await.unwrap();

        server_task.await.unwrap();
        (protocol_line, response)
    }

    #[tokio::test]
    async fn g65_negotiation_jsonrpc_responds() {
        let (protocol_line, response) = negotiated_exchange(
            "PROTOCOLS: jsonrpc\n",
            Some("{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"health.liveness\"}\n"),
        )
        .await;
        assert_eq!(protocol_line.trim(), "PROTOCOL: jsonrpc");
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["result"]["alive"], true);
    }

    #[tokio::test]
    async fn g65_negotiation_tarpc_selects_tarpc() {
        let (protocol_line, _) = negotiated_exchange("PROTOCOLS: tarpc,jsonrpc\n", None).await;
        assert_eq!(protocol_line.trim(), "PROTOCOL: tarpc");
    }
}

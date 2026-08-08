//! Transport-injected server with first-byte protocol detection.
//!
//! Supports two modes:
//! - **Phase 2 (C2)**: First-byte riboCipher detection on JSON-RPC socket
//! - **Phase 3 (G65)**: Protocol negotiation on single socket (tarpc or JSON-RPC)

use anyhow::Result;
use serde::{Deserialize, Serialize};
use swarmvine_core::protocol_negotiation::{IpcProtocol, negotiate_server};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{error, info, warn};

/// Structured transport endpoint — wire-compatible with songbird_types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "transport")]
pub enum TransportEndpoint {
    /// Unix Domain Socket.
    #[serde(rename = "uds")]
    Uds { path: String },
    /// TCP socket.
    #[serde(rename = "tcp")]
    Tcp { host: String, port: u16 },
    /// Mesh relay (not directly bindable, routed via Songbird).
    #[serde(rename = "mesh_relay")]
    MeshRelay { peer_id: String, capability: String },
}

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

    let socket_dir = std::env::var(swarmvine_core::env_keys::BIOMEOS_SOCKET_DIR).unwrap_or_else(|_| {
        let runtime_dir =
            std::env::var(swarmvine_core::env_keys::XDG_RUNTIME_DIR).unwrap_or_else(|_| "/tmp".to_owned());
        format!("{runtime_dir}/biomeos")
    });

    let filename = match family_id.filter(|id| !id.is_empty() && *id != "default") {
        Some(fid) => format!("{primal_name}-{fid}.sock"),
        None => format!("{primal_name}.sock"),
    };

    TransportEndpoint::Uds {
        path: std::path::PathBuf::from(&socket_dir)
            .join(&filename)
            .to_string_lossy()
            .into_owned(),
    }
}

/// Run the server on the given (or default) transport endpoint.
///
/// When `negotiate` is true, uses G65 protocol negotiation to multiplex
/// tarpc and JSON-RPC on a single socket. Otherwise runs Phase 2 (JSON-RPC only).
pub async fn run(
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
            let socket_path = std::path::PathBuf::from(path);
            if let Some(parent) = socket_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let _ = tokio::fs::remove_file(&socket_path).await;

            let listener = tokio::net::UnixListener::bind(&socket_path)?;
            if negotiate {
                info!("Listening on unix://{path} [G65 protocol negotiation]");
            } else {
                info!("Listening on unix://{path}");
            }

            let announce_socket = socket_path.clone();
            let announce_family = family_id.unwrap_or("ecoPrimal").to_owned();
            let announce_name = primal_name.to_owned();
            let register_name = primal_name.to_owned();
            let register_endpoint = serde_json::to_value(&endpoint).unwrap_or_default();
            tokio::spawn(async move {
                crate::announce::announce_to_biomeos(
                    &announce_name,
                    &announce_socket,
                    &announce_family,
                )
                .await;
                crate::announce::register_with_songbird(
                    &register_name,
                    &register_endpoint,
                )
                .await;
            });

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
                    _ = swarmvine_core::platform_signal::shutdown_signal() => {
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
                    _ = swarmvine_core::platform_signal::shutdown_signal() => {
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

/// riboCipher signal bytes (Wave 111 — RIBOCIPHER_TRANSPORT_SIGNAL_STANDARD.md).
const SIGNAL_CLEAR: u8 = 0xEC;
const SIGNAL_MITO: u8 = 0xED;
const SIGNAL_NUCLEAR: u8 = 0xEE;

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
        SIGNAL_CLEAR => {
            // riboCipher clear signal — read protocol type
            let mut proto = [0u8; 1];
            if stream.read_exact(&mut proto).await.is_err() {
                return;
            }
            match proto[0] {
                0x01 => handle_jsonrpc(BufReader::new(stream), primal, gate).await,
                0x00 => {
                    // Health probe — respond with empty JSON-RPC success
                    if let Err(e) = tokio::io::AsyncWriteExt::write_all(
                        &mut stream,
                        b"{\"jsonrpc\":\"2.0\",\"result\":\"ok\",\"id\":0}\n",
                    ).await {
                        warn!("Probe write error: {e}");
                    }
                }
                other => warn!("Unsupported riboCipher protocol type: 0x{other:02X}"),
            }
        }
        SIGNAL_MITO => {
            // Mito-obfuscated signal — consume 4-byte HMAC tag, route as JSON-RPC
            let mut tag = [0u8; 4];
            if stream.read_exact(&mut tag).await.is_err() {
                return;
            }
            handle_jsonrpc(BufReader::new(stream), primal, gate).await;
        }
        SIGNAL_NUCLEAR => {
            // Nuclear-sealed signal — consume 6-byte payload, route as JSON-RPC
            let mut payload = [0u8; 6];
            if stream.read_exact(&mut payload).await.is_err() {
                return;
            }
            handle_jsonrpc(BufReader::new(stream), primal, gate).await;
        }
        b'{' => {
            // DEPRECATED: unsignalled legacy JSON-RPC (Wave 112 — ERROR level, Wave 113 rejects)
            error!("DEPRECATED: unsignalled connection — prepend [0xEC, 0x01] for riboCipher");
            handle_jsonrpc_legacy(stream, primal, gate, b'{').await;
        }
        other => {
            error!("DEPRECATED: unsignalled connection (first byte 0x{other:02X}) — not yet handled");
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

        let response = crate::dispatch::handle_request(line.trim(), primal, gate);
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
    line.push(first_byte as char);
    match reader.read_line(&mut line).await {
        Ok(0) => return,
        Err(e) => {
            warn!("Read error: {e}");
            return;
        }
        Ok(_) => {}
    }
    let response = crate::dispatch::handle_request(line.trim(), primal, gate);
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
async fn handle_negotiated_connection<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static>(
    mut stream: S,
    primal: &swarmvine_core::SwarmVinePrimal,
    gate: &crate::method_gate::MethodGate,
) {
    let server_supported = IpcProtocol::all_supported();

    match negotiate_server(&mut stream, &server_supported, 100).await {
        Ok(Some(IpcProtocol::Tarpc)) => {
            info!("G65: tarpc selected — handing off to tarpc handler");
            crate::tarpc_server::handle_tarpc_stream(stream).await;
        }
        Ok(Some(IpcProtocol::JsonRpc)) | Ok(None) => {
            handle_jsonrpc(BufReader::new(stream), primal, gate).await;
        }
        Err(e) => {
            warn!("Protocol negotiation error: {e} — falling back to JSON-RPC");
            handle_jsonrpc(BufReader::new(stream), primal, gate).await;
        }
    }
}

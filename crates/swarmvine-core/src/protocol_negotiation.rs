// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! G65 Protocol Negotiation — single-socket protocol selection.
//!
//! Enables automatic protocol selection between JSON-RPC and tarpc at connection time.
//! Phase 3 of cephalization: one socket serves both protocols via negotiation.
//!
//! Wire format:
//! ```text
//! Client → Server: "PROTOCOLS: tarpc,jsonrpc\n"
//! Server → Client: "PROTOCOL: tarpc\n"
//! ```
//!
//! No `PROTOCOLS:` line = legacy client → assume JSON-RPC.

use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tracing::{info, warn};

/// RPC protocol variants supported by the ecoPrimals ecosystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum IpcProtocol {
    /// JSON-RPC 2.0 — text-based, backward-compatible default.
    #[default]
    JsonRpc,
    /// tarpc — binary, type-safe, high-performance.
    Tarpc,
}

impl fmt::Display for IpcProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.wire_name())
    }
}

impl IpcProtocol {
    /// Wire name used in negotiation lines.
    #[must_use]
    pub const fn wire_name(&self) -> &'static str {
        match self {
            Self::JsonRpc => "jsonrpc",
            Self::Tarpc => "tarpc",
        }
    }

    /// Parse from wire name.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "jsonrpc" | "json-rpc" | "json_rpc" => Some(Self::JsonRpc),
            "tarpc" | "binary" => Some(Self::Tarpc),
            _ => None,
        }
    }

    /// All supported protocols (tarpc preferred).
    #[must_use]
    pub const fn all_supported() -> &'static [Self] {
        &[Self::Tarpc, Self::JsonRpc]
    }
}

/// Select best protocol from client's preference list.
#[must_use]
pub fn select_protocol(
    client_prefs: &[IpcProtocol],
    server_supports: &[IpcProtocol],
) -> IpcProtocol {
    for proto in client_prefs {
        if server_supports.contains(proto) {
            return *proto;
        }
    }
    IpcProtocol::JsonRpc
}

/// Result of G65 protocol negotiation.
#[derive(Debug)]
pub enum NegotiateResult {
    /// Client sent `PROTOCOLS:` header and a protocol was selected.
    Negotiated(IpcProtocol),
    /// Client sent a non-PROTOCOLS line (legacy JSON-RPC). The consumed
    /// line is returned so the caller can feed it to the JSON-RPC handler
    /// without data loss.
    Legacy(String),
    /// Timeout or empty read — no data consumed from the transport.
    Timeout,
}

/// Server-side negotiation: read client request, select best, respond.
///
/// Returns [`NegotiateResult::Legacy`] with the consumed line if the client
/// sends raw JSON-RPC instead of a `PROTOCOLS:` header, preventing the
/// data-loss bug where `BufReader` consumed the request line during
/// protocol detection.
///
/// # Errors
///
/// Returns an error if writing the negotiated protocol response to the transport
/// fails. Read failures and timeouts yield [`NegotiateResult::Timeout`] for
/// backward compatibility.
pub async fn negotiate_server<T>(
    transport: &mut T,
    server_supported: &[IpcProtocol],
    timeout_ms: u64,
) -> Result<NegotiateResult, std::io::Error>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut reader = BufReader::new(transport);
    let mut line = String::new();

    let read_result = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        reader.read_line(&mut line),
    )
    .await;

    match read_result {
        Ok(Ok(n)) if n > 0 => {
            if line.trim().starts_with("PROTOCOLS: ") {
                let protocols: Vec<IpcProtocol> = line
                    .trim()
                    .strip_prefix("PROTOCOLS: ")
                    .unwrap_or("")
                    .split(',')
                    .filter_map(|s| IpcProtocol::parse(s.trim()))
                    .collect();

                if protocols.is_empty() {
                    return Ok(NegotiateResult::Legacy(line));
                }

                let selected = select_protocol(&protocols, server_supported);
                let response = format!("PROTOCOL: {}\n", selected.wire_name());
                reader.get_mut().write_all(response.as_bytes()).await?;
                reader.get_mut().flush().await?;
                info!("server negotiated: {selected}");
                Ok(NegotiateResult::Negotiated(selected))
            } else {
                warn!("no protocol negotiation, assuming JSON-RPC");
                Ok(NegotiateResult::Legacy(line))
            }
        }
        Ok(Err(e)) => {
            warn!("negotiation read error: {e}");
            Ok(NegotiateResult::Timeout)
        }
        _ => Ok(NegotiateResult::Timeout),
    }
}

#[cfg(test)]
mod tests {
    use super::{IpcProtocol, NegotiateResult, negotiate_server, select_protocol};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[test]
    fn wire_name_for_variants() {
        assert_eq!(IpcProtocol::JsonRpc.wire_name(), "jsonrpc");
        assert_eq!(IpcProtocol::Tarpc.wire_name(), "tarpc");
    }

    #[test]
    fn parse_accepts_aliases() {
        for name in ["jsonrpc", "json-rpc", "json_rpc"] {
            assert_eq!(IpcProtocol::parse(name), Some(IpcProtocol::JsonRpc));
        }
        for name in ["tarpc", "binary"] {
            assert_eq!(IpcProtocol::parse(name), Some(IpcProtocol::Tarpc));
        }
    }

    #[test]
    fn parse_unknown_returns_none() {
        assert_eq!(IpcProtocol::parse("grpc"), None);
        assert_eq!(IpcProtocol::parse(""), None);
    }

    #[test]
    fn all_supported_contains_both_variants() {
        let supported = IpcProtocol::all_supported();
        assert!(supported.contains(&IpcProtocol::Tarpc));
        assert!(supported.contains(&IpcProtocol::JsonRpc));
        assert_eq!(supported.len(), 2);
    }

    #[test]
    fn select_protocol_prefers_client_order() {
        let client = [IpcProtocol::JsonRpc, IpcProtocol::Tarpc];
        let server = [IpcProtocol::Tarpc, IpcProtocol::JsonRpc];
        assert_eq!(select_protocol(&client, &server), IpcProtocol::JsonRpc);

        let client = [IpcProtocol::Tarpc, IpcProtocol::JsonRpc];
        assert_eq!(select_protocol(&client, &server), IpcProtocol::Tarpc);
    }

    #[test]
    fn select_protocol_falls_back_to_json_rpc() {
        let client = [IpcProtocol::Tarpc];
        let server = [IpcProtocol::JsonRpc];
        assert_eq!(select_protocol(&client, &server), IpcProtocol::JsonRpc);
    }

    #[test]
    fn default_is_json_rpc() {
        assert_eq!(IpcProtocol::default(), IpcProtocol::JsonRpc);
    }

    #[test]
    fn display_matches_wire_name() {
        assert_eq!(IpcProtocol::JsonRpc.to_string(), "jsonrpc");
        assert_eq!(IpcProtocol::Tarpc.to_string(), "tarpc");
    }

    #[tokio::test]
    async fn negotiation_roundtrip_duplex() {
        let (client, server) = tokio::io::duplex(256);
        let (mut client, mut server) = (client, server);

        let server_task = tokio::spawn(async move {
            negotiate_server(&mut server, IpcProtocol::all_supported(), 1000).await
        });

        client
            .write_all(b"PROTOCOLS: tarpc,jsonrpc\n")
            .await
            .unwrap();
        client.flush().await.unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line.trim(), "PROTOCOL: tarpc");

        let result = server_task.await.unwrap().unwrap();
        assert!(matches!(result, NegotiateResult::Negotiated(IpcProtocol::Tarpc)));
    }

    #[tokio::test]
    async fn negotiation_legacy_client_returns_consumed_line() {
        let (client, server) = tokio::io::duplex(256);
        let (mut client, mut server) = (client, server);

        let server_task = tokio::spawn(async move {
            negotiate_server(&mut server, IpcProtocol::all_supported(), 1000).await
        });

        client.write_all(b"{\"jsonrpc\":\"2.0\"}\n").await.unwrap();
        client.flush().await.unwrap();

        let result = server_task.await.unwrap().unwrap();
        match result {
            NegotiateResult::Legacy(line) => {
                assert_eq!(line.trim(), "{\"jsonrpc\":\"2.0\"}");
            }
            other => panic!("expected Legacy, got {other:?}"),
        }
    }
}

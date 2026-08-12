// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! End-to-end integration tests for the swarmVine server.

use std::time::Duration;
use swarmvine_core::ribocipher;
use swarmvine_core::{PrimalHealth, PrimalLifecycle, PrimalState, SwarmVinePrimal};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[cfg(unix)]
use std::path::Path;
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(unix)]
use swarmvine_server::test_support::TestServer;

/// Create a primal, exercise lifecycle transitions, and verify health.
#[tokio::test]
async fn server_lifecycle() {
    let mut primal = SwarmVinePrimal::with_node_id("lifecycle-it".to_owned());
    assert_eq!(primal.state(), PrimalState::Created);
    assert!(!primal.health_status().is_healthy());

    primal.start().await.unwrap();
    assert_eq!(primal.state(), PrimalState::Running);
    assert!(primal.health_status().is_healthy());

    let report = primal.health_check().await.unwrap();
    assert_eq!(report.name, "swarmVine");
    assert!(report.readiness);

    primal.stop().await.unwrap();
    assert_eq!(primal.state(), PrimalState::Stopped);
    assert!(!primal.health_status().is_healthy());
}

/// Start a UDS server, send `health.check`, and verify JSON-RPC response shape.
#[cfg(unix)]
#[tokio::test]
async fn json_rpc_roundtrip() {
    let server = TestServer::start("json_rpc_roundtrip", true).await;
    let response = uds_json_rpc(
        &server.uds_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"health.check"}"#,
        Some(&ribocipher::CLEAR_JSONRPC),
    )
    .await;

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    assert!(response["result"]["status"].is_string());
    assert!(response["result"]["liveness"].is_boolean());
    assert!(response["result"]["readiness"].is_boolean());
    assert_eq!(response["result"]["node_id"], "it-json_rpc_roundtrip");
}

/// Inject gossip via JSON-RPC and query it back on the same connection.
#[cfg(unix)]
#[tokio::test]
async fn gossip_inject_and_query_via_rpc() {
    let server = TestServer::start("gossip_inject_query", true).await;
    let key = "capability.advertise:it-gossip:peer";

    let inject_resp = uds_json_rpc(
        &server.uds_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"gossip.inject","params":{{"topic":"tower","key":"{key}","payload":{{"capabilities":["content.get"]}}}}}}"#
        ),
        Some(&ribocipher::CLEAR_JSONRPC),
    )
    .await;
    assert_eq!(inject_resp["result"]["result"], "Accepted");

    let query_resp = uds_json_rpc(
        &server.uds_path,
        r#"{"jsonrpc":"2.0","id":3,"method":"gossip.query","params":{"topic":"tower","key_prefix":"capability.advertise"}}"#,
        Some(&ribocipher::CLEAR_JSONRPC),
    )
    .await;

    assert_eq!(query_resp["result"]["count"], 1);
    let entries = query_resp["result"]["entries"].as_array().unwrap();
    assert_eq!(entries[0]["key"], key);
    assert_eq!(entries[0]["origin_gate"], "it-gossip_inject_query");
}

/// Two TCP gossip clients: one spreads, the other queries.
#[cfg(unix)]
#[tokio::test]
async fn gossip_spread_cross_connection() {
    let server = TestServer::start("gossip_spread", true).await;
    let spread_key = "cas.have:spread-it";

    let spread_req = format!(
        r#"{{"jsonrpc":"2.0","id":10,"method":"gossip.spread","params":{{"from":"peerA","entry":{{"topic":"data","key":"{spread_key}","payload":{{"hashes":["abc123"]}},"origin_gate":"peerA","nonce":"deadbeef00000001deadbeef00000002","ttl":5,"version":1,"created_at_epoch":1723100000,"expires_at_epoch":1923100000}}}}}}"#
    );

    let spread_resp = tcp_json_rpc(
        server.gossip_port,
        &spread_req,
        Some(&ribocipher::CLEAR_JSONRPC),
    )
    .await;
    assert_eq!(spread_resp["result"]["accepted"], 1);

    let query_req = format!(
        r#"{{"jsonrpc":"2.0","id":11,"method":"gossip.query","params":{{"topic":"data","key_prefix":"{spread_key}"}}}}"#
    );
    let query_resp = tcp_json_rpc(
        server.gossip_port,
        &query_req,
        Some(&ribocipher::CLEAR_JSONRPC),
    )
    .await;

    assert_eq!(query_resp["result"]["count"], 1);
    assert_eq!(query_resp["result"]["entries"][0]["key"], spread_key);
}

/// G65 negotiate mode: legacy client skips PROTOCOLS line and still gets JSON-RPC.
#[cfg(unix)]
#[tokio::test]
async fn protocol_negotiation_jsonrpc() {
    temp_env::async_with_vars(
        [(swarmvine_core::env_keys::NEGOTIATE_TIMEOUT_MS, Some("50"))],
        async {
            let server = TestServer::start("protocol_neg", false).await;

            let stream = UnixStream::connect(&server.uds_path)
                .await
                .expect("connect uds");

            // Legacy client: open connection, wait past negotiation timeout, then send JSON-RPC.
            tokio::time::sleep(Duration::from_millis(80)).await;

            let response = exchange_json_rpc(
                stream,
                r#"{"jsonrpc":"2.0","id":20,"method":"health.liveness"}"#,
                None,
            )
            .await;

            assert_eq!(response["jsonrpc"], "2.0");
            assert_eq!(response["result"]["alive"], true);
        },
    )
    .await;
}

/// riboCipher clear prefix (0xEC 0x01) before JSON-RPC payload.
#[cfg(unix)]
#[tokio::test]
async fn ribocipher_clear_prefix() {
    let server = TestServer::start("ribocipher_clear", true).await;

    let response = uds_json_rpc(
        &server.uds_path,
        r#"{"jsonrpc":"2.0","id":30,"method":"health.check"}"#,
        Some(&ribocipher::CLEAR_JSONRPC),
    )
    .await;

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 30);
    assert!(response["result"]["liveness"].as_bool().unwrap());
}

#[cfg(unix)]
async fn uds_json_rpc(path: &Path, request: &str, prefix: Option<&[u8]>) -> serde_json::Value {
    let stream = UnixStream::connect(path).await.expect("connect uds");
    exchange_json_rpc(stream, request, prefix).await
}

#[cfg(unix)]
async fn tcp_json_rpc(port: u16, request: &str, prefix: Option<&[u8]>) -> serde_json::Value {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("connect tcp");
    exchange_json_rpc(stream, request, prefix).await
}

async fn exchange_json_rpc<S>(stream: S, request: &str, prefix: Option<&[u8]>) -> serde_json::Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut stream = stream;
    if let Some(p) = prefix {
        stream.write_all(p).await.unwrap();
    }
    let line = if request.ends_with('\n') {
        request.to_owned()
    } else {
        format!("{request}\n")
    };
    stream.write_all(line.as_bytes()).await.unwrap();
    stream.shutdown().await.unwrap();

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response).await.unwrap();
    serde_json::from_str(response.trim()).expect("parse json-rpc response")
}

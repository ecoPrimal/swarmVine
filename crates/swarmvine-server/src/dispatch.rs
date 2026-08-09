//! JSON-RPC 2.0 method dispatch with gossip engine handlers.

use crate::method_gate::MethodGate;
use swarmvine_core::ribocipher;
use swarmvine_core::{GossipEntry, GossipTopic, IngestResult, PrimalHealth};

fn skunkbat_sock_path() -> String {
    std::env::var(swarmvine_core::env_keys::SKUNKBAT_SOCKET).unwrap_or_else(|_| {
        swarmvine_core::platform_paths::runtime_socket_dir()
            .join("skunkbat.sock")
            .to_string_lossy()
            .into_owned()
    })
}

/// Whether the vine-bat pre-accept hook is enabled. Disabled in test builds
/// unless `SWARMVINE_SKUNKBAT_SOCK` is explicitly set.
fn vine_bat_enabled() -> bool {
    if cfg!(test) {
        std::env::var(swarmvine_core::env_keys::SKUNKBAT_SOCKET).is_ok()
    } else {
        true
    }
}

const PRIMAL_NAME: &str = "swarmVine";
const PRIMAL_VERSION: &str = env!("CARGO_PKG_VERSION");

pub(crate) const METHODS: &[&str] = &[
    "health.liveness",
    "health.readiness",
    "health.check",
    "capabilities.list",
    "btsp.negotiate",
    "primal.announce",
    "gossip.spread",
    "gossip.inject",
    "gossip.query",
    "gossip.status",
    "gossip.peers",
    "gossip.subscribe",
];

/// Dispatch a JSON-RPC request and return the response string.
pub(crate) async fn handle_request(
    raw: &str,
    primal: &swarmvine_core::SwarmVinePrimal,
    gate: &MethodGate,
) -> String {
    let req: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return error_response(&serde_json::Value::Null, -32700, "Parse error"),
    };

    let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let method = req
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");

    if let Err(denial) = gate.check(method) {
        return error_response(&id, denial.code, &denial.message);
    }

    let params = req
        .get("params")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    let result = match method {
        "health.liveness" => serde_json::json!({ "alive": true }),
        "health.readiness" => {
            let ready = primal.is_ready();
            serde_json::json!({
                "ready": ready,
                "capabilities": METHODS,
            })
        }
        "health.check" => {
            let status = primal.health_status();
            serde_json::json!({
                "status": format!("{status:?}"),
                "liveness": primal.is_live(),
                "readiness": primal.is_ready(),
                "node_id": primal.engine().node_id(),
            })
        }
        "capabilities.list" | "capability.list" => {
            serde_json::json!({
                "primal": PRIMAL_NAME,
                "version": PRIMAL_VERSION,
                "methods": METHODS,
                "protocol": "jsonrpc-2.0",
                "transport": ["uds", "tcp"],
                "gossip_domains": ["tower", "data", "compute"],
            })
        }
        "btsp.negotiate" => {
            serde_json::json!({
                "cipher": "null",
                "server_nonce": null,
            })
        }
        "primal.announce" => {
            serde_json::json!({
                "primal": PRIMAL_NAME,
                "version": PRIMAL_VERSION,
                "capabilities": crate::announce::capabilities(),
                "methods": METHODS,
                "signal_tiers": crate::announce::signal_tiers(),
            })
        }

        // --- Gossip methods ---
        "gossip.spread" => handle_gossip_spread(primal, &params).await,
        "gossip.inject" => handle_gossip_inject(primal, &params).await,
        "gossip.query" => handle_gossip_query(primal, &params).await,
        "gossip.status" => handle_gossip_status(primal).await,
        "gossip.peers" => handle_gossip_peers(primal).await,
        "gossip.subscribe" => handle_gossip_subscribe(primal, &params).await,

        _ => return error_response(&id, -32601, "Method not found"),
    };

    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
    .to_string()
}

/// Handle `gossip.spread` — receive gossip entries from mesh peers.
///
/// Accepts a single entry or a batch.
async fn handle_gossip_spread(
    primal: &swarmvine_core::SwarmVinePrimal,
    params: &serde_json::Value,
) -> serde_json::Value {
    let from_peer = params
        .get("from")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");

    // Support single entry or batch (reference slice — no array clone)
    let entry_refs: Vec<&serde_json::Value> =
        if let Some(arr) = params.get("entries").and_then(serde_json::Value::as_array) {
            arr.iter().collect()
        } else if let Some(entry) = params.get("entry") {
            vec![entry]
        } else {
            return serde_json::json!({ "error": "Missing 'entry' or 'entries' param" });
        };

    let mut accepted = 0u32;
    let mut rejected = 0u32;

    for raw_entry in entry_refs {
        let entry: GossipEntry = if let Ok(e) = serde_json::from_value(raw_entry.clone()) {
            e
        } else {
            rejected += 1;
            continue;
        };

        // Vine-bat loop: pre-accept validation via skunkBat metadata.analyze
        if !vine_bat_preaccept(&entry).await {
            tracing::debug!(key = %entry.key, "vine-bat REJECT from skunkBat");
            rejected += 1;
            continue;
        }

        match primal.engine().ingest(entry, Some(from_peer)).await {
            IngestResult::Accepted => accepted += 1,
            _ => rejected += 1,
        }
    }

    serde_json::json!({
        "accepted": accepted,
        "rejected": rejected,
    })
}

/// Handle `gossip.inject` — inject a locally-originated entry.
async fn handle_gossip_inject(
    primal: &swarmvine_core::SwarmVinePrimal,
    params: &serde_json::Value,
) -> serde_json::Value {
    let topic = match params.get("topic").and_then(serde_json::Value::as_str) {
        Some("tower") => GossipTopic::Tower,
        Some("data") => GossipTopic::Data,
        Some("compute") => GossipTopic::Compute,
        _ => {
            return serde_json::json!({ "error": "Invalid or missing 'topic' (tower|data|compute)" });
        }
    };

    let key = match params.get("key").and_then(serde_json::Value::as_str) {
        Some(k) => k.to_owned(),
        None => return serde_json::json!({ "error": "Missing 'key'" }),
    };

    let payload = params
        .get("payload")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    let result = primal.engine().inject(topic, key, payload).await;

    serde_json::json!({
        "result": format!("{result:?}"),
        "node_id": primal.engine().node_id(),
    })
}

/// Handle `gossip.query` — query the gossip table.
async fn handle_gossip_query(
    primal: &swarmvine_core::SwarmVinePrimal,
    params: &serde_json::Value,
) -> serde_json::Value {
    let topic = match params.get("topic").and_then(serde_json::Value::as_str) {
        Some("tower") => GossipTopic::Tower,
        Some("data") => GossipTopic::Data,
        Some("compute") => GossipTopic::Compute,
        _ => {
            return serde_json::json!({ "error": "Invalid or missing 'topic' (tower|data|compute)" });
        }
    };

    let prefix = params.get("key_prefix").and_then(serde_json::Value::as_str);

    let entries = primal.engine().query(topic, prefix).await;

    serde_json::json!({
        "topic": topic,
        "count": entries.len(),
        "entries": entries,
    })
}

/// Handle `gossip.status` — current gossip table statistics.
async fn handle_gossip_status(primal: &swarmvine_core::SwarmVinePrimal) -> serde_json::Value {
    let stats = primal.engine().stats().await;
    serde_json::json!(stats)
}

/// Handle `gossip.peers` — per-peer exchange statistics.
async fn handle_gossip_peers(primal: &swarmvine_core::SwarmVinePrimal) -> serde_json::Value {
    let peers = primal.engine().peer_stats().await;
    serde_json::json!({
        "node_id": primal.engine().node_id(),
        "peer_count": peers.len(),
        "peers": peers,
    })
}

/// Handle `gossip.subscribe` — snapshot-based subscription.
///
/// JSON-RPC is request/response, so true push streaming requires protocol
/// evolution (SSE / WebSocket / tarpc streaming). This handler returns a
/// current-state snapshot of entries matching the subscription filter, plus
/// the subscriber count so callers can confirm the broadcast channel is live.
/// Callers poll this method for updates until streaming is available.
async fn handle_gossip_subscribe(
    primal: &swarmvine_core::SwarmVinePrimal,
    params: &serde_json::Value,
) -> serde_json::Value {
    let topic = match params.get("topic").and_then(serde_json::Value::as_str) {
        Some("tower") => GossipTopic::Tower,
        Some("data") => GossipTopic::Data,
        Some("compute") => GossipTopic::Compute,
        _ => {
            return serde_json::json!({ "error": "Invalid or missing 'topic' (tower|data|compute)" });
        }
    };

    let prefix = params.get("key_prefix").and_then(serde_json::Value::as_str);
    let entries = primal.engine().query(topic, prefix).await;

    let subscriber_count = primal.engine().subscriber_count();

    serde_json::json!({
        "mode": "snapshot",
        "topic": topic,
        "count": entries.len(),
        "entries": entries,
        "subscriber_count": subscriber_count,
        "note": "Poll this method for updates. Broadcast channel is live for tarpc streaming subscribers.",
    })
}

/// Vine-bat pre-accept: call skunkBat `metadata.analyze` to validate a gossip entry
/// before accepting it from a remote peer. If skunkBat is unreachable, default to allow
/// (graceful degradation — bat validates when available, vine spreads regardless).
async fn vine_bat_preaccept(entry: &GossipEntry) -> bool {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::timeout;

    if !vine_bat_enabled() {
        return true;
    }
    let sock_path = skunkbat_sock_path();
    if !std::path::Path::new(&sock_path).exists() {
        return true;
    }

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "metadata.analyze",
        "params": {
            "topic": entry.topic,
            "key": entry.key,
            "origin_gate": entry.origin_gate,
            "ttl": entry.ttl,
            "payload": entry.payload,
            "created_at_epoch": entry.created_at_epoch,
            "expires_at_epoch": entry.expires_at_epoch,
        },
    });

    let endpoint = swarmvine_core::transport::TransportEndpoint::Uds {
        path: sock_path.clone(),
    };
    let Ok(Ok(stream)) = timeout(
        Duration::from_millis(500),
        swarmvine_core::transport::connect_transport(&endpoint),
    )
    .await
    else {
        tracing::debug!("vine-bat: skunkBat unreachable — default allow");
        return true;
    };

    let (mut rd, mut wr) = tokio::io::split(stream);

    let Ok(request_str) = serde_json::to_string(&request) else {
        return true;
    };
    let mut frame = ribocipher::CLEAR_JSONRPC.to_vec();
    frame.extend_from_slice(request_str.as_bytes());
    frame.push(b'\n');

    if wr.write_all(&frame).await.is_err() {
        return true;
    }
    let _ = wr.shutdown().await;

    let mut buf = Vec::with_capacity(4096);
    let _ = timeout(Duration::from_millis(500), rd.read_to_end(&mut buf)).await;

    // Strip riboCipher prefix if present
    let json_start = if buf.len() >= 2
        && buf[0] == ribocipher::SIGNAL_CLEAR
        && buf[1] == ribocipher::PROTOCOL_JSONRPC
    {
        2
    } else {
        0
    };
    let text = match std::str::from_utf8(&buf[json_start..]) {
        Ok(t) => t.trim(),
        Err(_) => return true,
    };

    if let Ok(val) = serde_json::from_str::<serde_json::Value>(text) {
        let verdict = val
            .get("result")
            .and_then(|r| r.get("verdict"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("allow");
        return match verdict {
            "deny" | "reject" => false,
            _ => true, // allow, warn → pass through
        };
    }
    true
}

fn error_response(id: &serde_json::Value, code: i32, message: &str) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::method_gate::MethodGate;

    fn make_primal() -> swarmvine_core::SwarmVinePrimal {
        swarmvine_core::SwarmVinePrimal::with_node_id("testGate".to_owned())
    }

    fn make_gate() -> MethodGate {
        MethodGate::permissive()
    }

    #[tokio::test]
    async fn liveness_returns_alive() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"health.liveness"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["alive"], true);
    }

    #[tokio::test]
    async fn capabilities_list_includes_gossip_domains() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":2,"method":"capabilities.list"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["primal"], PRIMAL_NAME);
        let domains = resp["result"]["gossip_domains"].as_array().unwrap();
        assert_eq!(domains.len(), 3);
    }

    #[tokio::test]
    async fn unknown_method_returns_error() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":3,"method":"unknown.method"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn invalid_json_returns_parse_error() {
        let primal = make_primal();
        let gate = make_gate();
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request("not json", &primal, &gate).await).unwrap();
        assert_eq!(resp["error"]["code"], -32700);
    }

    #[tokio::test]
    async fn btsp_negotiate_returns_null_cipher() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"btsp.negotiate","params":{"session_id":"test","preferred_cipher":"chacha20-poly1305","bond_type":"Covalent"}}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["cipher"], "null");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gossip_inject_and_query() {
        let primal = make_primal();
        let gate = make_gate();

        let inject_req = r#"{"jsonrpc":"2.0","id":5,"method":"gossip.inject","params":{"topic":"tower","key":"capability.advertise:testGate:nestGate","payload":{"capabilities":["content.get"]}}}"#;
        let inject_resp: serde_json::Value =
            serde_json::from_str(&handle_request(inject_req, &primal, &gate).await).unwrap();
        assert_eq!(inject_resp["result"]["result"], "Accepted");

        let query_req =
            r#"{"jsonrpc":"2.0","id":6,"method":"gossip.query","params":{"topic":"tower"}}"#;
        let query_resp: serde_json::Value =
            serde_json::from_str(&handle_request(query_req, &primal, &gate).await).unwrap();
        assert_eq!(query_resp["result"]["count"], 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gossip_status_returns_stats() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":7,"method":"gossip.status"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert!(resp["result"]["tower_entries"].is_number());
        assert!(resp["result"]["data_entries"].is_number());
        assert!(resp["result"]["compute_entries"].is_number());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gossip_peers_returns_node_id() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":8,"method":"gossip.peers"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["node_id"], "testGate");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gossip_spread_accepts_entry() {
        let primal = make_primal();
        let gate = make_gate();

        let req = r#"{"jsonrpc":"2.0","id":9,"method":"gossip.spread","params":{"from":"gate1","entry":{"topic":"data","key":"cas.have:gate1","payload":{"hashes":["abc"]},"origin_gate":"gate1","nonce":"deadbeef00000001deadbeef00000002","ttl":5,"version":1,"created_at_epoch":1723100000,"expires_at_epoch":1923100000}}}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["accepted"], 1);
    }

    #[tokio::test]
    async fn gossip_subscribe_returns_snapshot() {
        let primal = make_primal();
        let gate = make_gate();
        let req =
            r#"{"jsonrpc":"2.0","id":10,"method":"gossip.subscribe","params":{"topic":"tower"}}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["mode"], "snapshot");
        assert_eq!(resp["result"]["count"], 0);
        assert!(resp["result"]["subscriber_count"].as_u64().is_some());
    }

    #[tokio::test]
    async fn gossip_subscribe_requires_topic() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":10,"method":"gossip.subscribe"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert!(resp["result"]["error"].as_str().is_some());
    }

    #[tokio::test]
    async fn primal_announce_returns_expected_data() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":11,"method":"primal.announce"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["primal"], PRIMAL_NAME);
        assert_eq!(resp["result"]["version"], PRIMAL_VERSION);
        assert_eq!(
            resp["result"]["capabilities"],
            serde_json::json!(crate::announce::capabilities())
        );
        assert_eq!(
            resp["result"]["signal_tiers"],
            serde_json::json!(crate::announce::signal_tiers())
        );
        let methods = resp["result"]["methods"].as_array().unwrap();
        assert!(methods.iter().any(|m| m == "gossip.spread"));
    }

    #[tokio::test]
    async fn capability_list_legacy_alias_works() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":12,"method":"capability.list"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["primal"], PRIMAL_NAME);
        assert_eq!(resp["result"]["protocol"], "jsonrpc-2.0");
        let domains = resp["result"]["gossip_domains"].as_array().unwrap();
        assert_eq!(domains.len(), 3);
    }

    #[tokio::test]
    async fn health_readiness_returns_ready_and_capabilities() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":13,"method":"health.readiness"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert!(resp["result"]["ready"].is_boolean());
        let capabilities = resp["result"]["capabilities"].as_array().unwrap();
        assert!(capabilities.iter().any(|m| m == "health.liveness"));
        assert!(capabilities.iter().any(|m| m == "gossip.spread"));
    }

    #[tokio::test]
    async fn health_check_includes_node_id() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":14,"method":"health.check"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["node_id"], "testGate");
        assert!(resp["result"]["status"].is_string());
        assert!(resp["result"]["liveness"].is_boolean());
        assert!(resp["result"]["readiness"].is_boolean());
    }

    /// Vertebrate evolution self-audit (Wave 157a):
    /// Verify every method in `capability_registry.toml` has a real handler,
    /// and no unknown method silently returns a health response.
    #[tokio::test]
    async fn self_audit_rpc_surface_matches_registry() {
        let registry_methods: &[&str] = &[
            // [capabilities.health]
            "health.liveness",
            "health.readiness",
            "health.check",
            // [capabilities.lifecycle]
            "capabilities.list",
            "primal.announce",
            "btsp.negotiate",
            // [capabilities.gossip]
            "gossip.spread",
            "gossip.inject",
            "gossip.query",
            "gossip.subscribe",
            "gossip.status",
            "gossip.peers",
        ];

        // Every registry method must be in METHODS
        for method in registry_methods {
            assert!(
                METHODS.contains(method),
                "capability_registry.toml declares '{method}' but METHODS array is missing it"
            );
        }

        // Every METHODS entry must be in registry (no undeclared surface)
        for method in METHODS {
            assert!(
                registry_methods.contains(method),
                "METHODS declares '{method}' but it's not in capability_registry.toml"
            );
        }

        // Every registry method must return a result, not a -32601 error
        let primal = make_primal();
        let gate = make_gate();
        for method in registry_methods {
            let req = format!(
                r#"{{"jsonrpc":"2.0","id":99,"method":"{method}","params":{{"topic":"tower","key":"self_audit_test","from":"testGate","entry":{{"topic":"tower","key":"self_audit","payload":{{}},"origin_gate":"testGate","nonce":"aaaa000000000000bbbb000000000000","ttl":5,"version":1,"created_at_epoch":1723100000,"expires_at_epoch":1923100000}}}}}}"#
            );
            let resp: serde_json::Value =
                serde_json::from_str(&handle_request(&req, &primal, &gate).await).unwrap();
            assert!(
                resp.get("result").is_some(),
                "Method '{method}' returned error instead of result: {resp}"
            );
        }

        // Unknown methods MUST return -32601 (no bearDog silent-health anti-pattern)
        let unknown_req = r#"{"jsonrpc":"2.0","id":99,"method":"nonexistent.method"}"#;
        let unknown_resp: serde_json::Value =
            serde_json::from_str(&handle_request(unknown_req, &primal, &gate).await).unwrap();
        assert_eq!(
            unknown_resp["error"]["code"], -32601,
            "Unknown method must return -32601, not a result"
        );
        assert!(
            unknown_resp.get("result").is_none(),
            "Unknown method must NOT return a result (no silent health fallback)"
        );
    }

    #[tokio::test]
    async fn gossip_inject_missing_key_returns_error() {
        let primal = make_primal();
        let gate = make_gate();
        let req =
            r#"{"jsonrpc":"2.0","id":20,"method":"gossip.inject","params":{"topic":"tower"}}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["error"], "Missing 'key'");
    }

    #[tokio::test]
    async fn gossip_inject_invalid_topic_returns_error() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":21,"method":"gossip.inject","params":{"topic":"invalid","key":"some.key"}}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert!(
            resp["result"]["error"]
                .as_str()
                .unwrap()
                .contains("Invalid or missing 'topic'")
        );
    }

    #[tokio::test]
    async fn gossip_query_invalid_topic_returns_error() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":22,"method":"gossip.query","params":{"topic":"bogus"}}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert!(
            resp["result"]["error"]
                .as_str()
                .unwrap()
                .contains("Invalid or missing 'topic'")
        );
    }

    #[tokio::test]
    async fn gossip_spread_missing_entries_returns_error() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":23,"method":"gossip.spread","params":{"from":"peer1"}}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(
            resp["result"]["error"],
            "Missing 'entry' or 'entries' param"
        );
    }

    #[tokio::test]
    async fn gossip_spread_malformed_entry_increments_rejected() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":24,"method":"gossip.spread","params":{"from":"peer1","entries":[{"not":"a gossip entry"}]}}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate).await).unwrap();
        assert_eq!(resp["result"]["accepted"], 0);
        assert_eq!(resp["result"]["rejected"], 1);
    }
}

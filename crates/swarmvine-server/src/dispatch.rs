//! JSON-RPC 2.0 method dispatch with gossip engine handlers.

use swarmvine_core::{GossipEntry, GossipTopic, IngestResult, PrimalHealth};
use crate::method_gate::MethodGate;

fn skunkbat_sock_path() -> String {
    std::env::var("SWARMVINE_SKUNKBAT_SOCK")
        .unwrap_or_else(|_| "/run/membrane/skunkbat.sock".to_string())
}

/// Whether the vine-bat pre-accept hook is enabled. Disabled in test builds
/// unless SWARMVINE_SKUNKBAT_SOCK is explicitly set.
fn vine_bat_enabled() -> bool {
    if cfg!(test) {
        std::env::var("SWARMVINE_SKUNKBAT_SOCK").is_ok()
    } else {
        true
    }
}

const PRIMAL_NAME: &str = "swarmVine";
const PRIMAL_VERSION: &str = env!("CARGO_PKG_VERSION");

pub const METHODS: &[&str] = &[
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
];

/// Dispatch a JSON-RPC request and return the response string.
///
/// Gossip methods are async — this wrapper runs them on the current runtime.
pub fn handle_request(
    raw: &str,
    primal: &swarmvine_core::SwarmVinePrimal,
    gate: &MethodGate,
) -> String {
    let req: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return error_response(serde_json::Value::Null, -32700, "Parse error"),
    };

    let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let method = req
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");

    if let Err(denial) = gate.check(method) {
        return error_response(id, denial.code, &denial.message);
    }

    let params = req.get("params").cloned().unwrap_or(serde_json::json!({}));

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

        // --- Gossip methods (async — block on current runtime) ---

        "gossip.spread" => {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(
                    handle_gossip_spread(primal, &params),
                )
            })
        }
        "gossip.inject" => {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(
                    handle_gossip_inject(primal, &params),
                )
            })
        }
        "gossip.query" => {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(
                    handle_gossip_query(primal, &params),
                )
            })
        }
        "gossip.status" => {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(
                    handle_gossip_status(primal),
                )
            })
        }
        "gossip.peers" => {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(
                    handle_gossip_peers(primal),
                )
            })
        }

        _ => return error_response(id, -32601, "Method not found"),
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

    // Support single entry or batch
    let entries_raw = if let Some(arr) = params.get("entries").and_then(serde_json::Value::as_array)
    {
        arr.clone()
    } else if let Some(entry) = params.get("entry") {
        vec![entry.clone()]
    } else {
        return serde_json::json!({ "error": "Missing 'entry' or 'entries' param" });
    };

    let mut accepted = 0u32;
    let mut rejected = 0u32;

    for raw_entry in entries_raw {
        let entry: GossipEntry = match serde_json::from_value(raw_entry) {
            Ok(e) => e,
            Err(_) => {
                rejected += 1;
                continue;
            }
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
        _ => return serde_json::json!({ "error": "Invalid or missing 'topic' (tower|data|compute)" }),
    };

    let key = match params.get("key").and_then(serde_json::Value::as_str) {
        Some(k) => k.to_owned(),
        None => return serde_json::json!({ "error": "Missing 'key'" }),
    };

    let payload = params
        .get("payload")
        .cloned()
        .unwrap_or(serde_json::json!({}));

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
        _ => return serde_json::json!({ "error": "Invalid or missing 'topic' (tower|data|compute)" }),
    };

    let prefix = params
        .get("key_prefix")
        .and_then(serde_json::Value::as_str);

    let entries = primal.engine().query(topic, prefix).await;

    serde_json::json!({
        "topic": topic,
        "count": entries.len(),
        "entries": entries,
    })
}

/// Handle `gossip.status` — current gossip table statistics.
async fn handle_gossip_status(
    primal: &swarmvine_core::SwarmVinePrimal,
) -> serde_json::Value {
    let stats = primal.engine().stats().await;
    serde_json::json!(stats)
}

/// Handle `gossip.peers` — per-peer exchange statistics.
async fn handle_gossip_peers(
    primal: &swarmvine_core::SwarmVinePrimal,
) -> serde_json::Value {
    let peers = primal.engine().peer_stats().await;
    serde_json::json!({
        "node_id": primal.engine().node_id(),
        "peer_count": peers.len(),
        "peers": peers,
    })
}

/// Vine-bat pre-accept: call skunkBat `metadata.analyze` to validate a gossip entry
/// before accepting it from a remote peer. If skunkBat is unreachable, default to allow
/// (graceful degradation — bat validates when available, vine spreads regardless).
async fn vine_bat_preaccept(entry: &GossipEntry) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::timeout;
    use std::time::Duration;

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

    let stream = match timeout(
        Duration::from_millis(500),
        tokio::net::UnixStream::connect(&sock_path),
    )
    .await
    {
        Ok(Ok(s)) => s,
        _ => {
            tracing::debug!("vine-bat: skunkBat unreachable — default allow");
            return true;
        }
    };

    let (mut rd, mut wr) = stream.into_split();

    let mut frame = vec![0xEC, 0x01];
    frame.extend_from_slice(serde_json::to_string(&request).unwrap_or_default().as_bytes());
    frame.push(b'\n');

    if wr.write_all(&frame).await.is_err() {
        return true;
    }
    let _ = wr.shutdown().await;

    let mut buf = Vec::with_capacity(4096);
    let _ = timeout(Duration::from_millis(500), rd.read_to_end(&mut buf)).await;

    // Strip riboCipher prefix if present
    let json_start = if buf.len() >= 2 && buf[0] == 0xEC && buf[1] == 0x01 { 2 } else { 0 };
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

fn error_response(id: serde_json::Value, code: i32, message: &str) -> String {
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

    #[test]
    fn liveness_returns_alive() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"health.liveness"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate)).unwrap();
        assert_eq!(resp["result"]["alive"], true);
    }

    #[test]
    fn capabilities_list_includes_gossip_domains() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":2,"method":"capabilities.list"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate)).unwrap();
        assert_eq!(resp["result"]["primal"], PRIMAL_NAME);
        let domains = resp["result"]["gossip_domains"].as_array().unwrap();
        assert_eq!(domains.len(), 3);
    }

    #[test]
    fn unknown_method_returns_error() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":3,"method":"unknown.method"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate)).unwrap();
        assert_eq!(resp["error"]["code"], -32601);
    }

    #[test]
    fn invalid_json_returns_parse_error() {
        let primal = make_primal();
        let gate = make_gate();
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request("not json", &primal, &gate)).unwrap();
        assert_eq!(resp["error"]["code"], -32700);
    }

    #[test]
    fn btsp_negotiate_returns_null_cipher() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"btsp.negotiate","params":{"session_id":"test","preferred_cipher":"chacha20-poly1305","bond_type":"Covalent"}}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate)).unwrap();
        assert_eq!(resp["result"]["cipher"], "null");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gossip_inject_and_query() {
        let primal = make_primal();
        let gate = make_gate();

        let inject_req = r#"{"jsonrpc":"2.0","id":5,"method":"gossip.inject","params":{"topic":"tower","key":"capability.advertise:testGate:nestGate","payload":{"capabilities":["content.get"]}}}"#;
        let inject_resp: serde_json::Value =
            serde_json::from_str(&handle_request(inject_req, &primal, &gate)).unwrap();
        assert_eq!(inject_resp["result"]["result"], "Accepted");

        let query_req = r#"{"jsonrpc":"2.0","id":6,"method":"gossip.query","params":{"topic":"tower"}}"#;
        let query_resp: serde_json::Value =
            serde_json::from_str(&handle_request(query_req, &primal, &gate)).unwrap();
        assert_eq!(query_resp["result"]["count"], 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gossip_status_returns_stats() {
        let primal = make_primal();
        let gate = make_gate();
        let req = r#"{"jsonrpc":"2.0","id":7,"method":"gossip.status"}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate)).unwrap();
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
            serde_json::from_str(&handle_request(req, &primal, &gate)).unwrap();
        assert_eq!(resp["result"]["node_id"], "testGate");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gossip_spread_accepts_entry() {
        let primal = make_primal();
        let gate = make_gate();

        let req = r#"{"jsonrpc":"2.0","id":9,"method":"gossip.spread","params":{"from":"gate1","entry":{"topic":"data","key":"cas.have:gate1","payload":{"hashes":["abc"]},"origin_gate":"gate1","nonce":"deadbeef00000001deadbeef00000002","ttl":5,"version":1,"created_at_epoch":1723100000,"expires_at_epoch":1923100000}}}"#;
        let resp: serde_json::Value =
            serde_json::from_str(&handle_request(req, &primal, &gate)).unwrap();
        assert_eq!(resp["result"]["accepted"], 1);
    }
}

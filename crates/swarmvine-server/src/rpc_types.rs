// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! JSON-RPC 2.0 wire types and response construction helpers.

/// JSON-RPC protocol version string.
pub(crate) const JSONRPC_VERSION: &str = "2.0";

/// Registered primal name returned by capability and announce handlers.
pub(crate) const PRIMAL_NAME: &str = "swarmVine";

/// Crate version string for capability and announce handlers.
pub(crate) const PRIMAL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// JSON-RPC methods exposed by this server.
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

/// Parsed JSON-RPC request fields from a wire payload.
pub(crate) struct ParsedRequest {
    pub id: serde_json::Value,
    pub method: String,
    pub params: serde_json::Value,
}

/// Parse a JSON-RPC request string into structured fields.
///
/// # Errors
///
/// Returns the raw payload when JSON parsing fails.
pub(crate) fn parse_request(raw: &str) -> Result<ParsedRequest, ()> {
    let req: serde_json::Value = serde_json::from_str(raw).map_err(|_| ())?;
    Ok(ParsedRequest {
        id: req.get("id").cloned().unwrap_or(serde_json::Value::Null),
        method: req
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned(),
        params: req
            .get("params")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
    })
}

/// Build a JSON-RPC 2.0 request object.
#[must_use]
pub(crate) fn build_request(
    id: &serde_json::Value,
    method: &str,
    params: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "method": method,
        "params": params,
    })
}

/// Build a JSON-RPC 2.0 success response string.
#[must_use]
pub(crate) fn success_response(id: &serde_json::Value, result: &serde_json::Value) -> String {
    serde_json::json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "result": result,
    })
    .to_string()
}

/// Build a JSON-RPC 2.0 error response string.
#[must_use]
pub(crate) fn error_response(id: &serde_json::Value, code: i32, message: &str) -> String {
    serde_json::json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "error": { "code": code, "message": message },
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_request_extracts_fields() {
        let parsed =
            parse_request(r#"{"jsonrpc":"2.0","id":42,"method":"health.check","params":{"x":1}}"#)
                .unwrap();
        assert_eq!(parsed.id, serde_json::json!(42));
        assert_eq!(parsed.method, "health.check");
        assert_eq!(parsed.params["x"], 1);
    }

    #[test]
    fn parse_request_defaults_missing_fields() {
        let parsed = parse_request(r#"{"jsonrpc":"2.0"}"#).unwrap();
        assert!(parsed.id.is_null());
        assert_eq!(parsed.method, "");
        assert_eq!(parsed.params, serde_json::json!({}));
    }

    #[test]
    fn parse_request_rejects_invalid_json() {
        assert!(parse_request("not-json").is_err());
    }

    #[test]
    fn build_request_includes_protocol_fields() {
        let req = build_request(
            &serde_json::json!(1),
            "gossip.query",
            &serde_json::json!({"topic": "tower"}),
        );
        assert_eq!(req["jsonrpc"], JSONRPC_VERSION);
        assert_eq!(req["method"], "gossip.query");
    }

    #[test]
    fn success_and_error_responses_are_valid_jsonrpc() {
        let ok = success_response(&serde_json::json!(7), &serde_json::json!({"alive": true}));
        let ok_val: serde_json::Value = serde_json::from_str(&ok).unwrap();
        assert_eq!(ok_val["result"]["alive"], true);

        let err = error_response(&serde_json::json!(8), -32601, "Method not found");
        let err_val: serde_json::Value = serde_json::from_str(&err).unwrap();
        assert_eq!(err_val["error"]["code"], -32601);
    }
}

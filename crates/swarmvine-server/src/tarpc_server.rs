//! tarpc binary RPC listener for intra-gate composition.
//!
//! Dual-socket pattern (G64 Cephalization):
//! - JSON-RPC on `{name}.sock` — the lifecycle anchor
//! - tarpc on `{name}.tarpc.sock` — spawned as background task
//!
//! On non-Unix platforms, the UDS listener is unavailable. `start_tarpc_listener`
//! returns immediately with a log message. tarpc-over-TCP is a future evolution.

use anyhow::Result;
#[cfg(unix)]
use futures_util::StreamExt;
#[cfg(unix)]
use swarmvine_core::tarpc_service::SwarmVineService;
#[cfg(unix)]
use tarpc::server::{self, Channel};
use tracing::info;
#[cfg(unix)]
use tracing::warn;

/// tarpc socket path — same convention as JSON-RPC but with `.tarpc.sock` suffix.
///
/// Uses `platform_paths::runtime_socket_dir()` for consistent resolution
/// across all socket paths (no duplicated `/tmp` fallbacks).
#[cfg(unix)]
fn tarpc_endpoint(primal_name: &str, family_id: Option<&str>) -> String {
    let socket_dir = std::env::var(swarmvine_core::env_keys::BIOMEOS_SOCKET_DIR).map_or_else(
        |_| swarmvine_core::platform_paths::runtime_socket_dir(),
        std::path::PathBuf::from,
    );

    let filename = family_id
        .filter(|id| !id.is_empty() && *id != "default")
        .map_or_else(
            || format!("{primal_name}.tarpc.sock"),
            |fid| format!("{primal_name}-{fid}.tarpc.sock"),
        );

    socket_dir.join(&filename).to_string_lossy().into_owned()
}

/// Start the tarpc binary RPC listener alongside the JSON-RPC server.
///
/// This runs as a background task — the JSON-RPC listener remains the
/// primary accept loop and the primal's lifecycle anchor.
///
/// On non-Unix platforms, tarpc UDS transport is unavailable. This function
/// logs a notice and returns immediately. tarpc-over-TCP is a future evolution
/// (the `tcp` feature is already enabled in the workspace).
#[cfg(unix)]
pub(crate) async fn start_tarpc_listener(
    primal_name: &str,
    family_id: Option<&str>,
    primal: std::sync::Arc<swarmvine_core::SwarmVinePrimal>,
) -> Result<()> {
    let path = tarpc_endpoint(primal_name, family_id);
    let socket_path = std::path::PathBuf::from(&path);

    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let _ = tokio::fs::remove_file(&socket_path).await;

    let mut listener =
        tarpc::serde_transport::unix::listen(&socket_path, tokio_serde::formats::Bincode::default)
            .await?;
    info!("tarpc listening on unix://{path}");

    tokio::spawn(async move {
        while let Some(result) = listener.next().await {
            match result {
                Ok(transport) => {
                    let primal = primal.clone();
                    tokio::spawn(async move {
                        let handler = SwarmVineServiceHandler { primal };
                        server::BaseChannel::with_defaults(transport)
                            .execute(handler.serve())
                            .for_each(|response| async move {
                                tokio::spawn(response);
                            })
                            .await;
                    });
                }
                Err(e) => {
                    warn!("tarpc accept error: {e}");
                }
            }
        }
    });

    Ok(())
}

/// Non-Unix stub — tarpc UDS transport is not available.
#[cfg(not(unix))]
pub(crate) async fn start_tarpc_listener(
    _primal_name: &str,
    _family_id: Option<&str>,
    _primal: std::sync::Arc<swarmvine_core::SwarmVinePrimal>,
) -> Result<()> {
    info!("tarpc listener not available on this platform (UDS-only)");
    Ok(())
}

/// G65: Handle a pre-negotiated tarpc stream (single-socket mode).
///
/// Called when protocol negotiation selected tarpc on the shared socket.
pub(crate) fn handle_tarpc_stream<
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
>(
    _stream: S,
) {
    // tarpc 0.37 requires its own listener for transport framing (bincode + length-prefix).
    // Single-socket G65 tarpc requires tarpc upstream support for arbitrary AsyncRead/Write.
    // Track: https://github.com/google/tarpc/issues — convergence target: tarpc 0.38+.
    tracing::warn!(
        "G65 tarpc negotiated but single-socket transport not yet supported by tarpc 0.37"
    );
}

/// tarpc service handler — bridges the tarpc trait to the primal implementation.
#[cfg(unix)]
#[derive(Clone)]
struct SwarmVineServiceHandler {
    primal: std::sync::Arc<swarmvine_core::SwarmVinePrimal>,
}

#[cfg(unix)]
impl swarmvine_core::tarpc_service::SwarmVineService for SwarmVineServiceHandler {
    async fn health_liveness(self, _: tarpc::context::Context) -> bool {
        use swarmvine_core::PrimalHealth;
        self.primal.health_status().is_healthy()
    }

    async fn health_readiness(self, _: tarpc::context::Context) -> bool {
        use swarmvine_core::PrimalLifecycle;
        self.primal.state().is_running()
    }

    async fn health_check(
        self,
        _: tarpc::context::Context,
    ) -> swarmvine_core::tarpc_service::HealthCheckResponse {
        use swarmvine_core::PrimalHealth;
        swarmvine_core::tarpc_service::HealthCheckResponse {
            status: format!("{:?}", self.primal.health_status()),
            primal: "swarmVine".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }

    async fn capabilities_list(
        self,
        _: tarpc::context::Context,
    ) -> swarmvine_core::tarpc_service::CapabilitiesResponse {
        swarmvine_core::tarpc_service::CapabilitiesResponse {
            primal: "swarmVine".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            methods: crate::dispatch::METHODS
                .iter()
                .map(|&s| s.to_owned())
                .collect(),
        }
    }

    async fn gossip_inject(
        self,
        _: tarpc::context::Context,
        topic: String,
        key: String,
        payload: String,
    ) -> String {
        let Ok(topic) = topic.parse::<swarmvine_core::GossipTopic>() else {
            return serde_json::json!({"error": "invalid topic"}).to_string();
        };
        let payload_val: serde_json::Value = serde_json::from_str(&payload).unwrap_or_default();
        let result = self.primal.engine().inject(topic, key, payload_val).await;
        serde_json::json!({"result": format!("{result:?}")}).to_string()
    }

    async fn gossip_query(
        self,
        _: tarpc::context::Context,
        topic: String,
        key_prefix: Option<String>,
    ) -> String {
        let Ok(topic) = topic.parse::<swarmvine_core::GossipTopic>() else {
            return serde_json::json!({"error": "invalid topic"}).to_string();
        };
        let entries = self
            .primal
            .engine()
            .query(topic, key_prefix.as_deref())
            .await;
        serde_json::json!({"entries": entries, "count": entries.len()}).to_string()
    }

    async fn gossip_status(self, _: tarpc::context::Context) -> String {
        let stats = self.primal.engine().stats().await;
        serde_json::json!(stats).to_string()
    }

    async fn gossip_peers(self, _: tarpc::context::Context) -> String {
        let peers = self.primal.engine().peer_stats().await;
        serde_json::json!({"peers": peers, "node_id": self.primal.engine().node_id()}).to_string()
    }

    async fn gossip_subscribe(
        self,
        _: tarpc::context::Context,
        topic: String,
        key_prefix: Option<String>,
    ) -> String {
        let Ok(topic) = topic.parse::<swarmvine_core::GossipTopic>() else {
            return serde_json::json!({"error": "invalid topic"}).to_string();
        };
        let entries = self
            .primal
            .engine()
            .query(topic, key_prefix.as_deref())
            .await;
        let subs = self.primal.engine().subscriber_count();
        serde_json::json!({
            "mode": "snapshot",
            "count": entries.len(),
            "entries": entries,
            "subscriber_count": subs,
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tarpc_endpoint_uses_biomeos_socket_dir() {
        temp_env::with_var(
            swarmvine_core::env_keys::BIOMEOS_SOCKET_DIR,
            Some("/custom/socket/dir"),
            || {
                assert_eq!(
                    tarpc_endpoint("swarmVine", None),
                    "/custom/socket/dir/swarmVine.tarpc.sock"
                );
                assert_eq!(
                    tarpc_endpoint("swarmVine", Some("ecoPrimal")),
                    "/custom/socket/dir/swarmVine-ecoPrimal.tarpc.sock"
                );
            },
        );
    }

    #[test]
    fn tarpc_endpoint_default_family_uses_unsuffixed_name() {
        temp_env::with_var(
            swarmvine_core::env_keys::BIOMEOS_SOCKET_DIR,
            Some("/tmp/test-sockets"),
            || {
                assert_eq!(
                    tarpc_endpoint("swarmVine", Some("default")),
                    "/tmp/test-sockets/swarmVine.tarpc.sock"
                );
                assert_eq!(
                    tarpc_endpoint("swarmVine", Some("")),
                    "/tmp/test-sockets/swarmVine.tarpc.sock"
                );
            },
        );
    }

    #[test]
    fn tarpc_endpoint_falls_back_to_xdg_runtime_dir() {
        temp_env::with_vars(
            [
                (swarmvine_core::env_keys::BIOMEOS_SOCKET_DIR, None),
                (
                    swarmvine_core::env_keys::XDG_RUNTIME_DIR,
                    Some("/run/user/9999"),
                ),
            ],
            || {
                assert_eq!(
                    tarpc_endpoint("swarmVine", None),
                    "/run/user/9999/biomeos/swarmVine.tarpc.sock"
                );
            },
        );
    }

    async fn running_handler() -> SwarmVineServiceHandler {
        use swarmvine_core::PrimalLifecycle;
        let mut primal = swarmvine_core::SwarmVinePrimal::with_node_id("testGate".to_owned());
        primal.start().await.unwrap();
        SwarmVineServiceHandler {
            primal: std::sync::Arc::new(primal),
        }
    }

    #[tokio::test]
    async fn handler_health_liveness_returns_true_when_running() {
        use swarmvine_core::tarpc_service::SwarmVineService;
        let handler = running_handler().await;
        let ctx = tarpc::context::current();
        assert!(SwarmVineService::health_liveness(handler, ctx).await);
    }

    #[tokio::test]
    async fn handler_health_readiness_returns_true_when_running() {
        use swarmvine_core::tarpc_service::SwarmVineService;
        let handler = running_handler().await;
        let ctx = tarpc::context::current();
        assert!(SwarmVineService::health_readiness(handler, ctx).await);
    }

    #[tokio::test]
    async fn handler_health_check_returns_swarmvine_name() {
        use swarmvine_core::tarpc_service::SwarmVineService;
        let handler = running_handler().await;
        let ctx = tarpc::context::current();
        let resp = SwarmVineService::health_check(handler, ctx).await;
        assert_eq!(resp.primal, "swarmVine");
        assert_eq!(resp.version, env!("CARGO_PKG_VERSION"));
        assert!(resp.status.contains("Healthy"));
    }

    #[tokio::test]
    async fn handler_capabilities_list_includes_gossip_methods() {
        use swarmvine_core::tarpc_service::SwarmVineService;
        let handler = running_handler().await;
        let ctx = tarpc::context::current();
        let resp = SwarmVineService::capabilities_list(handler, ctx).await;
        assert_eq!(resp.primal, "swarmVine");
        assert!(resp.methods.contains(&"gossip.spread".to_owned()));
        assert!(resp.methods.contains(&"gossip.inject".to_owned()));
        assert!(resp.methods.contains(&"gossip.query".to_owned()));
        assert_eq!(resp.methods.len(), crate::dispatch::METHODS.len());
    }

    #[tokio::test]
    async fn handler_gossip_inject_and_query() {
        use swarmvine_core::tarpc_service::SwarmVineService;
        let handler = running_handler().await;
        let ctx = tarpc::context::current();
        let inject_resp = SwarmVineService::gossip_inject(
            handler.clone(),
            ctx,
            "tower".to_owned(),
            "capability.advertise:testGate:nestGate".to_owned(),
            r#"{"capabilities":["content.get"]}"#.to_owned(),
        )
        .await;
        let inject_json: serde_json::Value = serde_json::from_str(&inject_resp).unwrap();
        assert_eq!(inject_json["result"], "Accepted");

        let ctx = tarpc::context::current();
        let query_resp =
            SwarmVineService::gossip_query(handler, ctx, "tower".to_owned(), None).await;
        let query_json: serde_json::Value = serde_json::from_str(&query_resp).unwrap();
        assert_eq!(query_json["count"], 1);
        assert_eq!(query_json["entries"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn handler_gossip_status_returns_entry_counts() {
        use swarmvine_core::tarpc_service::SwarmVineService;
        let handler = running_handler().await;
        let ctx = tarpc::context::current();
        let status_json = SwarmVineService::gossip_status(handler, ctx).await;
        let stats: serde_json::Value = serde_json::from_str(&status_json).unwrap();
        assert!(stats["tower_entries"].is_number());
        assert!(stats["data_entries"].is_number());
        assert!(stats["compute_entries"].is_number());
    }

    #[tokio::test]
    async fn handler_gossip_peers_returns_node_id() {
        use swarmvine_core::tarpc_service::SwarmVineService;
        let handler = running_handler().await;
        let ctx = tarpc::context::current();
        let peers = SwarmVineService::gossip_peers(handler, ctx).await;
        let parsed: serde_json::Value = serde_json::from_str(&peers).unwrap();
        assert_eq!(parsed["node_id"], "testGate");
        assert!(parsed["peers"].is_object());
    }

    #[tokio::test]
    async fn handler_gossip_inject_rejects_invalid_topic() {
        use swarmvine_core::tarpc_service::SwarmVineService;
        let handler = running_handler().await;
        let ctx = tarpc::context::current();
        let resp = SwarmVineService::gossip_inject(
            handler,
            ctx,
            "invalid".to_owned(),
            "some.key".to_owned(),
            "{}".to_owned(),
        )
        .await;
        let parsed: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(parsed["error"], "invalid topic");
    }

    #[tokio::test]
    async fn handler_gossip_subscribe_returns_snapshot() {
        use swarmvine_core::tarpc_service::SwarmVineService;
        let handler = running_handler().await;

        SwarmVineService::gossip_inject(
            handler.clone(),
            tarpc::context::current(),
            "data".to_owned(),
            "cas.have:testGate".to_owned(),
            "{}".to_owned(),
        )
        .await;

        let resp = SwarmVineService::gossip_subscribe(
            handler,
            tarpc::context::current(),
            "data".to_owned(),
            None,
        )
        .await;
        let parsed: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(parsed["mode"], "snapshot");
        assert_eq!(parsed["count"], 1);
    }
}

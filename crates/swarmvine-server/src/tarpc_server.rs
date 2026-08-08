//! tarpc binary RPC listener for intra-gate composition.
//!
//! Dual-socket pattern (G64 Cephalization):
//! - JSON-RPC on `{name}.sock` — the lifecycle anchor
//! - tarpc on `{name}.tarpc.sock` — spawned as background task

use anyhow::Result;
use futures::StreamExt;
use tarpc::server::{self, Channel};
use swarmvine_core::tarpc_service::SwarmVineService;
use tracing::{info, warn};

/// tarpc socket path — same convention as JSON-RPC but with `.tarpc.sock` suffix.
fn tarpc_endpoint(primal_name: &str, family_id: Option<&str>) -> String {
    let socket_dir = std::env::var(swarmvine_core::env_keys::BIOMEOS_SOCKET_DIR).unwrap_or_else(|_| {
        let runtime_dir =
            std::env::var(swarmvine_core::env_keys::XDG_RUNTIME_DIR).unwrap_or_else(|_| "/tmp".to_owned());
        format!("{runtime_dir}/biomeos")
    });

    let filename = match family_id.filter(|id| !id.is_empty() && *id != "default") {
        Some(fid) => format!("{primal_name}-{fid}.tarpc.sock"),
        None => format!("{primal_name}.tarpc.sock"),
    };

    std::path::PathBuf::from(&socket_dir)
        .join(&filename)
        .to_string_lossy()
        .into_owned()
}

/// Start the tarpc binary RPC listener alongside the JSON-RPC server.
///
/// This runs as a background task — the JSON-RPC listener remains the
/// primary accept loop and the primal's lifecycle anchor.
pub async fn start_tarpc_listener(
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
                            .for_each(|response| async move { tokio::spawn(response); })
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

/// G65: Handle a pre-negotiated tarpc stream (single-socket mode).
///
/// Called when protocol negotiation selected tarpc on the shared socket.
pub async fn handle_tarpc_stream<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static>(
    _stream: S,
) {
    // In Phase 3, the negotiated stream is wrapped as a tarpc transport.
    // Full implementation wires through tarpc::serde_transport on the raw stream.
    // Placeholder: tarpc framing on pre-connected stream requires transport adaptation.
    tracing::debug!("G65 tarpc stream handoff (stub — full wiring in convergence)");
}

/// tarpc service handler — bridges the tarpc trait to the primal implementation.
#[derive(Clone)]
struct SwarmVineServiceHandler {
    primal: std::sync::Arc<swarmvine_core::SwarmVinePrimal>,
}

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
            methods: vec![
                "health.liveness".to_owned(),
                "health.readiness".to_owned(),
                "health.check".to_owned(),
                "capabilities.list".to_owned(),
            ],
        }
    }
}

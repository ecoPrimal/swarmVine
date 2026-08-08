//! tarpc service definition for high-performance binary IPC.
//!
//! Dual-protocol architecture (G64 Cephalization):
//! - JSON-RPC on `{name}.sock` — bootstrap, discovery, diagnostics, browser
//! - tarpc on `{name}.tarpc.sock` — intra-gate composition, sub-ms binary framing
//!
//! The tarpc service mirrors the JSON-RPC capability surface but eliminates
//! serde roundtrips for high-frequency callers within the same NUCLEUS.
//!
//! The baseline methods (`health_liveness`, `health_readiness`, `health_check`,
//! `capabilities_list`) align with `sourdough_core::tarpc_service::PrimalService`.
//! This trait adds domain-specific methods for swarmVine.

/// tarpc service trait for swarmVine.
///
/// Extends the baseline `PrimalService` contract with domain-specific methods.
/// High-frequency intra-gate callers use this for zero-serde composition.
#[tarpc::service]
pub trait SwarmVineService {
    /// Health liveness check (mirrors `health.liveness`).
    async fn health_liveness() -> bool;

    /// Health readiness check (mirrors `health.readiness`).
    async fn health_readiness() -> bool;

    /// Full health report (mirrors `health.check`).
    async fn health_check() -> HealthCheckResponse;

    /// List capabilities (mirrors `capabilities.list`).
    async fn capabilities_list() -> CapabilitiesResponse;
}

/// Response type for `health_check` over tarpc.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HealthCheckResponse {
    pub status: String,
    pub primal: String,
    pub version: String,
}

/// Response type for `capabilities_list` over tarpc.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CapabilitiesResponse {
    pub primal: String,
    pub version: String,
    pub methods: Vec<String>,
}

/// Connect to a remote swarmVine primal via its tarpc socket.
///
/// Returns a tarpc client stub ready for sub-ms binary RPC calls.
/// Falls back to `$XDG_RUNTIME_DIR/biomeos/{name}.tarpc.sock` if no path given.
///
/// # Errors
///
/// Returns an error if the socket doesn't exist or connection fails.
pub async fn connect(socket_path: Option<&str>) -> Result<SwarmVineServiceClient, std::io::Error> {
    let default_path = {
        let runtime_dir = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".to_owned());
        format!("{runtime_dir}/biomeos/swarmvine.tarpc.sock")
    };
    let path = socket_path.unwrap_or(&default_path);

    let transport = tarpc::serde_transport::unix::connect(
        path,
        tokio_serde::formats::Bincode::default,
    )
    .await?;

    Ok(SwarmVineServiceClient::new(tarpc::client::Config::default(), transport).spawn())
}

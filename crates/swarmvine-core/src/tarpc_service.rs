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

    /// Inject a gossip entry (mirrors `gossip.inject`).
    async fn gossip_inject(topic: String, key: String, payload: String) -> String;

    /// Query gossip table (mirrors `gossip.query`).
    async fn gossip_query(topic: String, key_prefix: Option<String>) -> String;

    /// Get gossip statistics (mirrors `gossip.status`).
    async fn gossip_status() -> String;

    /// Get peer statistics (mirrors `gossip.peers`).
    async fn gossip_peers() -> String;

    /// Subscribe snapshot (mirrors `gossip.subscribe`).
    /// Returns a JSON snapshot of current entries for the given topic.
    async fn gossip_subscribe(topic: String, key_prefix: Option<String>) -> String;
}

/// Response type for `health_check` over tarpc.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HealthCheckResponse {
    /// Health status string.
    pub status: String,
    /// Primal name.
    pub primal: String,
    /// Primal version.
    pub version: String,
}

/// Response type for `capabilities_list` over tarpc.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CapabilitiesResponse {
    /// Primal name.
    pub primal: String,
    /// Primal version.
    pub version: String,
    /// List of supported methods.
    pub methods: Vec<String>,
}

/// Connect to a remote swarmVine primal via its tarpc socket.
///
/// Returns a tarpc client stub ready for sub-ms binary RPC calls.
/// Falls back to `$XDG_RUNTIME_DIR/biomeos/{name}.tarpc.sock` if no path given.
///
/// On non-Unix platforms, returns `Unsupported` — tarpc UDS transport is
/// not available. tarpc-over-TCP is a future evolution.
///
/// # Errors
///
/// Returns an error if the socket doesn't exist, connection fails, or the
/// platform does not support UDS.
#[cfg(unix)]
pub async fn connect(socket_path: Option<&str>) -> Result<SwarmVineServiceClient, std::io::Error> {
    let default_path = crate::platform_paths::runtime_socket_dir()
        .join("swarmvine.tarpc.sock")
        .to_string_lossy()
        .into_owned();
    let path = socket_path.unwrap_or(&default_path);

    let transport =
        tarpc::serde_transport::unix::connect(path, tokio_serde::formats::Bincode::default).await?;

    Ok(SwarmVineServiceClient::new(tarpc::client::Config::default(), transport).spawn())
}

/// Non-Unix stub — tarpc UDS transport is not available on this platform.
///
/// # Errors
///
/// Always returns `Unsupported`.
#[cfg(not(unix))]
pub async fn connect(_socket_path: Option<&str>) -> Result<SwarmVineServiceClient, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "tarpc UDS transport not available on this platform",
    ))
}

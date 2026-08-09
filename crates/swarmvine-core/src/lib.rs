//! # swarmVine Core
//!
//! Core library for the swarmVine primal — epidemic gossip engine for
//! capability, data, and compute propagation across the Tower Atomic mesh.
//!
//! Self-contained: all primal DNA (traits, types, patterns) is defined here.
//! This primal discovers other primals at runtime via JSON-RPC 2.0 IPC.
//! High-performance intra-gate callers use the tarpc service (G64 cephalization).

pub mod domain_types;
pub mod env_keys;
pub mod error;
pub mod gossip;
pub mod health;
pub mod lifecycle;
pub mod platform_paths;
pub mod platform_signal;
pub mod platform_substrate;
pub mod protocol_negotiation;
pub mod ribocipher;
pub mod tarpc_service;
pub mod transport;

pub use domain_types::{BloomFilter, ComputeCapacity, DepotManifest, DepotManifestEntry};
pub use error::{PrimalError, PrimalResult};
pub use gossip::{
    GossipEngine, GossipEntry, GossipNotification, GossipStats, GossipTopic, IngestResult,
    PeerStats,
};
pub use health::{HealthReport, HealthStatus, PrimalHealth};
pub use lifecycle::{PrimalLifecycle, PrimalState};

/// Resolve the gate node identity from environment or system hostname.
///
/// Tiered: `GATE_ID` env → `HOSTNAME` env → OS hostname → `"unknown"`.
#[must_use]
pub fn resolve_node_id() -> String {
    std::env::var("GATE_ID")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_owned())
        })
}

/// The swarmVine primal.
pub struct SwarmVinePrimal {
    state: PrimalState,
    engine: GossipEngine,
}

impl SwarmVinePrimal {
    /// Create a new primal instance with the given node identity.
    #[must_use]
    pub fn with_node_id(node_id: String) -> Self {
        Self {
            state: PrimalState::Created,
            engine: GossipEngine::new(node_id),
        }
    }

    /// Create a new primal instance (derives node ID from hostname).
    #[must_use]
    pub fn new() -> Self {
        Self::with_node_id(resolve_node_id())
    }

    /// Access the gossip engine.
    #[must_use]
    pub const fn engine(&self) -> &GossipEngine {
        &self.engine
    }
}

impl Default for SwarmVinePrimal {
    fn default() -> Self {
        Self::new()
    }
}

impl PrimalLifecycle for SwarmVinePrimal {
    fn state(&self) -> PrimalState {
        self.state
    }

    async fn start(&mut self) -> Result<(), PrimalError> {
        if !self.state.can_start() {
            return Err(PrimalError::lifecycle("cannot start from current state"));
        }
        self.state = PrimalState::Running;
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), PrimalError> {
        if !self.state.can_stop() {
            return Err(PrimalError::lifecycle("cannot stop from current state"));
        }
        self.state = PrimalState::Stopped;
        Ok(())
    }
}

impl PrimalHealth for SwarmVinePrimal {
    fn health_status(&self) -> HealthStatus {
        if self.state.is_running() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        }
    }

    async fn health_check(&self) -> Result<HealthReport, PrimalError> {
        Ok(HealthReport::new("swarmVine", env!("CARGO_PKG_VERSION"))
            .with_status(self.health_status()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lifecycle() {
        let mut primal = SwarmVinePrimal::with_node_id("test".to_owned());
        assert_eq!(primal.state(), PrimalState::Created);

        primal.start().await.unwrap();
        assert_eq!(primal.state(), PrimalState::Running);

        primal.stop().await.unwrap();
        assert_eq!(primal.state(), PrimalState::Stopped);
    }

    #[tokio::test]
    async fn test_health() {
        let mut primal = SwarmVinePrimal::with_node_id("test".to_owned());
        primal.start().await.unwrap();

        assert!(primal.health_status().is_healthy());

        let report = primal.health_check().await.unwrap();
        assert_eq!(report.name, "swarmVine");
    }

    #[tokio::test]
    async fn test_engine_access() {
        let primal = SwarmVinePrimal::with_node_id("testGate".to_owned());
        assert_eq!(primal.engine().node_id(), "testGate");
    }

    #[test]
    fn new_derives_node_id_from_env_or_hostname() {
        let primal = SwarmVinePrimal::new();
        assert!(!primal.engine().node_id().is_empty());
    }

    #[test]
    fn default_constructs_primal() {
        let primal = SwarmVinePrimal::default();
        assert_eq!(primal.state(), PrimalState::Created);
        assert!(!primal.engine().node_id().is_empty());
    }

    #[tokio::test]
    async fn full_lifecycle_create_start_health_stop() {
        let mut primal = SwarmVinePrimal::with_node_id("lifecycleGate".to_owned());
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
    }
}

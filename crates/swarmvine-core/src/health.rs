//! Health check traits for observability.
//!
//! Every primal needs to be observable. This module provides health check
//! traits usable by orchestrators, load balancers, and monitoring systems.

use crate::error::PrimalError;
use serde::{Deserialize, Serialize};

/// Overall health status of a primal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Healthy and ready to serve requests.
    Healthy,
    /// Unhealthy but may recover.
    Degraded {
        /// Reason for degraded status.
        reason: String,
    },
    /// Unhealthy and not serving requests.
    Unhealthy {
        /// Reason for unhealthy status.
        reason: String,
    },
    /// Health unknown (e.g., startup in progress).
    Unknown,
}

impl HealthStatus {
    /// Check if the status is healthy.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    /// Check if the status allows serving requests.
    #[must_use]
    pub const fn is_serving(&self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded { .. })
    }
}

/// Health report for a primal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthReport {
    /// Primal name.
    pub name: String,
    /// Primal version.
    pub version: String,
    /// Overall status.
    pub status: HealthStatus,
    /// Liveness (is the process alive?).
    pub liveness: bool,
    /// Readiness (can it serve requests?).
    pub readiness: bool,
}

impl HealthReport {
    /// Create a new health report.
    #[must_use]
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            status: HealthStatus::Unknown,
            liveness: true,
            readiness: false,
        }
    }

    /// Set status.
    #[must_use]
    pub fn with_status(mut self, status: HealthStatus) -> Self {
        self.readiness = status.is_serving();
        self.status = status;
        self
    }
}

/// Health check trait for primals.
///
/// Implement this to provide health information about your primal.
pub trait PrimalHealth: Send + Sync {
    /// Get the current health status (quick check).
    fn health_status(&self) -> HealthStatus;

    /// Perform a full health check (may be expensive).
    ///
    /// # Errors
    ///
    /// Returns an error if the health check itself fails.
    fn health_check(
        &self,
    ) -> impl std::future::Future<Output = Result<HealthReport, PrimalError>> + Send;

    /// Check liveness (is the process alive?).
    fn is_live(&self) -> bool {
        true
    }

    /// Check readiness (can it serve requests?).
    fn is_ready(&self) -> bool {
        self.health_status().is_serving()
    }
}

#[cfg(test)]
mod tests {
    use super::{HealthReport, HealthStatus};

    #[test]
    fn is_healthy_only_for_healthy_variant() {
        assert!(HealthStatus::Healthy.is_healthy());
        assert!(
            !HealthStatus::Degraded {
                reason: "slow".to_owned()
            }
            .is_healthy()
        );
        assert!(
            !HealthStatus::Unhealthy {
                reason: "down".to_owned()
            }
            .is_healthy()
        );
        assert!(!HealthStatus::Unknown.is_healthy());
    }

    #[test]
    fn is_serving_for_healthy_and_degraded() {
        assert!(HealthStatus::Healthy.is_serving());
        assert!(
            HealthStatus::Degraded {
                reason: "partial".to_owned()
            }
            .is_serving()
        );
        assert!(
            !HealthStatus::Unhealthy {
                reason: "offline".to_owned()
            }
            .is_serving()
        );
        assert!(!HealthStatus::Unknown.is_serving());
    }

    #[test]
    fn health_report_new_defaults() {
        let report = HealthReport::new("swarmVine", "0.1.0");
        assert_eq!(report.name, "swarmVine");
        assert_eq!(report.version, "0.1.0");
        assert_eq!(report.status, HealthStatus::Unknown);
        assert!(report.liveness);
        assert!(!report.readiness);
    }

    #[test]
    fn with_status_sets_readiness_from_serving() {
        let healthy = HealthReport::new("p", "1").with_status(HealthStatus::Healthy);
        assert_eq!(healthy.status, HealthStatus::Healthy);
        assert!(healthy.readiness);

        let degraded = HealthReport::new("p", "1").with_status(HealthStatus::Degraded {
            reason: "lag".to_owned(),
        });
        assert!(degraded.readiness);

        let unhealthy = HealthReport::new("p", "1").with_status(HealthStatus::Unhealthy {
            reason: "fail".to_owned(),
        });
        assert!(!unhealthy.readiness);

        let unknown = HealthReport::new("p", "1").with_status(HealthStatus::Unknown);
        assert!(!unknown.readiness);
    }
}

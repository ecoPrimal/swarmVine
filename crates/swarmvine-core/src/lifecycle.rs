// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! Primal lifecycle management.
//!
//! Every primal has a lifecycle: created, running, stopped.
//! This module provides the state machine and trait for managing it.

use crate::error::PrimalError;
use serde::{Deserialize, Serialize};

/// State of a primal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrimalState {
    /// Not yet started.
    Created,
    /// Starting up.
    Starting,
    /// Running normally.
    Running,
    /// Stopping.
    Stopping,
    /// Stopped.
    Stopped,
    /// Failed.
    Failed,
}

impl PrimalState {
    /// Check if the primal is running.
    #[must_use]
    pub const fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }

    /// Check if the primal can be started.
    #[must_use]
    pub const fn can_start(&self) -> bool {
        matches!(self, Self::Created | Self::Stopped | Self::Failed)
    }

    /// Check if the primal can be stopped.
    #[must_use]
    pub const fn can_stop(&self) -> bool {
        matches!(self, Self::Running)
    }
}

impl std::fmt::Display for PrimalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "created"),
            Self::Starting => write!(f, "starting"),
            Self::Running => write!(f, "running"),
            Self::Stopping => write!(f, "stopping"),
            Self::Stopped => write!(f, "stopped"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Lifecycle trait for primals.
///
/// Implement this to define how your primal starts, stops, and reloads.
pub trait PrimalLifecycle: Send + Sync {
    /// Get the current state.
    fn state(&self) -> PrimalState;

    /// Start the primal.
    ///
    /// # Errors
    ///
    /// Returns an error if startup fails.
    fn start(&mut self) -> impl std::future::Future<Output = Result<(), PrimalError>> + Send;

    /// Stop the primal.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown fails.
    fn stop(&mut self) -> impl std::future::Future<Output = Result<(), PrimalError>> + Send;

    /// Reload configuration (default: stop then start).
    ///
    /// # Errors
    ///
    /// Returns an error if reload fails.
    fn reload(&mut self) -> impl std::future::Future<Output = Result<(), PrimalError>> + Send {
        async {
            self.stop().await?;
            self.start().await
        }
    }

    /// Handle a shutdown signal (default: calls stop).
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown fails.
    fn shutdown(&mut self) -> impl std::future::Future<Output = Result<(), PrimalError>> + Send {
        async { self.stop().await }
    }
}

#[cfg(test)]
mod tests {
    use super::PrimalState;

    #[test]
    fn is_running_only_when_running() {
        assert!(!PrimalState::Created.is_running());
        assert!(!PrimalState::Starting.is_running());
        assert!(PrimalState::Running.is_running());
        assert!(!PrimalState::Stopping.is_running());
        assert!(!PrimalState::Stopped.is_running());
        assert!(!PrimalState::Failed.is_running());
    }

    #[test]
    fn can_start_from_created_stopped_failed() {
        assert!(PrimalState::Created.can_start());
        assert!(!PrimalState::Starting.can_start());
        assert!(!PrimalState::Running.can_start());
        assert!(!PrimalState::Stopping.can_start());
        assert!(PrimalState::Stopped.can_start());
        assert!(PrimalState::Failed.can_start());
    }

    #[test]
    fn can_stop_only_when_running() {
        assert!(!PrimalState::Created.can_stop());
        assert!(!PrimalState::Starting.can_stop());
        assert!(PrimalState::Running.can_stop());
        assert!(!PrimalState::Stopping.can_stop());
        assert!(!PrimalState::Stopped.can_stop());
        assert!(!PrimalState::Failed.can_stop());
    }

    #[test]
    fn display_formats_each_state() {
        assert_eq!(PrimalState::Created.to_string(), "created");
        assert_eq!(PrimalState::Starting.to_string(), "starting");
        assert_eq!(PrimalState::Running.to_string(), "running");
        assert_eq!(PrimalState::Stopping.to_string(), "stopping");
        assert_eq!(PrimalState::Stopped.to_string(), "stopped");
        assert_eq!(PrimalState::Failed.to_string(), "failed");
    }

    #[test]
    fn serde_roundtrip() {
        for state in [
            PrimalState::Created,
            PrimalState::Starting,
            PrimalState::Running,
            PrimalState::Stopping,
            PrimalState::Stopped,
            PrimalState::Failed,
        ] {
            let json = serde_json::to_string(&state).unwrap();
            let decoded: PrimalState = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, state);
        }
    }
}

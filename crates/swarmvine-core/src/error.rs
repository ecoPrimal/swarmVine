// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! Common error types for this primal.
//!
//! Extend this enum with domain-specific variants as your primal evolves.

use thiserror::Error;

/// Result type alias for primal operations.
pub type PrimalResult<T> = Result<T, PrimalError>;

/// Common errors that any primal might encounter.
#[derive(Debug, Error)]
pub enum PrimalError {
    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Lifecycle error (start/stop/reload).
    #[error("lifecycle error: {0}")]
    Lifecycle(String),

    /// Health check error.
    #[error("health error: {0}")]
    Health(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Network error.
    #[error("network error: {0}")]
    Network(String),

    /// Timeout.
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// Resource not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Invalid input.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),

    /// Dependency error (upstream service failed).
    #[error("dependency error: {service}: {message}")]
    Dependency {
        /// Name of the dependency that failed.
        service: String,
        /// Error message.
        message: String,
    },
}

impl PrimalError {
    /// Create a configuration error.
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Create a lifecycle error.
    pub fn lifecycle(msg: impl Into<String>) -> Self {
        Self::Lifecycle(msg.into())
    }

    /// Create a dependency error.
    pub fn dependency(service: impl Into<String>, msg: impl Into<String>) -> Self {
        Self::Dependency {
            service: service.into(),
            message: msg.into(),
        }
    }

    /// Check if this is a retryable error.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Network(_) | Self::Timeout(_) | Self::Dependency { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::PrimalError;
    use std::io;

    #[test]
    fn constructors_create_expected_variants() {
        let config = PrimalError::config("bad config");
        assert!(matches!(config, PrimalError::Config(_)));

        let lifecycle = PrimalError::lifecycle("start failed");
        assert!(matches!(lifecycle, PrimalError::Lifecycle(_)));

        let dependency = PrimalError::dependency("songBird", "unreachable");
        assert!(matches!(dependency, PrimalError::Dependency { .. }));
    }

    #[test]
    fn is_retryable_true_for_transient_errors() {
        assert!(PrimalError::Network("conn reset".to_owned()).is_retryable());
        assert!(PrimalError::Timeout("deadline".to_owned()).is_retryable());
        assert!(PrimalError::dependency("upstream", "503").is_retryable());
    }

    #[test]
    fn is_retryable_false_for_permanent_errors() {
        assert!(!PrimalError::config("x").is_retryable());
        assert!(!PrimalError::lifecycle("x").is_retryable());
        assert!(!PrimalError::Health("x".to_owned()).is_retryable());
        assert!(!PrimalError::Serialization("x".to_owned()).is_retryable());
        assert!(!PrimalError::NotFound("x".to_owned()).is_retryable());
        assert!(!PrimalError::InvalidInput("x".to_owned()).is_retryable());
        assert!(!PrimalError::Internal("x".to_owned()).is_retryable());
    }

    #[test]
    fn display_formats_messages() {
        assert_eq!(
            PrimalError::config("missing key").to_string(),
            "configuration error: missing key"
        );
        assert_eq!(
            PrimalError::lifecycle("already running").to_string(),
            "lifecycle error: already running"
        );
        assert_eq!(
            PrimalError::dependency("neural-api", "timeout").to_string(),
            "dependency error: neural-api: timeout"
        );
        assert_eq!(
            PrimalError::Health("probe failed".to_owned()).to_string(),
            "health error: probe failed"
        );
    }

    #[test]
    fn from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "gone");
        let primal_err: PrimalError = io_err.into();
        assert!(matches!(primal_err, PrimalError::Io(_)));
        assert!(primal_err.to_string().contains("gone"));
    }
}

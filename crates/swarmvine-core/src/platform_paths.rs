//! Platform-aware directory resolution for primal data storage.
//!
//! Resolves config, data, runtime, cache, and log directories per platform
//! without silicon deism. Supports Linux (XDG), macOS (~/Library), Windows
//! (%APPDATA%), and mobile sandbox pass-through.
//!
//! Environment overrides always win: `BIOMEOS_CONFIG_DIR`, `BIOMEOS_DATA_DIR`,
//! `BIOMEOS_RUNTIME_DIR`, `BIOMEOS_CACHE_DIR`, `BIOMEOS_LOG_DIR`.

use std::path::PathBuf;

/// Environment variable names for directory overrides.
pub mod env_overrides {
    /// Override for configuration directory.
    pub const CONFIG_DIR: &str = "BIOMEOS_CONFIG_DIR";
    /// Override for persistent data directory.
    pub const DATA_DIR: &str = "BIOMEOS_DATA_DIR";
    /// Override for runtime ephemeral directory.
    pub const RUNTIME_DIR: &str = "BIOMEOS_RUNTIME_DIR";
    /// Override for cache directory.
    pub const CACHE_DIR: &str = "BIOMEOS_CACHE_DIR";
    /// Override for log directory.
    pub const LOG_DIR: &str = "BIOMEOS_LOG_DIR";
}

const NAMESPACE: &str = "biomeos";

/// Resolved directory paths for a primal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimalDirs {
    /// Configuration files.
    pub config: PathBuf,
    /// Persistent data.
    pub data: PathBuf,
    /// Runtime ephemeral state (sockets, PID files).
    pub runtime: PathBuf,
    /// Cache (deletable without data loss).
    pub cache: PathBuf,
    /// Log files.
    pub logs: PathBuf,
}

impl PrimalDirs {
    /// Resolve all directories for the current platform.
    #[must_use]
    pub fn resolve(primal_name: &str) -> Self {
        Self {
            config: resolve_config(primal_name),
            data: resolve_data(primal_name),
            runtime: resolve_runtime(),
            cache: resolve_cache(primal_name),
            logs: resolve_logs(primal_name),
        }
    }

    /// Ensure all directories exist with appropriate permissions.
    ///
    /// # Errors
    ///
    /// Returns an error if directory creation fails.
    pub fn ensure(&self) -> std::io::Result<()> {
        use super::platform_substrate::{PlatformAccess, ensure_dir_with_access};
        ensure_dir_with_access(&self.config, PlatformAccess::OwnerFull)?;
        ensure_dir_with_access(&self.data, PlatformAccess::OwnerFull)?;
        ensure_dir_with_access(&self.runtime, PlatformAccess::OwnerFull)?;
        ensure_dir_with_access(&self.cache, PlatformAccess::PublicExecute)?;
        ensure_dir_with_access(&self.logs, PlatformAccess::PublicExecute)?;
        Ok(())
    }

    /// Get the socket path within the runtime directory.
    #[must_use]
    pub fn socket_path(&self, primal_name: &str) -> PathBuf {
        self.runtime.join(format!("{primal_name}.sock"))
    }

    /// Get the PID file path within the runtime directory.
    #[must_use]
    pub fn pid_path(&self, primal_name: &str) -> PathBuf {
        self.runtime.join(format!("{primal_name}.pid"))
    }
}

fn resolve_config(primal_name: &str) -> PathBuf {
    if let Ok(dir) = std::env::var(env_overrides::CONFIG_DIR) {
        return PathBuf::from(dir).join(primal_name);
    }
    platform_config_base().join(NAMESPACE).join(primal_name)
}

fn resolve_data(primal_name: &str) -> PathBuf {
    if let Ok(dir) = std::env::var(env_overrides::DATA_DIR) {
        return PathBuf::from(dir).join(primal_name);
    }
    platform_data_base().join(NAMESPACE).join(primal_name)
}

fn resolve_runtime() -> PathBuf {
    if let Ok(dir) = std::env::var(env_overrides::RUNTIME_DIR) {
        return PathBuf::from(dir);
    }
    platform_runtime_base().join(NAMESPACE)
}

fn resolve_cache(primal_name: &str) -> PathBuf {
    if let Ok(dir) = std::env::var(env_overrides::CACHE_DIR) {
        return PathBuf::from(dir).join(primal_name);
    }
    platform_cache_base().join(NAMESPACE).join(primal_name)
}

fn resolve_logs(primal_name: &str) -> PathBuf {
    if let Ok(dir) = std::env::var(env_overrides::LOG_DIR) {
        return PathBuf::from(dir).join(primal_name);
    }
    platform_logs_base().join(NAMESPACE).join(primal_name)
}

#[cfg(target_os = "linux")]
fn platform_config_base() -> PathBuf {
    std::env::var("XDG_CONFIG_HOME").map_or_else(|_| home_dir().join(".config"), PathBuf::from)
}
#[cfg(target_os = "macos")]
fn platform_config_base() -> PathBuf {
    home_dir().join("Library").join("Application Support")
}
#[cfg(target_os = "windows")]
fn platform_config_base() -> PathBuf {
    std::env::var("APPDATA").map_or_else(|_| home_dir().join("AppData").join("Roaming"), PathBuf::from)
}
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn platform_config_base() -> PathBuf {
    std::env::var("XDG_CONFIG_HOME").map_or_else(|_| home_dir().join(".config"), PathBuf::from)
}

#[cfg(target_os = "linux")]
fn platform_data_base() -> PathBuf {
    std::env::var("XDG_DATA_HOME").map_or_else(|_| home_dir().join(".local").join("share"), PathBuf::from)
}
#[cfg(target_os = "macos")]
fn platform_data_base() -> PathBuf {
    home_dir().join("Library").join("Application Support")
}
#[cfg(target_os = "windows")]
fn platform_data_base() -> PathBuf {
    std::env::var("APPDATA").map_or_else(|_| home_dir().join("AppData").join("Roaming"), PathBuf::from)
}
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn platform_data_base() -> PathBuf {
    std::env::var("XDG_DATA_HOME").map_or_else(|_| home_dir().join(".local").join("share"), PathBuf::from)
}

#[cfg(target_os = "linux")]
fn platform_runtime_base() -> PathBuf {
    std::env::var("XDG_RUNTIME_DIR").map_or_else(|_| PathBuf::from("/tmp"), PathBuf::from)
}
#[cfg(target_os = "macos")]
fn platform_runtime_base() -> PathBuf {
    std::env::var("XDG_RUNTIME_DIR")
        .or_else(|_| std::env::var("TMPDIR"))
        .map_or_else(|_| PathBuf::from("/tmp"), PathBuf::from)
}
#[cfg(target_os = "windows")]
fn platform_runtime_base() -> PathBuf {
    std::env::var("TEMP")
        .or_else(|_| std::env::var("TMP"))
        .map_or_else(|_| PathBuf::from(r"C:\Temp"), PathBuf::from)
}
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn platform_runtime_base() -> PathBuf {
    std::env::var("XDG_RUNTIME_DIR")
        .or_else(|_| std::env::var("TMPDIR"))
        .map_or_else(|_| PathBuf::from("/tmp"), PathBuf::from)
}

#[cfg(target_os = "linux")]
fn platform_cache_base() -> PathBuf {
    std::env::var("XDG_CACHE_HOME").map_or_else(|_| home_dir().join(".cache"), PathBuf::from)
}
#[cfg(target_os = "macos")]
fn platform_cache_base() -> PathBuf {
    home_dir().join("Library").join("Caches")
}
#[cfg(target_os = "windows")]
fn platform_cache_base() -> PathBuf {
    std::env::var("LOCALAPPDATA").map_or_else(|_| home_dir().join("AppData").join("Local"), PathBuf::from)
}
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn platform_cache_base() -> PathBuf {
    std::env::var("XDG_CACHE_HOME").map_or_else(|_| home_dir().join(".cache"), PathBuf::from)
}

#[cfg(target_os = "linux")]
fn platform_logs_base() -> PathBuf {
    std::env::var("XDG_STATE_HOME").map_or_else(|_| home_dir().join(".local").join("state"), PathBuf::from)
}
#[cfg(target_os = "macos")]
fn platform_logs_base() -> PathBuf {
    home_dir().join("Library").join("Logs")
}
#[cfg(target_os = "windows")]
fn platform_logs_base() -> PathBuf {
    std::env::var("LOCALAPPDATA").map_or_else(|_| home_dir().join("AppData").join("Local"), PathBuf::from)
}
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn platform_logs_base() -> PathBuf {
    std::env::var("XDG_STATE_HOME").map_or_else(|_| home_dir().join(".local").join("state"), PathBuf::from)
}

fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map_or_else(|_| PathBuf::from("/tmp"), PathBuf::from)
}

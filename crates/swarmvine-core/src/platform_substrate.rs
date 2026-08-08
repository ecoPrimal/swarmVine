//! G68 Platform Substrate Abstraction — eliminate silicon deism beyond transport.
//!
//! Two abstraction layers provided here (L3 device backends are domain-specific):
//! - **L1 Links**: `platform_link()` — symlink on Unix, junction/hard-link on Windows
//! - **L2 Permissions**: `PlatformAccess` — POSIX mode bits on Unix, ACL-compatible on Windows
//!
//! The test: "Does this primal do *less* on Windows, or the *same thing differently*?"
//! If less → silicon deism. If differently → platform abstraction.

use std::io;
use std::path::Path;

// ─── L1: Links ─────────────────────────────────────────────────────────────

/// Create a platform-appropriate link from `original` to `link`.
///
/// - **Unix**: Symbolic link.
/// - **Windows**: Hard link (file) or symlink (directory).
/// - **Other**: Hard link.
pub fn platform_link(original: &Path, link: &Path) -> io::Result<()> {
    platform_link_impl(original, link)
}

#[cfg(unix)]
fn platform_link_impl(original: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(original, link)
}

#[cfg(windows)]
fn platform_link_impl(original: &Path, link: &Path) -> io::Result<()> {
    if original.is_dir() {
        std::os::windows::fs::symlink_dir(original, link)
    } else {
        std::os::windows::fs::symlink_file(original, link)
            .or_else(|_| std::fs::hard_link(original, link))
    }
}

#[cfg(not(any(unix, windows)))]
fn platform_link_impl(original: &Path, link: &Path) -> io::Result<()> {
    std::fs::hard_link(original, link)
}

/// Check if a path is a symbolic link.
pub fn is_symlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|m| m.file_type().is_symlink())
        .unwrap_or(false)
}

// ─── L2: Permissions ───────────────────────────────────────────────────────

/// Platform-neutral access level for filesystem objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlatformAccess {
    /// Owner-only read+write (0o600 on Unix).
    OwnerReadWrite,
    /// Owner read+write+execute (0o700 on Unix).
    OwnerFull,
    /// Owner read+write, group+other read (0o644 on Unix).
    PublicRead,
    /// Owner read+write+execute, group+other read+execute (0o755 on Unix).
    PublicExecute,
    /// No access for anyone except owner read (0o400 on Unix).
    Readonly,
    /// Custom Unix mode bits (no-op on non-Unix).
    #[cfg(unix)]
    Custom(u32),
}

impl PlatformAccess {
    /// Apply this access level to the file at `path`.
    pub fn apply(&self, path: &Path) -> io::Result<()> {
        apply_access(path, *self)
    }
}

#[cfg(unix)]
fn apply_access(path: &Path, access: PlatformAccess) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = match access {
        PlatformAccess::OwnerReadWrite => 0o600,
        PlatformAccess::OwnerFull => 0o700,
        PlatformAccess::PublicRead => 0o644,
        PlatformAccess::PublicExecute => 0o755,
        PlatformAccess::Readonly => 0o400,
        PlatformAccess::Custom(m) => m,
    };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn apply_access(path: &Path, access: PlatformAccess) -> io::Result<()> {
    let readonly = matches!(access, PlatformAccess::Readonly);
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_readonly(readonly);
    std::fs::set_permissions(path, perms)
}

/// Query the effective access level of a file.
pub fn query_access(path: &Path) -> io::Result<PlatformAccess> {
    query_access_impl(path)
}

#[cfg(unix)]
fn query_access_impl(path: &Path) -> io::Result<PlatformAccess> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    Ok(match mode {
        0o600 => PlatformAccess::OwnerReadWrite,
        0o700 => PlatformAccess::OwnerFull,
        0o644 => PlatformAccess::PublicRead,
        0o755 => PlatformAccess::PublicExecute,
        0o400 => PlatformAccess::Readonly,
        other => PlatformAccess::Custom(other),
    })
}

#[cfg(not(unix))]
fn query_access_impl(path: &Path) -> io::Result<PlatformAccess> {
    let perms = std::fs::metadata(path)?.permissions();
    if perms.readonly() {
        Ok(PlatformAccess::Readonly)
    } else {
        Ok(PlatformAccess::PublicRead)
    }
}

/// Ensure a directory exists with the specified access level.
pub fn ensure_dir_with_access(path: &Path, access: PlatformAccess) -> io::Result<()> {
    std::fs::create_dir_all(path)?;
    access.apply(path)
}

/// Ensure a file's parent directory exists with owner-only access.
pub fn ensure_secure_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir_with_access(parent, PlatformAccess::OwnerFull)?;
    }
    Ok(())
}

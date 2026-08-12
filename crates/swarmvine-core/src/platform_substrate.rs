// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

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
///
/// # Errors
///
/// Returns an error if the platform link operation fails.
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
#[must_use]
pub fn is_symlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink())
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
    ///
    /// # Errors
    ///
    /// Returns an error if permissions cannot be read or applied to `path`.
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
///
/// # Errors
///
/// Returns an error if metadata for `path` cannot be read.
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
///
/// # Errors
///
/// Returns an error if directory creation or permission application fails.
pub fn ensure_dir_with_access(path: &Path, access: PlatformAccess) -> io::Result<()> {
    std::fs::create_dir_all(path)?;
    access.apply(path)
}

/// Ensure a file's parent directory exists with owner-only access.
///
/// # Errors
///
/// Returns an error if the parent directory cannot be created or secured.
pub fn ensure_secure_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir_with_access(parent, PlatformAccess::OwnerFull)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        PlatformAccess, ensure_dir_with_access, ensure_secure_parent, is_symlink, platform_link,
        query_access,
    };
    use std::fs;
    use std::path::Path;

    fn temp_dir(prefix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", std::process::id()))
    }

    #[test]
    fn platform_link_creates_working_symlink() {
        let base = temp_dir("swarmvine-link");
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();

        let original = base.join("original.txt");
        fs::write(&original, b"hello").unwrap();

        let link = base.join("link.txt");
        platform_link(&original, &link).unwrap();

        assert!(is_symlink(&link));
        assert_eq!(fs::read_to_string(&link).unwrap(), "hello");

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn is_symlink_detects_symlinks_and_not_regular_files() {
        let base = temp_dir("swarmvine-symlink-detect");
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();

        let file = base.join("regular.txt");
        fs::write(&file, b"data").unwrap();
        assert!(!is_symlink(&file));

        let link = base.join("linked.txt");
        platform_link(&file, &link).unwrap();
        assert!(is_symlink(&link));

        let _ = fs::remove_dir_all(&base);
    }

    #[cfg(unix)]
    #[test]
    fn platform_access_posix_modes() {
        use std::os::unix::fs::PermissionsExt;

        let base = temp_dir("swarmvine-access");
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();

        let cases: &[(PlatformAccess, u32)] = &[
            (PlatformAccess::OwnerReadWrite, 0o600),
            (PlatformAccess::OwnerFull, 0o700),
            (PlatformAccess::PublicRead, 0o644),
            (PlatformAccess::PublicExecute, 0o755),
            (PlatformAccess::Readonly, 0o400),
        ];

        for (access, expected_mode) in cases {
            let path = base.join(format!("mode-{expected_mode:o}"));
            fs::write(&path, b"x").unwrap();
            access.apply(&path).unwrap();
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, *expected_mode);
            assert_eq!(query_access(&path).unwrap(), *access);
        }

        let custom_path = base.join("custom");
        fs::write(&custom_path, b"x").unwrap();
        PlatformAccess::Custom(0o640).apply(&custom_path).unwrap();
        assert_eq!(
            query_access(&custom_path).unwrap(),
            PlatformAccess::Custom(0o640)
        );

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn ensure_dir_with_access_creates_directory() {
        let dir = temp_dir("swarmvine-ensure-dir");
        let _ = fs::remove_dir_all(&dir);

        ensure_dir_with_access(&dir, PlatformAccess::OwnerFull).unwrap();
        assert!(dir.is_dir());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn ensure_secure_parent_creates_parent() {
        let base = temp_dir("swarmvine-secure-parent");
        let _ = fs::remove_dir_all(&base);
        let nested = base.join("nested").join("child.sock");

        ensure_secure_parent(&nested).unwrap();
        assert!(nested.parent().is_some_and(Path::is_dir));

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn query_access_roundtrips_with_apply() {
        let base = temp_dir("swarmvine-query-roundtrip");
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();

        let file = base.join("secret");
        fs::write(&file, b"data").unwrap();

        for access in [
            PlatformAccess::OwnerReadWrite,
            PlatformAccess::OwnerFull,
            PlatformAccess::PublicRead,
            PlatformAccess::PublicExecute,
            PlatformAccess::Readonly,
        ] {
            access.apply(&file).unwrap();
            assert_eq!(query_access(&file).unwrap(), access);
        }

        let _ = fs::remove_dir_all(&base);
    }
}

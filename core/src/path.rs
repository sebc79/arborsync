//! Canonical paths, interest, overlap, and reserved names (`spec.md` §1–§3, §6).

use std::path::{Path, PathBuf};

use thiserror::Error;

/// Reserved directory at a checkout root and at `central_root`.
pub const RESERVED_TMP: &str = ".arborsync-tmp";
/// Reserved conflict sidecar directory at a checkout root and at `central_root`.
pub const RESERVED_CONFLICTS: &str = ".arborsync-conflicts";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PathError {
    #[error("path is not valid UTF-8")]
    NotUtf8,
    #[error("path is not absolute: {0}")]
    NotAbsolute(String),
    #[error("path contains '.' or '..' after normalization: {0}")]
    DotComponent(String),
    #[error("path is outside the bound root")]
    EscapesRoot,
}

/// A checkout is interested in canonical path `P` iff `central` is a prefix
/// of `P`: `P == central` or `P` starts with `central` + `/`. `/` matches
/// everything. Sibling `/src` does not match `/src2` (`spec.md` §2).
pub fn is_interested(central: &str, path: &str) -> bool {
    if central == "/" {
        return path.starts_with('/');
    }
    path == central || path.starts_with(&format!("{central}/"))
}

/// Reserved names are skipped only at the tree root being walked
/// (`spec.md` §6).
pub fn is_reserved_root_entry(name: &str) -> bool {
    name == RESERVED_TMP || name == RESERVED_CONFLICTS
}

/// Convert a host path under `root` (already canonicalized) to a logical
/// canonical path. `root` is `central_root` on the master, or a checkout
/// `local` on the slave (then join with that checkout’s `central`).
pub fn host_to_canonical(root: &Path, host_path: &Path) -> Result<String, PathError> {
    let _ = (root, host_path);
    todo!("spec §1: canonical paths never include central_root or checkout local")
}

/// Join a checkout `central` prefix with a path relative to `local`.
pub fn join_central(central: &str, relative: &str) -> Result<String, PathError> {
    let _ = (central, relative);
    todo!("spec §1 / §6: slave canonical = central joined with relative(local, os_path)")
}

/// Normalize a canonical path: absolute, UTF-8, no `.` / `..` (`spec.md` §4).
pub fn normalize_canonical(path: &str) -> Result<String, PathError> {
    let _ = path;
    todo!("spec §4 / subscriptions.md: reject non-absolute and dot components")
}

/// Two absolute paths overlap iff they are equal or one is a parent of the
/// other after canonicalization (`spec.md` §3). `/opt/a` and `/opt/a/b`
/// overlap; `/opt/a` and `/opt/ab` do not.
pub fn local_paths_overlap(a: &Path, b: &Path) -> bool {
    let _ = (a, b);
    todo!("spec §3: local path overlap")
}

/// `{checkout_local}/.arborsync-conflicts/{canonical-relative}--{content_hash_hex[0..16]}`
/// (`spec.md` §8).
pub fn conflict_sidecar_path(
    checkout_local: &Path,
    canonical: &str,
    content_hash: &[u8; 32],
) -> PathBuf {
    let _ = (checkout_local, canonical, content_hash);
    todo!("spec §8: conflict sidecar path")
}

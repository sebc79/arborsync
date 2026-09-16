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
    let root_str = root.to_str().ok_or(PathError::NotUtf8)?;
    let host_str = host_path.to_str().ok_or(PathError::NotUtf8)?;
    if !root.is_absolute() {
        return Err(PathError::NotAbsolute(root_str.into()));
    }
    if !host_path.is_absolute() {
        return Err(PathError::NotAbsolute(host_str.into()));
    }
    let rel = host_path
        .strip_prefix(root)
        .map_err(|_| PathError::EscapesRoot)?;
    if rel.as_os_str().is_empty() {
        return Ok("/".into());
    }
    let rel_str = rel.to_str().ok_or(PathError::NotUtf8)?;
    let mut canonical = String::with_capacity(1 + rel_str.len());
    canonical.push('/');
    canonical.push_str(rel_str);
    normalize_canonical(&canonical)
}

/// Join a checkout `central` prefix with a path relative to `local`.
pub fn join_central(central: &str, relative: &str) -> Result<String, PathError> {
    let central = normalize_canonical(central)?;
    if relative.is_empty() {
        return Ok(central);
    }
    let relative = relative.trim_start_matches('/');
    if central == "/" {
        normalize_canonical(&format!("/{relative}"))
    } else {
        normalize_canonical(&format!("{central}/{relative}"))
    }
}

/// Normalize a canonical path: absolute, UTF-8, no `.` / `..` (`spec.md` §4).
pub fn normalize_canonical(path: &str) -> Result<String, PathError> {
    if !path.starts_with('/') {
        return Err(PathError::NotAbsolute(path.into()));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        if part.is_empty() {
            continue;
        }
        if part == "." || part == ".." {
            return Err(PathError::DotComponent(path.into()));
        }
        parts.push(part);
    }
    if parts.is_empty() {
        return Ok("/".into());
    }
    Ok(format!("/{}", parts.join("/")))
}

/// Two absolute paths overlap iff they are equal or one is a parent of the
/// other after canonicalization (`spec.md` §3). `/opt/a` and `/opt/a/b`
/// overlap; `/opt/a` and `/opt/ab` do not.
pub fn local_paths_overlap(a: &Path, b: &Path) -> bool {
    let a: Vec<_> = a.components().collect();
    let b: Vec<_> = b.components().collect();
    a.starts_with(&b) || b.starts_with(&a)
}

/// `{checkout_local}/.arborsync-conflicts/{canonical-relative}--{content_hash_hex[0..16]}`
/// (`spec.md` §8).
pub fn conflict_sidecar_path(
    checkout_local: &Path,
    canonical: &str,
    content_hash: &[u8; 32],
) -> PathBuf {
    let relative = canonical.trim_start_matches('/');
    let hex16 = hex_prefix(content_hash, 16);
    checkout_local
        .join(RESERVED_CONFLICTS)
        .join(format!("{relative}--{hex16}"))
}

fn hex_prefix(bytes: &[u8], hex_chars: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(hex_chars);
    for &b in bytes {
        if out.len() == hex_chars {
            break;
        }
        out.push(HEX[(b >> 4) as usize] as char);
        if out.len() == hex_chars {
            break;
        }
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::hash::ContentHash;

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
    #[error("path is not canonical: {value} normalizes to {normalized}")]
    NonCanonical { value: String, normalized: String },
    #[error("not a single path component: {0}")]
    BadEntryName(String),
}

/// A logical path in the central hierarchy: absolute, UTF-8, no `.` or `..`,
/// no empty components, no trailing slash (`spec.md` §1, §4).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CanonicalPath(String);

impl CanonicalPath {
    pub fn root() -> Self {
        Self("/".into())
    }

    /// Parse external input. Rejects a path that normalizes to something
    /// spelled differently, so `//src` is an error rather than a silent
    /// rewrite to `/src`.
    pub fn parse(value: &str) -> Result<Self, PathError> {
        let normalized = normalize_canonical(value)?;
        if normalized != value {
            return Err(PathError::NonCanonical {
                value: value.into(),
                normalized,
            });
        }
        Ok(Self(normalized))
    }

    pub(crate) fn from_stored(value: &str) -> Self {
        debug_assert_eq!(Self::parse(value).as_ref().map(Self::as_str), Ok(value));
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn name(&self) -> &str {
        match self.0.rsplit_once('/') {
            Some((_, name)) if !name.is_empty() => name,
            _ => "/",
        }
    }

    /// `None` at the hierarchy root. `/src/a` yields `/src`, `/src` yields `/`.
    pub fn parent(&self) -> Option<CanonicalPath> {
        if self.0 == "/" {
            return None;
        }
        let (head, _) = self.0.rsplit_once('/')?;
        if head.is_empty() {
            return Some(Self::root());
        }
        Some(Self(head.into()))
    }

    /// Strict ancestors, nearest first, ending at `/`. Empty at `/`.
    pub fn ancestors(&self) -> impl Iterator<Item = CanonicalPath> {
        let mut next = self.parent();
        std::iter::from_fn(move || {
            let current = next.take()?;
            next = current.parent();
            Some(current)
        })
    }

    /// True iff `other` is this path or below it: `/src` covers `/src` and
    /// `/src/foo.rs`, but not the sibling `/src2` (`spec.md` §2). `/` covers
    /// everything.
    pub fn covers(&self, other: &CanonicalPath) -> bool {
        if self.0 == "/" {
            return true;
        }
        match other.0.strip_prefix(self.0.as_str()) {
            Some(rest) => rest.is_empty() || rest.starts_with('/'),
            None => false,
        }
    }

    /// True when the first component is a reserved sidecar name
    /// (`spec.md` §6). `/.arborsync-tmp/x` is reserved. `/src/.arborsync-tmp`
    /// is not.
    pub fn has_reserved_root_name(&self) -> bool {
        self.as_str()
            .trim_start_matches('/')
            .split('/')
            .next()
            .is_some_and(is_reserved_root_entry)
    }
}

impl Serialize for CanonicalPath {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CanonicalPath {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

/// One component of a path: not empty, no `/`, not `.` or `..`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntryName(String);

impl EntryName {
    pub fn parse(value: &str) -> Result<Self, PathError> {
        if value.is_empty() || value == "." || value == ".." || value.contains('/') {
            return Err(PathError::BadEntryName(value.into()));
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for EntryName {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EntryName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

/// Reserved names are skipped only at the tree root being walked
/// (`spec.md` §6).
pub fn is_reserved_root_entry(name: &str) -> bool {
    name == RESERVED_TMP || name == RESERVED_CONFLICTS
}

pub fn local_to_canonical(
    local: &Path,
    central: &CanonicalPath,
    host_path: &Path,
) -> Result<CanonicalPath, PathError> {
    let relative = host_to_canonical(local, host_path)?;
    if relative.as_str() == "/" {
        return Ok(central.clone());
    }
    join_central(central, relative.as_str().trim_start_matches('/'))
}

/// Convert a host path under `root` (already canonicalized) to a logical
/// canonical path.
pub fn host_to_canonical(root: &Path, host_path: &Path) -> Result<CanonicalPath, PathError> {
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
        return Ok(CanonicalPath::root());
    }
    let rel_str = rel.to_str().ok_or(PathError::NotUtf8)?;
    let mut canonical = String::with_capacity(1 + rel_str.len());
    canonical.push('/');
    canonical.push_str(rel_str);
    Ok(CanonicalPath(normalize_canonical(&canonical)?))
}

/// The inverse of [`host_to_canonical`]: `central_root` plus the canonical
/// path's components. `/` is the root itself.
pub fn canonical_to_host(central_root: &Path, path: &CanonicalPath) -> PathBuf {
    let relative = path.as_str().trim_start_matches('/');
    if relative.is_empty() {
        return central_root.to_path_buf();
    }
    central_root.join(relative)
}

pub fn strip_central(
    central: &CanonicalPath,
    path: &CanonicalPath,
) -> Result<CanonicalPath, PathError> {
    if !central.covers(path) {
        return Err(PathError::EscapesRoot);
    }
    if path == central {
        return Ok(CanonicalPath::root());
    }
    if central.as_str() == "/" {
        return Ok(path.clone());
    }
    CanonicalPath::parse(
        path.as_str()
            .strip_prefix(central.as_str())
            .expect("covers"),
    )
}

pub fn join_central(central: &CanonicalPath, relative: &str) -> Result<CanonicalPath, PathError> {
    if relative.is_empty() {
        return Ok(central.clone());
    }
    let relative = relative.trim_start_matches('/');
    let joined = if central.as_str() == "/" {
        format!("/{relative}")
    } else {
        format!("{}/{relative}", central.as_str())
    };
    Ok(CanonicalPath(normalize_canonical(&joined)?))
}

fn normalize_canonical(path: &str) -> Result<String, PathError> {
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
    canonical: &CanonicalPath,
    content_hash: &ContentHash,
) -> PathBuf {
    let relative = canonical.as_str().trim_start_matches('/');
    let hex16 = hex_prefix(content_hash.as_bytes(), 16);
    checkout_local
        .join(RESERVED_CONFLICTS)
        .join(format!("{relative}--{hex16}"))
}

#[cfg(test)]
mod stored {
    use super::*;

    #[test]
    fn from_stored_matches_parse_for_a_canonical_path() {
        assert_eq!(
            CanonicalPath::from_stored("/src/foo.rs"),
            CanonicalPath::parse("/src/foo.rs").unwrap()
        );
        assert_eq!(CanonicalPath::from_stored("/"), CanonicalPath::root());
    }
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

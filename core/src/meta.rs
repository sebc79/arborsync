use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::hash::ContentHash;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EntryKind {
    File = 1,
    Dir = 2,
    Symlink = 3,
}

impl From<EntryKind> for u8 {
    fn from(kind: EntryKind) -> Self {
        kind as u8
    }
}

impl TryFrom<u8> for EntryKind {
    type Error = MetaError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::File),
            2 => Ok(Self::Dir),
            3 => Ok(Self::Symlink),
            other => Err(MetaError::UnknownKind(other)),
        }
    }
}

impl Serialize for EntryKind {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(u8::from(*self))
    }
}

impl<'de> Deserialize<'de> for EntryKind {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u8::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("unknown entry kind {0}")]
    UnknownKind(u8),
}

/// Index row for a path. Directories still have a row (`kind = Dir`,
/// `size = 0`, `content_hash = ContentHash::ZERO`); the Merkle identity of a
/// directory is `DirNode`, stored separately (`spec.md` §6).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FileMetadata {
    pub kind: EntryKind,
    pub size: u64,
    pub mtime_ns: i64,
    pub mode: u32,
    pub content_hash: ContentHash,
}

impl FileMetadata {
    pub fn file(size: u64, mtime_ns: i64, mode: u32, content_hash: ContentHash) -> Self {
        Self {
            kind: EntryKind::File,
            size,
            mtime_ns,
            mode,
            content_hash,
        }
    }

    pub fn directory(mtime_ns: i64, mode: u32) -> Self {
        Self {
            kind: EntryKind::Dir,
            size: 0,
            mtime_ns,
            mode,
            content_hash: ContentHash::ZERO,
        }
    }

    pub fn symlink(target_len: u64, mtime_ns: i64, mode: u32, content_hash: ContentHash) -> Self {
        Self {
            kind: EntryKind::Symlink,
            size: target_len,
            mtime_ns,
            mode,
            content_hash,
        }
    }
}

pub fn hash_bytes(bytes: &[u8]) -> ContentHash {
    ContentHash::from_bytes(*blake3::hash(bytes).as_bytes())
}

/// Streaming so a large file never lands in memory whole.
pub fn hash_file(host: &Path) -> Result<ContentHash, io::Error> {
    let mut hasher = blake3::Hasher::new();
    io::copy(&mut fs::File::open(host)?, &mut hasher)?;
    Ok(ContentHash::from_bytes(*hasher.finalize().as_bytes()))
}

/// Read one host path as an index row (`spec.md` §6). `None` for a path
/// that is gone, for `PermissionDenied`, and for the types the spec skips:
/// device files, sockets, FIFOs. Symlinks are stored, never followed.
pub fn collect_from_path(host: &Path) -> Result<Option<FileMetadata>, io::Error> {
    let md = match fs::symlink_metadata(host) {
        Ok(md) => md,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            log::warn!("skipping {}: permission denied", host.display());
            return Ok(None);
        }
        Err(err) => return Err(err),
    };
    let mode = md.mode();
    let mtime_ns = md.mtime() * 1_000_000_000 + md.mtime_nsec();
    let kind = md.file_type();

    if kind.is_symlink() {
        let target = match fs::read_link(host) {
            Ok(target) => target,
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                log::warn!("skipping {}: permission denied", host.display());
                return Ok(None);
            }
            Err(err) => return Err(err),
        };
        let bytes = target.as_os_str().as_bytes();
        return Ok(Some(FileMetadata::symlink(
            bytes.len() as u64,
            mtime_ns,
            mode,
            hash_bytes(bytes),
        )));
    }
    if kind.is_dir() {
        return Ok(Some(FileMetadata::directory(mtime_ns, mode)));
    }
    if !kind.is_file() {
        log::warn!("skipping special file {}", host.display());
        return Ok(None);
    }
    let content_hash = match hash_file(host) {
        Ok(hash) => hash,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            log::warn!("skipping {}: permission denied", host.display());
            return Ok(None);
        }
        Err(err) => return Err(err),
    };
    Ok(Some(FileMetadata::file(
        md.len(),
        mtime_ns,
        mode,
        content_hash,
    )))
}

pub enum Inspected {
    Ready(FileMetadata),
    NeedHash(PathBuf),
    Absent,
}

/// Stat, symlink, and directory only. A file whose mtime, size, and kind match
/// `previous` is `Ready` and reuses that content hash. A file miss is `NeedHash`.
pub fn inspect_for_hash(
    host: &Path,
    previous: Option<&FileMetadata>,
) -> Result<Inspected, io::Error> {
    let md = match fs::symlink_metadata(host) {
        Ok(md) => md,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Inspected::Absent),
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            log::warn!("skipping {}: permission denied", host.display());
            return Ok(Inspected::Absent);
        }
        Err(err) => return Err(err),
    };
    let mode = md.mode();
    let mtime_ns = md.mtime() * 1_000_000_000 + md.mtime_nsec();
    let ft = md.file_type();
    if ft.is_symlink() || ft.is_dir() {
        return Ok(match collect_from_path(host)? {
            Some(meta) => Inspected::Ready(meta),
            None => Inspected::Absent,
        });
    }
    if !ft.is_file() {
        log::warn!("skipping special file {}", host.display());
        return Ok(Inspected::Absent);
    }
    let size = md.len();
    if let Some(prev) = previous {
        if prev.kind == EntryKind::File && prev.size == size && prev.mtime_ns == mtime_ns {
            return Ok(Inspected::Ready(FileMetadata {
                kind: EntryKind::File,
                size,
                mtime_ns,
                mode,
                content_hash: prev.content_hash,
            }));
        }
    }
    Ok(Inspected::NeedHash(host.to_path_buf()))
}

/// Re-stat `host` and reuse `previous.content_hash` when kind, size, and
/// mtime match (`spec.md` §6 / §7). Mode is not a miss. Missing row or a
/// miss falls through to [`collect_from_path`].
pub fn collect_for_rescan(
    host: &Path,
    previous: Option<&FileMetadata>,
) -> Result<Option<FileMetadata>, io::Error> {
    match inspect_for_hash(host, previous)? {
        Inspected::Ready(meta) => Ok(Some(meta)),
        Inspected::Absent => Ok(None),
        Inspected::NeedHash(_) => collect_from_path(host),
    }
}

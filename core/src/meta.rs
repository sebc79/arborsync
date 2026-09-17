use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

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
/// that is gone and for the types the spec skips: device files, sockets,
/// FIFOs. Symlinks are stored, never followed.
pub fn collect_from_path(host: &Path) -> Result<Option<FileMetadata>, io::Error> {
    let md = match fs::symlink_metadata(host) {
        Ok(md) => md,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let mode = md.mode();
    let mtime_ns = md.mtime() * 1_000_000_000 + md.mtime_nsec();
    let kind = md.file_type();

    if kind.is_symlink() {
        let target = fs::read_link(host)?;
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
        return Ok(None);
    }
    Ok(Some(FileMetadata::file(
        md.len(),
        mtime_ns,
        mode,
        hash_file(host)?,
    )))
}

pub fn collect_for_rescan(
    host: &Path,
    previous: Option<&FileMetadata>,
) -> Result<Option<FileMetadata>, io::Error> {
    collect_from_path_cached(host, previous)
}

pub fn collect_from_path_cached(
    host: &Path,
    previous: Option<&FileMetadata>,
) -> Result<Option<FileMetadata>, io::Error> {
    let md = match fs::symlink_metadata(host) {
        Ok(md) => md,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let mode = md.mode();
    let mtime_ns = md.mtime() * 1_000_000_000 + md.mtime_nsec();
    let ft = md.file_type();
    let (kind, size) = if ft.is_symlink() {
        (EntryKind::Symlink, md.len())
    } else if ft.is_dir() {
        (EntryKind::Dir, 0)
    } else if ft.is_file() {
        (EntryKind::File, md.len())
    } else {
        return Ok(None);
    };
    if let Some(prev) = previous {
        if prev.kind == kind && prev.size == size && prev.mtime_ns == mtime_ns && prev.mode == mode
        {
            return Ok(Some(FileMetadata {
                kind,
                size,
                mtime_ns,
                mode,
                content_hash: prev.content_hash,
            }));
        }
    }
    collect_from_path(host)
}

//! File metadata and entry kinds (`spec.md` §6).

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// `File = 1`, `Dir = 2`, `Symlink = 3`.
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
/// `size = 0`, `content_hash = [0; 32]`); the Merkle identity of a
/// directory is `DirNode`, stored separately (`spec.md` §6).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FileMetadata {
    pub kind: EntryKind,
    pub size: u64,
    pub mtime_ns: i64,
    pub mode: u32,
    pub content_hash: [u8; 32],
}

impl FileMetadata {
    pub fn file(size: u64, mtime_ns: i64, mode: u32, content_hash: [u8; 32]) -> Self {
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
            content_hash: [0; 32],
        }
    }

    pub fn symlink(target_len: u64, mtime_ns: i64, mode: u32, content_hash: [u8; 32]) -> Self {
        Self {
            kind: EntryKind::Symlink,
            size: target_len,
            mtime_ns,
            mode,
            content_hash,
        }
    }
}

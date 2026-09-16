use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File, Permissions};
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use filetime::FileTime;

use crate::hash::ContentHash;
use crate::meta::{EntryKind, FileMetadata, hash_bytes};
use crate::path::{CanonicalPath, RESERVED_TMP, canonical_to_host, conflict_sidecar_path};

#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("staged bytes do not hash to the announced content hash")]
    ContentMismatch,
}

fn at(path: &Path) -> impl Fn(io::Error) -> ApplyError + '_ {
    move |source| ApplyError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Whole-file (or symlink target) bytes for an accepted CAS.
pub trait ContentHook {
    fn fetch(&mut self, want: ContentHash) -> ContentBytes;
}

pub enum ContentBytes {
    Whole(Vec<u8>),
    /// Reply with `SignatureRequest` and write nothing.
    AskSender,
}

#[derive(Default)]
pub struct MemoryContent {
    bodies: HashMap<ContentHash, Vec<u8>>,
}

impl MemoryContent {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn offer(&mut self, hash: ContentHash, bytes: Vec<u8>) {
        self.bodies.insert(hash, bytes);
    }
}

impl ContentHook for MemoryContent {
    fn fetch(&mut self, want: ContentHash) -> ContentBytes {
        match self.bodies.get(&want) {
            Some(bytes) => ContentBytes::Whole(bytes.clone()),
            None => ContentBytes::AskSender,
        }
    }
}

pub struct WholeFileLater;

impl ContentHook for WholeFileLater {
    fn fetch(&mut self, _want: ContentHash) -> ContentBytes {
        ContentBytes::AskSender
    }
}

/// Bytes under `{central_root}/.arborsync-tmp`, not yet live.
#[must_use]
pub struct StagedContent {
    tmp: PathBuf,
}

/// Bytes whose BLAKE3 matched the announced hash. Only [`StagedContent::verify`]
/// produces one, so unhashed bytes cannot reach the live tree.
#[must_use]
pub struct VerifiedContent {
    tmp: PathBuf,
}

impl StagedContent {
    pub fn write(central_root: &Path, bytes: &[u8]) -> Result<Self, ApplyError> {
        let dir = central_root.join(RESERVED_TMP);
        fs::create_dir_all(&dir).map_err(at(&dir))?;
        let tmp = dir.join(unique_name());
        let mut file = File::create(&tmp).map_err(at(&tmp))?;
        file.write_all(bytes).map_err(at(&tmp))?;
        file.sync_all().map_err(at(&tmp))?;
        Ok(Self { tmp })
    }

    /// Re-reads the staged file rather than the caller's buffer, so a short
    /// write fails here instead of publishing truncated bytes.
    pub fn verify(self, want: ContentHash) -> Result<VerifiedContent, ApplyError> {
        let written = fs::read(&self.tmp).map_err(at(&self.tmp))?;
        if hash_bytes(&written) != want {
            let _ = fs::remove_file(&self.tmp);
            return Err(ApplyError::ContentMismatch);
        }
        Ok(VerifiedContent { tmp: self.tmp })
    }
}

impl VerifiedContent {
    /// The rename is the publish point. A crash before it leaves the live
    /// path untouched and a stray tmp file that `Master::open` wipes; a
    /// crash after it leaves an index the next rescan repairs.
    pub fn publish(self, target: &Path, meta: &FileMetadata) -> Result<(), ApplyError> {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(at(parent))?;
        }
        fs::set_permissions(&self.tmp, Permissions::from_mode(meta.mode & 0o7777))
            .map_err(at(&self.tmp))?;
        filetime::set_file_mtime(&self.tmp, file_time(meta.mtime_ns)).map_err(at(&self.tmp))?;
        fs::rename(&self.tmp, target).map_err(at(target))
    }
}

pub fn atomic_put(
    central_root: &Path,
    path: &CanonicalPath,
    meta: &FileMetadata,
    bytes: &[u8],
) -> Result<(), ApplyError> {
    let target = canonical_to_host(central_root, path);
    StagedContent::write(central_root, bytes)?
        .verify(meta.content_hash)?
        .publish(&target, meta)
}

pub fn atomic_symlink(
    central_root: &Path,
    path: &CanonicalPath,
    meta: &FileMetadata,
    target_bytes: &[u8],
) -> Result<(), ApplyError> {
    if hash_bytes(target_bytes) != meta.content_hash {
        return Err(ApplyError::ContentMismatch);
    }
    let link = canonical_to_host(central_root, path);
    if let Some(parent) = link.parent() {
        fs::create_dir_all(parent).map_err(at(parent))?;
    }
    remove_if_present(&link)?;
    std::os::unix::fs::symlink(OsStr::from_bytes(target_bytes), &link).map_err(at(&link))?;
    let mtime = file_time(meta.mtime_ns);
    filetime::set_symlink_file_times(&link, mtime, mtime).map_err(at(&link))
}

pub fn mkdir_live(
    central_root: &Path,
    path: &CanonicalPath,
    meta: &FileMetadata,
) -> Result<(), ApplyError> {
    let dir = canonical_to_host(central_root, path);
    fs::create_dir_all(&dir).map_err(at(&dir))?;
    fs::set_permissions(&dir, Permissions::from_mode(meta.mode & 0o7777)).map_err(at(&dir))?;
    filetime::set_file_mtime(&dir, file_time(meta.mtime_ns)).map_err(at(&dir))
}

pub fn remove_live(central_root: &Path, path: &CanonicalPath) -> Result<(), ApplyError> {
    let host = canonical_to_host(central_root, path);
    match fs::symlink_metadata(&host) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(at(&host)(err)),
        Ok(md) if md.is_dir() => fs::remove_dir_all(&host).map_err(at(&host)),
        Ok(_) => fs::remove_file(&host).map_err(at(&host)),
    }
}

pub fn sidecar_if_content_differs(
    central_root: &Path,
    path: &CanonicalPath,
    previous: &FileMetadata,
    incoming: ContentHash,
) -> Result<(), ApplyError> {
    if previous.kind == EntryKind::Dir || previous.content_hash == incoming {
        return Ok(());
    }
    let live = canonical_to_host(central_root, path);
    let bytes = match previous.kind {
        EntryKind::Symlink => fs::read_link(&live)
            .map_err(at(&live))?
            .as_os_str()
            .as_bytes()
            .to_vec(),
        _ => fs::read(&live).map_err(at(&live))?,
    };
    let sidecar = conflict_sidecar_path(central_root, path, &previous.content_hash);
    if let Some(parent) = sidecar.parent() {
        fs::create_dir_all(parent).map_err(at(parent))?;
    }
    fs::write(&sidecar, bytes).map_err(at(&sidecar))
}

pub fn wipe_tmp(central_root: &Path) -> Result<(), ApplyError> {
    let dir = central_root.join(RESERVED_TMP);
    match fs::remove_dir_all(&dir) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(at(&dir)(err)),
        Ok(()) => Ok(()),
    }
}

fn remove_if_present(path: &Path) -> Result<(), ApplyError> {
    match fs::remove_file(path) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(at(path)(err)),
        Ok(()) => Ok(()),
    }
}

fn file_time(mtime_ns: i64) -> FileTime {
    FileTime::from_unix_time(
        mtime_ns.div_euclid(1_000_000_000),
        mtime_ns.rem_euclid(1_000_000_000) as u32,
    )
}

fn unique_name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!(
        "{}-{nanos}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

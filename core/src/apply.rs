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
use crate::path::{CanonicalPath, RESERVED_TMP, confine_host};

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
    #[error("path leaves the checkout root")]
    EscapesRoot,
}

fn host_in_root(root: &Path, path: &CanonicalPath) -> Result<PathBuf, ApplyError> {
    confine_host(root, path).map_err(|_| ApplyError::EscapesRoot)
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
            create_dir_all_with_mode(parent, 0o755)?;
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
    let target = host_in_root(central_root, path)?;
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
    let link = host_in_root(central_root, path)?;
    if let Some(parent) = link.parent() {
        create_dir_all_with_mode(parent, 0o755)?;
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
    let dir = host_in_root(central_root, path)?;
    create_dir_all_with_mode(&dir, meta.mode & 0o7777)?;
    apply_mode_and_mtime(&dir, meta)
}

/// Create `dir` and any missing parents, then set `mode` on each directory
/// this call created so umask does not win over an announced mode.
fn create_dir_all_with_mode(dir: &Path, mode: u32) -> Result<(), ApplyError> {
    let mut missing = Vec::new();
    let mut cur = dir.to_path_buf();
    loop {
        match fs::symlink_metadata(&cur) {
            Ok(_) => break,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                missing.push(cur.clone());
                match cur.parent() {
                    Some(parent) if parent != cur => cur = parent.to_path_buf(),
                    _ => break,
                }
            }
            Err(err) => return Err(at(&cur)(err)),
        }
    }
    fs::create_dir_all(dir).map_err(at(dir))?;
    for created in missing.iter().rev() {
        fs::set_permissions(created, Permissions::from_mode(mode)).map_err(at(created))?;
    }
    Ok(())
}

pub fn rename_live(
    root: &Path,
    from: &CanonicalPath,
    to: &CanonicalPath,
    meta: &FileMetadata,
) -> Result<(), ApplyError> {
    let from_host = host_in_root(root, from)?;
    let to_host = host_in_root(root, to)?;
    if let Some(parent) = to_host.parent() {
        create_dir_all_with_mode(parent, 0o755)?;
    }
    fs::rename(&from_host, &to_host).map_err(at(&to_host))?;
    match meta.kind {
        EntryKind::Symlink => {
            let mtime = file_time(meta.mtime_ns);
            filetime::set_symlink_file_times(&to_host, mtime, mtime).map_err(at(&to_host))
        }
        EntryKind::File | EntryKind::Dir => apply_mode_and_mtime(&to_host, meta),
    }
}

pub fn remove_live(central_root: &Path, path: &CanonicalPath) -> Result<(), ApplyError> {
    let host = host_in_root(central_root, path)?;
    match fs::symlink_metadata(&host) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(at(&host)(err)),
        Ok(md) if md.is_dir() => remove_dir_children_first(&host),
        Ok(_) => fs::remove_file(&host).map_err(at(&host)),
    }
}

pub fn replace_live(
    root: &Path,
    path: &CanonicalPath,
    new: &FileMetadata,
    body: &[u8],
    previous: Option<&FileMetadata>,
) -> Result<(), ApplyError> {
    if previous.is_some_and(|prev| prev.kind != new.kind) {
        remove_live(root, path)?;
    }
    match new.kind {
        EntryKind::Dir => mkdir_live(root, path, new),
        EntryKind::File => atomic_put(root, path, new, body),
        EntryKind::Symlink => atomic_symlink(root, path, new, body),
    }
}

fn remove_dir_children_first(dir: &Path) -> Result<(), ApplyError> {
    for entry in fs::read_dir(dir).map_err(at(dir))? {
        let entry = entry.map_err(at(dir))?;
        let child = entry.path();
        match fs::symlink_metadata(&child) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(at(&child)(err)),
            Ok(md) if md.is_dir() => remove_dir_children_first(&child)?,
            Ok(_) => fs::remove_file(&child).map_err(at(&child))?,
        }
    }
    fs::remove_dir(dir).map_err(at(dir))
}

pub fn sidecar_if_content_differs(
    live: &Path,
    sidecar: &Path,
    previous: &FileMetadata,
    incoming: ContentHash,
) -> Result<(), ApplyError> {
    if previous.kind == EntryKind::Dir || previous.content_hash == incoming {
        return Ok(());
    }
    let bytes = match previous.kind {
        EntryKind::Symlink => fs::read_link(live)
            .map_err(at(live))?
            .as_os_str()
            .as_bytes()
            .to_vec(),
        _ => fs::read(live).map_err(at(live))?,
    };
    if let Some(parent) = sidecar.parent() {
        fs::create_dir_all(parent).map_err(at(parent))?;
    }
    fs::write(sidecar, bytes).map_err(at(sidecar))
}

pub fn try_read_file_or_link(host: &Path) -> io::Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(host) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
        Ok(md) if md.file_type().is_symlink() => {
            Ok(Some(fs::read_link(host)?.as_os_str().as_bytes().to_vec()))
        }
        Ok(md) if md.file_type().is_file() => Ok(Some(fs::read(host)?)),
        Ok(_) => Ok(None),
    }
}

pub fn wipe_tmp(central_root: &Path) -> Result<(), ApplyError> {
    let dir = central_root.join(RESERVED_TMP);
    match fs::remove_dir_all(&dir) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(at(&dir)(err)),
        Ok(()) => Ok(()),
    }
}

fn apply_mode_and_mtime(path: &Path, meta: &FileMetadata) -> Result<(), ApplyError> {
    if let Err(err) = fs::set_permissions(path, Permissions::from_mode(meta.mode & 0o7777)) {
        if err.kind() == io::ErrorKind::PermissionDenied {
            log::warn!("skipping mode on {}: permission denied", path.display());
        } else {
            return Err(at(path)(err));
        }
    }
    if let Err(err) = filetime::set_file_mtime(path, file_time(meta.mtime_ns)) {
        if err.kind() == io::ErrorKind::PermissionDenied {
            log::warn!("skipping mtime on {}: permission denied", path.display());
        } else {
            return Err(at(path)(err));
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{MetadataExt, symlink};

    use super::*;

    unsafe extern "C" {
        fn umask(mask: u32) -> u32;
    }

    #[test]
    fn atomic_put_does_not_write_through_a_symlink_that_leaves_the_root() {
        let unique = unique_name();
        let base = std::env::temp_dir().join(format!("arborsync-escape-{unique}"));
        let root = base.join("root");
        let outside = base.join("outside");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        symlink(&outside, root.join("escape")).unwrap();
        let bytes = b"pwned";
        let meta = FileMetadata::file(bytes.len() as u64, 0, 0o100644, hash_bytes(bytes));
        let path = CanonicalPath::parse("/escape/pwned").unwrap();

        let result = atomic_put(&root, &path, &meta, bytes);

        assert!(
            !outside.join("pwned").exists(),
            "wrote outside the checkout root"
        );
        assert!(matches!(result, Err(ApplyError::EscapesRoot)));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn mkdir_live_uses_announced_mode_not_umask() {
        let unique = unique_name();
        let base = std::env::temp_dir().join(format!("arborsync-mkdir-mode-{unique}"));
        let root = base.join("root");
        fs::create_dir_all(&root).unwrap();
        let path = CanonicalPath::parse("/nested/dir").unwrap();
        let meta = FileMetadata::directory(1_700_000_000_000, 0o40750);

        let old = unsafe { umask(0o077) };
        let result = mkdir_live(&root, &path, &meta);
        unsafe {
            umask(old);
        }
        result.unwrap();

        let host = root.join("nested/dir");
        let mode = fs::symlink_metadata(&host).unwrap().mode() & 0o7777;
        assert_eq!(mode, 0o0750, "leaf must keep the announced mode");
        let parent_mode = fs::symlink_metadata(root.join("nested"))
            .unwrap()
            .mode()
            & 0o7777;
        assert_eq!(
            parent_mode, 0o0750,
            "dirs created for this announce must keep the announced mode"
        );
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn file_parents_are_0755_under_a_restrictive_umask() {
        let unique = unique_name();
        let base = std::env::temp_dir().join(format!("arborsync-file-parent-{unique}"));
        let root = base.join("root");
        fs::create_dir_all(&root).unwrap();
        let bytes = b"hello";
        let meta = FileMetadata::file(bytes.len() as u64, 0, 0o100644, hash_bytes(bytes));
        let path = CanonicalPath::parse("/nested/hello.txt").unwrap();

        let old = unsafe { umask(0o077) };
        let result = atomic_put(&root, &path, &meta, bytes);
        unsafe {
            umask(old);
        }
        result.unwrap();

        let parent_mode = fs::symlink_metadata(root.join("nested")).unwrap().mode() & 0o777;
        assert_eq!(parent_mode, 0o755);
        let _ = fs::remove_dir_all(&base);
    }
}

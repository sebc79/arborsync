//! Test doubles and temp-dir fixtures for TDD.
//!
//! Enabled in unit tests and via the `test-support` feature so integration
//! tests can build isolated master/slave trees without a live QUIC stack.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use filetime::FileTime;
use tempfile::TempDir;
use thiserror::Error;

use crate::config::{CheckoutConfig, MasterConfig, SlaveAcl, SlaveConfig};
use crate::meta::FileMetadata;
use crate::path::{RESERVED_CONFLICTS, RESERVED_TMP, is_interested};
use crate::protocol::ProtocolMessage;
use crate::storage::{CheckoutId, Storage, WriteBatch};

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("poisoned lock")]
    Poisoned,
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Clone, Default)]
struct MemoryInner {
    meta: HashMap<(String, String), FileMetadata>,
    dir_nodes: HashMap<(String, String), [u8; 32]>,
    last_synced: HashMap<(String, String), [u8; 32]>,
}

/// In-memory [`Storage`] for unit tests. `open` ignores the path.
pub struct MemoryStorage {
    inner: Mutex<MemoryInner>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(MemoryInner::default()),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, MemoryInner>, MemoryError> {
        self.inner.lock().map_err(|_| MemoryError::Poisoned)
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    type Error = MemoryError;
    type WriteBatch<'a> = MemoryWriteBatch<'a>;

    fn open(_path: &Path) -> Result<Self, Self::Error> {
        Ok(Self::new())
    }

    fn get_meta(&self, ck: &CheckoutId, path: &str) -> Result<Option<FileMetadata>, Self::Error> {
        Ok(self
            .lock()?
            .meta
            .get(&(ck.0.clone(), path.to_string()))
            .cloned())
    }

    fn get_dir_node(&self, ck: &CheckoutId, path: &str) -> Result<Option<[u8; 32]>, Self::Error> {
        Ok(self
            .lock()?
            .dir_nodes
            .get(&(ck.0.clone(), path.to_string()))
            .copied())
    }

    fn get_last_synced(
        &self,
        ck: &CheckoutId,
        path: &str,
    ) -> Result<Option<[u8; 32]>, Self::Error> {
        Ok(self
            .lock()?
            .last_synced
            .get(&(ck.0.clone(), path.to_string()))
            .copied())
    }

    fn range_meta(
        &self,
        ck: &CheckoutId,
        prefix: &str,
    ) -> Result<Vec<(String, FileMetadata)>, Self::Error> {
        let inner = self.lock()?;
        let mut out: Vec<_> = inner
            .meta
            .iter()
            .filter(|((id, path), _)| id == &ck.0 && is_interested(prefix, path))
            .map(|((_, path), meta)| (path.clone(), meta.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    fn range_dir_nodes(
        &self,
        ck: &CheckoutId,
        prefix: &str,
    ) -> Result<Vec<(String, [u8; 32])>, Self::Error> {
        let inner = self.lock()?;
        let mut out: Vec<_> = inner
            .dir_nodes
            .iter()
            .filter(|((id, path), _)| id == &ck.0 && is_interested(prefix, path))
            .map(|((_, path), node)| (path.clone(), *node))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    fn begin_write(&self) -> Result<Self::WriteBatch<'_>, Self::Error> {
        let snapshot = self.lock()?.clone();
        Ok(MemoryWriteBatch {
            store: self,
            inner: snapshot,
        })
    }

    fn delete_checkout(&self, ck: &CheckoutId) -> Result<(), Self::Error> {
        let mut inner = self.lock()?;
        inner.meta.retain(|(id, _), _| id != &ck.0);
        inner.dir_nodes.retain(|(id, _), _| id != &ck.0);
        inner.last_synced.retain(|(id, _), _| id != &ck.0);
        Ok(())
    }
}

pub struct MemoryWriteBatch<'a> {
    store: &'a MemoryStorage,
    inner: MemoryInner,
}

impl WriteBatch for MemoryWriteBatch<'_> {
    type Error = MemoryError;

    fn put_meta(
        &mut self,
        ck: &CheckoutId,
        path: &str,
        meta: &FileMetadata,
    ) -> Result<(), Self::Error> {
        self.inner
            .meta
            .insert((ck.0.clone(), path.to_string()), meta.clone());
        Ok(())
    }

    fn del_meta(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Self::Error> {
        self.inner.meta.remove(&(ck.0.clone(), path.to_string()));
        Ok(())
    }

    fn del_meta_prefix(&mut self, ck: &CheckoutId, prefix: &str) -> Result<(), Self::Error> {
        self.inner
            .meta
            .retain(|(id, path), _| !(id == &ck.0 && is_interested(prefix, path)));
        Ok(())
    }

    fn put_dir_node(
        &mut self,
        ck: &CheckoutId,
        path: &str,
        node: [u8; 32],
    ) -> Result<(), Self::Error> {
        self.inner
            .dir_nodes
            .insert((ck.0.clone(), path.to_string()), node);
        Ok(())
    }

    fn del_dir_node(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Self::Error> {
        self.inner
            .dir_nodes
            .remove(&(ck.0.clone(), path.to_string()));
        Ok(())
    }

    fn del_dir_prefix(&mut self, ck: &CheckoutId, prefix: &str) -> Result<(), Self::Error> {
        self.inner
            .dir_nodes
            .retain(|(id, path), _| !(id == &ck.0 && is_interested(prefix, path)));
        Ok(())
    }

    fn put_last_synced(
        &mut self,
        ck: &CheckoutId,
        path: &str,
        file_node: [u8; 32],
    ) -> Result<(), Self::Error> {
        self.inner
            .last_synced
            .insert((ck.0.clone(), path.to_string()), file_node);
        Ok(())
    }

    fn del_last_synced(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Self::Error> {
        self.inner
            .last_synced
            .remove(&(ck.0.clone(), path.to_string()));
        Ok(())
    }

    fn commit(self) -> Result<(), Self::Error> {
        *self.store.lock()? = self.inner;
        Ok(())
    }
}

/// One end of an in-memory control-stream pair (`spec.md` §12 Transport).
pub struct MemoryEndpoint {
    tx: Sender<ProtocolMessage>,
    rx: Mutex<Receiver<ProtocolMessage>>,
}

impl MemoryEndpoint {
    pub fn send(&self, msg: ProtocolMessage) {
        self.tx.send(msg).expect("peer dropped");
    }

    pub fn recv(&self) -> ProtocolMessage {
        self.rx
            .lock()
            .expect("poisoned")
            .recv()
            .expect("peer dropped")
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Option<ProtocolMessage> {
        self.rx.lock().expect("poisoned").recv_timeout(timeout).ok()
    }
}

/// Bidirectional in-memory control link (master ↔ slave).
pub fn memory_link() -> (MemoryEndpoint, MemoryEndpoint) {
    let (a_tx, a_rx) = mpsc::channel();
    let (b_tx, b_rx) = mpsc::channel();
    (
        MemoryEndpoint {
            tx: b_tx,
            rx: Mutex::new(a_rx),
        },
        MemoryEndpoint {
            tx: a_tx,
            rx: Mutex::new(b_rx),
        },
    )
}

/// Isolated directory tree for writing host files.
pub struct TempTree {
    pub dir: TempDir,
}

impl Default for TempTree {
    fn default() -> Self {
        Self::new()
    }
}

impl TempTree {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("tempdir"),
        }
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    pub fn builder(&self) -> TreeBuilder {
        TreeBuilder {
            root: self.path().to_path_buf(),
        }
    }
}

/// Fluent writer for files, dirs, and symlinks under a host root.
pub struct TreeBuilder {
    root: PathBuf,
}

impl TreeBuilder {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn mkdir(&self, rel: &str) -> PathBuf {
        let path = self.root.join(rel);
        fs::create_dir_all(&path).expect("mkdir");
        path
    }

    pub fn file(&self, rel: &str, bytes: &[u8]) -> PathBuf {
        let path = self.root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("parent");
        }
        fs::write(&path, bytes).expect("write");
        path
    }

    pub fn symlink(&self, rel: &str, target: &str) -> PathBuf {
        let path = self.root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("parent");
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(target, &path).expect("symlink");
        #[cfg(not(unix))]
        panic!("symlink fixtures require unix");
        path
    }

    pub fn set_mtime_ns(&self, rel: &str, mtime_ns: i64) {
        set_mtime_ns(&self.root.join(rel), mtime_ns);
    }

    #[cfg(unix)]
    pub fn set_mode(&self, rel: &str, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            self.root.join(rel),
            fs::Permissions::from_mode(mode & 0o7777),
        )
        .expect("chmod");
    }
}

pub fn set_mtime_ns(path: &Path, mtime_ns: i64) {
    let seconds = mtime_ns.div_euclid(1_000_000_000);
    let nanos = mtime_ns.rem_euclid(1_000_000_000) as u32;
    filetime::set_file_mtime(path, FileTime::from_unix_time(seconds, nanos)).expect("mtime");
}

/// Isolated master + slave checkout layout for later integration tests.
///
/// ```text
/// {tmp}/
///   master/central/          # central_root
///   master/index.redb
///   master/master.toml
///   slaves/{id}/checkouts/{checkout_id}/
///   slaves/{id}/cache.redb
///   slaves/{id}/slave.toml
/// ```
pub struct SyncSandbox {
    pub tmp: TempDir,
}

impl Default for SyncSandbox {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncSandbox {
    pub fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(tmp.path().join("master/central")).expect("central_root");
        Self { tmp }
    }

    pub fn path(&self) -> &Path {
        self.tmp.path()
    }

    pub fn central_root(&self) -> PathBuf {
        self.path().join("master/central")
    }

    pub fn master_db(&self) -> PathBuf {
        self.path().join("master/index.redb")
    }

    pub fn master_config_path(&self) -> PathBuf {
        self.path().join("master/master.toml")
    }

    pub fn slave_root(&self, slave_id: &str) -> PathBuf {
        self.path().join("slaves").join(slave_id)
    }

    pub fn slave_db(&self, slave_id: &str) -> PathBuf {
        self.slave_root(slave_id).join("cache.redb")
    }

    /// Create a checkout local path and the reserved sidecar directories.
    pub fn add_checkout(&self, slave_id: &str, checkout_id: &str) -> PathBuf {
        let local = self
            .slave_root(slave_id)
            .join("checkouts")
            .join(checkout_id);
        fs::create_dir_all(&local).expect("checkout");
        fs::create_dir_all(local.join(RESERVED_TMP)).expect("tmp");
        fs::create_dir_all(local.join(RESERVED_CONFLICTS)).expect("conflicts");
        local
    }

    pub fn tree(&self, host_root: &Path) -> TreeBuilder {
        TreeBuilder::new(host_root)
    }

    /// Spec §16 scenario: one slave with `/src` and `/` checkouts.
    pub fn overlap_backup_slave(&self) -> OverlapLayout {
        let src = self.add_checkout("backup-1", "src");
        let bak = self.add_checkout("backup-1", "bak");
        OverlapLayout { src, bak }
    }

    pub fn write_master_config(&self, slaves: Vec<SlaveAcl>) -> PathBuf {
        let cfg = MasterConfig {
            central_root: self.central_root().to_string_lossy().into_owned(),
            listen_addr: "127.0.0.1:0".into(),
            master_key_path: self
                .path()
                .join("master/master.key")
                .to_string_lossy()
                .into_owned(),
            db_path: self.master_db().to_string_lossy().into_owned(),
            log_level: "debug".into(),
            watcher_debounce_ms: 200,
            rescan_interval_seconds: 60,
            max_checkouts_per_slave: 100,
            max_connections: 100,
            max_connection_attempts_per_minute: 60,
            slaves,
        };
        let path = self.master_config_path();
        fs::write(&path, cfg.to_toml().expect("toml")).expect("write master.toml");
        path
    }

    pub fn write_slave_config(
        &self,
        slave_id: &str,
        checkouts: Vec<CheckoutConfig>,
        master_public_keys: Vec<String>,
    ) -> PathBuf {
        let root = self.slave_root(slave_id);
        fs::create_dir_all(&root).expect("slave root");
        let cfg = SlaveConfig {
            slave_id: slave_id.into(),
            master_addr: "127.0.0.1:8443".into(),
            slave_key_path: root.join("slave.key").to_string_lossy().into_owned(),
            master_public_keys,
            db_path: self.slave_db(slave_id).to_string_lossy().into_owned(),
            log_level: "debug".into(),
            max_checkouts_per_slave: 100,
            watcher_debounce_ms: 200,
            rescan_interval_seconds: 60,
            checkouts,
        };
        let path = root.join("slave.toml");
        fs::write(&path, cfg.to_toml().expect("toml")).expect("write slave.toml");
        path
    }
}

pub struct OverlapLayout {
    pub src: PathBuf,
    pub bak: PathBuf,
}

/// Independent encoding of `spec.md` §5 FileNode, for test oracles.
pub fn expected_file_node(meta: &FileMetadata) -> [u8; 32] {
    let mut buf = Vec::with_capacity(1 + 32 + 8 + 8 + 4);
    buf.push(u8::from(meta.kind));
    buf.extend_from_slice(&meta.content_hash);
    buf.extend_from_slice(&meta.size.to_be_bytes());
    buf.extend_from_slice(&meta.mtime_ns.to_be_bytes());
    buf.extend_from_slice(&meta.mode.to_be_bytes());
    *blake3::hash(&buf).as_bytes()
}

/// Independent encoding of `spec.md` §5 DirNode.
pub fn expected_dir_node(mut entries: Vec<(u8, String, [u8; 32])>) -> [u8; 32] {
    entries.sort_by(|a, b| a.1.as_bytes().cmp(b.1.as_bytes()));
    let mut buf = Vec::new();
    for (kind, name, hash) in entries {
        buf.push(kind);
        let name_bytes = name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&hash);
    }
    *blake3::hash(&buf).as_bytes()
}

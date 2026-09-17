use std::collections::HashMap;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::apply;
use crate::config::LoadedSlave;
use crate::hash::{ContentHash, FileNode};
use crate::index;
use crate::merkle::file_node;
use crate::meta::{self, EntryKind, FileMetadata, hash_bytes};
use crate::path::{
    CanonicalPath, PathError, canonical_to_host, conflict_sidecar_path, is_reserved_root_entry,
    strip_central,
};
use crate::protocol::{BulkHeader, CheckoutRef, ProtocolMessage};
use crate::storage::{CheckoutId, Storage, WriteBatch};
use crate::transfer::{self, BulkTransfer, signature_for};

pub use crate::apply::{ApplyError, ContentBytes, ContentHook, MemoryContent, WholeFileLater};
pub use crate::master::LocalEvent;

#[derive(Debug)]
pub enum Reply {
    Send(Vec<ProtocolMessage>),
    Hangup { reason: String },
    Bulk(BulkTransfer),
}

#[derive(Debug, thiserror::Error)]
pub enum SlaveError {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(transparent)]
    Apply(#[from] apply::ApplyError),
    #[error("index: {0}")]
    Index(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    Path(#[from] PathError),
    #[error("unknown checkout {0}")]
    UnknownCheckout(String),
}

impl SlaveError {
    fn index<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Index(Box::new(err))
    }

    fn io(path: &std::path::Path) -> impl Fn(io::Error) -> Self + '_ {
        move |source| Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaAction {
    Apply,
    NoopRefresh,
    SidecarThenApply,
    ApplyMetaOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteAction {
    AlreadyGone,
    Remove,
    SidecarThenRemove,
}

pub fn decide_incoming(
    local: Option<&FileMetadata>,
    basis: Option<FileNode>,
    new: &FileMetadata,
) -> ReplicaAction {
    let Some(local) = local else {
        return ReplicaAction::Apply;
    };
    let live = file_node(local);
    if live == file_node(new) {
        return ReplicaAction::NoopRefresh;
    }
    if basis == Some(live) {
        return ReplicaAction::Apply;
    }
    if local.content_hash != new.content_hash {
        return ReplicaAction::SidecarThenApply;
    }
    ReplicaAction::ApplyMetaOnly
}

pub fn decide_master_won_delete(
    local: Option<&FileMetadata>,
    basis: FileNode,
    last_synced: Option<FileNode>,
) -> DeleteAction {
    let Some(local) = local else {
        return DeleteAction::AlreadyGone;
    };
    let live = file_node(local);
    if live == basis || last_synced == Some(live) {
        return DeleteAction::Remove;
    }
    if local.kind == EntryKind::Dir {
        return DeleteAction::Remove;
    }
    DeleteAction::SidecarThenRemove
}

struct InflightEntry {
    hash: ContentHash,
    until: Instant,
}

struct Inflight {
    window: Duration,
    entries: HashMap<CanonicalPath, InflightEntry>,
}

impl Inflight {
    fn new(debounce: Duration) -> Self {
        Self {
            window: debounce * 2,
            entries: HashMap::new(),
        }
    }

    fn arm(&mut self, path: CanonicalPath, hash: ContentHash) {
        let until = Instant::now() + self.window;
        self.entries.insert(path, InflightEntry { hash, until });
    }

    fn consume_if_echo(&mut self, path: &CanonicalPath, hash: &ContentHash) -> bool {
        let now = Instant::now();
        self.entries.retain(|_, entry| entry.until > now);
        if self.entries.get(path).is_some_and(|e| &e.hash == hash) {
            self.entries.remove(path);
            return true;
        }
        false
    }
}

struct Checkout {
    id: CheckoutId,
    central: CanonicalPath,
    local: PathBuf,
    inflight: Inflight,
}

struct PendingApply {
    checkout_id: String,
    path: CanonicalPath,
    new: FileMetadata,
    retried: bool,
}

pub struct Slave<S: Storage, C: ContentHook> {
    cfg: LoadedSlave,
    store: S,
    bodies: C,
    checkouts: HashMap<String, Checkout>,
    pending: HashMap<(String, CanonicalPath), PendingApply>,
}

impl<S: Storage, C: ContentHook> Slave<S, C> {
    pub fn open(cfg: LoadedSlave, store: S, bodies: C) -> Result<Self, SlaveError> {
        let debounce = Duration::from_millis(cfg.watcher_debounce_ms());
        let mut checkouts = HashMap::new();
        for loaded in cfg.checkouts() {
            let configured = loaded.local().to_path_buf();
            fs::create_dir_all(&configured).map_err(SlaveError::io(&configured))?;
            let local = fs::canonicalize(&configured).map_err(SlaveError::io(&configured))?;
            apply::wipe_tmp(&local)?;
            checkouts.insert(
                loaded.id().as_str().to_string(),
                Checkout {
                    id: loaded.id().clone(),
                    central: loaded.central().clone(),
                    local,
                    inflight: Inflight::new(debounce),
                },
            );
        }
        Ok(Self {
            cfg,
            store,
            bodies,
            checkouts,
            pending: HashMap::new(),
        })
    }

    pub fn pin_check(&self, peer: [u8; 32]) -> Result<(), Reply> {
        if self.cfg.pins_master().iter().any(|pin| pin == &peer) {
            Ok(())
        } else {
            Err(Reply::Hangup {
                reason: "master pin miss".into(),
            })
        }
    }

    pub fn subscribe(&self) -> ProtocolMessage {
        ProtocolMessage::Subscribe {
            slave_id: self.cfg.slave_id().into(),
            checkouts: self
                .cfg
                .checkouts()
                .iter()
                .map(|checkout| CheckoutRef {
                    id: checkout.id().as_str().into(),
                    central: checkout.central().clone(),
                })
                .collect(),
        }
    }

    pub fn slave_id(&self) -> &str {
        self.cfg.slave_id()
    }

    pub fn checkout_local(&self, id: &str) -> Option<&std::path::Path> {
        self.checkouts.get(id).map(|c| c.local.as_path())
    }

    pub fn master_addr(&self) -> &str {
        self.cfg.master_addr()
    }

    pub fn watched_checkouts(&self) -> Vec<(String, PathBuf, CanonicalPath)> {
        self.checkouts
            .iter()
            .map(|(id, checkout)| (id.clone(), checkout.local.clone(), checkout.central.clone()))
            .collect()
    }

    pub fn handle(&mut self, msg: ProtocolMessage) -> Result<Reply, SlaveError> {
        match msg {
            ProtocolMessage::SubscribeAck { .. } => Ok(Reply::Send(Vec::new())),
            ProtocolMessage::SubscribeReject { reason, .. } => Ok(Reply::Hangup { reason }),
            ProtocolMessage::FileAnnounce {
                checkout_id,
                path,
                new,
                basis,
            } => self.on_announce(checkout_id, path, new, basis),
            ProtocolMessage::Delete {
                checkout_id,
                path,
                basis,
            } => self.on_delete(checkout_id, path, basis),
            ProtocolMessage::CasAccept {
                checkout_id,
                path,
                file_node,
            } => {
                self.on_cas_accept(&checkout_id, &path, file_node)?;
                Ok(Reply::Send(Vec::new()))
            }
            ProtocolMessage::CasReject {
                checkout_id,
                path,
                current,
            } => self.on_cas_reject(checkout_id, path, current),
            ProtocolMessage::Disconnect { reason } => Ok(Reply::Hangup { reason }),
            ProtocolMessage::SignatureRequest {
                checkout_id,
                path,
                want_hash,
                signature,
            } => self.on_signature_request(checkout_id, path, want_hash, signature),
            other => Ok(Reply::Send(vec![ProtocolMessage::Error {
                code: "unsupported".into(),
                message: format!("{other:?}"),
            }])),
        }
    }

    pub fn apply_bulk(&mut self, header: BulkHeader, body: &[u8]) -> Result<Reply, SlaveError> {
        let key = (header.checkout_id.clone(), header.path.clone());
        let Some(pending) = self.pending.get(&key) else {
            return Ok(Reply::Send(vec![ProtocolMessage::Error {
                code: "unknown_transfer".into(),
                message: header.path.as_str().into(),
            }]));
        };
        if pending.new.content_hash != header.want_hash {
            return Ok(Reply::Send(vec![ProtocolMessage::Error {
                code: "unknown_transfer".into(),
                message: header.path.as_str().into(),
            }]));
        }
        let host = self.host_for(&header.checkout_id, &header.path)?;
        let basis = read_host_bytes(&host)?;
        match transfer::reconstruct(header.encoding, body, basis.as_deref()) {
            Ok(bytes) if hash_bytes(&bytes) == header.want_hash => {
                let pending = self.pending.remove(&key).expect("pending");
                self.finish_apply(&pending.checkout_id, pending.path, pending.new, &bytes)
            }
            _ => {
                let pending = self.pending.get_mut(&key).expect("pending");
                if pending.retried {
                    self.pending.remove(&key);
                    return Ok(Reply::Send(vec![ProtocolMessage::Error {
                        code: "keep_live".into(),
                        message: header.path.as_str().into(),
                    }]));
                }
                pending.retried = true;
                Ok(Reply::Send(vec![ProtocolMessage::SignatureRequest {
                    checkout_id: pending.checkout_id.clone(),
                    path: pending.path.clone(),
                    want_hash: pending.new.content_hash,
                    signature: Vec::new(),
                }]))
            }
        }
    }

    pub fn note_local(
        &mut self,
        checkout_id: &str,
        event: LocalEvent,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        match event {
            LocalEvent::Changed(path) => self.note_changed(checkout_id, path),
            LocalEvent::Removed(path) => self.note_removed(checkout_id, &path),
        }
    }

    pub fn meta(
        &self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<Option<FileMetadata>, SlaveError> {
        let checkout = self.checkout(checkout_id)?;
        self.store
            .get_meta(&checkout.id, path)
            .map_err(SlaveError::index)
    }

    pub fn last_synced(
        &self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<Option<FileNode>, SlaveError> {
        let checkout = self.checkout(checkout_id)?;
        self.store
            .get_last_synced(&checkout.id, path)
            .map_err(SlaveError::index)
    }

    fn on_announce(
        &mut self,
        checkout_id: String,
        path: CanonicalPath,
        new: FileMetadata,
        basis: Option<FileNode>,
    ) -> Result<Reply, SlaveError> {
        let current = self.meta(&checkout_id, &path)?;
        match decide_incoming(current.as_ref(), basis, &new) {
            ReplicaAction::NoopRefresh => {
                self.write_last_synced(&checkout_id, &path, Some(file_node(&new)))?;
                Ok(Reply::Send(Vec::new()))
            }
            ReplicaAction::ApplyMetaOnly => {
                self.apply_meta(&checkout_id, &path, &new, current.as_ref())?;
                Ok(Reply::Send(Vec::new()))
            }
            ReplicaAction::SidecarThenApply => {
                self.sidecar_local(&checkout_id, &path, current.as_ref(), new.content_hash)?;
                self.apply_new(&checkout_id, path, new)
            }
            ReplicaAction::Apply => self.apply_new(&checkout_id, path, new),
        }
    }

    fn on_delete(
        &mut self,
        checkout_id: String,
        path: CanonicalPath,
        basis: FileNode,
    ) -> Result<Reply, SlaveError> {
        let current = self.meta(&checkout_id, &path)?;
        let last_synced = self.last_synced(&checkout_id, &path)?;
        match decide_master_won_delete(current.as_ref(), basis, last_synced) {
            DeleteAction::AlreadyGone => {
                self.write_last_synced(&checkout_id, &path, None)?;
                Ok(Reply::Send(Vec::new()))
            }
            DeleteAction::SidecarThenRemove => {
                self.sidecar_local(&checkout_id, &path, current.as_ref(), ContentHash::ZERO)?;
                self.remove_path(&checkout_id, &path, current.as_ref())?;
                Ok(Reply::Send(Vec::new()))
            }
            DeleteAction::Remove => {
                self.remove_path(&checkout_id, &path, current.as_ref())?;
                Ok(Reply::Send(Vec::new()))
            }
        }
    }

    fn on_cas_accept(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
        node: Option<FileNode>,
    ) -> Result<(), SlaveError> {
        self.write_last_synced(checkout_id, path, node)
    }

    fn on_cas_reject(
        &mut self,
        checkout_id: String,
        path: CanonicalPath,
        current: Option<FileMetadata>,
    ) -> Result<Reply, SlaveError> {
        let local = self.meta(&checkout_id, &path)?;
        if let (Some(local), Some(winner)) = (&local, &current) {
            self.sidecar_local(&checkout_id, &path, Some(local), winner.content_hash)?;
        } else if let Some(local) = &local {
            self.sidecar_local(&checkout_id, &path, Some(local), ContentHash::ZERO)?;
        }
        match current {
            None => {
                self.remove_path(&checkout_id, &path, local.as_ref())?;
                Ok(Reply::Send(Vec::new()))
            }
            Some(winner) => self.apply_new(&checkout_id, path, winner),
        }
    }

    fn apply_new(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
        new: FileMetadata,
    ) -> Result<Reply, SlaveError> {
        match new.kind {
            EntryKind::Dir => return self.finish_apply(checkout_id, path, new, &[]),
            EntryKind::File | EntryKind::Symlink => match self.bodies.fetch(new.content_hash) {
                ContentBytes::AskSender => {
                    let host = self.host_for(checkout_id, &path)?;
                    let live = read_host_bytes(&host)?;
                    let signature = signature_for(new.kind, live.as_deref());
                    let want_hash = new.content_hash;
                    self.pending.insert(
                        (checkout_id.to_string(), path.clone()),
                        PendingApply {
                            checkout_id: checkout_id.into(),
                            path: path.clone(),
                            new,
                            retried: false,
                        },
                    );
                    return Ok(Reply::Send(vec![ProtocolMessage::SignatureRequest {
                        checkout_id: checkout_id.into(),
                        path,
                        want_hash,
                        signature,
                    }]));
                }
                ContentBytes::Whole(body) => self.finish_apply(checkout_id, path, new, &body),
            },
        }
    }

    fn finish_apply(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
        new: FileMetadata,
        body: &[u8],
    ) -> Result<Reply, SlaveError> {
        let relative = {
            let checkout = self.checkout(checkout_id)?;
            strip_central(&checkout.central, &path)?
        };
        match new.kind {
            EntryKind::Dir => {
                self.index_ancestors(checkout_id, &path)?;
                let local = self.checkout(checkout_id)?.local.clone();
                apply::mkdir_live(&local, &relative, &new)?;
            }
            EntryKind::File => {
                self.index_ancestors(checkout_id, &path)?;
                let local = self.checkout(checkout_id)?.local.clone();
                apply::atomic_put(&local, &relative, &new, body)?;
                self.checkout_mut(checkout_id)?
                    .inflight
                    .arm(path.clone(), new.content_hash);
            }
            EntryKind::Symlink => {
                self.index_ancestors(checkout_id, &path)?;
                let local = self.checkout(checkout_id)?.local.clone();
                apply::atomic_symlink(&local, &relative, &new, body)?;
                self.checkout_mut(checkout_id)?
                    .inflight
                    .arm(path.clone(), new.content_hash);
            }
        }
        let ck = self.checkout(checkout_id)?.id.clone();
        index::commit_leaf(
            &self.store,
            &ck,
            &path,
            Some(&new),
            index::LastSynced::AdoptLeaf,
        )
        .map_err(SlaveError::index)?;
        Ok(Reply::Send(Vec::new()))
    }

    fn on_signature_request(
        &mut self,
        checkout_id: String,
        path: CanonicalPath,
        want_hash: ContentHash,
        signature: Vec<u8>,
    ) -> Result<Reply, SlaveError> {
        let host = self.host_for(&checkout_id, &path)?;
        let Some(source) = read_host_bytes(&host)? else {
            return Ok(Reply::Send(vec![ProtocolMessage::Error {
                code: "missing_hash".into(),
                message: path.as_str().into(),
            }]));
        };
        match transfer::fulfill(checkout_id, path.clone(), want_hash, &source, &signature) {
            Ok(xfer) => Ok(Reply::Bulk(xfer)),
            Err(transfer::TransferError::HashMismatch) => {
                Ok(Reply::Send(vec![ProtocolMessage::Error {
                    code: "missing_hash".into(),
                    message: path.as_str().into(),
                }]))
            }
            Err(err) => Ok(Reply::Send(vec![ProtocolMessage::Error {
                code: "transfer".into(),
                message: err.to_string(),
            }])),
        }
    }

    fn host_for(&self, checkout_id: &str, path: &CanonicalPath) -> Result<PathBuf, SlaveError> {
        let checkout = self.checkout(checkout_id)?;
        let relative = strip_central(&checkout.central, path)?;
        Ok(canonical_to_host(&checkout.local, &relative))
    }

    fn apply_meta(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
        new: &FileMetadata,
        _previous: Option<&FileMetadata>,
    ) -> Result<(), SlaveError> {
        let (local, relative) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.local.clone(),
                strip_central(&checkout.central, path)?,
            )
        };
        let host = canonical_to_host(&local, &relative);
        match new.kind {
            EntryKind::Dir => apply::mkdir_live(&local, &relative, new)?,
            EntryKind::File => {
                apply::atomic_put(
                    &local,
                    &relative,
                    new,
                    &fs::read(&host).map_err(SlaveError::io(&host))?,
                )?;
            }
            EntryKind::Symlink => {
                let target = fs::read_link(&host).map_err(SlaveError::io(&host))?;
                apply::atomic_symlink(&local, &relative, new, target.as_os_str().as_bytes())?;
            }
        }
        let ck = self.checkout(checkout_id)?.id.clone();
        index::commit_leaf(
            &self.store,
            &ck,
            path,
            Some(new),
            index::LastSynced::AdoptLeaf,
        )
        .map_err(SlaveError::index)?;
        Ok(())
    }

    fn remove_path(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
        _previous: Option<&FileMetadata>,
    ) -> Result<(), SlaveError> {
        let (local, relative) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.local.clone(),
                strip_central(&checkout.central, path)?,
            )
        };
        apply::remove_live(&local, &relative)?;
        let ck = self.checkout(checkout_id)?.id.clone();
        index::commit_leaf(&self.store, &ck, path, None, index::LastSynced::AdoptLeaf)
            .map_err(SlaveError::index)?;
        Ok(())
    }

    fn sidecar_local(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
        previous: Option<&FileMetadata>,
        incoming: ContentHash,
    ) -> Result<(), SlaveError> {
        let Some(previous) = previous else {
            return Ok(());
        };
        let checkout = self.checkout(checkout_id)?;
        let relative = strip_central(&checkout.central, path)?;
        let live = canonical_to_host(&checkout.local, &relative);
        let sidecar = conflict_sidecar_path(&checkout.local, path, &previous.content_hash);
        apply::sidecar_if_content_differs(&live, &sidecar, previous, incoming)?;
        Ok(())
    }

    fn write_last_synced(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
        node: Option<FileNode>,
    ) -> Result<(), SlaveError> {
        let ck = self.checkout(checkout_id)?.id.clone();
        let mut batch = self.store.begin_write().map_err(SlaveError::index)?;
        match node {
            Some(node) => batch
                .put_last_synced(&ck, path, node)
                .map_err(SlaveError::index)?,
            None => batch
                .del_last_synced(&ck, path)
                .map_err(SlaveError::index)?,
        }
        batch.commit().map_err(SlaveError::index)
    }

    fn note_changed(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        if is_reserved(&path) {
            return Ok(Vec::new());
        }
        let (local, relative) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.local.clone(),
                strip_central(&checkout.central, &path)?,
            )
        };
        let host = canonical_to_host(&local, &relative);
        let Some(found) = meta::collect_from_path(&host).map_err(SlaveError::io(&host))? else {
            return self.note_removed(checkout_id, &path);
        };
        if self
            .checkout_mut(checkout_id)?
            .inflight
            .consume_if_echo(&path, &found.content_hash)
        {
            return Ok(Vec::new());
        }
        let last_synced = self.last_synced(checkout_id, &path)?;
        if last_synced == Some(file_node(&found)) {
            return Ok(Vec::new());
        }
        let ck = self.checkout(checkout_id)?.id.clone();
        self.index_ancestors(checkout_id, &path)?;
        index::commit_leaf(
            &self.store,
            &ck,
            &path,
            Some(&found),
            index::LastSynced::Keep,
        )
        .map_err(SlaveError::index)?;
        Ok(vec![ProtocolMessage::FileAnnounce {
            checkout_id: checkout_id.into(),
            path,
            new: found,
            basis: last_synced,
        }])
    }

    fn note_removed(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        if is_reserved(path) {
            return Ok(Vec::new());
        }
        let Some(previous) = self.meta(checkout_id, path)? else {
            return Ok(Vec::new());
        };
        let last_synced = self.last_synced(checkout_id, path)?;
        let basis = last_synced.unwrap_or_else(|| file_node(&previous));
        let ck = self.checkout(checkout_id)?.id.clone();
        index::commit_leaf(&self.store, &ck, path, None, index::LastSynced::Keep)
            .map_err(SlaveError::index)?;
        Ok(vec![ProtocolMessage::Delete {
            checkout_id: checkout_id.into(),
            path: path.clone(),
            basis,
        }])
    }

    fn index_ancestors(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<(), SlaveError> {
        let mut ancestors: Vec<CanonicalPath> = path.ancestors().collect();
        ancestors.reverse();
        let (ck, local, central) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.id.clone(),
                checkout.local.clone(),
                checkout.central.clone(),
            )
        };
        for dir in ancestors {
            if !central.covers(&dir) {
                continue;
            }
            if self
                .store
                .get_meta(&ck, &dir)
                .map_err(SlaveError::index)?
                .is_some()
            {
                continue;
            }
            let relative = strip_central(&central, &dir)?;
            let host = canonical_to_host(&local, &relative);
            fs::create_dir_all(&host).map_err(SlaveError::io(&host))?;
            let Some(found) = meta::collect_from_path(&host).map_err(SlaveError::io(&host))? else {
                continue;
            };
            index::commit_leaf(
                &self.store,
                &ck,
                &dir,
                Some(&found),
                index::LastSynced::AdoptLeaf,
            )
            .map_err(SlaveError::index)?;
        }
        Ok(())
    }

    fn checkout(&self, id: &str) -> Result<&Checkout, SlaveError> {
        self.checkouts
            .get(id)
            .ok_or_else(|| SlaveError::UnknownCheckout(id.into()))
    }

    fn checkout_mut(&mut self, id: &str) -> Result<&mut Checkout, SlaveError> {
        self.checkouts
            .get_mut(id)
            .ok_or_else(|| SlaveError::UnknownCheckout(id.into()))
    }
}

fn read_host_bytes(host: &Path) -> Result<Option<Vec<u8>>, SlaveError> {
    match fs::symlink_metadata(host) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(SlaveError::io(host)(err)),
        Ok(md) if md.file_type().is_symlink() => {
            let target = fs::read_link(host).map_err(SlaveError::io(host))?;
            Ok(Some(target.as_os_str().as_bytes().to_vec()))
        }
        Ok(_) => Ok(Some(fs::read(host).map_err(SlaveError::io(host))?)),
    }
}

fn is_reserved(path: &CanonicalPath) -> bool {
    path.as_str()
        .trim_start_matches('/')
        .split('/')
        .next()
        .is_some_and(is_reserved_root_entry)
}

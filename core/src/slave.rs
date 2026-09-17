//! One synchronous writer of each checkout tree and that checkout’s index.
//! The binary serializes QUIC and `notify` into [`Slave::handle`] and
//! [`Slave::note_local`].

use std::collections::HashMap;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::apply;
use crate::config::LoadedSlave;
use crate::hash::{ContentHash, FileNode};
use crate::index;
use crate::merkle::file_node;
use crate::meta::{self, EntryKind, FileMetadata};
use crate::path::{
    CanonicalPath, PathError, canonical_to_host, checkout_relative, conflict_sidecar_path,
    is_reserved_root_entry,
};
use crate::protocol::{CheckoutRef, ProtocolMessage};
use crate::storage::{CheckoutId, Storage, WriteBatch};

pub use crate::apply::{ApplyError, ContentBytes, ContentHook, MemoryContent, WholeFileLater};
pub use crate::master::LocalEvent;

#[derive(Debug)]
pub enum Reply {
    Send(Vec<ProtocolMessage>),
    Hangup { reason: String },
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

/// Slave-side table for a master `FileAnnounce` (`spec.md` §8).
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

/// Replica delete (`spec.md` §8). Master already won.
pub fn decide_replica_delete(
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

pub struct Slave<S: Storage, C: ContentHook> {
    cfg: LoadedSlave,
    store: S,
    bodies: C,
    checkouts: HashMap<String, Checkout>,
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
            other => Ok(Reply::Send(vec![ProtocolMessage::Error {
                code: "unsupported".into(),
                message: format!("{other:?}"),
            }])),
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
        match decide_replica_delete(current.as_ref(), basis, last_synced) {
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
        let relative = {
            let checkout = self.checkout(checkout_id)?;
            checkout_relative(&checkout.central, &path)?
        };
        match new.kind {
            EntryKind::Dir => {
                self.index_ancestors(checkout_id, &path)?;
                let local = self.checkout(checkout_id)?.local.clone();
                apply::mkdir_live(&local, &relative, &new)?;
            }
            EntryKind::File | EntryKind::Symlink => match self.bodies.fetch(new.content_hash) {
                ContentBytes::AskSender => {
                    return Ok(Reply::Send(vec![ProtocolMessage::SignatureRequest {
                        checkout_id: checkout_id.into(),
                        path,
                        want_hash: new.content_hash,
                        signature: Vec::new(),
                    }]));
                }
                ContentBytes::Whole(body) => {
                    self.index_ancestors(checkout_id, &path)?;
                    let local = self.checkout(checkout_id)?.local.clone();
                    if new.kind == EntryKind::File {
                        apply::atomic_put(&local, &relative, &new, &body)?;
                    } else {
                        apply::atomic_symlink(&local, &relative, &new, &body)?;
                    }
                    self.checkout_mut(checkout_id)?
                        .inflight
                        .arm(path.clone(), new.content_hash);
                }
            },
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
                checkout_relative(&checkout.central, path)?,
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
                checkout_relative(&checkout.central, path)?,
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
        let relative = checkout_relative(&checkout.central, path)?;
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
                checkout_relative(&checkout.central, &path)?,
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
            let relative = checkout_relative(&central, &dir)?;
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

fn is_reserved(path: &CanonicalPath) -> bool {
    path.as_str()
        .trim_start_matches('/')
        .split('/')
        .next()
        .is_some_and(is_reserved_root_entry)
}

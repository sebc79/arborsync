use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::apply;
use crate::bottleneck::{Stage, Wait, Waiting};
use crate::config::{ConfigError, LoadedSlave, ReloadError, SlaveReload};
use crate::crawl::{Crawl, DirListPage, RescanWalk, WalkError, Walked, STEP_BUDGET};
use crate::hash::{ContentHash, FileNode};
use crate::hashing::{HashDone, HashKey, HashNeed, HashOutcome, HashPlan};
use crate::index;
use crate::inflight::Inflight;
use crate::merkle::{file_node, DirChild};
use crate::meta::{self, EntryKind, FileMetadata, Inspected};
use crate::path::{
    canonical_to_host, confine_host, conflict_sidecar_path, join_central, local_paths_overlap,
    strip_central, CanonicalPath, EntryName, PathError,
};
use crate::protocol::{BulkEncoding, BulkHeader, CheckoutRef, ProtocolMessage};
use crate::reconcile::{decide_child, WalkAction};
use crate::status::{Queues, SlaveStatus, StatusLedger};
use crate::storage::{CheckoutId, Storage, WriteBatch};
use crate::transfer::{self, BulkTransfer};
use crate::tune::Tune;
use crate::watch::LocalEvent;

pub use crate::apply::{ApplyError, ContentBytes, ContentHook, MemoryContent, WholeFileLater};
pub use crate::crawl::{RescanStat, RescanStated};

struct StagedAnnounce {
    checkout_id: String,
    ck: CheckoutId,
    path: CanonicalPath,
    found: FileMetadata,
    last_synced: Option<FileNode>,
    dirty: bool,
}

#[derive(Debug)]
pub enum Reply {
    Send(Vec<ProtocolMessage>),
    Hangup { reason: String },
    Bulk(BulkTransfer),
}

/// Work that can run without the slave mutex: stream or patch into tmp, then BLAKE3.
pub enum ApplyBulkPlan {
    Done(Reply),
    Reconstruct(ApplyBulkJob),
}

pub struct ApplyBulkJob {
    key: (String, CanonicalPath),
    encoding: BulkEncoding,
    stage_root: PathBuf,
    /// Streamed copy of the live basis for a delta. Not the file bytes.
    basis_tmp: Option<PathBuf>,
    want_hash: ContentHash,
}

pub enum ApplyBulkOutcome {
    Ready {
        key: (String, CanonicalPath),
        verified: apply::VerifiedContent,
        want_hash: ContentHash,
    },
    Failed {
        key: (String, CanonicalPath),
        path: CanonicalPath,
    },
}

impl Drop for ApplyBulkJob {
    fn drop(&mut self) {
        if let Some(path) = self.basis_tmp.take() {
            let _ = fs::remove_file(path);
        }
    }
}

impl ApplyBulkJob {
    pub fn is_delta(&self) -> bool {
        self.encoding == BulkEncoding::Delta
    }

    pub fn open_stage(&self) -> Result<apply::BulkStage, SlaveError> {
        Ok(apply::BulkStage::create(&self.stage_root)?)
    }

    pub fn complete_whole(self, stage: apply::BulkStage) -> ApplyBulkOutcome {
        let want = self.want_hash;
        self.outcome_from(stage.finish(want))
    }

    pub fn complete_delta(self, body: Vec<u8>) -> ApplyBulkOutcome {
        let staged = apply::stage_delta(
            &self.stage_root,
            self.basis_tmp.as_deref(),
            &body,
            self.want_hash,
        );
        self.outcome_from(staged)
    }

    fn outcome_from(
        self,
        staged: Result<apply::VerifiedContent, apply::ApplyError>,
    ) -> ApplyBulkOutcome {
        let path = self.key.1.clone();
        let key = self.key.clone();
        let want_hash = self.want_hash;
        match staged {
            Ok(verified) => ApplyBulkOutcome::Ready {
                key,
                verified,
                want_hash,
            },
            Err(_) => ApplyBulkOutcome::Failed { key, path },
        }
    }
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
    #[error(transparent)]
    Reload(#[from] ReloadError),
    #[error(transparent)]
    Config(#[from] ConfigError),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenameAction {
    Apply,
    NoopRefresh,
    SidecarThenApply,
    CreateAtTo,
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
    if local.kind != new.kind {
        return ReplicaAction::SidecarThenApply;
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
    last_content: Option<ContentHash>,
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
    if last_content == Some(local.content_hash) {
        return DeleteAction::Remove;
    }
    DeleteAction::SidecarThenRemove
}

pub fn decide_rename(
    from_local: Option<&FileMetadata>,
    to_local: Option<&FileMetadata>,
    from_basis: FileNode,
    last_synced_from: Option<FileNode>,
    to_new: &FileMetadata,
) -> RenameAction {
    match from_local {
        None if to_local.is_some_and(|local| file_node(local) == file_node(to_new)) => {
            RenameAction::NoopRefresh
        }
        None => RenameAction::CreateAtTo,
        Some(from) => {
            let live = file_node(from);
            if live == from_basis || last_synced_from == Some(live) {
                RenameAction::Apply
            } else if from.kind == EntryKind::Dir {
                RenameAction::Apply
            } else {
                RenameAction::SidecarThenApply
            }
        }
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

/// Queues the slave binary owns and the core cannot see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkState {
    pub connected: bool,
    pub waits: Vec<Wait>,
    pub work_depth: usize,
}

impl LinkState {
    pub fn offline(work_depth: usize) -> Self {
        Self {
            connected: false,
            waits: Vec::new(),
            work_depth,
        }
    }
}

pub struct Slave<S: Storage, C: ContentHook> {
    cfg: LoadedSlave,
    store: S,
    bodies: C,
    checkouts: HashMap<String, Checkout>,
    pending: Waiting<(String, CanonicalPath), PendingApply>,
    pending_pulls: Waiting<(String, CanonicalPath), ()>,
    pending_renames: Waiting<(String, CanonicalPath), CanonicalPath>,
    denied_centrals: HashSet<CanonicalPath>,
    crawl: Crawl,
    hashing: Waiting<HashKey, ()>,
    pending_hash: Vec<HashNeed>,
    dirs: HashMap<CheckoutId, index::DirChildren>,
    status: StatusLedger,
    cas_since_dir_list: HashMap<String, u64>,
    dir_list_sent: HashMap<(String, CanonicalPath, Option<EntryName>), u64>,
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
        reject_resolved_overlap(&checkouts)?;
        let mut slave = Self {
            cfg,
            store,
            bodies,
            checkouts,
            pending: Waiting::new(Stage::OriginBytes),
            pending_pulls: Waiting::new(Stage::Reconcile),
            pending_renames: Waiting::new(Stage::Reconcile),
            denied_centrals: HashSet::new(),
            crawl: Crawl::default(),
            hashing: Waiting::new(Stage::Hashing),
            pending_hash: Vec::new(),
            dirs: HashMap::new(),
            status: StatusLedger::default(),
            cas_since_dir_list: HashMap::new(),
            dir_list_sent: HashMap::new(),
        };
        let mut ids: Vec<String> = slave.checkouts.keys().cloned().collect();
        ids.sort();
        for id in ids {
            let ck = slave.checkout(&id)?.id.clone();
            if index::root_is_dirty(&slave.store, &ck).map_err(SlaveError::index)? {
                slave.rescan(&id)?;
            }
        }
        Ok(slave)
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

    pub fn subscribe(&mut self) -> ProtocolMessage {
        let msg = self.subscribe_message();
        self.status.outbound(None, &msg);
        msg
    }

    fn subscribe_message(&self) -> ProtocolMessage {
        ProtocolMessage::Subscribe {
            slave_id: self.cfg.slave_id().into(),
            checkouts: self
                .cfg
                .checkouts()
                .iter()
                .filter(|checkout| !self.denied_centrals.contains(checkout.central()))
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

    pub fn tune(&self) -> &Tune {
        self.cfg.tune()
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

    pub fn watcher_debounce_ms(&self) -> u64 {
        self.cfg.watcher_debounce_ms()
    }

    pub fn rescan_interval_seconds(&self) -> u64 {
        self.cfg.rescan_interval_seconds()
    }

    pub fn status_interval_seconds(&self) -> u64 {
        self.cfg.status_interval_seconds()
    }

    pub fn take_status(&mut self, link: LinkState) -> SlaveStatus {
        let mut waits: Vec<Wait> = [
            self.pending.oldest(),
            self.pending_pulls.oldest(),
            self.pending_renames.oldest(),
            self.hashing.oldest(),
        ]
        .into_iter()
        .flatten()
        .collect();
        waits.extend(link.waits);
        let depth = |stage| {
            waits
                .iter()
                .find(|wait| wait.stage == stage)
                .map_or(0, |wait| wait.depth)
        };
        let queues = Queues {
            pending: self.pending.len(),
            pending_pulls: self.pending_pulls.len(),
            pending_renames: self.pending_renames.len(),
            parked: depth(Stage::FulfillParked),
            sending: depth(Stage::FulfillRead),
            work: link.work_depth,
            ..Queues::default()
        };
        self.status.take_slave(link.connected, queues, &waits)
    }

    pub fn crawl_pending(&self) -> bool {
        self.crawl.pending()
    }

    pub fn crawl_runnable(&self) -> bool {
        self.crawl.runnable()
    }

    /// Leftover dir-list pages must not enqueue more asks while a fulfill is
    /// in flight. Rescan hashing can still step; those announces are not pages.
    pub fn should_step_crawl(&self, serving: bool) -> bool {
        self.crawl.runnable() && !(serving && self.crawl.has_pages())
    }

    fn write_index(
        &mut self,
        ck: &CheckoutId,
        path: &CanonicalPath,
        leaf: Option<&FileMetadata>,
        last_synced: index::LastSynced,
    ) -> Result<(), SlaveError> {
        self.write_indexes(
            ck,
            [index::LeafChange {
                path,
                meta: leaf,
                last_synced,
            }],
        )
    }

    fn write_indexes<'a>(
        &mut self,
        ck: &CheckoutId,
        changes: impl IntoIterator<Item = index::LeafChange<'a>>,
    ) -> Result<(), SlaveError> {
        index::commit_leaves_with(
            &self.store,
            ck,
            changes,
            self.dirs.entry(ck.clone()).or_default(),
        )
        .map_err(SlaveError::index)
    }

    pub fn crawl_step(&mut self) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let plan = self.step_crawl(STEP_BUDGET)?;
        self.pending_hash.extend(plan.hash);
        for msg in &plan.send {
            self.status.outbound(None, msg);
        }
        Ok(plan.send)
    }

    pub fn crawl_has_pages(&self) -> bool {
        self.crawl.has_pages()
    }

    pub fn rescan_wants_stat(&self) -> bool {
        self.crawl
            .front_rescan()
            .is_some_and(RescanWalk::wants_stat)
    }

    pub fn take_rescan_stats(&mut self) -> Result<Vec<RescanStat>, SlaveError> {
        let Some(walk) = self.crawl.front_rescan_mut() else {
            return Ok(Vec::new());
        };
        let ck = walk.ck.clone();
        walk.take_unstated(STEP_BUDGET, |path| self.store.get_meta(&ck, path))
            .map_err(into_slave_walk_err)
    }

    pub fn apply_rescan_stats(
        &mut self,
        done: Vec<RescanStated>,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        if self.crawl.front_rescan().is_none() {
            return Ok(Vec::new());
        }
        let front_id = self
            .crawl
            .front_rescan()
            .expect("front checked")
            .checkout_id
            .clone();
        let mut plan = HashPlan::default();
        let mut first_err: Option<SlaveError> = None;
        for stated in done {
            let outcome = {
                let Some(walk) = self.crawl.front_rescan_mut() else {
                    break;
                };
                if stated.checkout_id() != walk.checkout_id {
                    None
                } else {
                    Some(walk.apply_stated(stated))
                }
            };
            let Some(outcome) = outcome else {
                continue;
            };
            match outcome {
                Ok(Some(row)) => {
                    if first_err.is_none() {
                        match self.absorb_walked(&front_id, std::iter::once(row)) {
                            Ok(part) => plan.append(part),
                            Err(err) => first_err = Some(err),
                        }
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some(into_slave_walk_err(err));
                    }
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        if let Some(walk) = self.crawl.take_finished_rescan() {
            plan.send.extend(self.commit_rescan(walk)?);
            self.status.rescan(None);
        }
        self.pending_hash.extend(plan.hash);
        for msg in &plan.send {
            self.status.outbound(None, msg);
        }
        Ok(plan.send)
    }

    pub fn take_hash_jobs(&mut self) -> Vec<HashNeed> {
        std::mem::take(&mut self.pending_hash)
    }

    pub fn drain_hashes(&mut self) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let jobs = self.take_hash_jobs();
        self.drain_hash_jobs(jobs)
    }

    pub fn start_hashed(&mut self, key: &HashKey) {
        self.hashing.remove(key);
    }

    pub fn hashing_wait(&self) -> Option<Wait> {
        self.hashing.oldest()
    }

    pub fn request_rescan(&mut self, checkout_id: &str) -> Result<(), SlaveError> {
        self.start_rescan(checkout_id)
    }

    pub fn finish_crawl(&mut self) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let mut out = Vec::new();
        while self.crawl.pending() {
            let plan = self.step_crawl(usize::MAX)?;
            for msg in &plan.send {
                self.status.outbound(None, msg);
            }
            out.extend(plan.send);
            out.extend(self.drain_hash_jobs(plan.hash)?);
        }
        Ok(out)
    }

    pub fn note_status_error(&mut self, slave: Option<&str>, reason: impl Into<String>) {
        self.status.error(slave, reason);
    }

    fn record_reply(&mut self, reply: &Reply) {
        match reply {
            Reply::Send(msgs) => {
                for msg in msgs {
                    self.status.outbound(None, msg);
                }
            }
            Reply::Hangup { reason } => {
                self.status.error(None, format!("hangup:{reason}"));
            }
            Reply::Bulk(xfer) => self.status.bulk_out(None, xfer.body.len() as u64),
        }
    }

    pub fn log_level(&self) -> &str {
        self.cfg.log_level()
    }

    pub fn reload(&mut self, next: LoadedSlave) -> Result<SlaveReload, SlaveError> {
        let plan = self.cfg.plan_reload(&next)?;
        let debounce = Duration::from_millis(next.watcher_debounce_ms());
        let debounce_changed = next.watcher_debounce_ms() != self.cfg.watcher_debounce_ms();

        let mut next_locals: HashMap<String, PathBuf> = self
            .checkouts
            .iter()
            .filter(|(id, _)| !plan.removed.iter().any(|removed| removed == *id))
            .map(|(id, checkout)| (id.clone(), checkout.local.clone()))
            .collect();
        let mut added: Vec<(String, Checkout)> = Vec::new();
        for id in &plan.added {
            let loaded = next
                .checkout(id)
                .ok_or_else(|| SlaveError::UnknownCheckout(id.clone()))?;
            let configured = loaded.local().to_path_buf();
            fs::create_dir_all(&configured).map_err(SlaveError::io(&configured))?;
            let local = fs::canonicalize(&configured).map_err(SlaveError::io(&configured))?;
            next_locals.insert(id.clone(), local.clone());
            added.push((
                id.clone(),
                Checkout {
                    id: loaded.id().clone(),
                    central: loaded.central().clone(),
                    local,
                    inflight: Inflight::new(debounce),
                },
            ));
        }
        reject_resolved_locals(&next_locals)?;
        for (_, checkout) in &added {
            apply::wipe_tmp(&checkout.local)?;
        }

        for id in &plan.removed {
            if let Some(checkout) = self.checkouts.remove(id) {
                self.dirs.remove(&checkout.id);
                self.store
                    .delete_checkout(&checkout.id)
                    .map_err(SlaveError::index)?;
            }
            self.pending.retain(|(checkout, _), _| checkout != id);
            self.pending_pulls.retain(|(checkout, _), _| checkout != id);
            self.pending_renames
                .retain(|(checkout, _), _| checkout != id);
            self.crawl.drop_checkout(id);
        }
        for (id, checkout) in added {
            self.checkouts.insert(id, checkout);
        }
        if debounce_changed {
            for checkout in self.checkouts.values_mut() {
                checkout.inflight.set_window(debounce);
            }
        }

        let pins_changed = self.cfg.pins_master() != next.pins_master();
        self.cfg = next;
        if plan.resubscribe || pins_changed {
            self.denied_centrals.clear();
        }
        Ok(plan)
    }

    pub fn handle(&mut self, msg: ProtocolMessage) -> Result<Reply, SlaveError> {
        self.status.inbound(None, &msg);
        let reply = self.handle_message(msg)?;
        self.record_reply(&reply);
        Ok(reply)
    }

    fn handle_message(&mut self, msg: ProtocolMessage) -> Result<Reply, SlaveError> {
        match msg {
            ProtocolMessage::SubscribeAck { .. } => self.on_subscribe_ack(),
            ProtocolMessage::SubscribeReject {
                reason,
                denied_centrals,
            } => self.on_subscribe_reject(reason, denied_centrals),
            ProtocolMessage::RootAck {
                checkout_id,
                path,
                matched,
                ..
            } => self.on_root_ack(&checkout_id, path, matched),
            ProtocolMessage::DirListResponse {
                checkout_id,
                path,
                after,
                entries,
                more,
            } => self.on_dir_list(checkout_id, path, after, entries, more),
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
            ProtocolMessage::Rename {
                checkout_id,
                from,
                to,
                from_basis,
                to_new,
            } => self.on_rename(checkout_id, from, to, from_basis, to_new),
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
            ProtocolMessage::Error { code, message } => {
                self.status.error(None, format!("error:{code}:{message}"));
                Ok(Reply::Send(Vec::new()))
            }
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
        self.begin_apply_bulk(body.len() as u64);
        let reply = match self.prepare_apply_bulk(header)? {
            ApplyBulkPlan::Done(reply) => reply,
            ApplyBulkPlan::Reconstruct(job) => {
                let outcome = if job.is_delta() {
                    job.complete_delta(body.to_vec())
                } else {
                    let mut stage = job.open_stage()?;
                    stage
                        .write_all(body)
                        .map_err(SlaveError::io(stage.path()))?;
                    job.complete_whole(stage)
                };
                self.finish_apply_bulk(outcome)?
            }
        };
        self.end_apply_bulk(&reply);
        Ok(reply)
    }

    pub fn begin_apply_bulk(&mut self, bytes: u64) {
        self.status.bulk_in(None, bytes);
    }

    pub fn end_apply_bulk(&mut self, reply: &Reply) {
        match reply {
            Reply::Send(msgs) if msgs.is_empty() => self.status.apply_ok(None),
            Reply::Send(msgs) => {
                if msgs
                    .iter()
                    .any(|msg| matches!(msg, ProtocolMessage::Error { .. }))
                {
                    self.status.apply_fail(None);
                }
            }
            _ => {}
        }
        self.record_reply(reply);
    }

    pub fn prepare_apply_bulk(&mut self, header: BulkHeader) -> Result<ApplyBulkPlan, SlaveError> {
        let key = (header.checkout_id.clone(), header.path.clone());
        let Some(pending) = self.pending.get(&key) else {
            return Ok(ApplyBulkPlan::Done(self.accept_if_live_matches(&header)?));
        };
        if pending.new.content_hash != header.want_hash {
            return Ok(ApplyBulkPlan::Done(self.accept_if_live_matches(&header)?));
        }
        let host = self.host_for(&header.checkout_id, &header.path)?;
        let previous = self.meta(&header.checkout_id, &header.path)?;
        let stage_root = self.checkout(&header.checkout_id)?.local.clone();
        let basis_tmp = if header.encoding == BulkEncoding::Whole
            || previous
                .as_ref()
                .is_some_and(|prev| prev.kind != pending.new.kind)
        {
            None
        } else {
            apply::snapshot_basis(&host, &stage_root)?
        };
        Ok(ApplyBulkPlan::Reconstruct(ApplyBulkJob {
            key,
            encoding: header.encoding,
            stage_root,
            basis_tmp,
            want_hash: header.want_hash,
        }))
    }

    pub fn finish_apply_bulk(&mut self, outcome: ApplyBulkOutcome) -> Result<Reply, SlaveError> {
        match outcome {
            ApplyBulkOutcome::Ready {
                key,
                verified,
                want_hash,
            } => {
                let Some(pending) = self.pending.get(&key) else {
                    return self.accept_if_live_matches(&BulkHeader {
                        checkout_id: key.0,
                        path: key.1,
                        want_hash,
                        encoding: BulkEncoding::Whole,
                        size: 0,
                    });
                };
                if want_hash != pending.new.content_hash {
                    return self.accept_if_live_matches(&BulkHeader {
                        checkout_id: key.0.clone(),
                        path: key.1.clone(),
                        want_hash: pending.new.content_hash,
                        encoding: BulkEncoding::Whole,
                        size: 0,
                    });
                }
                let pending = self.pending.remove(&key).expect("pending");
                self.finish_verified(
                    &pending.checkout_id,
                    pending.path,
                    pending.new,
                    verified,
                )
            }
            ApplyBulkOutcome::Failed { key, path } => {
                let Some(pending) = self.pending.get_mut(&key) else {
                    return Ok(Reply::Send(vec![ProtocolMessage::Error {
                        code: "unknown_transfer".into(),
                        message: path.as_str().into(),
                    }]));
                };
                if pending.retried {
                    self.pending.remove(&key);
                    return Ok(Reply::Send(vec![ProtocolMessage::Error {
                        code: "keep_live".into(),
                        message: path.as_str().into(),
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

    fn accept_if_live_matches(&self, header: &BulkHeader) -> Result<Reply, SlaveError> {
        let host = self.host_for(&header.checkout_id, &header.path)?;
        if apply::content_matches(&host, header.want_hash).map_err(SlaveError::io(&host))? {
            return Ok(Reply::Send(Vec::new()));
        }
        Ok(Reply::Send(vec![ProtocolMessage::Error {
            code: "unknown_transfer".into(),
            message: header.path.as_str().into(),
        }]))
    }

    pub fn plan_local(
        &mut self,
        checkout_id: &str,
        event: LocalEvent,
    ) -> Result<HashPlan, SlaveError> {
        let plan = match event {
            LocalEvent::Changed(path) => self.note_changed(checkout_id, path)?,
            LocalEvent::Metadata(path) => self.note_metadata(checkout_id, path)?,
            LocalEvent::Removed(path) => HashPlan::send(self.note_removed(checkout_id, &path)?),
            LocalEvent::Renamed { from, to } => self.note_renamed(checkout_id, from, to)?,
        };
        self.status.local(None);
        for msg in &plan.send {
            self.status.outbound(None, msg);
        }
        Ok(plan)
    }

    pub fn note_local(
        &mut self,
        checkout_id: &str,
        event: LocalEvent,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let plan = self.plan_local(checkout_id, event)?;
        self.drain_plan(plan)
    }

    pub fn commit_hashed(&mut self, done: HashDone) -> Result<Vec<ProtocolMessage>, SlaveError> {
        self.commit_hashed_batch([done])
    }

    pub fn commit_hashed_batch(
        &mut self,
        dones: impl IntoIterator<Item = HashDone>,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let mut out = Vec::new();
        let mut staged = Vec::new();
        for done in dones {
            self.hashing.remove(&done.key);
            let HashKey::Checkout { id, path } = done.key else {
                continue;
            };
            self.finish_rescan_hash(&id, &path, &done.outcome);
            match done.outcome {
                HashOutcome::File(found) => {
                    if self.meta(&id, &path)?.as_ref() == Some(&found) {
                        continue;
                    }
                    let current = self.meta(&id, &path)?;
                    if current.as_ref() != done.previous.as_ref() {
                        let plan = self.note_changed(&id, path)?;
                        self.pending_hash.extend(plan.hash);
                        out.extend(plan.send);
                        continue;
                    }
                    if let Some(row) = self.stage_announce(&id, path, found)? {
                        staged.push(row);
                    }
                }
                HashOutcome::Absent => {
                    out.extend(self.note_removed(&id, &path)?);
                }
                HashOutcome::Io(kind) => {
                    log::warn!("hash {}: {kind}", path.as_str());
                }
            }
        }
        out.extend(self.commit_staged(staged)?);
        Ok(out)
    }

    fn drain_plan(&mut self, plan: HashPlan) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let mut out = plan.send;
        out.extend(self.drain_hash_jobs(plan.hash)?);
        Ok(out)
    }

    fn drain_hash_jobs(&mut self, jobs: Vec<HashNeed>) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let mut out = Vec::new();
        let mut jobs = jobs;
        loop {
            if jobs.is_empty() {
                break;
            }
            let mut dones = Vec::with_capacity(jobs.len());
            for need in jobs {
                self.start_hashed(&need.key);
                dones.push(need.run());
            }
            out.extend(self.commit_hashed_batch(dones)?);
            jobs = self.take_hash_jobs();
        }
        Ok(out)
    }

    fn enqueue_hash(&mut self, need: HashNeed) -> HashPlan {
        self.hashing.insert(need.key.clone(), (), Instant::now());
        HashPlan {
            send: Vec::new(),
            hash: vec![need],
        }
    }

    fn finish_rescan_hash(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
        outcome: &HashOutcome,
    ) {
        let Some(walk) = self.crawl.front_rescan_mut() else {
            return;
        };
        if walk.checkout_id != checkout_id {
            return;
        }
        walk.awaiting_hash.remove(path);
        if let HashOutcome::File(found) = outcome {
            walk.found.insert(path.clone(), found.clone());
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

    fn last_synced_content(
        &self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<Option<ContentHash>, SlaveError> {
        let checkout = self.checkout(checkout_id)?;
        self.store
            .get_last_synced_content(&checkout.id, path)
            .map_err(SlaveError::index)
    }

    pub fn rescan(&mut self, checkout_id: &str) -> Result<Vec<ProtocolMessage>, SlaveError> {
        self.start_rescan(checkout_id)?;
        let mut out = Vec::new();
        while self.crawl.has_rescan() {
            let plan = self.step_crawl(usize::MAX)?;
            out.extend(plan.send);
            out.extend(self.drain_hash_jobs(plan.hash)?);
        }
        for msg in &out {
            self.status.outbound(None, msg);
        }
        Ok(out)
    }

    fn on_announce(
        &mut self,
        checkout_id: String,
        path: CanonicalPath,
        new: FileMetadata,
        basis: Option<FileNode>,
    ) -> Result<Reply, SlaveError> {
        if self.reserved_inbound(&checkout_id, &path)? {
            return Ok(Reply::Send(Vec::new()));
        }
        if self
            .pending_pulls
            .remove(&(checkout_id.clone(), path.clone()))
            .is_some()
        {
            return self.apply_new(&checkout_id, path, new);
        }
        let current = self.meta(&checkout_id, &path)?;
        match decide_incoming(current.as_ref(), basis, &new) {
            ReplicaAction::NoopRefresh => {
                self.write_last_synced(
                    &checkout_id,
                    &path,
                    Some(file_node(&new)),
                    Some(new.content_hash),
                )?;
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
        if self.reserved_inbound(&checkout_id, &path)? {
            return Ok(Reply::Send(Vec::new()));
        }
        let current = self.meta(&checkout_id, &path)?;
        let last_synced = self.last_synced(&checkout_id, &path)?;
        let last_content = self.last_synced_content(&checkout_id, &path)?;
        match decide_master_won_delete(current.as_ref(), basis, last_synced, last_content) {
            DeleteAction::AlreadyGone => {
                self.write_last_synced(&checkout_id, &path, None, None)?;
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

    fn on_rename(
        &mut self,
        checkout_id: String,
        from: CanonicalPath,
        to: CanonicalPath,
        from_basis: FileNode,
        to_new: FileMetadata,
    ) -> Result<Reply, SlaveError> {
        if self.reserved_inbound(&checkout_id, &from)?
            || self.reserved_inbound(&checkout_id, &to)?
        {
            return Ok(Reply::Send(Vec::new()));
        }
        let from_local = self.meta(&checkout_id, &from)?;
        let to_local = self.meta(&checkout_id, &to)?;
        let last_synced_from = self.last_synced(&checkout_id, &from)?;
        match decide_rename(
            from_local.as_ref(),
            to_local.as_ref(),
            from_basis,
            last_synced_from,
            &to_new,
        ) {
            RenameAction::NoopRefresh => {
                self.write_last_synced(&checkout_id, &from, None, None)?;
                self.write_last_synced(
                    &checkout_id,
                    &to,
                    Some(file_node(&to_new)),
                    Some(to_new.content_hash),
                )?;
                Ok(Reply::Send(Vec::new()))
            }
            RenameAction::CreateAtTo => {
                self.write_last_synced(&checkout_id, &from, None, None)?;
                self.apply_new(&checkout_id, to, to_new)
            }
            RenameAction::SidecarThenApply => {
                self.sidecar_local(
                    &checkout_id,
                    &from,
                    from_local.as_ref(),
                    to_new.content_hash,
                )?;
                self.sidecar_local(&checkout_id, &to, to_local.as_ref(), to_new.content_hash)?;
                self.apply_rename(&checkout_id, from, to, to_new)?;
                Ok(Reply::Send(Vec::new()))
            }
            RenameAction::Apply => {
                self.sidecar_local(&checkout_id, &to, to_local.as_ref(), to_new.content_hash)?;
                self.apply_rename(&checkout_id, from, to, to_new)?;
                Ok(Reply::Send(Vec::new()))
            }
        }
    }

    fn apply_rename(
        &mut self,
        checkout_id: &str,
        from: CanonicalPath,
        to: CanonicalPath,
        to_new: FileMetadata,
    ) -> Result<(), SlaveError> {
        let (local, from_rel, to_rel) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.local.clone(),
                strip_central(&checkout.central, &from)?,
                strip_central(&checkout.central, &to)?,
            )
        };
        let hash = match to_new.kind {
            EntryKind::Dir => ContentHash::ZERO,
            EntryKind::File | EntryKind::Symlink => to_new.content_hash,
        };
        self.checkout_mut(checkout_id)?
            .inflight
            .arm(to.clone(), hash);
        if let Err(err) = apply::rename_live(&local, &from_rel, &to_rel, &to_new) {
            self.checkout_mut(checkout_id)?.inflight.disarm(&to);
            return Err(err.into());
        }
        let ck = self.checkout(checkout_id)?.id.clone();
        self.write_index(&ck, &from, None, index::LastSynced::AdoptLeaf)?;
        self.index_ancestors(checkout_id, &to)?;
        self.write_index(&ck, &to, Some(&to_new), index::LastSynced::AdoptLeaf)?;
        if to_new.kind == EntryKind::Dir {
            self.reindex_descendants(checkout_id, &to, index::LastSynced::AdoptLeaf)?;
        }
        Ok(())
    }

    fn on_cas_accept(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
        node: Option<FileNode>,
    ) -> Result<(), SlaveError> {
        let content = match node {
            Some(accepted) => self
                .meta(checkout_id, path)?
                .and_then(|local| (file_node(&local) == accepted).then_some(local.content_hash)),
            None => None,
        };
        self.write_last_synced(checkout_id, path, node, content)?;
        self.clear_pending_rename(checkout_id, path);
        *self
            .cas_since_dir_list
            .entry(checkout_id.to_string())
            .or_insert(0) += 1;
        Ok(())
    }

    fn on_cas_reject(
        &mut self,
        checkout_id: String,
        path: CanonicalPath,
        current: Option<FileMetadata>,
    ) -> Result<Reply, SlaveError> {
        if let Some((from, to)) = self.take_pending_rename(&checkout_id, &path) {
            if self.undo_rename_disk(&checkout_id, &from, &to).is_err() {
                let local_to = self.meta(&checkout_id, &to)?;
                let incoming = current
                    .as_ref()
                    .map(|winner| winner.content_hash)
                    .unwrap_or(ContentHash::ZERO);
                self.sidecar_local(&checkout_id, &to, local_to.as_ref(), incoming)?;
                return match current {
                    None => {
                        self.remove_path(
                            &checkout_id,
                            &to,
                            self.meta(&checkout_id, &to)?.as_ref(),
                        )?;
                        Ok(Reply::Send(Vec::new()))
                    }
                    Some(winner) => self.apply_new(&checkout_id, to, winner),
                };
            }
        }
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
        let previous = self.meta(checkout_id, &path)?;
        match new.kind {
            EntryKind::Dir => return self.finish_apply(checkout_id, path, new, &[]),
            EntryKind::File | EntryKind::Symlink => match self.bodies.fetch(new.content_hash) {
                ContentBytes::AskSender => {
                    let live = if previous.as_ref().is_some_and(|live| live.kind != new.kind) {
                        None
                    } else {
                        let host = self.host_for(checkout_id, &path)?;
                        apply::try_read_file_or_link(&host).map_err(SlaveError::io(&host))?
                    };
                    let kind = new.kind;
                    let want_hash = new.content_hash;
                    self.pending.insert(
                        (checkout_id.to_string(), path.clone()),
                        PendingApply {
                            checkout_id: checkout_id.into(),
                            path: path.clone(),
                            new,
                            retried: false,
                        },
                        Instant::now(),
                    );
                    return Ok(Reply::Send(vec![transfer::signature_request(
                        checkout_id,
                        path,
                        want_hash,
                        kind,
                        live.as_deref(),
                    )]));
                }
                ContentBytes::Whole(body) => self.finish_apply(checkout_id, path, new, &body),
            },
        }
    }

    fn finish_verified(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
        new: FileMetadata,
        verified: apply::VerifiedContent,
    ) -> Result<Reply, SlaveError> {
        let previous = self.meta(checkout_id, &path)?;
        let (local, relative) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.local.clone(),
                strip_central(&checkout.central, &path)?,
            )
        };
        let hash = match new.kind {
            EntryKind::Dir => ContentHash::ZERO,
            EntryKind::File | EntryKind::Symlink => new.content_hash,
        };
        self.checkout_mut(checkout_id)?
            .inflight
            .arm(path.clone(), hash);
        if let Err(err) = self.index_ancestors(checkout_id, &path) {
            self.checkout_mut(checkout_id)?.inflight.disarm(&path);
            return Err(err);
        }
        if let Err(err) =
            apply::install_verified(&local, &relative, &new, previous.as_ref(), verified)
        {
            self.checkout_mut(checkout_id)?.inflight.disarm(&path);
            return Err(err.into());
        }
        let ck = self.checkout(checkout_id)?.id.clone();
        self.write_index(&ck, &path, Some(&new), index::LastSynced::AdoptLeaf)?;
        Ok(Reply::Send(Vec::new()))
    }

    fn finish_apply(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
        new: FileMetadata,
        body: &[u8],
    ) -> Result<Reply, SlaveError> {
        let previous = self.meta(checkout_id, &path)?;
        let (local, relative) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.local.clone(),
                strip_central(&checkout.central, &path)?,
            )
        };
        let hash = match new.kind {
            EntryKind::Dir => ContentHash::ZERO,
            EntryKind::File | EntryKind::Symlink => new.content_hash,
        };
        self.checkout_mut(checkout_id)?
            .inflight
            .arm(path.clone(), hash);
        if let Err(err) = self.index_ancestors(checkout_id, &path) {
            self.checkout_mut(checkout_id)?.inflight.disarm(&path);
            return Err(err);
        }
        if let Err(err) = apply::replace_live(&local, &relative, &new, body, previous.as_ref()) {
            self.checkout_mut(checkout_id)?.inflight.disarm(&path);
            return Err(err.into());
        }
        let ck = self.checkout(checkout_id)?.id.clone();
        self.write_index(&ck, &path, Some(&new), index::LastSynced::AdoptLeaf)?;
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
        fulfill_from_host(checkout_id, path, want_hash, &signature, &host)
    }

    pub fn bulk_host(
        &self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<PathBuf, SlaveError> {
        self.host_for(checkout_id, path)
    }

    pub fn announced_size(
        &self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<Option<u64>, SlaveError> {
        Ok(self.meta(checkout_id, path)?.map(|meta| meta.size))
    }

    pub fn note_inbound(&mut self, msg: &ProtocolMessage) {
        self.status.inbound(None, msg);
    }

    pub fn note_outbound(&mut self, msg: &ProtocolMessage) {
        self.status.outbound(None, msg);
    }

    pub fn note_bulk_out(&mut self, bytes: u64) {
        self.status.bulk_out(None, bytes);
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
        let hash = match new.kind {
            EntryKind::Dir => ContentHash::ZERO,
            EntryKind::File | EntryKind::Symlink => new.content_hash,
        };
        let file_bytes = match new.kind {
            EntryKind::File => Some(fs::read(&host).map_err(SlaveError::io(&host))?),
            _ => None,
        };
        let link_target = match new.kind {
            EntryKind::Symlink => {
                Some(fs::read_link(&host).map_err(SlaveError::io(&host))?)
            }
            _ => None,
        };
        self.checkout_mut(checkout_id)?
            .inflight
            .arm(path.clone(), hash);
        let applied = match new.kind {
            EntryKind::Dir => apply::mkdir_live(&local, &relative, new),
            EntryKind::File => {
                apply::atomic_put(&local, &relative, new, file_bytes.as_ref().unwrap())
            }
            EntryKind::Symlink => apply::atomic_symlink(
                &local,
                &relative,
                new,
                link_target.as_ref().unwrap().as_os_str().as_bytes(),
            ),
        };
        if let Err(err) = applied {
            self.checkout_mut(checkout_id)?.inflight.disarm(path);
            return Err(err.into());
        }
        let ck = self.checkout(checkout_id)?.id.clone();
        self.write_index(&ck, path, Some(new), index::LastSynced::AdoptLeaf)?;
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
        self.write_index(&ck, path, None, index::LastSynced::AdoptLeaf)?;
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
        content_hash: Option<ContentHash>,
    ) -> Result<(), SlaveError> {
        let ck = self.checkout(checkout_id)?.id.clone();
        let mut batch = self.store.begin_write().map_err(SlaveError::index)?;
        match node {
            Some(node) => batch
                .put_last_synced(&ck, path, node, content_hash)
                .map_err(SlaveError::index)?,
            None => batch
                .del_last_synced(&ck, path)
                .map_err(SlaveError::index)?,
        }
        batch.commit().map_err(SlaveError::index)
    }

    fn on_subscribe_reject(
        &mut self,
        reason: String,
        denied_centrals: Vec<CanonicalPath>,
    ) -> Result<Reply, SlaveError> {
        for central in denied_centrals {
            log::warn!("subscribe rejected {}: {reason}", central.as_str());
            self.denied_centrals.insert(central);
        }
        if self
            .cfg
            .checkouts()
            .iter()
            .any(|checkout| !self.denied_centrals.contains(checkout.central()))
        {
            Ok(Reply::Send(vec![self.subscribe_message()]))
        } else {
            Ok(Reply::Hangup { reason })
        }
    }

    fn on_subscribe_ack(&mut self) -> Result<Reply, SlaveError> {
        self.pending.clear();
        self.pending_pulls.clear();
        let mut ids: Vec<String> = self.checkouts.keys().cloned().collect();
        ids.sort();
        for id in &ids {
            self.start_rescan(id)?;
        }
        let plan = self.step_crawl(STEP_BUDGET)?;
        self.pending_hash.extend(plan.hash);
        let mut out = plan.send;
        if self.crawl.has_rescan() {
            let mut reports = self.root_reports_for(&ids)?;
            reports.extend(out);
            out = reports;
        }
        Ok(Reply::Send(out))
    }

    fn root_reports_for(&self, ids: &[String]) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let checkout = self.checkout(id)?;
            out.push(ProtocolMessage::RootReport {
                root: index::subtree_root(&self.store, &checkout.id, &checkout.central)
                    .map_err(SlaveError::index)?,
                path: checkout.central.clone(),
                checkout_id: id.clone(),
            });
        }
        Ok(out)
    }

    fn on_root_ack(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
        matched: bool,
    ) -> Result<Reply, SlaveError> {
        let checkout = self.checkout(checkout_id)?;
        if matched {
            return Ok(Reply::Send(Vec::new()));
        }
        let path = if path == checkout.central {
            path
        } else {
            checkout.central.clone()
        };
        Ok(Reply::Send(vec![self.dir_list_request(
            checkout_id,
            path,
            None,
        )]))
    }

    fn cas_epoch(&self, checkout_id: &str) -> u64 {
        self.cas_since_dir_list
            .get(checkout_id)
            .copied()
            .unwrap_or(0)
    }

    fn dir_list_request(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
        after: Option<EntryName>,
    ) -> ProtocolMessage {
        let epoch = self.cas_epoch(checkout_id);
        self.dir_list_sent
            .insert((checkout_id.to_string(), path.clone(), after.clone()), epoch);
        ProtocolMessage::DirListRequest {
            checkout_id: checkout_id.into(),
            path,
            after,
        }
    }

    fn on_dir_list(
        &mut self,
        checkout_id: String,
        path: CanonicalPath,
        after: Option<EntryName>,
        entries: Vec<DirChild>,
        more: bool,
    ) -> Result<Reply, SlaveError> {
        let ck = self.checkout(&checkout_id)?.id.clone();
        let local = index::list_children(&self.store, &ck, &path).map_err(SlaveError::index)?;
        let page_end = entries
            .last()
            .map(|child| child.name().as_str().to_string());

        let mut by_name: BTreeMap<String, (Option<DirChild>, Option<DirChild>)> = BTreeMap::new();
        for child in local {
            by_name.insert(child.name().as_str().to_string(), (Some(child), None));
        }
        for child in &entries {
            by_name
                .entry(child.name().as_str().to_string())
                .and_modify(|pair| pair.1 = Some(child.clone()))
                .or_insert((None, Some(child.clone())));
        }

        let list_epoch = self
            .dir_list_sent
            .remove(&(checkout_id.clone(), path.clone(), after.clone()))
            .unwrap_or(0);
        self.crawl.push_page(DirListPage::from_merge(
            checkout_id,
            path,
            after,
            more,
            page_end,
            list_epoch,
            by_name,
        ));
        let plan = self.step_crawl(STEP_BUDGET)?;
        self.pending_hash.extend(plan.hash);
        Ok(Reply::Send(plan.send))
    }

    fn ensure_local_dir(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<(), SlaveError> {
        let (local, relative) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.local.clone(),
                strip_central(&checkout.central, path)?,
            )
        };
        let host = confine_host(&local, &relative)?;
        fs::create_dir_all(&host).map_err(SlaveError::io(&host))?;
        let found = meta::collect_from_path(&host)
            .map_err(SlaveError::io(&host))?
            .unwrap_or_else(|| FileMetadata::directory(0, 0o040755));
        apply::mkdir_live(&local, &relative, &found)?;
        let ck = self.checkout(checkout_id)?.id.clone();
        if self.meta(checkout_id, path)?.is_none() {
            self.index_ancestors(checkout_id, path)?;
            self.write_index(&ck, path, Some(&found), index::LastSynced::Keep)?;
        }
        Ok(())
    }

    fn start_rescan(&mut self, checkout_id: &str) -> Result<(), SlaveError> {
        if self.crawl.rescanning(checkout_id) {
            return Ok(());
        }
        let (ck, local, central) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.id.clone(),
                checkout.local.clone(),
                checkout.central.clone(),
            )
        };
        let previous = self
            .store
            .get_meta(&ck, &central)
            .map_err(SlaveError::index)?;
        let root =
            meta::collect_for_rescan(&local, previous.as_ref()).map_err(SlaveError::io(&local))?;
        self.crawl.push_rescan(RescanWalk::begin(
            checkout_id.into(),
            ck,
            local,
            central,
            CanonicalPath::root(),
            root,
        )?);
        Ok(())
    }

    fn step_crawl(&mut self, budget: usize) -> Result<HashPlan, SlaveError> {
        if self.crawl.has_pages() {
            return self.step_dir_list(budget);
        }
        self.step_rescan(budget)
    }

    fn step_rescan(&mut self, budget: usize) -> Result<HashPlan, SlaveError> {
        let Some(walk) = self.crawl.front_rescan_mut() else {
            return Ok(HashPlan::default());
        };
        let ck = walk.ck.clone();
        let checkout_id = walk.checkout_id.clone();
        let newly = walk
            .collect(budget, |path| self.store.get_meta(&ck, path))
            .map_err(into_slave_walk_err)?;
        let mut plan = self.absorb_walked(&checkout_id, newly)?;
        let Some(walk) = self.crawl.take_finished_rescan() else {
            return Ok(plan);
        };
        plan.send.extend(self.commit_rescan(walk)?);
        self.status.rescan(None);
        Ok(plan)
    }

    fn absorb_walked(
        &mut self,
        checkout_id: &str,
        newly: impl IntoIterator<Item = Walked>,
    ) -> Result<HashPlan, SlaveError> {
        let mut plan = HashPlan::default();
        for row in newly {
            match row {
                Walked::Ready(path, found) => {
                    plan.send
                        .extend(self.announce_new_to_index(checkout_id, path, found)?);
                }
                Walked::Needs(need) => plan.append(self.enqueue_hash(need)),
            }
        }
        Ok(plan)
    }

    fn commit_rescan(&mut self, walk: RescanWalk) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let indexed: BTreeMap<_, _> = self
            .store
            .range_meta(&walk.ck, &walk.central)
            .map_err(SlaveError::index)?
            .into_iter()
            .collect();
        let mut changes = Vec::new();
        for path in indexed.keys() {
            if walk.found.contains_key(path) {
                continue;
            }
            if stat_still_present(&walk.local, &walk.central, path)? {
                continue;
            }
            changes.push(index::LeafChange {
                path,
                meta: None,
                last_synced: index::LastSynced::Keep,
            });
        }
        for (path, found) in &walk.found {
            if indexed.get(path) == Some(found) {
                continue;
            }
            changes.push(index::LeafChange {
                path,
                meta: Some(found),
                last_synced: index::LastSynced::Keep,
            });
        }
        index::commit_leaves(&self.store, &walk.ck, changes).map_err(SlaveError::index)?;
        index::repair_dir_nodes(&self.store, &walk.ck, &walk.central).map_err(SlaveError::index)?;
        self.dirs.remove(&walk.ck);
        self.root_reports_for(&[walk.checkout_id])
    }
}

fn stat_still_present(
    local: &Path,
    central: &CanonicalPath,
    path: &CanonicalPath,
) -> Result<bool, SlaveError> {
    let relative = strip_central(central, path)?;
    let host = canonical_to_host(local, &relative);
    match fs::symlink_metadata(&host) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            log::warn!("keeping {}: permission denied", host.display());
            Ok(true)
        }
        Err(err) => Err(SlaveError::io(&host)(err)),
    }
}

impl<S: Storage, C: ContentHook> Slave<S, C> {
    fn step_dir_list(&mut self, budget: usize) -> Result<HashPlan, SlaveError> {
        let (checkout_id, parent, batch, next, list_epoch) = {
            let Some(page) = self.crawl.front_page_mut() else {
                return Ok(HashPlan::default());
            };
            let checkout_id = page.checkout_id.clone();
            let parent = page.path.clone();
            let after = page.after.clone();
            let more = page.more;
            let page_end = page.page_end.clone();
            let list_epoch = page.list_epoch;
            let mut batch = Vec::new();
            let mut used = 0;
            while used < budget {
                let Some((entry, (slave_child, master_child))) = page.remaining.pop_front() else {
                    break;
                };
                if after
                    .as_ref()
                    .is_some_and(|cursor| entry.as_str() <= cursor.as_str())
                {
                    continue;
                }
                if more
                    && page_end
                        .as_ref()
                        .is_none_or(|end| entry.as_str() > end.as_str() && master_child.is_none())
                {
                    continue;
                }
                used += 1;
                batch.push((entry, slave_child, master_child));
            }
            let finished = page.remaining.is_empty();
            let next = if finished {
                page.next_page_request()
            } else {
                None
            };
            (checkout_id, parent, batch, next, list_epoch)
        };
        self.crawl.pop_page_if_empty();
        let mut plan = HashPlan::default();
        for (entry, slave_child, master_child) in batch {
            plan.append(self.apply_dir_child(
                &checkout_id,
                &parent,
                &entry,
                slave_child.as_ref(),
                master_child.as_ref(),
                list_epoch,
            )?);
        }
        if let Some(ProtocolMessage::DirListRequest {
            checkout_id,
            path,
            after,
        }) = next
        {
            plan.send
                .push(self.dir_list_request(&checkout_id, path, after));
        }
        Ok(plan)
    }

    fn apply_dir_child(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
        entry: &str,
        slave_child: Option<&DirChild>,
        master_child: Option<&DirChild>,
        list_epoch: u64,
    ) -> Result<HashPlan, SlaveError> {
        let child_path = join_central(path, entry)?;
        let last_synced = self.last_synced(checkout_id, &child_path)?;
        let local_meta = self.meta(checkout_id, &child_path)?;
        let local_file = local_meta.as_ref().map(file_node);
        let mut plan = HashPlan::default();
        match decide_child(slave_child, master_child, last_synced, local_file) {
            WalkAction::Matched => {}
            WalkAction::Recurse => {
                plan.send
                    .push(self.dir_list_request(checkout_id, child_path, None))
            }
            WalkAction::Pull => {
                let master_dir = matches!(master_child, Some(DirChild::Directory { .. }));
                let slave_dir = matches!(slave_child, Some(DirChild::Directory { .. }));
                if slave_child.is_some() && master_dir != slave_dir {
                    self.remove_path(checkout_id, &child_path, local_meta.as_ref())?;
                }
                if master_dir {
                    self.ensure_local_dir(checkout_id, &child_path)?;
                } else {
                    self.pending_pulls.insert(
                        (checkout_id.into(), child_path.clone()),
                        (),
                        Instant::now(),
                    );
                }
                plan.send
                    .push(self.dir_list_request(checkout_id, child_path.clone(), None));
            }
            WalkAction::AnnounceCreate | WalkAction::AnnounceCas => {
                if master_child.is_none() && last_synced.is_some() && local_file == last_synced {
                    self.write_last_synced(checkout_id, &child_path, None, None)?;
                    self.checkout_mut(checkout_id)?.inflight.disarm(&child_path);
                }
                let walk_dir = local_meta
                    .as_ref()
                    .is_some_and(|meta| meta.kind == EntryKind::Dir);
                if walk_dir {
                    plan.append(self.note_changed(checkout_id, child_path.clone())?);
                    plan.send
                        .push(self.dir_list_request(checkout_id, child_path.clone(), None));
                } else {
                    plan.append(self.note_changed(checkout_id, child_path)?);
                }
            }
            WalkAction::AnnounceDelete => {
                if self.cas_epoch(checkout_id) > list_epoch || self.crawl.rescanning(checkout_id)
                {
                    return Ok(plan);
                }
                plan.send
                    .extend(self.reconcile_delete(checkout_id, &child_path)?);
            }
        }
        Ok(plan)
    }

    fn walk_from(
        &self,
        checkout_id: &str,
        start_rel: CanonicalPath,
    ) -> Result<BTreeMap<CanonicalPath, FileMetadata>, SlaveError> {
        let (ck, local, central) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.id.clone(),
                checkout.local.clone(),
                checkout.central.clone(),
            )
        };
        let start_path = if start_rel.as_str() == "/" {
            central.clone()
        } else {
            join_central(&central, start_rel.as_str().trim_start_matches('/'))?
        };
        let host = canonical_to_host(&local, &start_rel);
        let previous = self
            .store
            .get_meta(&ck, &start_path)
            .map_err(SlaveError::index)?;
        let start_meta =
            meta::collect_for_rescan(&host, previous.as_ref()).map_err(SlaveError::io(&host))?;
        let mut walk = RescanWalk::begin(
            checkout_id.into(),
            ck.clone(),
            local,
            central,
            start_rel,
            start_meta,
        )?;
        let rows = walk
            .collect(usize::MAX, |path| self.store.get_meta(&ck, path))
            .map_err(into_slave_walk_err)?;
        for row in rows {
            if let Walked::Needs(need) = row {
                let key = need.key.clone();
                if let HashOutcome::File(found) = need.run().outcome {
                    let HashKey::Checkout { path, .. } = key else {
                        continue;
                    };
                    walk.found.insert(path, found);
                }
            }
        }
        Ok(walk.found)
    }

    fn note_changed(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
    ) -> Result<HashPlan, SlaveError> {
        let central = self.checkout(checkout_id)?.central.clone();
        if is_reserved(&central, &path) {
            return Ok(HashPlan::default());
        }
        let (local, relative) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.local.clone(),
                strip_central(&checkout.central, &path)?,
            )
        };
        let host = canonical_to_host(&local, &relative);
        let previous = self.meta(checkout_id, &path)?;
        match meta::inspect_for_hash(&host, previous.as_ref()).map_err(SlaveError::io(&host))? {
            Inspected::Ready(found) => Ok(HashPlan::send(self.announce_live(
                checkout_id,
                path,
                found,
            )?)),
            Inspected::Absent => Ok(HashPlan::send(self.note_removed(checkout_id, &path)?)),
            Inspected::NeedHash(host) => Ok(self.enqueue_hash(HashNeed {
                key: HashKey::Checkout {
                    id: checkout_id.into(),
                    path,
                },
                host,
                previous,
            })),
        }
    }

    fn announce_new_to_index(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
        found: FileMetadata,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        if self.meta(checkout_id, &path)?.as_ref() == Some(&found) {
            return Ok(Vec::new());
        }
        self.announce_live(checkout_id, path, found)
    }

    fn announce_live(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
        found: FileMetadata,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let Some(row) = self.stage_announce(checkout_id, path, found)? else {
            return Ok(Vec::new());
        };
        self.commit_staged(vec![row])
    }

    fn stage_announce(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
        found: FileMetadata,
    ) -> Result<Option<StagedAnnounce>, SlaveError> {
        let central = self.checkout(checkout_id)?.central.clone();
        if is_reserved(&central, &path) {
            return Ok(None);
        }
        if self
            .checkout_mut(checkout_id)?
            .inflight
            .consume_if_echo(&path, &found.content_hash)
        {
            return Ok(None);
        }
        let last_synced = self.last_synced(checkout_id, &path)?;
        if last_synced == Some(file_node(&found)) {
            return Ok(None);
        }
        let dirty = self.meta(checkout_id, &path)?.as_ref() != Some(&found);
        let ck = self.checkout(checkout_id)?.id.clone();
        Ok(Some(StagedAnnounce {
            checkout_id: checkout_id.into(),
            ck,
            path,
            found,
            last_synced,
            dirty,
        }))
    }

    fn commit_staged(
        &mut self,
        staged: Vec<StagedAnnounce>,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        for row in staged.iter().filter(|row| row.dirty) {
            self.index_ancestors(&row.checkout_id, &row.path)?;
        }
        let mut by_ck: HashMap<CheckoutId, Vec<usize>> = HashMap::new();
        for (i, row) in staged.iter().enumerate() {
            if row.dirty {
                by_ck.entry(row.ck.clone()).or_default().push(i);
            }
        }
        for (ck, idxs) in by_ck {
            let changes: Vec<index::LeafChange<'_>> = idxs
                .iter()
                .map(|&i| index::LeafChange {
                    path: &staged[i].path,
                    meta: Some(&staged[i].found),
                    last_synced: index::LastSynced::Keep,
                })
                .collect();
            self.write_indexes(&ck, changes)?;
        }
        Ok(staged
            .into_iter()
            .map(|row| ProtocolMessage::FileAnnounce {
                checkout_id: row.checkout_id,
                path: row.path,
                new: row.found,
                basis: row.last_synced,
            })
            .collect())
    }

    fn note_metadata(
        &mut self,
        checkout_id: &str,
        path: CanonicalPath,
    ) -> Result<HashPlan, SlaveError> {
        let central = self.checkout(checkout_id)?.central.clone();
        if is_reserved(&central, &path) {
            return Ok(HashPlan::default());
        }
        let (local, relative) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.local.clone(),
                strip_central(&checkout.central, &path)?,
            )
        };
        let host = canonical_to_host(&local, &relative);
        let previous = self.meta(checkout_id, &path)?;
        let found = match meta::inspect_for_hash(&host, previous.as_ref())
            .map_err(SlaveError::io(&host))?
        {
            Inspected::Ready(found) => found,
            Inspected::Absent => {
                return Ok(HashPlan::send(self.note_removed(checkout_id, &path)?));
            }
            Inspected::NeedHash(host) => {
                return Ok(self.enqueue_hash(HashNeed {
                    key: HashKey::Checkout {
                        id: checkout_id.into(),
                        path,
                    },
                    host,
                    previous,
                }));
            }
        };
        if self
            .checkout_mut(checkout_id)?
            .inflight
            .consume_if_echo(&path, &found.content_hash)
        {
            return Ok(HashPlan::default());
        }
        let last_synced = self.last_synced(checkout_id, &path)?;
        if last_synced == Some(file_node(&found)) {
            return Ok(HashPlan::default());
        }
        if previous.as_ref() == Some(&found) {
            return Ok(HashPlan::default());
        }
        let ck = self.checkout(checkout_id)?.id.clone();
        self.index_ancestors(checkout_id, &path)?;
        self.write_index(&ck, &path, Some(&found), index::LastSynced::Keep)?;
        Ok(HashPlan::send(vec![ProtocolMessage::FileAnnounce {
            checkout_id: checkout_id.into(),
            path,
            new: found,
            basis: last_synced,
        }]))
    }

    fn note_renamed(
        &mut self,
        checkout_id: &str,
        from: CanonicalPath,
        to: CanonicalPath,
    ) -> Result<HashPlan, SlaveError> {
        let central = self.checkout(checkout_id)?.central.clone();
        let from_reserved = is_reserved(&central, &from);
        let to_reserved = is_reserved(&central, &to);
        if from_reserved && to_reserved {
            return Ok(HashPlan::default());
        }
        if from_reserved {
            return self.note_changed(checkout_id, to);
        }
        if to_reserved {
            return Ok(HashPlan::send(self.note_removed(checkout_id, &from)?));
        }

        let from_meta = self.meta(checkout_id, &from)?;
        let from_basis = match &from_meta {
            Some(meta) => self
                .last_synced(checkout_id, &from)?
                .unwrap_or_else(|| file_node(meta)),
            None => {
                let (local, relative) = {
                    let checkout = self.checkout(checkout_id)?;
                    (
                        checkout.local.clone(),
                        strip_central(&checkout.central, &to)?,
                    )
                };
                let host = canonical_to_host(&local, &relative);
                if meta::collect_from_path(&host)
                    .map_err(SlaveError::io(&host))?
                    .is_some()
                {
                    return self.note_changed(checkout_id, to);
                }
                return Ok(HashPlan::default());
            }
        };

        let (local, relative) = {
            let checkout = self.checkout(checkout_id)?;
            (
                checkout.local.clone(),
                strip_central(&checkout.central, &to)?,
            )
        };
        let host = canonical_to_host(&local, &relative);
        let Some(found) =
            meta::collect_for_rescan(&host, from_meta.as_ref()).map_err(SlaveError::io(&host))?
        else {
            return Ok(HashPlan::send(self.note_removed(checkout_id, &from)?));
        };

        let ck = self.checkout(checkout_id)?.id.clone();
        self.write_index(&ck, &from, None, index::LastSynced::Keep)?;
        self.index_ancestors(checkout_id, &to)?;
        self.write_index(&ck, &to, Some(&found), index::LastSynced::Keep)?;
        if found.kind == EntryKind::Dir {
            self.reindex_descendants(checkout_id, &to, index::LastSynced::Keep)?;
        }
        self.pending_renames.insert(
            (checkout_id.into(), from.clone()),
            to.clone(),
            Instant::now(),
        );
        Ok(HashPlan::send(vec![ProtocolMessage::Rename {
            checkout_id: checkout_id.into(),
            from,
            to,
            from_basis,
            to_new: found,
        }]))
    }

    fn reindex_descendants(
        &mut self,
        checkout_id: &str,
        dir: &CanonicalPath,
        last_synced: index::LastSynced,
    ) -> Result<(), SlaveError> {
        let central = self.checkout(checkout_id)?.central.clone();
        let relative = strip_central(&central, dir)?;
        let disk = self.walk_from(checkout_id, relative)?;
        let ck = self.checkout(checkout_id)?.id.clone();
        for (path, found) in disk {
            if &path == dir {
                continue;
            }
            self.write_index(&ck, &path, Some(&found), last_synced)?;
        }
        Ok(())
    }

    fn clear_pending_rename(&mut self, checkout_id: &str, path: &CanonicalPath) {
        if self
            .pending_renames
            .remove(&(checkout_id.to_string(), path.clone()))
            .is_some()
        {
            return;
        }
        self.pending_renames
            .retain(|(ck, from), to| !(ck == checkout_id && (from == path || to == path)));
    }

    fn take_pending_rename(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Option<(CanonicalPath, CanonicalPath)> {
        if let Some(to) = self
            .pending_renames
            .remove(&(checkout_id.to_string(), path.clone()))
        {
            return Some((path.clone(), to));
        }
        let key = self
            .pending_renames
            .oldest_key_where(|key, to| key.0 == checkout_id && to == path)
            .cloned()?;
        let to = self.pending_renames.remove(&key)?;
        Some((key.1, to))
    }

    fn undo_rename_disk(
        &self,
        checkout_id: &str,
        from: &CanonicalPath,
        to: &CanonicalPath,
    ) -> Result<(), SlaveError> {
        let checkout = self.checkout(checkout_id)?;
        let from_rel = strip_central(&checkout.central, from)?;
        let to_rel = strip_central(&checkout.central, to)?;
        let from_host = confine_host(&checkout.local, &from_rel)?;
        let to_host = confine_host(&checkout.local, &to_rel)?;
        let to_exists = match fs::symlink_metadata(&to_host) {
            Ok(_) => true,
            Err(err) if err.kind() == io::ErrorKind::NotFound => false,
            Err(err) => return Err(SlaveError::io(&to_host)(err)),
        };
        let from_gone = match fs::symlink_metadata(&from_host) {
            Ok(_) => false,
            Err(err) if err.kind() == io::ErrorKind::NotFound => true,
            Err(err) => return Err(SlaveError::io(&from_host)(err)),
        };
        if to_exists && from_gone {
            if let Some(parent) = from_host.parent() {
                fs::create_dir_all(parent).map_err(SlaveError::io(parent))?;
            }
            fs::rename(&to_host, &from_host).map_err(SlaveError::io(&from_host))?;
        }
        Ok(())
    }

    fn note_removed(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let central = self.checkout(checkout_id)?.central.clone();
        if is_reserved(&central, path) {
            return Ok(Vec::new());
        }
        if self.checkout_mut(checkout_id)?.inflight.is_armed(path) {
            return Ok(Vec::new());
        }
        self.reconcile_delete(checkout_id, path)
    }

    fn reconcile_delete(
        &mut self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<Vec<ProtocolMessage>, SlaveError> {
        let Some(previous) = self.meta(checkout_id, path)? else {
            return Ok(Vec::new());
        };
        let last_synced = self.last_synced(checkout_id, path)?;
        let basis = last_synced.unwrap_or_else(|| file_node(&previous));
        let ck = self.checkout(checkout_id)?.id.clone();
        self.write_index(&ck, path, None, index::LastSynced::Keep)?;
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
            let host = confine_host(&local, &relative)?;
            fs::create_dir_all(&host).map_err(SlaveError::io(&host))?;
            let Some(found) = meta::collect_from_path(&host).map_err(SlaveError::io(&host))? else {
                continue;
            };
            self.write_index(&ck, &dir, Some(&found), index::LastSynced::AdoptLeaf)?;
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

    fn reserved_inbound(
        &self,
        checkout_id: &str,
        path: &CanonicalPath,
    ) -> Result<bool, SlaveError> {
        let central = self.checkout(checkout_id)?.central.clone();
        Ok(is_reserved(&central, path))
    }
}

pub fn fulfill_from_host(
    checkout_id: String,
    path: CanonicalPath,
    want_hash: ContentHash,
    signature: &[u8],
    host: &Path,
) -> Result<Reply, SlaveError> {
    let Some(source) = apply::try_read_file_or_link(host).map_err(SlaveError::io(host))? else {
        return Ok(Reply::Send(vec![ProtocolMessage::Error {
            code: "missing_hash".into(),
            message: path.as_str().into(),
        }]));
    };
    match transfer::fulfill(checkout_id, path.clone(), want_hash, &source, signature) {
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

fn into_slave_walk_err<E: std::error::Error + Send + Sync + 'static>(
    err: WalkError<E>,
) -> SlaveError {
    match err {
        WalkError::Io { path, source } => SlaveError::Io { path, source },
        WalkError::Index(err) => SlaveError::index(err),
        WalkError::Path(err) => err.into(),
    }
}

fn reject_resolved_overlap(checkouts: &HashMap<String, Checkout>) -> Result<(), SlaveError> {
    let locals: HashMap<String, PathBuf> = checkouts
        .iter()
        .map(|(id, checkout)| (id.clone(), checkout.local.clone()))
        .collect();
    reject_resolved_locals(&locals)
}

fn reject_resolved_locals(locals: &HashMap<String, PathBuf>) -> Result<(), SlaveError> {
    let paths: Vec<&PathBuf> = locals.values().collect();
    for (i, a) in paths.iter().enumerate() {
        for b in paths.iter().skip(i + 1) {
            if local_paths_overlap(a, b) {
                return Err(ConfigError::LocalOverlap {
                    a: a.display().to_string(),
                    b: b.display().to_string(),
                }
                .into());
            }
        }
    }
    Ok(())
}

fn is_reserved(central: &CanonicalPath, path: &CanonicalPath) -> bool {
    strip_central(central, path).is_ok_and(|relative| relative.has_reserved_root_name())
}

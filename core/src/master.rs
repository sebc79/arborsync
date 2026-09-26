//! One synchronous writer of the global index.
//! The binary serializes QUIC, `notify`, and rate limits into
//! [`Master::handle`], [`Master::prepare_delete`], [`Master::note_local`],
//! and [`Master::poll`].

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::apply;
use crate::bottleneck::{Gauge, Stage, Waiting};
use crate::config::{LoadedMaster, MasterReload, ReloadError};
use crate::hash::{ContentHash, FileNode, SubtreeRoot};
use crate::hashing::{HashDone, HashKey, HashNeed, HashOutcome, HashPlan};
use crate::index;
use crate::inflight::Inflight;
use crate::keys::format_hex_key;
use crate::merkle::file_node;
use crate::meta::{self, hash_bytes, EntryKind, FileMetadata, Inspected};
use crate::path::{
    canonical_to_host, confine_host, is_reserved_root_entry, join_central, strip_central,
    CanonicalPath, EntryName, PathError,
};
use crate::protocol::{page_dir_list, BulkHeader, CheckoutAck, CheckoutRef, ProtocolMessage};
use crate::status::{MasterStatus, PeerLive, Queues, StatusLedger};
use crate::storage::{CheckoutId, Storage};
use crate::transfer::{self, BulkTransfer};
use crate::tune::Tune;
use crate::watch::LocalEvent;

pub use crate::apply::{ApplyError, ContentBytes, ContentHook, MemoryContent, WholeFileLater};

/// Checkout id on the wire. Distinct from [`CheckoutId`], the index namespace.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CheckoutName(String);

impl CheckoutName {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SlaveId(String);

impl SlaveId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct InterestKey {
    slave: SlaveId,
    checkout: CheckoutName,
}

#[derive(Clone, Debug)]
pub enum Origin {
    Slave {
        slave: SlaveId,
        checkout: CheckoutName,
    },
    Local,
}

impl Origin {
    fn committed(&self, key: &InterestKey) -> bool {
        match self {
            Self::Slave { slave, checkout } => slave == &key.slave && checkout == &key.checkout,
            Self::Local => false,
        }
    }
}

#[derive(Debug)]
pub enum Reply {
    Send(ProtocolMessage),
    /// Inbound control that needs no response (for example a peer `Error`).
    Quiet,
    Hangup { reason: String, rate_limit: bool },
    Bulk(BulkTransfer),
}

/// `Reclaim` means the index commit and `CasAccept` already happened. The
/// caller deletes the live tree, then [`Master::finish_wipe`].
#[derive(Debug)]
pub enum PreparedDelete {
    Reply(ProtocolMessage),
    Later {
        checkout_id: String,
        path: CanonicalPath,
        basis: FileNode,
    },
    Reclaim {
        reply: ProtocolMessage,
        root: PathBuf,
        path: CanonicalPath,
    },
}

pub enum FulfillPlan {
    BulkHost {
        host: PathBuf,
        checkout_id: String,
        path: CanonicalPath,
        want_hash: ContentHash,
        signature: Vec<u8>,
    },
    Send(ProtocolMessage),
}

/// Work that can run without the master mutex: copia reconstruct + BLAKE3.
pub enum ApplyBulkPlan {
    Done(Reply),
    Reconstruct(ApplyBulkJob),
}

pub struct ApplyBulkJob {
    key: (String, CanonicalPath),
    encoding: crate::protocol::BulkEncoding,
    basis: Option<Vec<u8>>,
    want_hash: ContentHash,
}

pub enum ApplyBulkOutcome {
    Ready {
        key: (String, CanonicalPath),
        bytes: Vec<u8>,
    },
    Failed {
        key: (String, CanonicalPath),
        path: CanonicalPath,
    },
}

impl ApplyBulkJob {
    pub fn reconstruct(self, body: &[u8]) -> ApplyBulkOutcome {
        let path = self.key.1.clone();
        match transfer::reconstruct(self.encoding, body, self.basis.as_deref()) {
            Ok(bytes) if hash_bytes(&bytes) == self.want_hash => ApplyBulkOutcome::Ready {
                key: self.key,
                bytes,
            },
            _ => ApplyBulkOutcome::Failed {
                key: self.key,
                path,
            },
        }
    }
}

impl FulfillPlan {
    pub fn run(self) -> Result<Reply, MasterError> {
        match self {
            Self::Send(msg) => Ok(Reply::Send(msg)),
            Self::BulkHost {
                host,
                checkout_id,
                path,
                want_hash,
                signature,
            } => {
                let Some(source) =
                    apply::try_read_file_or_link(&host).map_err(MasterError::io(&host))?
                else {
                    return Ok(Reply::Send(missing_hash(&path)));
                };
                match transfer::fulfill(checkout_id, path.clone(), want_hash, &source, &signature) {
                    Ok(xfer) => Ok(Reply::Bulk(xfer)),
                    Err(transfer::TransferError::HashMismatch) => {
                        Ok(Reply::Send(missing_hash(&path)))
                    }
                    Err(err) => Ok(Reply::Send(ProtocolMessage::Error {
                        code: "transfer".into(),
                        message: err.to_string(),
                    })),
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MasterError {
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
    #[error("central_root holds a name that is not a canonical path component: {0}")]
    BadHostName(#[from] PathError),
}

impl MasterError {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasDecision {
    Accept,
    Reject { current: Option<FileMetadata> },
}

/// Pure CAS check. `new_kind` is `None` for a delete.
pub fn decide_cas(
    current: Option<&FileMetadata>,
    basis: Option<FileNode>,
    new_kind: Option<EntryKind>,
) -> CasDecision {
    let reject = || CasDecision::Reject {
        current: current.cloned(),
    };
    match (current, basis) {
        (None, None) if new_kind.is_some() => CasDecision::Accept,
        (Some(live), Some(basis)) => {
            if file_node(live) != basis {
                reject()
            } else {
                CasDecision::Accept
            }
        }
        _ => reject(),
    }
}

struct LiveSlave {
    peer: [u8; 32],
    checkouts: HashMap<CheckoutName, CanonicalPath>,
    outbox: Waiting<u64, ProtocolMessage>,
    next_seq: u64,
    writable: bool,
}

#[derive(Default)]
struct Roster {
    by_slave: HashMap<SlaveId, LiveSlave>,
    by_peer: HashMap<[u8; 32], SlaveId>,
    by_central: HashMap<CanonicalPath, HashSet<InterestKey>>,
}

impl Roster {
    fn install(
        &mut self,
        peer: [u8; 32],
        slave: SlaveId,
        checkouts: HashMap<CheckoutName, CanonicalPath>,
    ) -> Option<[u8; 32]> {
        if let Some(previous) = self.by_peer.get(&peer).cloned() {
            self.forget(&previous);
        }
        let displaced = self.by_slave.get(&slave).map(|live| live.peer);
        self.forget(&slave);
        for (checkout, central) in &checkouts {
            self.by_central
                .entry(central.clone())
                .or_default()
                .insert(InterestKey {
                    slave: slave.clone(),
                    checkout: checkout.clone(),
                });
        }
        self.by_peer.insert(peer, slave.clone());
        self.by_slave.insert(
            slave,
            LiveSlave {
                peer,
                checkouts,
                outbox: Waiting::new(Stage::Fanout),
                next_seq: 0,
                writable: true,
            },
        );
        displaced.filter(|old| old != &peer)
    }

    fn forget(&mut self, slave: &SlaveId) {
        let Some(live) = self.by_slave.remove(slave) else {
            return;
        };
        self.by_peer.remove(&live.peer);
        for (checkout, central) in &live.checkouts {
            let Some(keys) = self.by_central.get_mut(central) else {
                continue;
            };
            keys.remove(&InterestKey {
                slave: slave.clone(),
                checkout: checkout.clone(),
            });
            if keys.is_empty() {
                self.by_central.remove(central);
            }
        }
    }

    fn drop_checkout(&mut self, slave: &SlaveId, checkout: &CheckoutName) {
        let Some(live) = self.by_slave.get_mut(slave) else {
            return;
        };
        let Some(central) = live.checkouts.remove(checkout) else {
            return;
        };
        let Some(keys) = self.by_central.get_mut(&central) else {
            return;
        };
        keys.remove(&InterestKey {
            slave: slave.clone(),
            checkout: checkout.clone(),
        });
        if keys.is_empty() {
            self.by_central.remove(&central);
        }
    }

    fn disconnect_peer(&mut self, peer: &[u8; 32]) {
        let Some(slave) = self.by_peer.get(peer).cloned() else {
            return;
        };
        self.forget(&slave);
    }

    fn slave_of(&self, peer: &[u8; 32]) -> Option<&SlaveId> {
        self.by_peer.get(peer)
    }

    fn checkout_central(&self, peer: &[u8; 32], ck: &CheckoutName) -> Option<&CanonicalPath> {
        self.by_slave
            .get(self.by_peer.get(peer)?)?
            .checkouts
            .get(ck)
    }

    fn interested(&self, path: &CanonicalPath) -> Vec<InterestKey> {
        std::iter::once(path.clone())
            .chain(path.ancestors())
            .filter_map(|candidate| self.by_central.get(&candidate))
            .flatten()
            .cloned()
            .collect()
    }

    fn push(&mut self, key: &InterestKey, msg: ProtocolMessage) {
        let Some(live) = self.by_slave.get_mut(&key.slave) else {
            return;
        };
        if live.writable {
            live.outbox.insert(live.next_seq, msg, Instant::now());
            live.next_seq += 1;
        }
    }

    fn central_of(&self, key: &InterestKey) -> Option<&CanonicalPath> {
        self.by_slave.get(&key.slave)?.checkouts.get(&key.checkout)
    }

    fn push_peer_checkout(
        &mut self,
        peer: &[u8; 32],
        checkout: &CheckoutName,
        msg: ProtocolMessage,
    ) {
        let Some(slave) = self.by_peer.get(peer).cloned() else {
            return;
        };
        self.push(
            &InterestKey {
                slave,
                checkout: checkout.clone(),
            },
            msg,
        );
    }

    fn take_outbox(&mut self, peer: &[u8; 32]) -> Vec<ProtocolMessage> {
        let Some(slave) = self.by_peer.get(peer).cloned() else {
            return Vec::new();
        };
        match self.by_slave.get_mut(&slave) {
            Some(live) => live.outbox.drain_ordered(),
            None => Vec::new(),
        }
    }

    fn set_writable(&mut self, peer: &[u8; 32], writable: bool) -> u64 {
        let Some(slave) = self.by_peer.get(peer).cloned() else {
            return 0;
        };
        let Some(live) = self.by_slave.get_mut(&slave) else {
            return 0;
        };
        live.writable = writable;
        if writable {
            return 0;
        }
        let dropped = live.outbox.len() as u64;
        live.outbox.clear();
        dropped
    }
}

struct Session {
    slave: SlaveId,
    central: CanonicalPath,
}

enum NotSubscribed {
    NoSession,
    UnknownCheckout,
}

impl NotSubscribed {
    fn into_error(self, checkout: &CheckoutName) -> ProtocolMessage {
        ProtocolMessage::Error {
            code: "not_subscribed".into(),
            message: match self {
                Self::NoSession => "no live session for this key".into(),
                Self::UnknownCheckout => {
                    format!("checkout {} is not in the live set", checkout.as_str())
                }
            },
        }
    }
}

enum Fanout {
    Announce {
        new: FileMetadata,
        basis: Option<FileNode>,
    },
    Remove {
        basis: FileNode,
    },
    Rename {
        from: CanonicalPath,
        to: CanonicalPath,
        from_basis: FileNode,
        to_new: FileMetadata,
    },
}

impl Fanout {
    fn to_message(&self, checkout_id: String, path: &CanonicalPath) -> ProtocolMessage {
        match self {
            Self::Announce { new, basis } => ProtocolMessage::FileAnnounce {
                checkout_id,
                path: path.clone(),
                new: new.clone(),
                basis: *basis,
            },
            Self::Remove { basis } => ProtocolMessage::Delete {
                checkout_id,
                path: path.clone(),
                basis: *basis,
            },
            Self::Rename {
                from,
                to,
                from_basis,
                to_new,
            } => ProtocolMessage::Rename {
                checkout_id,
                from: from.clone(),
                to: to.clone(),
                from_basis: *from_basis,
                to_new: to_new.clone(),
            },
        }
    }
}

struct PendingApply {
    peer: [u8; 32],
    checkout_id: String,
    path: CanonicalPath,
    new: FileMetadata,
    /// Basis from the announce. Checked again at bulk finish against the live node.
    basis: Option<FileNode>,
    origin: Origin,
    previous: Option<FileMetadata>,
    retried: bool,
}

pub struct CentralWalk {
    pub ready: BTreeMap<CanonicalPath, FileMetadata>,
    pub needs: Vec<HashNeed>,
}

struct WipeSet {
    paths: Vec<CanonicalPath>,
}

impl WipeSet {
    fn overlaps(&self, path: &CanonicalPath) -> bool {
        self.paths
            .iter()
            .any(|wipe| wipe.covers(path) || path.covers(wipe))
    }

    fn covers(&self, path: &CanonicalPath) -> bool {
        self.paths.iter().any(|wipe| wipe.covers(path))
    }

    fn start(&mut self, path: CanonicalPath) {
        self.paths.push(path);
    }

    fn finish(&mut self, path: &CanonicalPath) {
        self.paths.retain(|wipe| wipe != path);
    }
}

enum DeleteClass {
    Reply(ProtocolMessage),
    Accepted {
        path: CanonicalPath,
        previous: FileMetadata,
        accept: ProtocolMessage,
        origin: Origin,
    },
}

pub struct Master<S: Storage, C: ContentHook> {
    cfg: LoadedMaster,
    store: S,
    central_root: PathBuf,
    roster: Roster,
    inflight: Inflight,
    bodies: C,
    pending: Waiting<(String, CanonicalPath), PendingApply>,
    hashing: Waiting<HashKey, ()>,
    dirs: index::DirChildren,
    status: StatusLedger,
    wipes: WipeSet,
}

impl<S: Storage, C: ContentHook> Master<S, C> {
    pub fn open(cfg: LoadedMaster, store: S, bodies: C) -> Result<Self, MasterError> {
        let configured = cfg.central_root().to_path_buf();
        fs::create_dir_all(&configured).map_err(MasterError::io(&configured))?;
        let central_root = fs::canonicalize(&configured).map_err(MasterError::io(&configured))?;
        apply::wipe_tmp(&central_root)?;

        let debounce = Duration::from_millis(cfg.watcher_debounce_ms());
        let mut master = Self {
            cfg,
            store,
            central_root,
            roster: Roster::default(),
            inflight: Inflight::new(debounce),
            bodies,
            pending: Waiting::new(Stage::OriginBytes),
            hashing: Waiting::new(Stage::Hashing),
            dirs: index::DirChildren::default(),
            status: StatusLedger::default(),
            wipes: WipeSet { paths: Vec::new() },
        };
        if index::root_is_dirty(&master.store, &CheckoutId::master()).map_err(MasterError::index)? {
            master.rescan()?;
        }
        Ok(master)
    }

    pub fn handle(&mut self, peer: [u8; 32], msg: ProtocolMessage) -> Result<Reply, MasterError> {
        let slave = self.status_slave(&peer);
        self.status.inbound(slave.as_deref(), &msg);
        let reply = self.handle_message(peer, msg)?;
        self.record_reply(slave.as_deref(), &reply);
        Ok(reply)
    }

    fn handle_message(
        &mut self,
        peer: [u8; 32],
        msg: ProtocolMessage,
    ) -> Result<Reply, MasterError> {
        if self.cfg.acl_for_public_key(&peer).is_none() {
            return Ok(Reply::Hangup {
                reason: "unknown static key".into(),
                rate_limit: true,
            });
        }
        match msg {
            ProtocolMessage::Subscribe {
                slave_id,
                checkouts,
            } => {
                if let Some(acl) = self.cfg.acl_for_public_key(&peer) {
                    if acl.id() != slave_id {
                        return Ok(Reply::Hangup {
                            reason: format!("slave_id {slave_id} is not bound to this key"),
                            rate_limit: true,
                        });
                    }
                }
                Ok(Reply::Send(self.on_subscribe(peer, slave_id, checkouts)?))
            }
            ProtocolMessage::FileAnnounce {
                checkout_id,
                path,
                new,
                basis,
            } => Ok(Reply::Send(self.on_announce(
                peer,
                checkout_id,
                path,
                new,
                basis,
            )?)),
            ProtocolMessage::Delete {
                checkout_id,
                path,
                basis,
            } => Ok(Reply::Send(self.on_delete(
                peer,
                checkout_id,
                path,
                basis,
            )?)),
            ProtocolMessage::Rename {
                checkout_id,
                from,
                to,
                from_basis,
                to_new,
            } => Ok(Reply::Send(self.on_rename(
                peer,
                checkout_id,
                from,
                to,
                from_basis,
                to_new,
            )?)),
            ProtocolMessage::Disconnect { .. } => {
                self.disconnect(peer);
                Ok(Reply::Hangup {
                    reason: "peer disconnect".into(),
                    rate_limit: false,
                })
            }
            ProtocolMessage::Error { code, message } => {
                let slave = self.status_slave(&peer);
                self.status
                    .error(slave.as_deref(), format!("error:{code}:{message}"));
                Ok(Reply::Quiet)
            }
            ProtocolMessage::SignatureRequest {
                checkout_id,
                path,
                want_hash,
                signature,
            } => self
                .plan_fulfill(peer, checkout_id, path, want_hash, signature)?
                .run(),
            ProtocolMessage::RootReport {
                checkout_id,
                path,
                root,
            } => Ok(Reply::Send(self.on_root_report(
                peer,
                checkout_id,
                path,
                root,
            )?)),
            ProtocolMessage::DirListRequest {
                checkout_id,
                path,
                after,
            } => Ok(Reply::Send(self.on_dir_list(
                peer,
                checkout_id,
                path,
                after,
            )?)),
            other => Ok(Reply::Send(ProtocolMessage::Error {
                code: "unsupported".into(),
                message: format!("{other:?}"),
            })),
        }
    }

    pub fn apply_bulk(
        &mut self,
        peer: [u8; 32],
        header: BulkHeader,
        body: &[u8],
    ) -> Result<Reply, MasterError> {
        self.begin_apply_bulk(peer, body.len() as u64);
        let reply = match self.prepare_apply_bulk(peer, header)? {
            ApplyBulkPlan::Done(reply) => reply,
            ApplyBulkPlan::Reconstruct(job) => {
                self.finish_apply_bulk(peer, job.reconstruct(body))?
            }
        };
        self.end_apply_bulk(peer, &reply);
        Ok(reply)
    }

    pub fn begin_apply_bulk(&mut self, peer: [u8; 32], bytes: u64) {
        let slave = self.status_slave(&peer);
        self.status.bulk_in(slave.as_deref(), bytes);
    }

    pub fn end_apply_bulk(&mut self, peer: [u8; 32], reply: &Reply) {
        let slave = self.status_slave(&peer);
        match reply {
            Reply::Send(ProtocolMessage::CasAccept { .. }) => {
                self.status.apply_ok(slave.as_deref());
            }
            Reply::Send(ProtocolMessage::Error { .. }) => {
                self.status.apply_fail(slave.as_deref());
            }
            _ => {}
        }
        self.record_reply(slave.as_deref(), reply);
    }

    /// Validate the pending apply and read the local basis under the mutex.
    /// Copia reconstruct and BLAKE3 run on [`ApplyBulkJob::reconstruct`] off the mutex.
    pub fn prepare_apply_bulk(
        &mut self,
        peer: [u8; 32],
        header: BulkHeader,
    ) -> Result<ApplyBulkPlan, MasterError> {
        let key = (header.checkout_id.clone(), header.path.clone());
        if self.inside_wipe(&header.path) {
            self.pending.remove(&key);
            return Ok(ApplyBulkPlan::Done(Reply::Send(wiping(&header.path))));
        }
        let Some(pending) = self.pending.get(&key) else {
            return Ok(ApplyBulkPlan::Done(self.accept_if_live_matches(&header)?));
        };
        if pending.peer != peer || pending.new.content_hash != header.want_hash {
            return Ok(ApplyBulkPlan::Done(self.accept_if_live_matches(&header)?));
        }
        let host = canonical_to_host(&self.central_root, &header.path);
        let basis = if pending
            .previous
            .as_ref()
            .is_some_and(|prev| prev.kind != pending.new.kind)
        {
            None
        } else {
            apply::try_read_file_or_link(&host).map_err(MasterError::io(&host))?
        };
        Ok(ApplyBulkPlan::Reconstruct(ApplyBulkJob {
            key,
            encoding: header.encoding,
            basis,
            want_hash: header.want_hash,
        }))
    }

    pub fn finish_apply_bulk(
        &mut self,
        peer: [u8; 32],
        outcome: ApplyBulkOutcome,
    ) -> Result<Reply, MasterError> {
        match outcome {
            ApplyBulkOutcome::Ready { key, bytes } => {
                let Some(pending) = self.pending.get(&key) else {
                    return self.accept_if_live_matches(&BulkHeader {
                        checkout_id: key.0,
                        path: key.1,
                        want_hash: hash_bytes(&bytes),
                        encoding: crate::protocol::BulkEncoding::Whole,
                        size: bytes.len() as u64,
                    });
                };
                if pending.peer != peer || hash_bytes(&bytes) != pending.new.content_hash {
                    return self.accept_if_live_matches(&BulkHeader {
                        checkout_id: key.0.clone(),
                        path: key.1.clone(),
                        want_hash: pending.new.content_hash,
                        encoding: crate::protocol::BulkEncoding::Whole,
                        size: bytes.len() as u64,
                    });
                }
                let pending = self.pending.remove(&key).expect("pending");
                let live = self.meta(&pending.path)?;
                if let CasDecision::Reject { current } =
                    decide_cas(live.as_ref(), pending.basis, Some(pending.new.kind))
                {
                    return Ok(Reply::Send(ProtocolMessage::CasReject {
                        checkout_id: pending.checkout_id,
                        path: pending.path,
                        current,
                    }));
                }
                self.publish(&pending.path, &pending.new, live.as_ref(), &bytes)?;
                self.commit(
                    &pending.origin,
                    &pending.path,
                    Some(&pending.new),
                    live.as_ref(),
                )?;
                Ok(Reply::Send(ProtocolMessage::CasAccept {
                    checkout_id: pending.checkout_id,
                    path: pending.path,
                    file_node: Some(file_node(&pending.new)),
                }))
            }
            ApplyBulkOutcome::Failed { key, path } => {
                let Some(pending) = self.pending.get_mut(&key) else {
                    return Ok(Reply::Send(ProtocolMessage::Error {
                        code: "unknown_transfer".into(),
                        message: path.as_str().into(),
                    }));
                };
                if pending.retried {
                    self.pending.remove(&key);
                    return Ok(Reply::Send(ProtocolMessage::Error {
                        code: "keep_live".into(),
                        message: path.as_str().into(),
                    }));
                }
                pending.retried = true;
                Ok(Reply::Send(ProtocolMessage::SignatureRequest {
                    checkout_id: pending.checkout_id.clone(),
                    path: pending.path.clone(),
                    want_hash: pending.new.content_hash,
                    signature: Vec::new(),
                }))
            }
        }
    }

    fn accept_if_live_matches(&self, header: &BulkHeader) -> Result<Reply, MasterError> {
        let host = canonical_to_host(&self.central_root, &header.path);
        let live = apply::try_read_file_or_link(&host).map_err(MasterError::io(&host))?;
        if live
            .as_deref()
            .is_some_and(|bytes| hash_bytes(bytes) == header.want_hash)
        {
            let node = self.meta(&header.path)?.as_ref().map(file_node);
            return Ok(Reply::Send(ProtocolMessage::CasAccept {
                checkout_id: header.checkout_id.clone(),
                path: header.path.clone(),
                file_node: node,
            }));
        }
        Ok(Reply::Send(ProtocolMessage::Error {
            code: "unknown_transfer".into(),
            message: header.path.as_str().into(),
        }))
    }

    pub fn tune(&self) -> &Tune {
        self.cfg.tune()
    }

    pub fn plan_local(&mut self, event: LocalEvent) -> Result<HashPlan, MasterError> {
        if self.event_inside_wipe(&event) {
            self.status.local(None);
            return Ok(HashPlan::default());
        }
        let plan = match event {
            LocalEvent::Changed(path) => self.note_changed(path)?,
            LocalEvent::Metadata(path) => self.note_metadata(path)?,
            LocalEvent::Removed(path) => {
                self.note_removed(&path)?;
                HashPlan::default()
            }
            LocalEvent::Renamed { from, to } => self.note_renamed(from, to)?,
        };
        self.status.local(None);
        Ok(plan)
    }

    pub fn note_local(&mut self, event: LocalEvent) -> Result<(), MasterError> {
        let plan = self.plan_local(event)?;
        self.drain_plan(plan)
    }

    pub fn commit_hashed(&mut self, done: HashDone) -> Result<HashPlan, MasterError> {
        self.hashing.remove(&done.key);
        let HashKey::Central(path) = done.key else {
            return Ok(HashPlan::default());
        };
        if self.inside_wipe(&path) {
            return Ok(HashPlan::default());
        }
        match done.outcome {
            HashOutcome::File(found) => {
                if self.meta(&path)?.as_ref() == Some(&found) {
                    return Ok(HashPlan::default());
                }
                let current = self.meta(&path)?;
                if current.as_ref() != done.previous.as_ref() {
                    return self.note_changed(path);
                }
                self.note_present(path, found)?;
                Ok(HashPlan::default())
            }
            HashOutcome::Absent => {
                self.note_removed(&path)?;
                Ok(HashPlan::default())
            }
            HashOutcome::Io(kind) => {
                log::warn!("hash {}: {kind}", path.as_str());
                Ok(HashPlan::default())
            }
        }
    }

    fn drain_plan(&mut self, plan: HashPlan) -> Result<(), MasterError> {
        for need in plan.hash {
            self.hashing.remove(&need.key);
            let next = self.commit_hashed(need.run())?;
            self.drain_plan(next)?;
        }
        Ok(())
    }

    pub fn start_hashed(&mut self, key: &HashKey) {
        self.hashing.remove(key);
    }

    pub fn rescan(&mut self) -> Result<(), MasterError> {
        let walk = self.walk_central_stats()?;
        let plan = self.adopt_survey(walk)?;
        for need in plan.hash {
            self.hashing.remove(&need.key);
            let next = self.commit_hashed(need.run())?;
            self.drain_plan(next)?;
        }
        Ok(())
    }

    pub fn plan_rescan(&mut self) -> Result<HashPlan, MasterError> {
        let walk = survey_central(&self.central_root, &self.store)?;
        self.adopt_survey(walk)
    }

    /// Commit a survey taken without the session mutex.
    ///
    /// A row is removed only when the path is still missing. A surveyed row is
    /// written only when a fresh stat still matches it. A file that appeared
    /// or changed while the walk ran is left for the session that wrote it.
    pub fn adopt_survey(&mut self, walk: CentralWalk) -> Result<HashPlan, MasterError> {
        self.apply_survey(&walk)?;
        let mut plan = HashPlan::default();
        for need in walk.needs {
            if !self.still_needs_hash(&need)? {
                continue;
            }
            self.hashing.insert(need.key.clone(), (), Instant::now());
            plan.hash.push(need);
        }
        self.status.rescan(None);
        Ok(plan)
    }

    fn apply_survey(&mut self, walk: &CentralWalk) -> Result<(), MasterError> {
        let indexed = self
            .store
            .range_meta(&CheckoutId::master(), &CanonicalPath::root())
            .map_err(MasterError::index)?;
        let hashing: HashSet<_> = walk
            .needs
            .iter()
            .filter_map(|need| match &need.key {
                HashKey::Central(path) => Some(path.clone()),
                HashKey::Checkout { .. } => None,
            })
            .collect();
        for (path, previous) in indexed {
            if self.inside_wipe(&path) || walk.ready.contains_key(&path) || hashing.contains(&path)
            {
                continue;
            }
            match stat_on_disk(&self.central_root, &path)? {
                OnDisk::Gone => {}
                OnDisk::Here | OnDisk::Unreadable => continue,
            }
            if self.meta(&path)?.is_none() {
                continue;
            }
            self.commit(&Origin::Local, &path, None, Some(&previous))?;
        }
        for (path, found) in &walk.ready {
            if self.inside_wipe(path) {
                continue;
            }
            let host = canonical_to_host(&self.central_root, path);
            match stat_on_disk(&self.central_root, path)? {
                OnDisk::Gone => {
                    if let Some(previous) = self.meta(path)? {
                        self.commit(&Origin::Local, path, None, Some(&previous))?;
                    }
                    continue;
                }
                OnDisk::Unreadable => continue,
                OnDisk::Here => {}
            }
            let Inspected::Ready(live) =
                meta::inspect_for_hash(&host, Some(found)).map_err(MasterError::io(&host))?
            else {
                continue;
            };
            if &live != found {
                continue;
            }
            let current = self.meta(path)?;
            if current.as_ref() == Some(&live) {
                continue;
            }
            self.commit(&Origin::Local, path, Some(&live), current.as_ref())?;
        }
        Ok(())
    }

    fn still_needs_hash(&self, need: &HashNeed) -> Result<bool, MasterError> {
        let HashKey::Central(path) = &need.key else {
            return Ok(true);
        };
        if self.inside_wipe(path) {
            return Ok(false);
        }
        let host = canonical_to_host(&self.central_root, path);
        match stat_on_disk(&self.central_root, path)? {
            OnDisk::Gone | OnDisk::Unreadable => return Ok(false),
            OnDisk::Here => {}
        }
        match meta::inspect_for_hash(&host, need.previous.as_ref())
            .map_err(MasterError::io(&host))?
        {
            Inspected::NeedHash(_) => Ok(true),
            Inspected::Ready(_) | Inspected::Absent => Ok(false),
        }
    }

    /// A slave's own verdict, off the session's datagram channel. Garbage, an
    /// unknown peer, and an old sequence are all dropped in silence. Nothing
    /// here can fail a session.
    pub fn observe_gauge(&mut self, peer: [u8; 32], frame: &[u8]) {
        let Some(gauge) = Gauge::decode(frame) else {
            return;
        };
        let Some(slave) = self.live_slave_id(&peer) else {
            return;
        };
        self.status.observe_gauge(&slave, gauge, Instant::now());
    }

    pub fn disconnect(&mut self, peer: [u8; 32]) {
        if let Some(slave) = self.live_slave_id(&peer) {
            self.status.forget_gauge(&slave);
        }
        self.roster.disconnect_peer(&peer);
        self.pending.retain(|_, row| row.peer != peer);
    }

    /// The id of a peer that has subscribed, which is the only peer a gauge can
    /// belong to. Unlike `status_slave`, an ACL row alone is not enough.
    fn live_slave_id(&self, peer: &[u8; 32]) -> Option<String> {
        self.roster.slave_of(peer).map(|id| id.as_str().to_string())
    }

    pub fn poll(&mut self, peer: [u8; 32]) -> Vec<ProtocolMessage> {
        let msgs = self.roster.take_outbox(&peer);
        let slave = self.status_slave(&peer);
        self.status.flushed(slave.as_deref(), msgs.len() as u64);
        msgs
    }

    pub fn set_writable(&mut self, peer: [u8; 32], writable: bool) {
        let dropped = self.roster.set_writable(&peer, writable);
        let slave = self.status_slave(&peer);
        self.status.fanout_dropped(slave.as_deref(), dropped);
    }

    pub fn central_root(&self) -> &std::path::Path {
        &self.central_root
    }

    pub fn storage_handle(&self) -> S
    where
        S: Clone,
    {
        self.store.clone()
    }

    pub fn authorize_peer(&self, peer: &[u8; 32]) -> Option<&str> {
        self.cfg.acl_for_public_key(peer).map(|acl| acl.id())
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

    pub fn take_status(&mut self) -> MasterStatus {
        let live = self
            .roster
            .by_slave
            .iter()
            .map(|(id, session)| {
                let oldest = self.pending.oldest_where(|_, row| row.peer == session.peer);
                let pending = oldest.as_ref().map_or(0, |wait| wait.depth);
                PeerLive {
                    slave_id: id.as_str().to_string(),
                    checkouts: session.checkouts.len(),
                    queues: Queues {
                        outbox: session.outbox.len(),
                        pending,
                        writable: session.writable,
                        ..Queues::default()
                    },
                    waits: [oldest, session.outbox.oldest()]
                        .into_iter()
                        .flatten()
                        .map(|wait| wait.about(id.as_str()))
                        .collect(),
                }
            })
            .collect();
        self.status.take_master(
            live,
            self.hashing.oldest().into_iter().collect(),
            self.cfg.status_interval_seconds(),
        )
    }

    pub fn note_status_error(&mut self, slave: Option<&str>, reason: impl Into<String>) {
        self.status.error(slave, reason);
    }

    fn status_slave(&self, peer: &[u8; 32]) -> Option<String> {
        self.cfg
            .acl_for_public_key(peer)
            .map(|acl| acl.id().to_string())
            .or_else(|| self.roster.slave_of(peer).map(|id| id.as_str().to_string()))
    }

    fn record_reply(&mut self, slave: Option<&str>, reply: &Reply) {
        match reply {
            Reply::Send(msg) => self.status.outbound(slave, msg),
            Reply::Quiet => {}
            Reply::Hangup { reason, .. } => {
                self.status.error(slave, format!("hangup:{reason}"));
            }
            Reply::Bulk(xfer) => self.status.bulk_out(slave, xfer.body.len() as u64),
        }
    }

    pub fn log_level(&self) -> &str {
        self.cfg.log_level()
    }

    pub fn reload(&mut self, next: LoadedMaster) -> Result<MasterReload, ReloadError> {
        let mut plan = self.cfg.plan_reload(&next)?;
        let debounce_changed = next.watcher_debounce_ms() != self.cfg.watcher_debounce_ms();
        self.cfg = next;
        if debounce_changed {
            self.inflight
                .set_window(Duration::from_millis(self.cfg.watcher_debounce_ms()));
        }
        let live: Vec<(SlaveId, [u8; 32], Vec<(CheckoutName, CanonicalPath)>)> = self
            .roster
            .by_slave
            .iter()
            .map(|(id, session)| {
                (
                    id.clone(),
                    session.peer,
                    session
                        .checkouts
                        .iter()
                        .map(|(checkout, central)| (checkout.clone(), central.clone()))
                        .collect(),
                )
            })
            .collect();
        let mut drop_peers = Vec::new();
        for (slave, peer, checkouts) in live {
            if self.authorize_peer(&peer).is_none() {
                drop_peers.push(peer);
                plan.drop_slave_ids.push(slave.as_str().to_string());
                self.roster.forget(&slave);
                continue;
            }
            for (checkout, central) in checkouts {
                if !self
                    .cfg
                    .acl_for_id(slave.as_str())
                    .is_some_and(|acl| acl.allows_central(&central))
                {
                    self.roster.drop_checkout(&slave, &checkout);
                }
            }
        }
        plan.drop_peers = drop_peers;
        plan.drop_slave_ids.sort();
        plan.drop_slave_ids.dedup();
        Ok(plan)
    }

    pub fn max_connections(&self) -> u32 {
        self.cfg.max_connections()
    }

    pub fn max_attempts_per_minute(&self) -> u32 {
        self.cfg.max_connection_attempts_per_minute()
    }

    pub fn meta(&self, path: &CanonicalPath) -> Result<Option<FileMetadata>, MasterError> {
        self.store
            .get_meta(&CheckoutId::master(), path)
            .map_err(MasterError::index)
    }

    fn on_subscribe(
        &mut self,
        peer: [u8; 32],
        slave_id: String,
        checkouts: Vec<CheckoutRef>,
    ) -> Result<ProtocolMessage, MasterError> {
        let Some(acl) = self.cfg.acl_for_public_key(&peer) else {
            return Ok(reject_all(&checkouts, "unknown static key"));
        };
        if checkouts.len() > self.cfg.max_checkouts_per_slave() as usize {
            return Ok(reject_all(&checkouts, "too many checkouts"));
        }

        let mut live: HashMap<CheckoutName, CanonicalPath> = HashMap::new();
        let mut denied = Vec::new();
        for checkout in &checkouts {
            if !acl.allows_central(&checkout.central) {
                denied.push(checkout.central.clone());
                continue;
            }
            if live
                .insert(CheckoutName::new(&checkout.id), checkout.central.clone())
                .is_some()
            {
                return Ok(reject_all(
                    &checkouts,
                    &format!("duplicate checkout id {}", checkout.id),
                ));
            }
        }
        if !denied.is_empty() {
            return Ok(ProtocolMessage::SubscribeReject {
                reason: "central is outside allowed_prefixes".into(),
                denied_centrals: denied,
            });
        }

        let slave = SlaveId::new(slave_id);
        if let Some(displaced) = self.roster.install(peer, slave.clone(), live) {
            log::info!(
                "slave {} replaced its session; {} is no longer live",
                slave.as_str(),
                format_hex_key(&displaced)
            );
        }
        let mut acks = Vec::with_capacity(checkouts.len());
        for checkout in checkouts {
            acks.push(CheckoutAck {
                master_root: index::subtree_root(
                    &self.store,
                    &CheckoutId::master(),
                    &checkout.central,
                )
                .map_err(MasterError::index)?,
                id: checkout.id,
                central: checkout.central,
            });
        }
        Ok(ProtocolMessage::SubscribeAck { checkouts: acks })
    }

    fn on_announce(
        &mut self,
        peer: [u8; 32],
        checkout_id: String,
        path: CanonicalPath,
        new: FileMetadata,
        basis: Option<FileNode>,
    ) -> Result<ProtocolMessage, MasterError> {
        let checkout = CheckoutName::new(checkout_id.clone());
        let session = match self.live_checkout(&peer, &checkout) {
            Ok(session) => session,
            Err(refusal) => return Ok(refusal.into_error(&checkout)),
        };
        if !session.central.covers(&path) {
            return Ok(outside_central(&path));
        }
        if path_is_reserved(&session.central, &path) {
            return Ok(reserved_name(&path));
        }
        if self.overlaps_wipe(&path) {
            return Ok(wiping(&path));
        }

        let current = self.meta(&path)?;
        if let CasDecision::Reject { current } = decide_cas(current.as_ref(), basis, Some(new.kind))
        {
            return Ok(ProtocolMessage::CasReject {
                checkout_id,
                path,
                current,
            });
        }

        match new.kind {
            EntryKind::Dir => {
                self.inflight.arm(path.clone(), ContentHash::ZERO);
                if let Err(err) = self.index_ancestors(&path) {
                    self.inflight.disarm(&path);
                    return Err(err);
                }
                if let Err(err) =
                    apply::replace_live(&self.central_root, &path, &new, &[], current.as_ref())
                {
                    self.inflight.disarm(&path);
                    return Err(err.into());
                }
            }
            EntryKind::File | EntryKind::Symlink => match self.bodies.fetch(new.content_hash) {
                ContentBytes::AskSender => {
                    let live = if current.as_ref().is_some_and(|live| live.kind != new.kind) {
                        None
                    } else {
                        let host = canonical_to_host(&self.central_root, &path);
                        apply::try_read_file_or_link(&host).map_err(MasterError::io(&host))?
                    };
                    let kind = new.kind;
                    let want_hash = new.content_hash;
                    self.pending.insert(
                        (checkout_id.clone(), path.clone()),
                        PendingApply {
                            peer,
                            checkout_id: checkout_id.clone(),
                            path: path.clone(),
                            new,
                            basis,
                            origin: Origin::Slave {
                                slave: session.slave,
                                checkout,
                            },
                            previous: current,
                            retried: false,
                        },
                        Instant::now(),
                    );
                    return Ok(transfer::signature_request(
                        checkout_id,
                        path,
                        want_hash,
                        kind,
                        live.as_deref(),
                    ));
                }
                ContentBytes::Whole(body) => {
                    self.publish(&path, &new, current.as_ref(), &body)?;
                }
            },
        }

        let committed = file_node(&new);
        let origin = Origin::Slave {
            slave: session.slave,
            checkout,
        };
        self.commit(&origin, &path, Some(&new), current.as_ref())?;
        Ok(ProtocolMessage::CasAccept {
            checkout_id,
            path,
            file_node: Some(committed),
        })
    }

    fn on_delete(
        &mut self,
        peer: [u8; 32],
        checkout_id: String,
        path: CanonicalPath,
        basis: FileNode,
    ) -> Result<ProtocolMessage, MasterError> {
        if self.overlaps_wipe(&path) {
            return Ok(wiping(&path));
        }
        match self.classify_delete(peer, checkout_id, path, basis)? {
            DeleteClass::Reply(msg) => Ok(msg),
            DeleteClass::Accepted {
                path,
                previous,
                accept,
                origin,
            } => {
                apply::remove_live(&self.central_root, &path)?;
                self.commit(&origin, &path, None, Some(&previous))?;
                Ok(accept)
            }
        }
    }

    /// Accept a slave delete in the index and leave the live tree for the caller.
    ///
    /// `Later` means `path` overlaps a reclaim already in progress. The caller
    /// retries the same `Delete` after [`Master::finish_wipe`].
    pub fn prepare_delete(
        &mut self,
        peer: [u8; 32],
        checkout_id: String,
        path: CanonicalPath,
        basis: FileNode,
    ) -> Result<PreparedDelete, MasterError> {
        let slave = self.status_slave(&peer);
        self.status.inbound(
            slave.as_deref(),
            &ProtocolMessage::Delete {
                checkout_id: checkout_id.clone(),
                path: path.clone(),
                basis,
            },
        );
        if self.overlaps_wipe(&path) {
            return Ok(PreparedDelete::Later {
                checkout_id,
                path,
                basis,
            });
        }
        match self.classify_delete(peer, checkout_id, path, basis)? {
            DeleteClass::Reply(msg) => {
                self.record_reply(slave.as_deref(), &Reply::Send(msg.clone()));
                Ok(PreparedDelete::Reply(msg))
            }
            DeleteClass::Accepted {
                path,
                previous,
                accept,
                origin,
            } => {
                self.wipes.start(path.clone());
                if let Err(err) = self.commit(&origin, &path, None, Some(&previous)) {
                    self.wipes.finish(&path);
                    return Err(err);
                }
                self.record_reply(slave.as_deref(), &Reply::Send(accept.clone()));
                Ok(PreparedDelete::Reclaim {
                    reply: accept,
                    root: self.central_root.clone(),
                    path,
                })
            }
        }
    }

    pub fn finish_wipe(&mut self, path: &CanonicalPath) {
        self.wipes.finish(path);
    }

    pub fn overlaps_wipe(&self, path: &CanonicalPath) -> bool {
        self.wipes.overlaps(path)
    }

    fn inside_wipe(&self, path: &CanonicalPath) -> bool {
        self.wipes.covers(path)
    }

    fn event_inside_wipe(&self, event: &LocalEvent) -> bool {
        match event {
            LocalEvent::Changed(path) | LocalEvent::Metadata(path) | LocalEvent::Removed(path) => {
                self.inside_wipe(path)
            }
            LocalEvent::Renamed { from, to } => self.inside_wipe(from) || self.inside_wipe(to),
        }
    }

    fn classify_delete(
        &mut self,
        peer: [u8; 32],
        checkout_id: String,
        path: CanonicalPath,
        basis: FileNode,
    ) -> Result<DeleteClass, MasterError> {
        let checkout = CheckoutName::new(checkout_id.clone());
        let session = match self.live_checkout(&peer, &checkout) {
            Ok(session) => session,
            Err(refusal) => return Ok(DeleteClass::Reply(refusal.into_error(&checkout))),
        };
        if !session.central.covers(&path) {
            return Ok(DeleteClass::Reply(outside_central(&path)));
        }
        if path_is_reserved(&session.central, &path) {
            return Ok(DeleteClass::Reply(reserved_name(&path)));
        }
        let current = self.meta(&path)?;
        if let CasDecision::Reject { current } = decide_cas(current.as_ref(), Some(basis), None) {
            return Ok(DeleteClass::Reply(ProtocolMessage::CasReject {
                checkout_id,
                path,
                current,
            }));
        }
        let previous = current.expect("delete CAS accepted a live path");
        let origin = Origin::Slave {
            slave: session.slave,
            checkout,
        };
        Ok(DeleteClass::Accepted {
            path: path.clone(),
            previous,
            accept: ProtocolMessage::CasAccept {
                checkout_id,
                path,
                file_node: None,
            },
            origin,
        })
    }

    fn on_rename(
        &mut self,
        peer: [u8; 32],
        checkout_id: String,
        from: CanonicalPath,
        to: CanonicalPath,
        from_basis: FileNode,
        to_new: FileMetadata,
    ) -> Result<ProtocolMessage, MasterError> {
        let checkout = CheckoutName::new(checkout_id.clone());
        let session = match self.live_checkout(&peer, &checkout) {
            Ok(session) => session,
            Err(refusal) => return Ok(refusal.into_error(&checkout)),
        };
        if !session.central.covers(&from) {
            return Ok(outside_central(&from));
        }
        if !session.central.covers(&to) {
            return Ok(outside_central(&to));
        }
        if self.overlaps_wipe(&from) || self.overlaps_wipe(&to) {
            return Ok(wiping(&from));
        }
        if path_is_reserved(&session.central, &from) {
            return Ok(reserved_name(&from));
        }
        if path_is_reserved(&session.central, &to) {
            return Ok(reserved_name(&to));
        }

        let current_from = self.meta(&from)?;
        let current_to = self.meta(&to)?;
        if let CasDecision::Reject { current } =
            decide_cas(current_to.as_ref(), None, Some(to_new.kind))
        {
            return Ok(ProtocolMessage::CasReject {
                checkout_id,
                path: to,
                current,
            });
        }
        if let CasDecision::Reject { current } =
            decide_cas(current_from.as_ref(), Some(from_basis), None)
        {
            return Ok(ProtocolMessage::CasReject {
                checkout_id,
                path: from,
                current,
            });
        }

        apply::rename_live(&self.central_root, &from, &to, &to_new)?;
        let hash = match to_new.kind {
            EntryKind::Dir => ContentHash::ZERO,
            EntryKind::File | EntryKind::Symlink => to_new.content_hash,
        };
        self.inflight.arm(to.clone(), hash);
        let origin = Origin::Slave {
            slave: session.slave,
            checkout: checkout.clone(),
        };
        let from_previous = current_from.expect("from CAS accepted a live path");
        self.commit_rename(&origin, &from, &to, &from_previous, &to_new)?;
        self.roster.push_peer_checkout(
            &peer,
            &checkout,
            ProtocolMessage::CasAccept {
                checkout_id: checkout_id.clone(),
                path: from,
                file_node: None,
            },
        );
        Ok(ProtocolMessage::CasAccept {
            checkout_id,
            path: to,
            file_node: Some(file_node(&to_new)),
        })
    }

    fn on_root_report(
        &self,
        peer: [u8; 32],
        checkout_id: String,
        path: CanonicalPath,
        root: SubtreeRoot,
    ) -> Result<ProtocolMessage, MasterError> {
        let checkout = CheckoutName::new(checkout_id.clone());
        let session = match self.live_checkout(&peer, &checkout) {
            Ok(session) => session,
            Err(refusal) => return Ok(refusal.into_error(&checkout)),
        };
        if path != session.central {
            return Ok(outside_central(&path));
        }
        let master_root = index::subtree_root(&self.store, &CheckoutId::master(), &path)
            .map_err(MasterError::index)?;
        Ok(ProtocolMessage::RootAck {
            checkout_id,
            path,
            matched: root == master_root,
            master_root,
        })
    }

    fn on_dir_list(
        &self,
        peer: [u8; 32],
        checkout_id: String,
        path: CanonicalPath,
        after: Option<EntryName>,
    ) -> Result<ProtocolMessage, MasterError> {
        let checkout = CheckoutName::new(checkout_id.clone());
        let session = match self.live_checkout(&peer, &checkout) {
            Ok(session) => session,
            Err(refusal) => return Ok(refusal.into_error(&checkout)),
        };
        if !session.central.covers(&path) {
            return Ok(outside_central(&path));
        }
        if path_is_reserved(&session.central, &path) {
            return Ok(reserved_name(&path));
        }
        match self.meta(&path)? {
            Some(meta) if meta.kind != EntryKind::Dir => Ok(ProtocolMessage::FileAnnounce {
                checkout_id,
                path,
                new: meta,
                basis: None,
            }),
            _ => {
                let entries = index::list_children(&self.store, &CheckoutId::master(), &path)
                    .map_err(MasterError::index)?;
                Ok(page_dir_list(checkout_id, path, after, entries))
            }
        }
    }

    fn live_checkout(
        &self,
        peer: &[u8; 32],
        checkout: &CheckoutName,
    ) -> Result<Session, NotSubscribed> {
        let Some(slave) = self.roster.slave_of(peer).cloned() else {
            return Err(NotSubscribed::NoSession);
        };
        let Some(central) = self.roster.checkout_central(peer, checkout).cloned() else {
            return Err(NotSubscribed::UnknownCheckout);
        };
        Ok(Session { slave, central })
    }

    fn write_index(
        &mut self,
        path: &CanonicalPath,
        leaf: Option<&FileMetadata>,
        last_synced: index::LastSynced,
    ) -> Result<(), MasterError> {
        index::commit_leaf_with(
            &self.store,
            &CheckoutId::master(),
            path,
            leaf,
            last_synced,
            &mut self.dirs,
        )
        .map_err(MasterError::index)
    }

    fn commit(
        &mut self,
        origin: &Origin,
        path: &CanonicalPath,
        new: Option<&FileMetadata>,
        previous: Option<&FileMetadata>,
    ) -> Result<(), MasterError> {
        self.write_index(path, new, index::LastSynced::AdoptLeaf)?;

        let payload = match (new, previous) {
            (Some(new), previous) => Fanout::Announce {
                new: new.clone(),
                basis: previous.map(file_node),
            },
            (None, Some(previous)) => Fanout::Remove {
                basis: file_node(previous),
            },
            (None, None) => return Ok(()),
        };
        for key in self.roster.interested(path) {
            if origin.committed(&key) {
                continue;
            }
            let checkout_id = key.checkout.as_str().to_string();
            self.roster
                .push(&key, payload.to_message(checkout_id, path));
        }
        Ok(())
    }

    fn commit_rename(
        &mut self,
        origin: &Origin,
        from: &CanonicalPath,
        to: &CanonicalPath,
        from_previous: &FileMetadata,
        to_new: &FileMetadata,
    ) -> Result<(), MasterError> {
        self.write_index(from, None, index::LastSynced::AdoptLeaf)?;
        self.index_ancestors(to)?;
        self.write_index(to, Some(to_new), index::LastSynced::AdoptLeaf)?;
        if to_new.kind == EntryKind::Dir {
            self.reindex_descendants(to, index::LastSynced::AdoptLeaf)?;
        }

        let mut keys: HashSet<InterestKey> = self.roster.interested(from).into_iter().collect();
        keys.extend(self.roster.interested(to));
        for key in keys {
            if origin.committed(&key) {
                continue;
            }
            let Some(central) = self.roster.central_of(&key).cloned() else {
                continue;
            };
            let checkout_id = key.checkout.as_str().to_string();
            let covers_from = central.covers(from);
            let covers_to = central.covers(to);
            let payload = match (covers_from, covers_to) {
                (true, true) => Fanout::Rename {
                    from: from.clone(),
                    to: to.clone(),
                    from_basis: file_node(from_previous),
                    to_new: to_new.clone(),
                },
                (true, false) => Fanout::Remove {
                    basis: file_node(from_previous),
                },
                (false, true) => Fanout::Announce {
                    new: to_new.clone(),
                    basis: None,
                },
                (false, false) => continue,
            };
            let path = if covers_from { from } else { to };
            self.roster
                .push(&key, payload.to_message(checkout_id, path));
        }
        Ok(())
    }

    fn index_ancestors(&mut self, path: &CanonicalPath) -> Result<(), MasterError> {
        let mut ancestors: Vec<CanonicalPath> = path.ancestors().collect();
        ancestors.reverse();
        for dir in ancestors {
            if self.meta(&dir)?.is_some() {
                continue;
            }
            let host = confine_host(&self.central_root, &dir)?;
            fs::create_dir_all(&host).map_err(MasterError::io(&host))?;
            let Some(found) = meta::collect_from_path(&host).map_err(MasterError::io(&host))?
            else {
                continue;
            };
            self.write_index(&dir, Some(&found), index::LastSynced::AdoptLeaf)?;
        }
        Ok(())
    }

    fn note_changed(&mut self, path: CanonicalPath) -> Result<HashPlan, MasterError> {
        if path.has_reserved_root_name() {
            return Ok(HashPlan::default());
        }
        let host = canonical_to_host(&self.central_root, &path);
        let previous = self.meta(&path)?;
        match meta::inspect_for_hash(&host, previous.as_ref()).map_err(MasterError::io(&host))? {
            Inspected::Ready(found) => {
                self.note_present(path, found)?;
                Ok(HashPlan::default())
            }
            Inspected::Absent => {
                self.note_removed(&path)?;
                Ok(HashPlan::default())
            }
            Inspected::NeedHash(host) => {
                let key = HashKey::Central(path);
                self.hashing.insert(key.clone(), (), Instant::now());
                Ok(HashPlan {
                    send: Vec::new(),
                    hash: vec![HashNeed {
                        key,
                        host,
                        previous,
                    }],
                })
            }
        }
    }

    fn note_metadata(&mut self, path: CanonicalPath) -> Result<HashPlan, MasterError> {
        self.note_changed(path)
    }

    fn note_present(
        &mut self,
        path: CanonicalPath,
        found: FileMetadata,
    ) -> Result<(), MasterError> {
        if self.inflight.consume_if_echo(&path, &found.content_hash) {
            return Ok(());
        }
        let current = self.meta(&path)?;
        if current.as_ref() == Some(&found) {
            return Ok(());
        }
        self.index_ancestors(&path)?;
        self.commit(&Origin::Local, &path, Some(&found), current.as_ref())
    }

    fn note_renamed(
        &mut self,
        from: CanonicalPath,
        to: CanonicalPath,
    ) -> Result<HashPlan, MasterError> {
        if from.has_reserved_root_name() || to.has_reserved_root_name() {
            return Ok(HashPlan::default());
        }
        let host_to = canonical_to_host(&self.central_root, &to);
        let from_previous = self.meta(&from)?;
        let Some(found) = meta::collect_for_rescan(&host_to, from_previous.as_ref())
            .map_err(MasterError::io(&host_to))?
        else {
            self.note_removed(&from)?;
            return Ok(HashPlan::default());
        };
        if self.inflight.consume_if_echo(&to, &found.content_hash) {
            return Ok(HashPlan::default());
        }
        let Some(from_previous) = from_previous else {
            self.note_present(to, found)?;
            return Ok(HashPlan::default());
        };
        let current_to = self.meta(&to)?;
        if current_to.as_ref() == Some(&found) && self.meta(&from)?.is_none() {
            return Ok(HashPlan::default());
        }
        self.commit_rename(&Origin::Local, &from, &to, &from_previous, &found)?;
        Ok(HashPlan::default())
    }

    fn reindex_descendants(
        &mut self,
        dir: &CanonicalPath,
        last_synced: index::LastSynced,
    ) -> Result<(), MasterError> {
        let disk = self.walk_prefix(dir)?;
        for (path, found) in disk {
            if &path == dir {
                continue;
            }
            let current = self.meta(&path)?;
            if current.as_ref() == Some(&found) {
                continue;
            }
            self.write_index(&path, Some(&found), last_synced)?;
        }
        Ok(())
    }

    fn publish(
        &mut self,
        path: &CanonicalPath,
        new: &FileMetadata,
        previous: Option<&FileMetadata>,
        body: &[u8],
    ) -> Result<(), MasterError> {
        self.inflight.arm(path.clone(), new.content_hash);
        if let Err(err) = self.index_ancestors(path) {
            self.inflight.disarm(path);
            return Err(err);
        }
        if let Err(err) = apply::replace_live(&self.central_root, path, new, body, previous) {
            self.inflight.disarm(path);
            return Err(err.into());
        }
        Ok(())
    }

    pub fn plan_fulfill(
        &mut self,
        peer: [u8; 32],
        checkout_id: String,
        path: CanonicalPath,
        want_hash: ContentHash,
        signature: Vec<u8>,
    ) -> Result<FulfillPlan, MasterError> {
        let slave = self.status_slave(&peer);
        self.status.inbound(
            slave.as_deref(),
            &ProtocolMessage::SignatureRequest {
                checkout_id: checkout_id.clone(),
                path: path.clone(),
                want_hash,
                signature: signature.clone(),
            },
        );
        let checkout = CheckoutName::new(checkout_id.clone());
        let session = match self.live_checkout(&peer, &checkout) {
            Ok(session) => session,
            Err(refusal) => return Ok(FulfillPlan::Send(refusal.into_error(&checkout))),
        };
        if !session.central.covers(&path) {
            return Ok(FulfillPlan::Send(outside_central(&path)));
        }
        if path_is_reserved(&session.central, &path) {
            return Ok(FulfillPlan::Send(reserved_name(&path)));
        }
        if self.inside_wipe(&path) {
            return Ok(FulfillPlan::Send(missing_hash(&path)));
        }
        Ok(FulfillPlan::BulkHost {
            host: canonical_to_host(&self.central_root, &path),
            checkout_id,
            path,
            want_hash,
            signature,
        })
    }

    fn note_removed(&mut self, path: &CanonicalPath) -> Result<(), MasterError> {
        if path.has_reserved_root_name() {
            return Ok(());
        }
        if self.inflight.is_armed(path) {
            return Ok(());
        }
        let Some(previous) = self.meta(path)? else {
            return Ok(());
        };
        self.commit(&Origin::Local, path, None, Some(&previous))
    }

    fn walk_central_stats(&self) -> Result<CentralWalk, MasterError> {
        survey_central(&self.central_root, &self.store)
    }

    #[allow(dead_code)]
    fn walk_central(&self) -> Result<BTreeMap<CanonicalPath, FileMetadata>, MasterError> {
        self.walk_prefix(&CanonicalPath::root())
    }

    fn walk_prefix(
        &self,
        start: &CanonicalPath,
    ) -> Result<BTreeMap<CanonicalPath, FileMetadata>, MasterError> {
        let mut found = BTreeMap::new();
        let start_prev = self.meta(start)?;
        let start_host = canonical_to_host(&self.central_root, start);
        if let Some(root) = meta::collect_for_rescan(&start_host, start_prev.as_ref())
            .map_err(MasterError::io(&start_host))?
        {
            found.insert(start.clone(), root);
        }
        let mut pending = vec![start.clone()];
        while let Some(dir) = pending.pop() {
            let host = canonical_to_host(&self.central_root, &dir);
            let entries = match fs::read_dir(&host) {
                Ok(entries) => entries,
                Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                    log::warn!("skipping {}: permission denied", host.display());
                    continue;
                }
                Err(err) => return Err(MasterError::io(&host)(err)),
            };
            for entry in entries {
                let entry = entry.map_err(MasterError::io(&host))?;
                let raw = entry.file_name();
                let Some(name) = raw.to_str() else {
                    log::warn!("skipping non-UTF-8 name under {}", host.display());
                    continue;
                };
                if dir.as_str() == "/" && is_reserved_root_entry(name) {
                    continue;
                }
                let child = join_central(&dir, name)?;
                let host_child = entry.path();
                let previous = self.meta(&child)?;
                let Some(meta) = meta::collect_for_rescan(&host_child, previous.as_ref())
                    .map_err(MasterError::io(&host_child))?
                else {
                    continue;
                };
                if meta.kind == EntryKind::Dir {
                    pending.push(child.clone());
                }
                found.insert(child, meta);
            }
        }
        Ok(found)
    }
}

/// Rewrite master directory hashes that do not match indexed children.
pub fn recompute_index<S: Storage>(store: &S) -> Result<bool, S::Error> {
    index::repair_dir_nodes(store, &CheckoutId::master(), &CanonicalPath::root())
}

/// Stat `central_root` and read index rows. Does not write.
pub fn survey_central<S: Storage>(root: &Path, store: &S) -> Result<CentralWalk, MasterError> {
    let mut ready = BTreeMap::new();
    let mut needs = Vec::new();
    let root_prev = master_meta(store, &CanonicalPath::root())?;
    match meta::inspect_for_hash(root, root_prev.as_ref()).map_err(MasterError::io(root))? {
        Inspected::Ready(found) => {
            ready.insert(CanonicalPath::root(), found);
        }
        Inspected::NeedHash(host) => {
            needs.push(HashNeed {
                key: HashKey::Central(CanonicalPath::root()),
                host,
                previous: root_prev,
            });
        }
        Inspected::Absent => {}
    }
    let mut pending = vec![CanonicalPath::root()];
    while let Some(dir) = pending.pop() {
        let host = canonical_to_host(root, &dir);
        let entries = match fs::read_dir(&host) {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                log::warn!("skipping {}: permission denied", host.display());
                continue;
            }
            Err(err) => return Err(MasterError::io(&host)(err)),
        };
        for entry in entries {
            let entry = entry.map_err(MasterError::io(&host))?;
            let raw = entry.file_name();
            let Some(name) = raw.to_str() else {
                log::warn!("skipping non-UTF-8 name under {}", host.display());
                continue;
            };
            if dir.as_str() == "/" && is_reserved_root_entry(name) {
                continue;
            }
            let child = join_central(&dir, name)?;
            let host_child = entry.path();
            let previous = master_meta(store, &child)?;
            match meta::inspect_for_hash(&host_child, previous.as_ref())
                .map_err(MasterError::io(&host_child))?
            {
                Inspected::Ready(found) => {
                    if found.kind == EntryKind::Dir {
                        pending.push(child.clone());
                    }
                    ready.insert(child, found);
                }
                Inspected::NeedHash(host) => {
                    needs.push(HashNeed {
                        key: HashKey::Central(child),
                        host,
                        previous,
                    });
                }
                Inspected::Absent => {}
            }
        }
    }
    Ok(CentralWalk { ready, needs })
}

fn master_meta<S: Storage>(
    store: &S,
    path: &CanonicalPath,
) -> Result<Option<FileMetadata>, MasterError> {
    store
        .get_meta(&CheckoutId::master(), path)
        .map_err(MasterError::index)
}

enum OnDisk {
    Here,
    Gone,
    Unreadable,
}

fn stat_on_disk(root: &Path, path: &CanonicalPath) -> Result<OnDisk, MasterError> {
    let host = canonical_to_host(root, path);
    match fs::symlink_metadata(&host) {
        Ok(_) => Ok(OnDisk::Here),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(OnDisk::Gone),
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => Ok(OnDisk::Unreadable),
        Err(err) => Err(MasterError::io(&host)(err)),
    }
}

pub fn reclaim_tree(root: &Path, path: &CanonicalPath) -> Result<(), MasterError> {
    apply::remove_live(root, path)?;
    Ok(())
}

fn wiping(path: &CanonicalPath) -> ProtocolMessage {
    ProtocolMessage::Error {
        code: "wiping".into(),
        message: path.as_str().into(),
    }
}

fn missing_hash(path: &CanonicalPath) -> ProtocolMessage {
    ProtocolMessage::Error {
        code: "missing_hash".into(),
        message: path.as_str().into(),
    }
}

fn path_is_reserved(central: &CanonicalPath, path: &CanonicalPath) -> bool {
    path.has_reserved_root_name()
        || strip_central(central, path).is_ok_and(|relative| relative.has_reserved_root_name())
}

fn reserved_name(path: &CanonicalPath) -> ProtocolMessage {
    ProtocolMessage::Error {
        code: "reserved_name".into(),
        message: path.as_str().into(),
    }
}

fn outside_central(path: &CanonicalPath) -> ProtocolMessage {
    ProtocolMessage::Error {
        code: "outside_central".into(),
        message: format!("{} is not under the subscribed central", path.as_str()),
    }
}

fn reject_all(checkouts: &[CheckoutRef], reason: &str) -> ProtocolMessage {
    ProtocolMessage::SubscribeReject {
        reason: reason.into(),
        denied_centrals: checkouts.iter().map(|c| c.central.clone()).collect(),
    }
}

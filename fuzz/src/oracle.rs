use std::collections::BTreeMap;

use arborsync_core::meta::hash_bytes;

use crate::schedule::{
    Actor, DiskOp, Layout, MtimeNs, RelPath, SecondView, UnixMode, WorldOp,
};
use crate::types::Finding;

/// Append-only log of what the fuzzer did. Daemons never write it.
pub(crate) struct Intent {
    layout: Layout,
    events: Vec<Event>,
    yard: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Event {
    Freeze(Actor),
    Thaw(Actor),
    Disk { op: DiskOp, snap: LocalSnap },
    World(WorldOp),
    Quiesce,
    Settle,
}

/// What [`crate::world::World::apply`] saw before thaw.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LocalSnap {
    Applied(SnapBody),
    /// `mknod` returned `EPERM`, or the live path lacked the op's precondition.
    /// [`project`] ignores this op, except [`DiskOp::Deny`], which still opens
    /// an unspecified epoch.
    Skipped,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum EntryKey {
    Path(RelPath),
    Raw { parent: RelPath, name: Vec<u8> },
}

impl EntryKey {
    fn label(&self) -> String {
        match self {
            EntryKey::Path(path) => path.join(),
            EntryKey::Raw { parent, name } => format!("{}/<raw {} bytes>", parent.join(), name.len()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SnapBody {
    File {
        bytes: Vec<u8>,
        mode: UnixMode,
        mtime: MtimeNs,
    },
    Dir {
        mode: UnixMode,
        mtime: MtimeNs,
        /// Set when this directory itself was mkdir'd or chmod'd.
        /// Child-only edits leave this false, and [`judge`] then skips
        /// mode and mtime.
        meta_authoritative: bool,
    },
    /// Equality is the target bytes. Disk mode is not compared.
    Symlink { target: Vec<u8> },
    Residue(Residue),
    Absent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Residue {
    Fifo,
    Socket,
    Device,
    NonUtf8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TreeSnap {
    pub nodes: BTreeMap<EntryKey, SnapBody>,
}

impl TreeSnap {
    fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
        }
    }
}

/// A slave conflict file. `losing_hash` is the content hash whose first
/// 16 hex characters are the sidecar's filename suffix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Sidecar {
    pub canonical: RelPath,
    pub losing_hash: [u8; 32],
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Observed {
    pub master: ActorView,
    /// Length equals the layout's slave count.
    pub slaves: Vec<ActorView>,
    pub yard: [u8; 32],
    pub dead: Vec<Actor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActorView {
    pub tree: TreeSnap,
    /// Parsed from `.arborsync-conflicts` at that root.
    /// Empty on a master that followed the spec.
    pub sidecars: Vec<Sidecar>,
    pub status: StatusSnap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StatusSnap {
    pub health: Health,
    pub pending: u64,
    pub rescan: u64,
    pub last_error: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Health {
    Idle,
    Busy,
    Stuck,
    Failed,
    Other(String),
}

/// Spec projection of an [`Intent`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Projection {
    Determined(Determined),
    /// A `Deny` landed since the previous settle. Tree bytes are not asserted.
    UnspecifiedIo { yard: [u8; 32] },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Determined {
    pub master: TreeSnap,
    pub master_error: ErrorExpect,
    pub slaves: Vec<SlaveExpect>,
    /// Yard digest captured at claim. A difference is an escape.
    pub yard: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SlaveExpect {
    pub tree: TreeSnap,
    pub sidecars: Vec<Sidecar>,
    pub error: ErrorExpect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ErrorExpect {
    Empty,
    CasReject(RelPath),
    KeepLive(RelPath),
}

impl Intent {
    pub(crate) fn new(layout: &Layout) -> Self {
        Self {
            layout: layout.clone(),
            events: Vec::new(),
            yard: [0; 32],
        }
    }

    pub(crate) fn set_yard(&mut self, yard: [u8; 32]) {
        self.yard = yard;
    }

    /// The only way to extend the log.
    pub(crate) fn push(&mut self, event: Event) {
        self.events.push(event);
    }
}

struct Fold {
    layout: Layout,
    master: TreeSnap,
    slaves: Vec<TreeSnap>,
    sidecars: Vec<Vec<Sidecar>>,
    slave_errors: Vec<ErrorExpect>,
    master_error: ErrorExpect,
    frozen: BTreeMap<Actor, ()>,
    thaw_order: Vec<Actor>,
    pending: Vec<(DiskOp, LocalSnap)>,
    epochs: Vec<TreeSnap>,
    deny: bool,
    restore_pending: bool,
    yard: [u8; 32],
}

impl Fold {
    fn new(layout: &Layout, yard: [u8; 32]) -> Self {
        let n = layout.slave_count().get() as usize;
        let mut fold = Self {
            layout: layout.clone(),
            master: TreeSnap::new(),
            slaves: (0..n).map(|_| TreeSnap::new()).collect(),
            sidecars: (0..n).map(|_| Vec::new()).collect(),
            slave_errors: (0..n).map(|_| ErrorExpect::Empty).collect(),
            master_error: ErrorExpect::Empty,
            frozen: BTreeMap::new(),
            thaw_order: Vec::new(),
            pending: Vec::new(),
            epochs: Vec::new(),
            deny: false,
            restore_pending: false,
            yard,
        };
        if matches!(
            layout,
            Layout::Two {
                second: SecondView::Nested,
            }
        ) {
            let path = RelPath::new(vec!["nested".into()]).expect("nested");
            let dir = SnapBody::Dir {
                mode: UnixMode::new(0o755).expect("mode"),
                mtime: MtimeNs::new(0),
                meta_authoritative: false,
            };
            fold.master
                .nodes
                .insert(EntryKey::Path(path.clone()), dir.clone());
            fold.slaves[0]
                .nodes
                .insert(EntryKey::Path(path), dir);
        }
        fold
    }
}

use std::collections::BTreeMap as FoldMap;

/// Fold `intent` into the trees the spec requires at the last event.
pub(crate) fn project(intent: &Intent) -> Projection {
    let mut fold = Fold::new(&intent.layout, intent.yard);
    let mut closed_deny = false;
    for event in &intent.events {
        closed_deny = false;
        match event {
            Event::Freeze(actor) => {
                fold.frozen.insert(*actor, ());
            }
            Event::Thaw(actor) => {
                fold.frozen.remove(actor);
                fold.thaw_order.retain(|item| item != actor);
                fold.thaw_order.push(*actor);
            }
            Event::Disk { op, snap } => {
                if matches!(op, DiskOp::Deny { .. }) {
                    fold.deny = true;
                }
                fold.pending.push((op.clone(), snap.clone()));
            }
            Event::World(WorldOp::Restore { epoch }) => {
                if let Some(tree) = fold.epochs.get(*epoch as usize) {
                    fold.master = tree.clone();
                    fold.restore_pending = true;
                }
            }
            Event::World(WorldOp::BadBulk { path }) => {
                fold.master_error = ErrorExpect::KeepLive(path.clone());
            }
            Event::World(WorldOp::Grammar(_) | WorldOp::StruckReload) => {}
            Event::Quiesce => {
                if !fold.deny {
                    commit(&mut fold);
                }
            }
            Event::Settle if fold.deny => {
                fold.pending.clear();
                fold.epochs.push(fold.master.clone());
                fold.deny = false;
                closed_deny = true;
            }
            Event::Settle => {
                commit(&mut fold);
                fold.epochs.push(fold.master.clone());
            }
        }
    }
    if fold.deny || closed_deny {
        return Projection::UnspecifiedIo { yard: fold.yard };
    }
    Projection::Determined(Determined {
        master: fold.master,
        master_error: fold.master_error,
        slaves: fold
            .slaves
            .into_iter()
            .zip(fold.sidecars)
            .zip(fold.slave_errors)
            .map(|((tree, sidecars), error)| SlaveExpect {
                tree,
                sidecars,
                error,
            })
            .collect(),
        yard: fold.yard,
    })
}

fn commit(fold: &mut Fold) {
    if fold.restore_pending {
        replicate_master(fold);
        fold.restore_pending = false;
        fold.pending.clear();
        for error in &mut fold.slave_errors {
            *error = ErrorExpect::Empty;
        }
        for sidecars in &mut fold.sidecars {
            sidecars.clear();
        }
        fold.master_error = ErrorExpect::Empty;
        return;
    }
    let basis: Vec<TreeSnap> = fold.slaves.clone();
    let batch = std::mem::take(&mut fold.pending);
    let mut later = Vec::new();
    let mut grouped: FoldMap<Actor, Vec<(DiskOp, LocalSnap)>> = FoldMap::new();
    for item in batch {
        grouped.entry(item.0.actor()).or_default().push(item);
    }
    for actor in fold.thaw_order.clone() {
        if fold.frozen.contains_key(&actor) {
            continue;
        }
        let Some(ops) = grouped.remove(&actor) else {
            continue;
        };
        for (op, snap) in ops {
            apply_one(fold, &basis, op, snap);
        }
    }
    for (actor, ops) in grouped {
        if fold.frozen.contains_key(&actor) {
            later.extend(ops);
        } else {
            for (op, snap) in ops {
                apply_one(fold, &basis, op, snap);
            }
        }
    }
    fold.pending = later;
}

fn apply_one(fold: &mut Fold, basis: &[TreeSnap], op: DiskOp, snap: LocalSnap) {
    if matches!(op, DiskOp::Deny { .. }) {
        fold.deny = true;
        return;
    }
    let LocalSnap::Applied(body) = snap else {
        return;
    };
    match op {
        DiskOp::RetouchSameStamp { actor, path, .. } => {
            write_actor(fold, actor, &path, body);
        }
        DiskOp::Special { actor, path, kind } => {
            let residue = match kind {
                crate::schedule::SpecialFile::Fifo => Residue::Fifo,
                crate::schedule::SpecialFile::Socket => Residue::Socket,
                crate::schedule::SpecialFile::Device => Residue::Device,
            };
            write_actor(fold, actor, &path, SnapBody::Residue(residue));
        }
        DiskOp::NonUtf8 { actor, parent, name } => {
            let tree = actor_tree_mut(fold, actor);
            ensure_parents(tree, &parent);
            tree.nodes.insert(
                EntryKey::Raw { parent, name },
                SnapBody::Residue(Residue::NonUtf8),
            );
        }
        DiskOp::Put {
            actor, path, ..
        } => publish_body(fold, basis, actor, &path, body),
        DiskOp::Chmod { actor, path, mode } => {
            chmod_actor(fold, actor, &path, mode);
            if actor == Actor::Master {
                replicate_mode(fold, &path, mode);
            }
        }
        DiskOp::Symlink { actor, path, .. } => publish_body(fold, basis, actor, &path, body),
        DiskOp::Mkdir { actor, path, .. } => {
            let mut dir = body;
            if let SnapBody::Dir {
                meta_authoritative, ..
            } = &mut dir
            {
                *meta_authoritative = true;
            }
            publish_body(fold, basis, actor, &path, dir);
        }
        DiskOp::Unlink { actor, path } => unlink_actor(fold, basis, actor, &path),
        DiskOp::Rename { actor, from, to } => rename_actor(fold, basis, actor, &from, &to, body),
        DiskOp::HardLink { actor, from, to } => {
            let source = actor_tree(fold, actor)
                .nodes
                .get(&EntryKey::Path(from))
                .cloned();
            if let Some(SnapBody::File { .. }) = source {
                publish_body(fold, basis, actor, &to, body);
            }
        }
        DiskOp::EscapeLink { actor, path, .. } => publish_body(fold, basis, actor, &path, body),
        DiskOp::Deny { .. } => {}
    }
}

fn publish_body(fold: &mut Fold, basis: &[TreeSnap], actor: Actor, path: &RelPath, body: SnapBody) {
    match actor {
        Actor::Master => {
            let global = path.clone();
            write_global(fold, &global, body.clone());
            fanout(fold, basis, &global, &body);
        }
        Actor::Slave(ix) => {
            let index = ix.get() as usize;
            let global = to_global(&fold.layout, index, path);
            let basis_body = basis
                .get(index)
                .and_then(|tree| tree.nodes.get(&EntryKey::Path(path.clone())))
                .cloned();
            let master_body = fold.master.nodes.get(&EntryKey::Path(global.clone())).cloned();
            if cas_accepts(basis_body.as_ref(), master_body.as_ref(), &body) {
                write_global(fold, &global, body.clone());
                fanout(fold, basis, &global, &body);
            } else if let Some(losing) = content_bytes(&body) {
                if content_differs(master_body.as_ref(), &body) {
                    push_sidecar(fold, index, path, losing);
                    fold.slave_errors[index] = ErrorExpect::CasReject(path.clone());
                }
                if let Some(master_body) = master_body {
                    write_actor(fold, actor, path, master_body);
                }
            } else {
                if let Some(master_body) = master_body {
                    write_actor(fold, actor, path, master_body);
                } else {
                    write_actor(fold, actor, path, body);
                }
            }
        }
    }
}

fn cas_accepts(basis: Option<&SnapBody>, master: Option<&SnapBody>, new_body: &SnapBody) -> bool {
    match master {
        None => basis.is_none() || same_content(basis, Some(new_body)),
        Some(master_body) => {
            same_content(Some(master_body), basis) || same_content(Some(master_body), Some(new_body))
        }
    }
}

fn fanout(fold: &mut Fold, basis: &[TreeSnap], global: &RelPath, body: &SnapBody) {
    for index in 0..fold.slaves.len() {
        let Some(local) = to_local(&fold.layout, index, global) else {
            continue;
        };
        let current = fold.slaves[index]
            .nodes
            .get(&EntryKey::Path(local.clone()))
            .cloned();
        let agreed = basis
            .get(index)
            .and_then(|tree| tree.nodes.get(&EntryKey::Path(local.clone())))
            .cloned();
        // A slave still on the agreed basis adopts the winner with no sidecar.
        if content_differs(current.as_ref(), body)
            && content_differs(current.as_ref(), agreed.as_ref().unwrap_or(&SnapBody::Absent))
        {
            if let Some(losing) = current.as_ref().and_then(content_bytes) {
                push_sidecar(fold, index, &local, losing);
                fold.slave_errors[index] = ErrorExpect::CasReject(local.clone());
            }
        }
        write_local(fold, index, &local, body.clone());
    }
}

fn replicate_mode(fold: &mut Fold, global: &RelPath, mode: UnixMode) {
    for index in 0..fold.slaves.len() {
        let Some(local) = to_local(&fold.layout, index, global) else {
            continue;
        };
        chmod_local(fold, index, &local, mode);
    }
}

fn unlink_actor(fold: &mut Fold, basis: &[TreeSnap], actor: Actor, path: &RelPath) {
    match actor {
        Actor::Master => {
            let previous = fold.master.nodes.get(&EntryKey::Path(path.clone())).cloned();
            remove_global(fold, path);
            for index in 0..fold.slaves.len() {
                let Some(local) = to_local(&fold.layout, index, path) else {
                    continue;
                };
                let current = fold.slaves[index]
                    .nodes
                    .get(&EntryKey::Path(local.clone()))
                    .cloned();
                if content_differs(current.as_ref(), previous.as_ref().unwrap_or(&SnapBody::Absent))
                {
                    if let Some(losing) = current.as_ref().and_then(content_bytes) {
                        push_sidecar(fold, index, &local, losing);
                    }
                }
                remove_local(fold, index, &local);
            }
        }
        Actor::Slave(ix) => {
            let index = ix.get() as usize;
            let global = to_global(&fold.layout, index, path);
            let basis_body = basis
                .get(index)
                .and_then(|tree| tree.nodes.get(&EntryKey::Path(path.clone())))
                .cloned();
            let master_body = fold.master.nodes.get(&EntryKey::Path(global.clone())).cloned();
            if same_content(basis_body.as_ref(), master_body.as_ref()) || master_body.is_none() {
                remove_global(fold, &global);
                for slave in 0..fold.slaves.len() {
                    if let Some(local) = to_local(&fold.layout, slave, &global) {
                        remove_local(fold, slave, &local);
                    }
                }
            } else if let Some(losing) = basis_body.as_ref().and_then(content_bytes) {
                push_sidecar(fold, index, path, losing);
                fold.slave_errors[index] = ErrorExpect::CasReject(path.clone());
                if let Some(master_body) = master_body {
                    write_actor(fold, actor, path, master_body);
                }
            }
        }
    }
}

fn rename_actor(
    fold: &mut Fold,
    _basis: &[TreeSnap],
    actor: Actor,
    from: &RelPath,
    to: &RelPath,
    body: SnapBody,
) {
    match actor {
        Actor::Master => {
            move_global(fold, from, to, body.clone());
            for index in 0..fold.slaves.len() {
                let (Some(local_from), Some(local_to)) = (
                    to_local(&fold.layout, index, from),
                    to_local(&fold.layout, index, to),
                ) else {
                    continue;
                };
                let source = fold.slaves[index]
                    .nodes
                    .get(&EntryKey::Path(local_from.clone()))
                    .cloned();
                let dest = fold.slaves[index]
                    .nodes
                    .get(&EntryKey::Path(local_to.clone()))
                    .cloned();
                if source.is_none() {
                    if content_differs(dest.as_ref(), &body) {
                        if let Some(losing) = dest.as_ref().and_then(content_bytes) {
                            push_sidecar(fold, index, &local_to, losing);
                            fold.slave_errors[index] = ErrorExpect::CasReject(local_to.clone());
                        }
                    }
                    write_local(fold, index, &local_to, body.clone());
                } else {
                    move_local(fold, index, &local_from, &local_to, body.clone());
                }
            }
        }
        Actor::Slave(ix) => {
            let index = ix.get() as usize;
            let from_g = to_global(&fold.layout, index, from);
            let to_g = to_global(&fold.layout, index, to);
            let master_from = fold.master.nodes.get(&EntryKey::Path(from_g.clone())).cloned();
            let master_to = fold.master.nodes.get(&EntryKey::Path(to_g.clone())).cloned();
            if master_from.is_none() {
                if content_differs(master_to.as_ref(), &body) {
                    if let Some(losing) = content_bytes(&body) {
                        push_sidecar(fold, index, to, losing);
                        fold.slave_errors[index] = ErrorExpect::CasReject(to.clone());
                    }
                }
                remove_local(fold, index, from);
                if let Some(master_to) = master_to {
                    write_local(fold, index, to, master_to);
                }
                return;
            }
            move_global(fold, &from_g, &to_g, body.clone());
            for slave in 0..fold.slaves.len() {
                let (Some(local_from), Some(local_to)) = (
                    to_local(&fold.layout, slave, &from_g),
                    to_local(&fold.layout, slave, &to_g),
                ) else {
                    continue;
                };
                move_local(fold, slave, &local_from, &local_to, body.clone());
            }
        }
    }
}

fn replicate_master(fold: &mut Fold) {
    for index in 0..fold.slaves.len() {
        fold.slaves[index].nodes.clear();
        for (key, body) in &fold.master.nodes {
            let EntryKey::Path(path) = key else {
                continue;
            };
            let Some(local) = to_local(&fold.layout, index, path) else {
                continue;
            };
            fold.slaves[index]
                .nodes
                .insert(EntryKey::Path(local), body.clone());
        }
    }
}

fn chmod_actor(fold: &mut Fold, actor: Actor, path: &RelPath, mode: UnixMode) {
    match actor {
        Actor::Master => chmod_global(fold, path, mode),
        Actor::Slave(ix) => chmod_local(fold, ix.get() as usize, path, mode),
    }
}

fn chmod_global(fold: &mut Fold, path: &RelPath, mode: UnixMode) {
    touch_mode(&mut fold.master, path, mode);
}

fn chmod_local(fold: &mut Fold, index: usize, path: &RelPath, mode: UnixMode) {
    touch_mode(&mut fold.slaves[index], path, mode);
}

fn touch_mode(tree: &mut TreeSnap, path: &RelPath, mode: UnixMode) {
    let key = EntryKey::Path(path.clone());
    match tree.nodes.get_mut(&key) {
        Some(SnapBody::File { mode: slot, .. }) => *slot = mode,
        Some(SnapBody::Dir {
            mode: slot,
            meta_authoritative,
            ..
        }) => {
            *slot = mode;
            *meta_authoritative = true;
        }
        _ => {}
    }
}

fn write_actor(fold: &mut Fold, actor: Actor, path: &RelPath, body: SnapBody) {
    match actor {
        Actor::Master => write_global(fold, path, body),
        Actor::Slave(ix) => write_local(fold, ix.get() as usize, path, body),
    }
}

fn write_global(fold: &mut Fold, path: &RelPath, body: SnapBody) {
    ensure_parents(&mut fold.master, path);
    fold.master.nodes.insert(EntryKey::Path(path.clone()), body);
}

fn write_local(fold: &mut Fold, index: usize, path: &RelPath, body: SnapBody) {
    ensure_parents(&mut fold.slaves[index], path);
    fold.slaves[index]
        .nodes
        .insert(EntryKey::Path(path.clone()), body);
}

fn remove_global(fold: &mut Fold, path: &RelPath) {
    remove_prefix(&mut fold.master, path);
}

fn remove_local(fold: &mut Fold, index: usize, path: &RelPath) {
    remove_prefix(&mut fold.slaves[index], path);
}

fn move_global(fold: &mut Fold, from: &RelPath, to: &RelPath, body: SnapBody) {
    move_prefix(&mut fold.master, from, to, body);
}

fn move_local(fold: &mut Fold, index: usize, from: &RelPath, to: &RelPath, body: SnapBody) {
    move_prefix(&mut fold.slaves[index], from, to, body);
}

fn remove_prefix(tree: &mut TreeSnap, path: &RelPath) {
    let prefix = path.parts().to_vec();
    tree.nodes.retain(|key, _| match key {
        EntryKey::Path(other) => !is_prefix(&prefix, other.parts()),
        EntryKey::Raw { parent, .. } => !is_prefix(&prefix, parent.parts()),
    });
}

fn move_prefix(tree: &mut TreeSnap, from: &RelPath, to: &RelPath, body: SnapBody) {
    let from_parts = from.parts().to_vec();
    let old: Vec<_> = tree
        .nodes
        .iter()
        .filter_map(|(key, value)| {
            let EntryKey::Path(path) = key else {
                return None;
            };
            path.parts()
                .strip_prefix(from_parts.as_slice())
                .map(|rest| (rest.to_vec(), value.clone()))
        })
        .collect();
    remove_prefix(tree, from);
    ensure_parents(tree, to);
    tree.nodes.insert(EntryKey::Path(to.clone()), body);
    for (rest, child) in old {
        if rest.is_empty() {
            continue;
        }
        let mut parts = to.parts().to_vec();
        parts.extend(rest);
        if let Ok(path) = RelPath::new(parts) {
            tree.nodes.entry(EntryKey::Path(path)).or_insert(child);
        }
    }
}

fn is_prefix(prefix: &[String], parts: &[String]) -> bool {
    parts.starts_with(prefix)
}

fn ensure_parents(tree: &mut TreeSnap, path: &RelPath) {
    let parts = path.parts();
    if parts.len() <= 1 {
        return;
    }
    for len in 1..parts.len() {
        let Ok(parent) = RelPath::new(parts[..len].to_vec()) else {
            continue;
        };
        tree.nodes.entry(EntryKey::Path(parent)).or_insert(SnapBody::Dir {
            mode: UnixMode::new(0o755).expect("mode"),
            mtime: MtimeNs::new(0),
            meta_authoritative: false,
        });
    }
}

fn actor_tree(fold: &Fold, actor: Actor) -> &TreeSnap {
    match actor {
        Actor::Master => &fold.master,
        Actor::Slave(ix) => &fold.slaves[ix.get() as usize],
    }
}

fn actor_tree_mut(fold: &mut Fold, actor: Actor) -> &mut TreeSnap {
    match actor {
        Actor::Master => &mut fold.master,
        Actor::Slave(ix) => &mut fold.slaves[ix.get() as usize],
    }
}

fn push_sidecar(fold: &mut Fold, index: usize, path: &RelPath, bytes: Vec<u8>) {
    let losing_hash = hash_bytes(&bytes).into_bytes();
    fold.sidecars[index].push(Sidecar {
        canonical: path.clone(),
        losing_hash,
        bytes,
    });
}

fn content_bytes(body: &SnapBody) -> Option<Vec<u8>> {
    match body {
        SnapBody::File { bytes, .. } => Some(bytes.clone()),
        SnapBody::Symlink { target } => Some(target.clone()),
        _ => None,
    }
}

fn same_content(left: Option<&SnapBody>, right: Option<&SnapBody>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(a), Some(b)) => match (content_bytes(a), content_bytes(b)) {
            (Some(x), Some(y)) => x == y,
            (None, None) => std::mem::discriminant(a) == std::mem::discriminant(b),
            _ => false,
        },
        _ => false,
    }
}

fn content_differs(left: Option<&SnapBody>, right: &SnapBody) -> bool {
    match (left.and_then(content_bytes), content_bytes(right)) {
        (Some(a), Some(b)) => a != b,
        _ => false,
    }
}

fn to_global(layout: &Layout, slave: usize, path: &RelPath) -> RelPath {
    if nested(layout, slave) {
        let mut parts = vec!["nested".to_string()];
        parts.extend(path.parts().iter().cloned());
        RelPath::new(parts).expect("nested path")
    } else {
        path.clone()
    }
}

fn to_local(layout: &Layout, slave: usize, global: &RelPath) -> Option<RelPath> {
    if !nested(layout, slave) {
        return Some(global.clone());
    }
    let parts = global.parts();
    if parts.first().map(String::as_str) != Some("nested") || parts.len() < 2 {
        return None;
    }
    RelPath::new(parts[1..].to_vec()).ok()
}

fn nested(layout: &Layout, slave: usize) -> bool {
    matches!(
        layout,
        Layout::Two {
            second: SecondView::Nested
        }
    ) && slave == 1
}

/// `None` means the observation matches the projection.
pub(crate) fn judge(projection: &Projection, observed: &Observed) -> Option<Finding> {
    if let Some(actor) = observed.dead.first() {
        return Some(Finding::Crash {
            actor: actor.label(),
            note: "process exited".into(),
        });
    }
    let views = std::iter::once((Actor::Master, &observed.master)).chain(
        observed
            .slaves
            .iter()
            .enumerate()
            .map(|(index, view)| {
                (
                    Actor::Slave(crate::schedule::SlaveIx::from_raw(index as u8)),
                    view,
                )
            }),
    );
    for (actor, view) in views {
        match &view.status.health {
            Health::Stuck | Health::Failed => {
                return Some(Finding::Protocol {
                    actor: actor.label(),
                    token: view.status.last_error.clone(),
                });
            }
            Health::Idle if view.status.pending == 0 => {}
            other => {
                return Some(Finding::Hang {
                    actor: actor.label(),
                    last_status: format!("{other:?} pending={}", view.status.pending),
                });
            }
        }
    }
    let yard = match projection {
        Projection::Determined(determined) => determined.yard,
        Projection::UnspecifiedIo { yard } => *yard,
    };
    if observed.yard != yard {
        return Some(Finding::Escaped {
            path: "yard".into(),
        });
    }
    if !observed.master.sidecars.is_empty() {
        let path = observed.master.sidecars[0].canonical.join();
        return Some(Finding::Mismatch {
            actor: "master".into(),
            path,
            detail: "sidecar".into(),
        });
    }
    let Projection::Determined(determined) = projection else {
        return None;
    };
    if let Some(found) = error_mismatch("master", &determined.master_error, &observed.master.status)
    {
        return Some(found);
    }
    if let Some(found) = judge_tree("master", &determined.master, &observed.master.tree) {
        return Some(found);
    }
    if determined.slaves.len() != observed.slaves.len() {
        return Some(Finding::Mismatch {
            actor: "slave".into(),
            path: String::new(),
            detail: "slave count".into(),
        });
    }
    for (index, (expect, view)) in determined.slaves.iter().zip(&observed.slaves).enumerate() {
        let actor = format!("slave:{index}");
        if let Some(found) = error_mismatch(&actor, &expect.error, &view.status) {
            return Some(found);
        }
        if let Some(found) = judge_tree(&actor, &expect.tree, &view.tree) {
            return Some(found);
        }
        if !same_sidecars(&expect.sidecars, &view.sidecars) {
            let path = expect
                .sidecars
                .first()
                .or(view.sidecars.first())
                .map(|sidecar| sidecar.canonical.join())
                .unwrap_or_default();
            return Some(Finding::Mismatch {
                actor,
                path,
                detail: "sidecar".into(),
            });
        }
    }
    None
}

fn error_mismatch(actor: &str, expect: &ErrorExpect, status: &StatusSnap) -> Option<Finding> {
    let error = status.last_error.trim();
    let ok = match expect {
        ErrorExpect::Empty => error.is_empty() || error == "-",
        ErrorExpect::CasReject(path) => error.contains("cas_reject") && error.contains(&path.join()),
        ErrorExpect::KeepLive(path) => {
            (error.contains("keep_live") || error.contains("error:keep_live"))
                && error.contains(&path.join())
        }
    };
    if ok {
        None
    } else {
        Some(Finding::Protocol {
            actor: actor.into(),
            token: status.last_error.clone(),
        })
    }
}

fn same_sidecars(expect: &[Sidecar], observed: &[Sidecar]) -> bool {
    let mut left = expect.to_vec();
    let mut right = observed.to_vec();
    left.sort_by(|a, b| (&a.canonical, &a.bytes).cmp(&(&b.canonical, &b.bytes)));
    right.sort_by(|a, b| (&a.canonical, &a.bytes).cmp(&(&b.canonical, &b.bytes)));
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right).all(|(want, got)| {
        want.canonical == got.canonical && want.bytes == got.bytes && want.losing_hash == got.losing_hash
    })
}

fn judge_tree(actor: &str, expect: &TreeSnap, observed: &TreeSnap) -> Option<Finding> {
    for (key, want) in &expect.nodes {
        match observed.nodes.get(key) {
            None => {
                return Some(Finding::Mismatch {
                    actor: actor.into(),
                    path: key.label(),
                    detail: "missing".into(),
                });
            }
            Some(got) => {
                if let Some(detail) = body_diff(want, got) {
                    return Some(Finding::Mismatch {
                        actor: actor.into(),
                        path: key.label(),
                        detail,
                    });
                }
            }
        }
    }
    for key in observed.nodes.keys() {
        if !expect.nodes.contains_key(key) {
            return Some(Finding::Mismatch {
                actor: actor.into(),
                path: key.label(),
                detail: "unexpected".into(),
            });
        }
    }
    None
}

fn body_diff(want: &SnapBody, got: &SnapBody) -> Option<String> {
    match (want, got) {
        (
            SnapBody::File {
                bytes: want_bytes,
                mode: want_mode,
                mtime: want_mtime,
            },
            SnapBody::File { bytes, mode, mtime },
        ) => {
            if want_bytes != bytes {
                return Some("bytes".into());
            }
            if want_mode != mode {
                return Some("mode".into());
            }
            if want_mtime != mtime {
                return Some("mtime".into());
            }
            None
        }
        (
            SnapBody::Dir {
                mode: want_mode,
                mtime: want_mtime,
                meta_authoritative,
            },
            SnapBody::Dir { mode, mtime, .. },
        ) => {
            if *meta_authoritative && (want_mode != mode || want_mtime != mtime) {
                return Some("meta".into());
            }
            None
        }
        (SnapBody::Symlink { target: want_target }, SnapBody::Symlink { target }) => {
            if want_target != target {
                Some("target".into())
            } else {
                None
            }
        }
        (SnapBody::Residue(want_kind), SnapBody::Residue(kind)) => {
            if want_kind != kind {
                Some("residue".into())
            } else {
                None
            }
        }
        (SnapBody::Absent, SnapBody::Absent) => None,
        _ => Some("kind".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::{Actor, DiskOp, MtimeNs, UnixMode};

    fn path(text: &str) -> RelPath {
        RelPath::new(text.split('/').map(|part| part.to_string()).collect()).unwrap()
    }

    fn file(bytes: &[u8], mode: u32, mtime: i64) -> SnapBody {
        SnapBody::File {
            bytes: bytes.to_vec(),
            mode: UnixMode::new(mode).unwrap(),
            mtime: MtimeNs::new(mtime),
        }
    }

    fn idle(last_error: &str) -> StatusSnap {
        StatusSnap {
            health: Health::Idle,
            pending: 0,
            rescan: 1,
            last_error: last_error.into(),
        }
    }

    fn view(tree: TreeSnap, sidecars: Vec<Sidecar>, last_error: &str) -> ActorView {
        ActorView {
            tree,
            sidecars,
            status: idle(last_error),
        }
    }

    fn tree(pairs: Vec<(RelPath, SnapBody)>) -> TreeSnap {
        TreeSnap {
            nodes: pairs
                .into_iter()
                .map(|(path, body)| (EntryKey::Path(path), body))
                .collect(),
        }
    }

    #[test]
    fn retouch_same_stamp_slave_keeps_old_bytes() {
        let hello = path("hello.txt");
        let mode = UnixMode::new(0o644).unwrap();
        let mtime = MtimeNs::new(10);
        let mut intent = Intent::new(&Layout::One);
        intent.push(Event::Freeze(Actor::Master));
        intent.push(Event::Disk {
            op: DiskOp::Put {
                actor: Actor::Master,
                path: hello.clone(),
                bytes: b"old!".to_vec(),
                mode,
                mtime,
            },
            snap: LocalSnap::Applied(file(b"old!", 0o644, 10)),
        });
        intent.push(Event::Thaw(Actor::Master));
        intent.push(Event::Settle);
        intent.push(Event::Freeze(Actor::Master));
        intent.push(Event::Disk {
            op: DiskOp::RetouchSameStamp {
                actor: Actor::Master,
                path: hello.clone(),
                bytes: b"new!".to_vec(),
            },
            snap: LocalSnap::Applied(file(b"new!", 0o644, 10)),
        });
        intent.push(Event::Thaw(Actor::Master));
        intent.push(Event::Settle);

        let projection = project(&intent);
        let master = tree(vec![(hello.clone(), file(b"new!", 0o644, 10))]);
        let slave_old = tree(vec![(hello.clone(), file(b"old!", 0o644, 10))]);
        let kept = Observed {
            master: view(master.clone(), vec![], "-"),
            slaves: vec![view(slave_old, vec![], "-")],
            yard: [0; 32],
            dead: vec![],
        };
        assert_eq!(judge(&projection, &kept), None);

        let slave_new = tree(vec![(hello.clone(), file(b"new!", 0o644, 10))]);
        let replicated = Observed {
            master: view(master, vec![], "-"),
            slaves: vec![view(slave_new, vec![], "-")],
            yard: [0; 32],
            dead: vec![],
        };
        assert_eq!(
            judge(&projection, &replicated),
            Some(Finding::Mismatch {
                actor: "slave:0".into(),
                path: "hello.txt".into(),
                detail: "bytes".into(),
            })
        );
    }

    #[test]
    fn content_divergent_slave_expects_sidecar_master_expects_none() {
        let hello = path("hello.txt");
        let mode = UnixMode::new(0o644).unwrap();
        let mtime = MtimeNs::new(10);
        let mut intent = Intent::new(&Layout::One);
        intent.push(Event::Freeze(Actor::Master));
        intent.push(Event::Disk {
            op: DiskOp::Put {
                actor: Actor::Master,
                path: hello.clone(),
                bytes: b"aaaa".to_vec(),
                mode,
                mtime,
            },
            snap: LocalSnap::Applied(file(b"aaaa", 0o644, 10)),
        });
        intent.push(Event::Thaw(Actor::Master));
        intent.push(Event::Settle);
        let slave = Actor::Slave(slave_ix());
        intent.push(Event::Freeze(Actor::Master));
        intent.push(Event::Freeze(slave));
        intent.push(Event::Disk {
            op: DiskOp::Put {
                actor: Actor::Master,
                path: hello.clone(),
                bytes: b"bbbb".to_vec(),
                mode,
                mtime,
            },
            snap: LocalSnap::Applied(file(b"bbbb", 0o644, 10)),
        });
        intent.push(Event::Disk {
            op: DiskOp::Put {
                actor: slave,
                path: hello.clone(),
                bytes: b"cccc".to_vec(),
                mode,
                mtime,
            },
            snap: LocalSnap::Applied(file(b"cccc", 0o644, 10)),
        });
        intent.push(Event::Thaw(Actor::Master));
        intent.push(Event::Thaw(slave));
        intent.push(Event::Settle);

        let projection = project(&intent);
        let Projection::Determined(determined) = &projection else {
            panic!("determined projection");
        };
        assert!(determined.slaves[0].sidecars.len() == 1);
        assert_eq!(determined.slaves[0].sidecars[0].bytes, b"cccc");
        assert_eq!(determined.slaves[0].sidecars[0].canonical, hello);
        let sidecar = determined.slaves[0].sidecars[0].clone();
        let master = tree(vec![(hello.clone(), file(b"bbbb", 0o644, 10))]);
        let slave = tree(vec![(hello.clone(), file(b"bbbb", 0o644, 10))]);
        let matched = Observed {
            master: view(master.clone(), vec![], "-"),
            slaves: vec![view(slave, vec![sidecar.clone()], "cas_reject:/src/hello.txt")],
            yard: [0; 32],
            dead: vec![],
        };
        assert_eq!(judge(&projection, &matched), None);

        let master_sidecar = Observed {
            master: view(master, vec![sidecar], "-"),
            slaves: matched_slaves(&matched),
            yard: [0; 32],
            dead: vec![],
        };
        assert_eq!(
            judge(&projection, &master_sidecar),
            Some(Finding::Mismatch {
                actor: "master".into(),
                path: "hello.txt".into(),
                detail: "sidecar".into(),
            })
        );
    }

    fn slave_ix() -> crate::schedule::SlaveIx {
        crate::schedule::SlaveIx::new(0, Layout::One.slave_count()).unwrap()
    }

    fn matched_slaves(observed: &Observed) -> Vec<ActorView> {
        observed.slaves.clone()
    }

    #[test]
    fn directory_mtime_is_not_a_mismatch_without_meta_authority() {
        let file_path = path("sub/file");
        let dir_path = path("sub");
        let mode = UnixMode::new(0o644).unwrap();
        let mtime = MtimeNs::new(10);
        let mut intent = Intent::new(&Layout::One);
        intent.push(Event::Freeze(Actor::Master));
        intent.push(Event::Disk {
            op: DiskOp::Put {
                actor: Actor::Master,
                path: file_path.clone(),
                bytes: b"body".to_vec(),
                mode,
                mtime,
            },
            snap: LocalSnap::Applied(file(b"body", 0o644, 10)),
        });
        intent.push(Event::Thaw(Actor::Master));
        intent.push(Event::Settle);

        let projection = project(&intent);
        let Projection::Determined(determined) = &projection else {
            panic!("determined projection");
        };
        match determined.master.nodes.get(&EntryKey::Path(dir_path.clone())) {
            Some(SnapBody::Dir {
                meta_authoritative: false,
                ..
            }) => {}
            other => panic!("parent dir meta is not authoritative: {other:?}"),
        }
        let nodes = vec![
            (
                dir_path,
                SnapBody::Dir {
                    mode: UnixMode::new(0o700).unwrap(),
                    mtime: MtimeNs::new(50),
                    meta_authoritative: false,
                },
            ),
            (file_path, file(b"body", 0o644, 10)),
        ];
        let observed = Observed {
            master: view(tree(nodes.clone()), vec![], "-"),
            slaves: vec![view(tree(nodes), vec![], "-")],
            yard: [0; 32],
            dead: vec![],
        };
        assert_eq!(judge(&projection, &observed), None);
    }
}

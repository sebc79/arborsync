use crate::hash::FileNode;
use crate::merkle::DirChild;
use crate::meta::EntryKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreeWay {
    Pull,
    AnnounceCas,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalkAction {
    Match,
    Recurse,
    Pull,
    AnnounceCreate,
    AnnounceDelete,
    AnnounceCas,
    TypeChange,
}

pub fn decide_three_way(
    local: FileNode,
    last_synced: Option<FileNode>,
    master: FileNode,
) -> ThreeWay {
    if last_synced == Some(local) && master != local {
        ThreeWay::Pull
    } else {
        ThreeWay::AnnounceCas
    }
}

pub fn decide_child(
    slave: Option<&DirChild>,
    master: Option<&DirChild>,
    last_synced: Option<FileNode>,
    local_file: Option<FileNode>,
) -> WalkAction {
    match (slave, master) {
        (None, None) => WalkAction::Match,
        (None, Some(_)) => WalkAction::Pull,
        (Some(slave), None) => slave_only(slave, last_synced, local_file),
        (Some(slave), Some(master)) => both_present(slave, master, last_synced, local_file),
    }
}

fn both_present(
    slave: &DirChild,
    master: &DirChild,
    last_synced: Option<FileNode>,
    local_file: Option<FileNode>,
) -> WalkAction {
    if slave.kind() != master.kind() {
        return WalkAction::TypeChange;
    }
    if child_hash(slave) == child_hash(master) {
        return WalkAction::Match;
    }
    if slave.kind() == EntryKind::Dir {
        return WalkAction::Recurse;
    }
    match decide_three_way(
        local_file
            .or_else(|| leaf_node(slave))
            .expect("file or symlink"),
        last_synced,
        leaf_node(master).expect("file or symlink"),
    ) {
        ThreeWay::Pull => WalkAction::Pull,
        ThreeWay::AnnounceCas => WalkAction::AnnounceCas,
    }
}

fn slave_only(
    slave: &DirChild,
    last_synced: Option<FileNode>,
    local_file: Option<FileNode>,
) -> WalkAction {
    let local = local_file.or_else(|| leaf_node(slave));
    match last_synced {
        None => WalkAction::AnnounceCreate,
        Some(synced) if local == Some(synced) => WalkAction::AnnounceDelete,
        Some(_) => WalkAction::AnnounceCas,
    }
}

fn leaf_node(child: &DirChild) -> Option<FileNode> {
    match child {
        DirChild::File { node, .. } | DirChild::Symlink { node, .. } => Some(*node),
        DirChild::Directory { .. } => None,
    }
}

fn child_hash(child: &DirChild) -> [u8; 32] {
    match child {
        DirChild::File { node, .. } | DirChild::Symlink { node, .. } => node.into_bytes(),
        DirChild::Directory { node, .. } => node.into_bytes(),
    }
}

use crate::hash::FileNode;
use crate::merkle::DirChild;
use crate::meta::EntryKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalkAction {
    Matched,
    Recurse,
    Pull,
    AnnounceCreate,
    AnnounceDelete,
    AnnounceCas,
    /// Slave-only under a restore epoch. Steady reconcile still announces that path.
    DropLocal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreeAuthority {
    Steady,
    Restore,
}

pub fn decide_under(
    authority: TreeAuthority,
    slave: Option<&DirChild>,
    master: Option<&DirChild>,
    last_synced: Option<FileNode>,
    local_node: Option<FileNode>,
) -> WalkAction {
    if authority == TreeAuthority::Steady {
        return decide_child(slave, master, last_synced, local_node);
    }
    let left = presence(slave);
    let right = presence(master);
    if left.is_some() && right.is_none() {
        return WalkAction::DropLocal;
    }
    if let (Some(left), Some(right)) = (left, right) {
        let both_dirs = left.kind == EntryKind::Dir && right.kind == EntryKind::Dir;
        if both_dirs && left.hash != right.hash {
            return WalkAction::Recurse;
        }
        if !both_dirs && (left.kind != right.kind || left.hash != right.hash) {
            return WalkAction::Pull;
        }
    }
    decide_child(slave, master, last_synced, local_node)
}

pub fn decide_child(
    slave: Option<&DirChild>,
    master: Option<&DirChild>,
    last_synced: Option<FileNode>,
    local_node: Option<FileNode>,
) -> WalkAction {
    match (presence(slave), presence(master)) {
        (None, None) => WalkAction::Matched,
        (Some(left), Some(right)) if left.kind == right.kind && left.hash == right.hash => {
            WalkAction::Matched
        }
        (
            Some(Presence {
                kind: EntryKind::Dir,
                ..
            }),
            Some(Presence {
                kind: EntryKind::Dir,
                ..
            }),
        ) => WalkAction::Recurse,
        (Some(left), Some(right)) if left.kind != right.kind => {
            if last_synced.is_none() || local_node == last_synced {
                WalkAction::Pull
            } else {
                WalkAction::AnnounceCas
            }
        }
        (None, Some(_)) => WalkAction::Pull,
        (Some(_), None) => match last_synced {
            Some(_) if local_node != last_synced => WalkAction::AnnounceCas,
            _ => WalkAction::AnnounceCreate,
        },
        (Some(_), Some(_)) => {
            if last_synced.is_some() && local_node == last_synced {
                WalkAction::Pull
            } else {
                WalkAction::AnnounceCas
            }
        }
    }
}

struct Presence {
    kind: EntryKind,
    hash: [u8; 32],
}

fn presence(child: Option<&DirChild>) -> Option<Presence> {
    child.map(|child| Presence {
        kind: child.kind(),
        hash: child_hash(child),
    })
}

fn child_hash(child: &DirChild) -> [u8; 32] {
    match child {
        DirChild::File { node, .. } | DirChild::Symlink { node, .. } => *node.as_bytes(),
        DirChild::Directory { node, .. } => *node.as_bytes(),
    }
}

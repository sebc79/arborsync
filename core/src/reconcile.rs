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
}

/// `slave` / `master` are that name's DirList entries (None = name absent on that side).
/// `last_synced` and `local_node` are FileNode for a file/symlink. For directories
/// `local_node` is unused; dir identity is the DirChild hash.
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
            None => WalkAction::AnnounceCreate,
            Some(synced) if local_node == Some(synced) => WalkAction::AnnounceDelete,
            Some(_) => WalkAction::AnnounceCas,
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

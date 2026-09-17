use arborsync_core::hash::ContentHash;
use arborsync_core::merkle::{dir_node, empty_dir_node, file_node, DirChild};
use arborsync_core::meta::FileMetadata;
use arborsync_core::reconcile::{decide_child, decide_three_way, ThreeWay, WalkAction};
use arborsync_core::test_support::name;

const MTIME: i64 = 1_700_000_000_000;

fn node(byte: u8) -> arborsync_core::hash::FileNode {
    file_node(&FileMetadata::file(
        3,
        MTIME,
        0o100644,
        ContentHash::from_bytes([byte; 32]),
    ))
}

fn file(label: &str, byte: u8) -> DirChild {
    DirChild::File {
        name: name(label),
        node: node(byte),
    }
}

fn symlink(label: &str, byte: u8) -> DirChild {
    DirChild::Symlink {
        name: name(label),
        node: node(byte),
    }
}

fn directory(label: &str, filled: bool) -> DirChild {
    let child = file("inner", 7);
    DirChild::Directory {
        name: name(label),
        node: if filled {
            dir_node(&[child])
        } else {
            empty_dir_node()
        },
    }
}

#[test]
fn decide_three_way_picks_pull_only_when_local_still_matches_last_synced() {
    let local = node(1);
    let master = node(2);
    let other = node(3);

    assert_eq!(decide_three_way(local, Some(local), master), ThreeWay::Pull);
    assert_eq!(
        decide_three_way(local, Some(master), master),
        ThreeWay::AnnounceCas
    );
    assert_eq!(
        decide_three_way(local, Some(other), master),
        ThreeWay::AnnounceCas
    );
}

#[test]
fn decide_child_covers_each_presence_rule() {
    let local = file("hello.txt", 1);
    let master = file("hello.txt", 2);
    let same = file("hello.txt", 1);
    let empty = directory("src", false);
    let filled = directory("src", true);
    let link = symlink("link", 1);
    let other_link = symlink("link", 2);
    let as_dir = directory("hello.txt", false);

    assert_eq!(decide_child(None, None, None, None), WalkAction::Match);
    assert_eq!(
        decide_child(Some(&same), Some(&local), None, Some(node(1))),
        WalkAction::Match
    );
    assert_eq!(
        decide_child(Some(&empty), Some(&empty), None, None),
        WalkAction::Match
    );
    assert_eq!(
        decide_child(Some(&link), Some(&link), None, Some(node(1))),
        WalkAction::Match
    );
    assert_eq!(
        decide_child(Some(&empty), Some(&filled), None, None),
        WalkAction::Recurse
    );
    assert_eq!(
        decide_child(Some(&local), Some(&master), Some(node(1)), Some(node(1))),
        WalkAction::Pull
    );
    assert_eq!(
        decide_child(Some(&local), Some(&master), Some(node(2)), Some(node(1))),
        WalkAction::AnnounceCas
    );
    assert_eq!(
        decide_child(Some(&local), Some(&master), Some(node(3)), Some(node(1))),
        WalkAction::AnnounceCas
    );
    assert_eq!(
        decide_child(Some(&link), Some(&other_link), Some(node(1)), None),
        WalkAction::Pull
    );
    assert_eq!(
        decide_child(Some(&local), Some(&as_dir), Some(node(1)), Some(node(1))),
        WalkAction::TypeChange
    );
    assert_eq!(
        decide_child(Some(&local), Some(&link), None, Some(node(1))),
        WalkAction::TypeChange
    );
    assert_eq!(
        decide_child(None, Some(&master), None, None),
        WalkAction::Pull
    );
    assert_eq!(
        decide_child(Some(&local), None, None, Some(node(1))),
        WalkAction::AnnounceCreate
    );
    assert_eq!(
        decide_child(Some(&local), None, Some(node(1)), None),
        WalkAction::AnnounceDelete
    );
    assert_eq!(
        decide_child(Some(&local), None, Some(node(9)), Some(node(1))),
        WalkAction::AnnounceCas
    );
}

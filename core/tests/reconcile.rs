use arborsync_core::hash::ContentHash;
use arborsync_core::merkle::{DirChild, dir_node, empty_dir_node, file_node};
use arborsync_core::meta::FileMetadata;
use arborsync_core::reconcile::{WalkAction, decide_child};
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
fn decide_child_equal_hashes_is_matched() {
    let same = file("hello.txt", 1);
    assert_eq!(
        decide_child(Some(&same), Some(&same), None, Some(node(1))),
        WalkAction::Matched
    );
}

#[test]
fn decide_child_both_dirs_different_hashes_is_recurse() {
    let empty = directory("src", false);
    let filled = directory("src", true);
    assert_eq!(
        decide_child(Some(&empty), Some(&filled), None, None),
        WalkAction::Recurse
    );
}

#[test]
fn decide_child_master_only_file_is_pull() {
    let master = file("hello.txt", 2);
    assert_eq!(
        decide_child(None, Some(&master), None, None),
        WalkAction::Pull
    );
}

#[test]
fn decide_child_slave_only_without_last_synced_is_announce_create() {
    let local = file("hello.txt", 1);
    assert_eq!(
        decide_child(Some(&local), None, None, Some(node(1))),
        WalkAction::AnnounceCreate
    );
}

#[test]
fn decide_child_slave_only_local_equals_last_synced_is_announce_create() {
    let local = file("hello.txt", 1);
    assert_eq!(
        decide_child(Some(&local), None, Some(node(1)), Some(node(1))),
        WalkAction::AnnounceCreate
    );
}

#[test]
fn decide_child_slave_only_dir_local_equals_last_synced_is_announce_create() {
    let local = directory("nested", false);
    assert_eq!(
        decide_child(Some(&local), None, Some(node(1)), Some(node(1))),
        WalkAction::AnnounceCreate
    );
}

#[test]
fn decide_child_slave_only_local_differs_from_last_synced_is_announce_cas() {
    let local = file("hello.txt", 1);
    assert_eq!(
        decide_child(Some(&local), None, Some(node(9)), Some(node(1))),
        WalkAction::AnnounceCas
    );
}

#[test]
fn decide_child_both_files_differ_local_equals_last_synced_is_pull() {
    let local = file("hello.txt", 1);
    let master = file("hello.txt", 2);
    assert_eq!(
        decide_child(Some(&local), Some(&master), Some(node(1)), Some(node(1))),
        WalkAction::Pull
    );
}

#[test]
fn decide_child_both_files_differ_master_equals_last_synced_is_announce_cas() {
    let local = file("hello.txt", 1);
    let master = file("hello.txt", 2);
    assert_eq!(
        decide_child(Some(&local), Some(&master), Some(node(2)), Some(node(1))),
        WalkAction::AnnounceCas
    );
}

#[test]
fn decide_child_both_files_differ_both_differ_from_last_synced_is_announce_cas() {
    let local = file("hello.txt", 1);
    let master = file("hello.txt", 2);
    assert_eq!(
        decide_child(Some(&local), Some(&master), Some(node(3)), Some(node(1))),
        WalkAction::AnnounceCas
    );
}

#[test]
fn decide_child_kind_change_local_equals_last_synced_is_pull() {
    let local = file("hello.txt", 1);
    let as_dir = directory("hello.txt", false);
    assert_eq!(
        decide_child(Some(&local), Some(&as_dir), Some(node(1)), Some(node(1))),
        WalkAction::Pull
    );
}

#[test]
fn decide_child_last_synced_none_both_files_differ_is_announce_cas() {
    let local = file("hello.txt", 1);
    let master = file("hello.txt", 2);
    assert_eq!(
        decide_child(Some(&local), Some(&master), None, Some(node(1))),
        WalkAction::AnnounceCas
    );
}

#[test]
fn decide_child_both_none_is_matched() {
    assert_eq!(decide_child(None, None, None, None), WalkAction::Matched);
}

#[test]
fn decide_child_equal_symlink_hashes_is_matched() {
    let link = symlink("link", 1);
    assert_eq!(
        decide_child(Some(&link), Some(&link), None, Some(node(1))),
        WalkAction::Matched
    );
}

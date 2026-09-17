use arborsync_core::config::SlaveAcl;
use arborsync_core::hash::ContentHash;
use arborsync_core::keys::format_hex_key;
use arborsync_core::master::{LocalEvent, Master, MemoryContent, Reply};
use arborsync_core::merkle::{dir_node, empty_dir_node, file_node, DirChild};
use arborsync_core::meta::{hash_bytes, EntryKind, FileMetadata};
use arborsync_core::protocol::{CheckoutRef, ProtocolMessage};
use arborsync_core::reconcile::{decide_child, decide_three_way, ThreeWay, WalkAction};
use arborsync_core::test_support::{name, p, MemoryStorage, SyncSandbox};
use arborsync_core::LoadedMaster;

const ALICE: [u8; 32] = [0xA1; 32];

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

fn alice_master(sandbox: &SyncSandbox) -> Master<MemoryStorage, MemoryContent> {
    let cfg_path = sandbox.write_master_config(vec![SlaveAcl {
        id: "dev-alice".into(),
        public_keys: vec![format_hex_key(&ALICE)],
        allowed_prefixes: vec!["/src".into()],
    }]);
    let cfg = LoadedMaster::load(&cfg_path).unwrap();
    Master::open(cfg, MemoryStorage::new(), MemoryContent::new()).unwrap()
}

fn subscribe() -> ProtocolMessage {
    ProtocolMessage::Subscribe {
        slave_id: "dev-alice".into(),
        checkouts: vec![CheckoutRef {
            id: "src".into(),
            central: p("/src"),
        }],
    }
}

fn send(reply: Reply) -> ProtocolMessage {
    match reply {
        Reply::Send(msg) => msg,
        other => panic!("expected Send, got {other:?}"),
    }
}

#[test]
fn root_report_on_empty_tree_matches_empty_dir_node() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    send(master.handle(ALICE, subscribe()).unwrap());

    match send(
        master
            .handle(
                ALICE,
                ProtocolMessage::RootReport {
                    checkout_id: "src".into(),
                    path: p("/src"),
                    root: empty_dir_node().into(),
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::RootAck {
            checkout_id,
            path,
            matched,
            master_root,
        } => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, p("/src"));
            assert!(matched);
            assert_eq!(master_root, empty_dir_node().into());
        }
        other => panic!("expected RootAck, got {other:?}"),
    }
}

#[test]
fn root_report_of_empty_hash_is_unmatched_after_a_real_file() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    send(master.handle(ALICE, subscribe()).unwrap());

    sandbox
        .tree(&sandbox.central_root())
        .file("src/hello.txt", b"hello");
    master
        .note_local(LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    let live = master.meta(&p("/src/hello.txt")).unwrap().unwrap();
    let subtree = dir_node(&[DirChild::File {
        name: name("hello.txt"),
        node: file_node(&live),
    }]);

    match send(
        master
            .handle(
                ALICE,
                ProtocolMessage::RootReport {
                    checkout_id: "src".into(),
                    path: p("/src"),
                    root: empty_dir_node().into(),
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::RootAck {
            matched,
            master_root,
            ..
        } => {
            assert!(!matched);
            assert_eq!(master_root, subtree.into());
        }
        other => panic!("expected RootAck, got {other:?}"),
    }
}

#[test]
fn dir_list_request_for_src_returns_the_indexed_child() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    send(master.handle(ALICE, subscribe()).unwrap());

    sandbox
        .tree(&sandbox.central_root())
        .file("src/hello.txt", b"hello");
    master
        .note_local(LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    let live = master.meta(&p("/src/hello.txt")).unwrap().unwrap();

    match send(
        master
            .handle(
                ALICE,
                ProtocolMessage::DirListRequest {
                    checkout_id: "src".into(),
                    path: p("/src"),
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::DirListResponse {
            checkout_id,
            path,
            entries,
        } => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, p("/src"));
            assert_eq!(
                entries,
                vec![DirChild::File {
                    name: name("hello.txt"),
                    node: file_node(&live),
                }]
            );
        }
        other => panic!("expected DirListResponse, got {other:?}"),
    }
}

#[test]
fn dir_list_request_for_a_file_returns_file_announce() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    send(master.handle(ALICE, subscribe()).unwrap());

    sandbox
        .tree(&sandbox.central_root())
        .file("src/hello.txt", b"hello");
    master
        .note_local(LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    let live = master.meta(&p("/src/hello.txt")).unwrap().unwrap();

    match send(
        master
            .handle(
                ALICE,
                ProtocolMessage::DirListRequest {
                    checkout_id: "src".into(),
                    path: p("/src/hello.txt"),
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::FileAnnounce {
            checkout_id,
            path,
            new,
            basis,
        } => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, p("/src/hello.txt"));
            assert_eq!(new, live);
            assert_eq!(new.kind, EntryKind::File);
            assert_eq!(new.content_hash, hash_bytes(b"hello"));
            assert_eq!(basis, None);
        }
        other => panic!("expected FileAnnounce, got {other:?}"),
    }
}

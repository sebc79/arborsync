use arborsync_core::LoadedMaster;
use arborsync_core::RedbStorage;
use arborsync_core::Storage;
use arborsync_core::LocalEvent;
use arborsync_core::config::{ReloadError, SlaveAcl};
use arborsync_core::hash::{ContentHash, FileNode};
use arborsync_core::keys::format_hex_key;
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;

use arborsync_core::master::{
    CasDecision, CentralWalk, Master, MemoryContent, PreparedDelete, Reply, WholeFileLater,
    decide_cas, reclaim_tree,
};
use arborsync_core::merkle::{self, DirChild, file_node};
use arborsync_core::meta::{EntryKind, FileMetadata, hash_bytes};
use arborsync_core::path::RESERVED_CONFLICTS;
use arborsync_core::protocol::{BulkEncoding, BulkHeader, CheckoutRef, ProtocolMessage};
use arborsync_core::test_support::{MemoryStorage, SyncSandbox, name, p};

const ALICE: [u8; 32] = [0xA1; 32];
const BACKUP: [u8; 32] = [0xB1; 32];
const MTIME: i64 = 1_700_000_000_000;

fn file(byte: u8) -> FileMetadata {
    FileMetadata::file(3, MTIME, 0o100644, ContentHash::from_bytes([byte; 32]))
}

fn dir() -> FileMetadata {
    FileMetadata::directory(MTIME, 0o040755)
}

fn slave_acl(id: &str, key: [u8; 32], prefixes: &[&str]) -> SlaveAcl {
    SlaveAcl {
        id: id.into(),
        public_keys: vec![format_hex_key(&key)],
        allowed_prefixes: prefixes.iter().map(|s| (*s).to_string()).collect(),
    }
}

fn subscribe(id: &str, checkouts: &[(&str, &str)]) -> ProtocolMessage {
    ProtocolMessage::Subscribe {
        slave_id: id.into(),
        checkouts: checkouts
            .iter()
            .map(|(id, central)| CheckoutRef {
                id: (*id).into(),
                central: p(central),
            })
            .collect(),
    }
}

fn two_slave_master(
    sandbox: &SyncSandbox,
    bodies: MemoryContent,
) -> Master<MemoryStorage, MemoryContent> {
    let cfg_path = sandbox.write_master_config(vec![
        slave_acl("dev-alice", ALICE, &["/src"]),
        slave_acl("backup-1", BACKUP, &["/"]),
    ]);
    let cfg = LoadedMaster::load(&cfg_path).unwrap();
    Master::open(cfg, MemoryStorage::new(), bodies).unwrap()
}

#[test]
fn cas_accepts_a_create_on_absence_or_a_matching_basis() {
    let live = file(1);
    let rival = file(2);
    let live_dir = dir();
    let stale_dir = FileMetadata::directory(MTIME, 0o040700);

    type Case<'a> = (
        &'a str,
        Option<&'a FileMetadata>,
        Option<FileNode>,
        Option<EntryKind>,
        CasDecision,
    );
    let cases: Vec<Case<'_>> = vec![
        (
            "create an absent file",
            None,
            None,
            Some(EntryKind::File),
            CasDecision::Accept,
        ),
        (
            "create an absent directory",
            None,
            None,
            Some(EntryKind::Dir),
            CasDecision::Accept,
        ),
        (
            "create over a live file",
            Some(&live),
            None,
            Some(EntryKind::File),
            CasDecision::Reject {
                current: Some(live.clone()),
            },
        ),
        (
            "update on a matching basis",
            Some(&live),
            Some(file_node(&live)),
            Some(EntryKind::File),
            CasDecision::Accept,
        ),
        (
            "update on a stale basis",
            Some(&live),
            Some(file_node(&rival)),
            Some(EntryKind::File),
            CasDecision::Reject {
                current: Some(live.clone()),
            },
        ),
        (
            "delete on a matching basis",
            Some(&live),
            Some(file_node(&live)),
            None,
            CasDecision::Accept,
        ),
        (
            "basis for a path that is gone",
            None,
            Some(file_node(&live)),
            Some(EntryKind::File),
            CasDecision::Reject { current: None },
        ),
        (
            "file replaced by a directory",
            Some(&live),
            Some(file_node(&live)),
            Some(EntryKind::Dir),
            CasDecision::Accept,
        ),
        (
            "directory replaced by a file",
            Some(&live_dir),
            Some(file_node(&live_dir)),
            Some(EntryKind::File),
            CasDecision::Accept,
        ),
        (
            "file replaced by a symlink",
            Some(&live),
            Some(file_node(&live)),
            Some(EntryKind::Symlink),
            CasDecision::Accept,
        ),
        (
            "kind change on a stale basis",
            Some(&live),
            Some(file_node(&rival)),
            Some(EntryKind::Dir),
            CasDecision::Reject {
                current: Some(live.clone()),
            },
        ),
        (
            "update a directory on a matching basis",
            Some(&live_dir),
            Some(file_node(&live_dir)),
            Some(EntryKind::Dir),
            CasDecision::Accept,
        ),
        (
            "update a directory on a stale basis",
            Some(&live_dir),
            Some(file_node(&stale_dir)),
            Some(EntryKind::Dir),
            CasDecision::Reject {
                current: Some(live_dir.clone()),
            },
        ),
        (
            "delete a directory on a matching basis",
            Some(&live_dir),
            Some(file_node(&live_dir)),
            None,
            CasDecision::Accept,
        ),
        (
            "delete a directory on a stale basis",
            Some(&live_dir),
            Some(file_node(&stale_dir)),
            None,
            CasDecision::Reject {
                current: Some(live_dir.clone()),
            },
        ),
    ];

    for (label, current, basis, new_kind, expected) in cases {
        assert_eq!(decide_cas(current, basis, new_kind), expected, "{label}");
    }
}

#[test]
fn successful_content_replace_writes_no_sidecar() {
    let sandbox = SyncSandbox::new();
    let old = b"old";
    let new = b"new";
    let old_hash = hash_bytes(old);
    let new_hash = hash_bytes(new);
    let mut bodies = MemoryContent::new();
    bodies.offer(old_hash, old.to_vec());
    bodies.offer(new_hash, new.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let previous = FileMetadata::file(old.len() as u64, MTIME, 0o100644, old_hash);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: previous.clone(),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept { .. }) => {}
        other => panic!("expected CasAccept, got {other:?}"),
    }

    let incoming = FileMetadata::file(new.len() as u64, MTIME, 0o100644, new_hash);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: incoming.clone(),
                basis: Some(file_node(&previous)),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            file_node: node, ..
        }) => assert_eq!(node, Some(file_node(&incoming))),
        other => panic!("expected CasAccept, got {other:?}"),
    }

    assert_eq!(
        std::fs::read(sandbox.central_root().join("src/hello.txt")).unwrap(),
        new
    );
    assert!(!sandbox.central_root().join(RESERVED_CONFLICTS).exists());
}

#[test]
fn handle_type_change_file_to_dir_accepts_and_fans_out() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master
        .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
        .unwrap();

    let previous = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: previous.clone(),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept { .. }) => {}
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert_eq!(master.poll(BACKUP).len(), 1);

    let new = dir();
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: new.clone(),
                basis: Some(file_node(&previous)),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            checkout_id,
            path,
            file_node: node,
        }) => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, p("/src/hello.txt"));
            assert_eq!(node, Some(file_node(&new)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }

    let host = sandbox.central_root().join("src/hello.txt");
    assert!(host.is_dir());
    assert_eq!(master.meta(&p("/src/hello.txt")).unwrap().unwrap(), new);

    let pushed = master.poll(BACKUP);
    assert_eq!(pushed.len(), 1);
    match &pushed[0] {
        ProtocolMessage::FileAnnounce {
            checkout_id,
            path,
            new: announced,
            basis,
        } => {
            assert_eq!(checkout_id, "bak");
            assert_eq!(path, &p("/src/hello.txt"));
            assert_eq!(announced, &new);
            assert_eq!(*basis, Some(file_node(&previous)));
        }
        other => panic!("expected FileAnnounce, got {other:?}"),
    }
    assert!(master.poll(ALICE).is_empty());

    master
        .note_local(LocalEvent::Removed(p("/src/hello.txt")))
        .unwrap();
    assert!(host.is_dir());
    assert_eq!(master.meta(&p("/src/hello.txt")).unwrap().unwrap(), new);
    assert!(master.poll(BACKUP).is_empty());
    assert!(master.poll(ALICE).is_empty());
}

#[test]
fn handle_type_change_file_to_symlink_and_back() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let file_hash = hash_bytes(hello);
    let target = b"somewhere";
    let link_hash = hash_bytes(target);
    let mut bodies = MemoryContent::new();
    bodies.offer(file_hash, hello.to_vec());
    bodies.offer(link_hash, target.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let previous = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, file_hash);
    master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: previous.clone(),
                basis: None,
            },
        )
        .unwrap();

    let link = FileMetadata::symlink(target.len() as u64, MTIME, 0o120777, link_hash);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: link.clone(),
                basis: Some(file_node(&previous)),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            file_node: node, ..
        }) => {
            assert_eq!(node, Some(file_node(&link)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }
    let host = sandbox.central_root().join("src/hello.txt");
    assert!(
        std::fs::symlink_metadata(&host)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read_link(&host).unwrap(),
        std::path::Path::new("somewhere")
    );
    assert_eq!(master.meta(&p("/src/hello.txt")).unwrap().unwrap(), link);

    let again = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, file_hash);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: again.clone(),
                basis: Some(file_node(&link)),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            file_node: node, ..
        }) => {
            assert_eq!(node, Some(file_node(&again)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert_eq!(std::fs::read(&host).unwrap(), hello);
    assert_eq!(master.meta(&p("/src/hello.txt")).unwrap().unwrap(), again);
}

#[test]
fn handle_type_change_dir_to_file_drops_children() {
    let sandbox = SyncSandbox::new();
    let kid_bytes = b"kid";
    let kid_hash = hash_bytes(kid_bytes);
    let file_bytes = b"now-a-file";
    let file_hash = hash_bytes(file_bytes);
    let mut bodies = MemoryContent::new();
    bodies.offer(kid_hash, kid_bytes.to_vec());
    bodies.offer(file_hash, file_bytes.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let nested = dir();
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/nested"),
                new: nested.clone(),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept { .. }) => {}
        other => panic!("expected CasAccept, got {other:?}"),
    }

    let kid = FileMetadata::file(kid_bytes.len() as u64, MTIME, 0o100644, kid_hash);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/nested/kid.txt"),
                new: kid,
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept { .. }) => {}
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert!(sandbox.central_root().join("src/nested/kid.txt").is_file());

    let replacement = FileMetadata::file(file_bytes.len() as u64, MTIME, 0o100644, file_hash);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/nested"),
                new: replacement.clone(),
                basis: Some(file_node(&nested)),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            file_node: node, ..
        }) => {
            assert_eq!(node, Some(file_node(&replacement)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }

    assert!(!sandbox.central_root().join("src/nested/kid.txt").exists());
    assert_eq!(
        std::fs::read(sandbox.central_root().join("src/nested")).unwrap(),
        file_bytes
    );
    assert_eq!(master.meta(&p("/src/nested/kid.txt")).unwrap(), None);
    assert_eq!(
        master.meta(&p("/src/nested")).unwrap().unwrap(),
        replacement
    );
    assert!(!sandbox.central_root().join(RESERVED_CONFLICTS).exists());
}

#[test]
fn handle_type_change_stale_basis_is_cas_reject() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let previous = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: previous.clone(),
                basis: None,
            },
        )
        .unwrap();

    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: dir(),
                basis: Some(file_node(&file(9))),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasReject { path, current, .. }) => {
            assert_eq!(path, p("/src/hello.txt"));
            assert_eq!(current, Some(previous.clone()));
        }
        other => panic!("expected CasReject, got {other:?}"),
    }

    let host = sandbox.central_root().join("src/hello.txt");
    assert!(host.is_file());
    assert_eq!(std::fs::read(&host).unwrap(), hello);
    assert_eq!(
        master.meta(&p("/src/hello.txt")).unwrap().unwrap(),
        previous
    );
}

#[test]
fn note_local_type_change_file_to_dir_fans_out_with_previous_basis() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
        .unwrap();

    sandbox
        .tree(&sandbox.central_root())
        .file("src/hello.txt", b"typed");
    master
        .note_local(LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    let previous = master.meta(&p("/src/hello.txt")).unwrap().unwrap();
    assert_eq!(master.poll(BACKUP).len(), 1);

    std::fs::remove_file(sandbox.central_root().join("src/hello.txt")).unwrap();
    std::fs::create_dir(sandbox.central_root().join("src/hello.txt")).unwrap();
    master
        .note_local(LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    let new = master.meta(&p("/src/hello.txt")).unwrap().unwrap();
    assert_eq!(new.kind, EntryKind::Dir);

    let pushed = master.poll(BACKUP);
    assert_eq!(pushed.len(), 1);
    match &pushed[0] {
        ProtocolMessage::FileAnnounce {
            path,
            new: announced,
            basis,
            ..
        } => {
            assert_eq!(path, &p("/src/hello.txt"));
            assert_eq!(announced, &new);
            assert_eq!(*basis, Some(file_node(&previous)));
        }
        other => panic!("expected FileAnnounce, got {other:?}"),
    }
}

#[test]
fn subscribe_cas_fanout() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);

    let ack = master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    match ack {
        Reply::Send(ProtocolMessage::SubscribeAck { checkouts }) => {
            assert_eq!(checkouts.len(), 1);
            assert_eq!(checkouts[0].id, "src");
            assert_eq!(checkouts[0].central, p("/src"));
            assert_eq!(checkouts[0].master_root, merkle::empty_dir_node().into());
        }
        other => panic!("expected SubscribeAck, got {other:?}"),
    }

    let denied = master
        .handle(ALICE, subscribe("dev-alice", &[("root", "/")]))
        .unwrap();
    match denied {
        Reply::Send(ProtocolMessage::SubscribeReject {
            denied_centrals, ..
        }) => assert_eq!(denied_centrals, vec![p("/")]),
        other => panic!("expected SubscribeReject, got {other:?}"),
    }

    master
        .handle(
            BACKUP,
            subscribe("backup-1", &[("src", "/src"), ("bak", "/")]),
        )
        .unwrap();

    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o644, hash);
    let accept = master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: new.clone(),
                basis: None,
            },
        )
        .unwrap();
    match accept {
        Reply::Send(ProtocolMessage::CasAccept {
            checkout_id,
            path,
            file_node: node,
        }) => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, p("/src/hello.txt"));
            assert_eq!(node, Some(file_node(&new)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert_eq!(
        std::fs::read(sandbox.central_root().join("src/hello.txt")).unwrap(),
        hello
    );
    assert_eq!(
        master.meta(&p("/src/hello.txt")).unwrap().unwrap(),
        new.clone()
    );

    let pushed = master.poll(BACKUP);
    assert_eq!(pushed.len(), 2);
    let mut ids: Vec<String> = pushed
        .into_iter()
        .map(|msg| match msg {
            ProtocolMessage::FileAnnounce {
                checkout_id,
                path,
                new: announced,
                basis,
            } => {
                assert_eq!(path, p("/src/hello.txt"));
                assert_eq!(announced, new);
                assert_eq!(basis, None);
                checkout_id
            }
            other => panic!("expected FileAnnounce, got {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, ["bak", "src"]);
    assert!(master.poll(ALICE).is_empty());

    let reject = master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new,
                basis: None,
            },
        )
        .unwrap();
    match reject {
        Reply::Send(ProtocolMessage::CasReject { current, .. }) => {
            assert_eq!(current.unwrap().content_hash, hash)
        }
        other => panic!("expected CasReject, got {other:?}"),
    }
    assert_eq!(
        std::fs::read(sandbox.central_root().join("src/hello.txt")).unwrap(),
        hello
    );
    assert!(master.poll(BACKUP).is_empty());
}

#[test]
fn directory_cas_create_chmod_and_delete() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master
        .handle(
            BACKUP,
            subscribe("backup-1", &[("src", "/src"), ("bak", "/")]),
        )
        .unwrap();

    let created = dir();
    let path = p("/src/nested");
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: path.clone(),
                new: created.clone(),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            checkout_id,
            path: accepted,
            file_node: node,
        }) => {
            assert_eq!(checkout_id, "src");
            assert_eq!(accepted, path);
            assert_eq!(node, Some(file_node(&created)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert!(sandbox.central_root().join("src/nested").is_dir());
    assert_eq!(master.meta(&path).unwrap().unwrap(), created);

    let pushed = master.poll(BACKUP);
    assert_eq!(pushed.len(), 2);
    let mut ids: Vec<String> = pushed
        .into_iter()
        .map(|msg| match msg {
            ProtocolMessage::FileAnnounce {
                checkout_id,
                path: announced,
                new,
                basis,
            } => {
                assert_eq!(announced, path);
                assert_eq!(new, created);
                assert_eq!(basis, None);
                checkout_id
            }
            other => panic!("expected FileAnnounce, got {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, ["bak", "src"]);
    assert!(master.poll(ALICE).is_empty());

    let chmodded = FileMetadata::directory(MTIME, 0o040700);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: path.clone(),
                new: chmodded.clone(),
                basis: Some(file_node(&created)),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            file_node: node, ..
        }) => assert_eq!(node, Some(file_node(&chmodded))),
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert_eq!(master.meta(&path).unwrap().unwrap().mode, 0o040700);
    let _ = master.poll(BACKUP);

    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: path.clone(),
                new: FileMetadata::directory(MTIME, 0o040711),
                basis: Some(file_node(&created)),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasReject { current, .. }) => {
            assert_eq!(current, Some(chmodded.clone()))
        }
        other => panic!("expected CasReject, got {other:?}"),
    }
    assert_eq!(master.meta(&path).unwrap().unwrap().mode, 0o040700);
    assert!(master.poll(BACKUP).is_empty());

    match master
        .handle(
            ALICE,
            ProtocolMessage::Delete {
                checkout_id: "src".into(),
                path: path.clone(),
                basis: file_node(&created),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasReject { current, .. }) => {
            assert_eq!(current, Some(chmodded.clone()))
        }
        other => panic!("expected CasReject, got {other:?}"),
    }
    assert!(sandbox.central_root().join("src/nested").is_dir());

    match master
        .handle(
            ALICE,
            ProtocolMessage::Delete {
                checkout_id: "src".into(),
                path: path.clone(),
                basis: file_node(&chmodded),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            checkout_id,
            path: accepted,
            file_node: node,
        }) => {
            assert_eq!(checkout_id, "src");
            assert_eq!(accepted, path);
            assert_eq!(node, None);
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert!(!sandbox.central_root().join("src/nested").exists());
    assert_eq!(master.meta(&path).unwrap(), None);

    let pushed = master.poll(BACKUP);
    assert_eq!(pushed.len(), 2);
    let mut ids: Vec<String> = pushed
        .into_iter()
        .map(|msg| match msg {
            ProtocolMessage::Delete {
                checkout_id,
                path: deleted,
                basis,
            } => {
                assert_eq!(deleted, path);
                assert_eq!(basis, file_node(&chmodded));
                checkout_id
            }
            other => panic!("expected Delete, got {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, ["bak", "src"]);
    assert!(master.poll(ALICE).is_empty());
}

#[test]
fn a_local_event_for_the_masters_own_write_does_not_fan_out_again() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master
        .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
        .unwrap();

    master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: FileMetadata::file(hello.len() as u64, MTIME, 0o644, hash),
                basis: None,
            },
        )
        .unwrap();
    assert_eq!(master.poll(BACKUP).len(), 1);

    master
        .note_local(LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    assert!(master.poll(BACKUP).is_empty());
}

#[test]
fn a_local_edit_the_master_did_not_write_fans_out() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
        .unwrap();

    sandbox
        .tree(&sandbox.central_root())
        .file("src/edit.txt", b"typed by hand");
    master
        .note_local(LocalEvent::Changed(p("/src/edit.txt")))
        .unwrap();

    let pushed = master.poll(BACKUP);
    assert_eq!(pushed.len(), 1);
    match &pushed[0] {
        ProtocolMessage::FileAnnounce {
            path, new, basis, ..
        } => {
            assert_eq!(path, &p("/src/edit.txt"));
            assert_eq!(new.content_hash, hash_bytes(b"typed by hand"));
            assert_eq!(*basis, None);
        }
        other => panic!("expected FileAnnounce, got {other:?}"),
    }

    std::fs::remove_file(sandbox.central_root().join("src/edit.txt")).unwrap();
    let removed = master.meta(&p("/src/edit.txt")).unwrap().unwrap();
    master
        .note_local(LocalEvent::Removed(p("/src/edit.txt")))
        .unwrap();
    assert_eq!(master.meta(&p("/src/edit.txt")).unwrap(), None);

    let pushed = master.poll(BACKUP);
    assert_eq!(pushed.len(), 1);
    match &pushed[0] {
        ProtocolMessage::Delete { path, basis, .. } => {
            assert_eq!(path, &p("/src/edit.txt"));
            assert_eq!(*basis, file_node(&removed));
        }
        other => panic!("expected Delete, got {other:?}"),
    }
}

#[test]
fn handle_rename_moves_the_file_accepts_both_paths_and_fans_out() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master
        .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
        .unwrap();

    sandbox
        .tree(&sandbox.central_root())
        .file("src/old.txt", b"moved");
    master
        .note_local(LocalEvent::Changed(p("/src/old.txt")))
        .unwrap();
    let _ = master.poll(BACKUP);
    let _ = master.poll(ALICE);
    let from_meta = master.meta(&p("/src/old.txt")).unwrap().unwrap();

    match master
        .handle(
            ALICE,
            ProtocolMessage::Rename {
                checkout_id: "src".into(),
                from: p("/src/old.txt"),
                to: p("/src/new.txt"),
                from_basis: file_node(&from_meta),
                to_new: from_meta.clone(),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            path,
            file_node: node,
            ..
        }) => {
            assert_eq!(path, p("/src/new.txt"));
            assert_eq!(node, Some(file_node(&from_meta)));
        }
        other => panic!("expected CasAccept on to, got {other:?}"),
    }

    assert!(sandbox.central_root().join("src/new.txt").exists());
    assert!(!sandbox.central_root().join("src/old.txt").exists());

    let alice = master.poll(ALICE);
    match &alice[..] {
        [
            ProtocolMessage::CasAccept {
                path,
                file_node: node,
                ..
            },
        ] => {
            assert_eq!(path, &p("/src/old.txt"));
            assert_eq!(*node, None);
        }
        other => panic!("expected CasAccept on from, got {other:?}"),
    }

    let backup = master.poll(BACKUP);
    match &backup[..] {
        [ProtocolMessage::Rename { from, to, .. }] => {
            assert_eq!(from, &p("/src/old.txt"));
            assert_eq!(to, &p("/src/new.txt"));
        }
        other => panic!("expected Rename fan-out, got {other:?}"),
    }
}

#[test]
fn handle_rename_rejects_a_stale_from_basis_and_leaves_disk() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    sandbox
        .tree(&sandbox.central_root())
        .file("src/old.txt", b"moved");
    master
        .note_local(LocalEvent::Changed(p("/src/old.txt")))
        .unwrap();
    let from_meta = master.meta(&p("/src/old.txt")).unwrap().unwrap();
    let stale = file(9);

    match master
        .handle(
            ALICE,
            ProtocolMessage::Rename {
                checkout_id: "src".into(),
                from: p("/src/old.txt"),
                to: p("/src/new.txt"),
                from_basis: file_node(&stale),
                to_new: from_meta,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasReject { path, .. }) => {
            assert_eq!(path, p("/src/old.txt"));
        }
        other => panic!("expected CasReject on from, got {other:?}"),
    }
    assert!(sandbox.central_root().join("src/old.txt").exists());
    assert!(!sandbox.central_root().join("src/new.txt").exists());
}

#[test]
fn handle_rename_rejects_when_to_already_exists() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    sandbox
        .tree(&sandbox.central_root())
        .file("src/old.txt", b"moved");
    sandbox
        .tree(&sandbox.central_root())
        .file("src/new.txt", b"taken");
    master
        .note_local(LocalEvent::Changed(p("/src/old.txt")))
        .unwrap();
    master
        .note_local(LocalEvent::Changed(p("/src/new.txt")))
        .unwrap();
    let from_meta = master.meta(&p("/src/old.txt")).unwrap().unwrap();

    match master
        .handle(
            ALICE,
            ProtocolMessage::Rename {
                checkout_id: "src".into(),
                from: p("/src/old.txt"),
                to: p("/src/new.txt"),
                from_basis: file_node(&from_meta),
                to_new: from_meta,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasReject { path, .. }) => {
            assert_eq!(path, p("/src/new.txt"));
        }
        other => panic!("expected CasReject on to, got {other:?}"),
    }
    assert!(sandbox.central_root().join("src/old.txt").exists());
    assert!(sandbox.central_root().join("src/new.txt").exists());
}

#[test]
fn subscribe_ack_reports_the_directory_hash_the_commit_rebuilt() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o644, hash);
    master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: new.clone(),
                basis: None,
            },
        )
        .unwrap();

    let expected = merkle::dir_node(&[DirChild::File {
        name: name("hello.txt"),
        node: file_node(&new),
    }]);
    match master
        .handle(BACKUP, subscribe("backup-1", &[("src", "/src")]))
        .unwrap()
    {
        Reply::Send(ProtocolMessage::SubscribeAck { checkouts }) => {
            assert_eq!(checkouts[0].master_root, expected.into())
        }
        other => panic!("expected SubscribeAck, got {other:?}"),
    }
}

#[test]
fn an_unwritable_peer_grows_no_outbox() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master
        .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
        .unwrap();

    let announce = |path: &str| ProtocolMessage::FileAnnounce {
        checkout_id: "src".into(),
        path: p(path),
        new: FileMetadata::file(hello.len() as u64, MTIME, 0o644, hash),
        basis: None,
    };

    master.set_writable(BACKUP, false);
    master.handle(ALICE, announce("/src/first.txt")).unwrap();
    assert!(master.poll(BACKUP).is_empty());

    master.set_writable(BACKUP, true);
    master.handle(ALICE, announce("/src/second.txt")).unwrap();
    let pushed = master.poll(BACKUP);
    assert_eq!(pushed.len(), 1);
    match &pushed[0] {
        ProtocolMessage::FileAnnounce { path, .. } => assert_eq!(path, &p("/src/second.txt")),
        other => panic!("expected FileAnnounce, got {other:?}"),
    }
}

#[test]
fn an_unknown_static_key_is_hung_up_before_any_index_read() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    match master
        .handle([0xEE; 32], subscribe("dev-alice", &[("src", "/src")]))
        .unwrap()
    {
        Reply::Hangup { .. } => {}
        other => panic!("expected Hangup, got {other:?}"),
    }
}

#[test]
fn an_announce_outside_the_subscribed_central_is_an_error_not_a_cas_reject() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let reply = master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/docs/hello.txt"),
                new: FileMetadata::file(hello.len() as u64, MTIME, 0o644, hash),
                basis: None,
            },
        )
        .unwrap();
    match reply {
        Reply::Send(ProtocolMessage::Error { code, .. }) => assert_eq!(code, "outside_central"),
        other => panic!("expected Error, got {other:?}"),
    }
    assert!(!sandbox.central_root().join("docs/hello.txt").exists());
}

#[test]
fn a_later_bulk_finish_loses_cas_against_the_live_node() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master
        .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
        .unwrap();

    let path = p("/src/hello.txt");
    let alice_body = b"alice-bytes";
    let backup_body = b"backup-bytes";
    let alice_new = FileMetadata::file(
        alice_body.len() as u64,
        MTIME,
        0o100644,
        hash_bytes(alice_body),
    );
    let backup_new = FileMetadata::file(
        backup_body.len() as u64,
        MTIME,
        0o100644,
        hash_bytes(backup_body),
    );
    for (peer, checkout_id, new) in [
        (ALICE, "src", alice_new.clone()),
        (BACKUP, "bak", backup_new.clone()),
    ] {
        match master
            .handle(
                peer,
                ProtocolMessage::FileAnnounce {
                    checkout_id: checkout_id.into(),
                    path: path.clone(),
                    new,
                    basis: None,
                },
            )
            .unwrap()
        {
            Reply::Send(ProtocolMessage::SignatureRequest { .. }) => {}
            other => panic!("expected SignatureRequest, got {other:?}"),
        }
    }

    let finish = |master: &mut Master<MemoryStorage, MemoryContent>,
                  peer: [u8; 32],
                  checkout_id: &str,
                  body: &[u8]| {
        master.apply_bulk(
            peer,
            BulkHeader {
                path: path.clone(),
                checkout_id: checkout_id.into(),
                want_hash: hash_bytes(body),
                encoding: BulkEncoding::Whole,
                size: body.len() as u64,
            },
            body,
        )
    };

    match finish(&mut master, BACKUP, "bak", backup_body).unwrap() {
        Reply::Send(ProtocolMessage::CasAccept { file_node: node, .. }) => {
            assert_eq!(node, Some(file_node(&backup_new)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }
    match finish(&mut master, ALICE, "src", alice_body).unwrap() {
        Reply::Send(ProtocolMessage::CasReject { current, .. }) => {
            assert_eq!(current.as_ref(), Some(&backup_new));
        }
        other => panic!("expected CasReject, got {other:?}"),
    }
    assert_eq!(
        std::fs::read(sandbox.central_root().join("src/hello.txt")).unwrap(),
        backup_body
    );
    assert_eq!(master.meta(&path).unwrap().as_ref(), Some(&backup_new));
}

#[test]
fn a_reserved_name_on_the_wire_is_rejected_and_not_indexed() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master
        .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
        .unwrap();

    let announced = [
        (BACKUP, "bak", "/.arborsync-tmp/scratch", ".arborsync-tmp/scratch"),
        (
            ALICE,
            "src",
            "/src/.arborsync-conflicts/noise",
            "src/.arborsync-conflicts/noise",
        ),
    ];
    for (peer, checkout_id, canonical, host) in announced {
        match master
            .handle(
                peer,
                ProtocolMessage::FileAnnounce {
                    checkout_id: checkout_id.into(),
                    path: p(canonical),
                    new: dir(),
                    basis: None,
                },
            )
            .unwrap()
        {
            Reply::Send(ProtocolMessage::Error { code, .. }) => assert_eq!(code, "reserved_name"),
            other => panic!("expected reserved_name, got {other:?}"),
        }
        assert_eq!(master.meta(&p(canonical)).unwrap(), None);
        assert!(!sandbox.central_root().join(host).exists());
    }

    let root_tmp = p("/.arborsync-tmp");
    let verbs = [
        ProtocolMessage::Delete {
            checkout_id: "bak".into(),
            path: root_tmp.clone(),
            basis: file_node(&dir()),
        },
        ProtocolMessage::Rename {
            checkout_id: "bak".into(),
            from: p("/keep.txt"),
            to: p("/.arborsync-conflicts/keep.txt"),
            from_basis: file_node(&file(1)),
            to_new: file(1),
        },
        ProtocolMessage::DirListRequest {
            checkout_id: "bak".into(),
            path: root_tmp.clone(),
            after: None,
        },
        ProtocolMessage::SignatureRequest {
            checkout_id: "bak".into(),
            path: p("/.arborsync-conflicts/noise"),
            want_hash: ContentHash::ZERO,
            signature: Vec::new(),
        },
    ];
    for msg in verbs {
        match master.handle(BACKUP, msg).unwrap() {
            Reply::Send(ProtocolMessage::Error { code, .. }) => assert_eq!(code, "reserved_name"),
            other => panic!("expected reserved_name, got {other:?}"),
        }
    }
}

#[test]
fn resubscribing_from_a_rotated_key_makes_the_old_key_inert() {
    const ALICE_NEW: [u8; 32] = [0xA2; 32];
    let sandbox = SyncSandbox::new();
    let cfg_path = sandbox.write_master_config(vec![SlaveAcl {
        id: "dev-alice".into(),
        public_keys: vec![format_hex_key(&ALICE), format_hex_key(&ALICE_NEW)],
        allowed_prefixes: vec!["/src".into()],
    }]);
    let cfg = LoadedMaster::load(&cfg_path).unwrap();
    let mut master = Master::open(cfg, MemoryStorage::new(), MemoryContent::new()).unwrap();

    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master
        .handle(ALICE_NEW, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: file(1),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::Error { code, .. }) => assert_eq!(code, "not_subscribed"),
        other => panic!("expected Error, got {other:?}"),
    }

    master.disconnect(ALICE);

    match master
        .handle(
            ALICE_NEW,
            ProtocolMessage::Delete {
                checkout_id: "src".into(),
                path: p("/src/absent.txt"),
                basis: file_node(&file(1)),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasReject { path, .. }) => {
            assert_eq!(path, p("/src/absent.txt"))
        }
        other => panic!("the live session survived the stale disconnect, got {other:?}"),
    }
}

#[test]
fn open_drops_an_indexed_file_that_is_gone_from_disk() {
    let sandbox = SyncSandbox::new();
    let cfg_path = sandbox.write_master_config(vec![slave_acl("dev-alice", ALICE, &["/src"])]);
    let cfg = LoadedMaster::load(&cfg_path).unwrap();
    let mut master = Master::open(
        cfg,
        RedbStorage::open(&sandbox.master_db()).unwrap(),
        MemoryContent::new(),
    )
    .unwrap();
    sandbox
        .tree(&sandbox.central_root())
        .file("src/hello.txt", b"hello");
    master.rescan().unwrap();
    assert!(master.meta(&p("/src/hello.txt")).unwrap().is_some());
    drop(master);
    std::fs::remove_file(sandbox.central_root().join("src/hello.txt")).unwrap();

    let cfg = LoadedMaster::load(&cfg_path).unwrap();
    let master = Master::open(
        cfg,
        RedbStorage::open(&sandbox.master_db()).unwrap(),
        MemoryContent::new(),
    )
    .unwrap();
    assert!(
        master.meta(&p("/src/hello.txt")).unwrap().is_none(),
        "a restart must forget a file the disk no longer has"
    );
}

#[test]
fn open_indexes_a_central_tree_that_predates_the_index() {
    let sandbox = SyncSandbox::new();
    let tree = sandbox.tree(&sandbox.central_root());
    tree.file("src/hello.txt", b"hello");
    tree.mkdir("docs");

    let master = two_slave_master(&sandbox, MemoryContent::new());

    let indexed = master.meta(&p("/src/hello.txt")).unwrap().unwrap();
    assert_eq!(indexed.kind, EntryKind::File);
    assert_eq!(indexed.size, 5);
    assert_eq!(indexed.content_hash, hash_bytes(b"hello"));
    assert_eq!(
        master.meta(&p("/docs")).unwrap().unwrap().kind,
        EntryKind::Dir
    );
    assert_eq!(
        master.meta(&p("/src")).unwrap().unwrap().kind,
        EntryKind::Dir
    );
}

fn root_report(id: &str, central: &str) -> ProtocolMessage {
    ProtocolMessage::RootReport {
        checkout_id: id.into(),
        path: p(central),
        root: merkle::empty_dir_node().into(),
    }
}

#[test]
fn reload_removes_acl_and_forgets_the_live_session() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let next = LoadedMaster::load(&sandbox.write_master_config(vec![slave_acl(
        "backup-1",
        BACKUP,
        &["/"],
    )]))
    .unwrap();
    let plan = master.reload(next).unwrap();
    assert!(plan.drop_peers.contains(&ALICE));
    assert_eq!(plan.drop_slave_ids, vec!["dev-alice".to_string()]);
    assert_eq!(master.authorize_peer(&ALICE), None);

    match master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap()
    {
        Reply::Hangup { reason, rate_limit } => {
            assert_eq!(reason, "unknown static key");
            assert!(rate_limit);
        }
        other => panic!("expected Hangup, got {other:?}"),
    }
}

#[test]
fn reload_tightens_prefix_and_drops_that_checkout() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(
            BACKUP,
            subscribe("backup-1", &[("src", "/src"), ("bak", "/")]),
        )
        .unwrap();

    let next = LoadedMaster::load(&sandbox.write_master_config(vec![
        slave_acl("dev-alice", ALICE, &["/src"]),
        slave_acl("backup-1", BACKUP, &["/src"]),
    ]))
    .unwrap();
    master.reload(next).unwrap();

    match master.handle(BACKUP, root_report("bak", "/")).unwrap() {
        Reply::Send(ProtocolMessage::Error { code, .. }) => assert_eq!(code, "not_subscribed"),
        other => panic!("expected not_subscribed, got {other:?}"),
    }
    match master.handle(BACKUP, root_report("src", "/src")).unwrap() {
        Reply::Send(ProtocolMessage::Error { code, .. }) => {
            assert_ne!(code, "not_subscribed")
        }
        Reply::Send(ProtocolMessage::RootAck { checkout_id, .. }) => {
            assert_eq!(checkout_id, "src")
        }
        other => panic!("expected RootAck or non-not_subscribed, got {other:?}"),
    }
}

#[test]
fn reload_keeps_old_cfg_when_listen_addr_changes() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let path = sandbox.write_master_config(vec![
        slave_acl("dev-alice", ALICE, &["/src"]),
        slave_acl("backup-1", BACKUP, &["/"]),
    ]);
    let text = std::fs::read_to_string(&path)
        .unwrap()
        .replace("127.0.0.1:0", "127.0.0.1:9443");
    let next = LoadedMaster::parse(&text).unwrap();
    match master.reload(next) {
        Err(ReloadError::RestartRequired { fields }) => {
            assert!(fields.contains(&"listen_addr".to_string()));
        }
        other => panic!("expected RestartRequired, got {other:?}"),
    }
    assert_eq!(master.authorize_peer(&ALICE), Some("dev-alice"));
}

#[test]
fn reload_accepts_an_extra_rotation_key() {
    const ALICE_NEW: [u8; 32] = [0xA2; 32];
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let next = LoadedMaster::load(&sandbox.write_master_config(vec![
        SlaveAcl {
            id: "dev-alice".into(),
            public_keys: vec![format_hex_key(&ALICE), format_hex_key(&ALICE_NEW)],
            allowed_prefixes: vec!["/src".into()],
        },
        slave_acl("backup-1", BACKUP, &["/"]),
    ]))
    .unwrap();
    master.reload(next).unwrap();
    assert_eq!(master.authorize_peer(&ALICE), Some("dev-alice"));
    assert_eq!(master.authorize_peer(&ALICE_NEW), Some("dev-alice"));
}

#[test]
fn slave_id_mismatch_is_hangup() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    match master
        .handle(ALICE, subscribe("backup-1", &[("src", "/src")]))
        .unwrap()
    {
        Reply::Hangup { reason, rate_limit } => {
            assert_eq!(reason, "slave_id backup-1 is not bound to this key");
            assert!(rate_limit);
        }
        other => panic!("expected Hangup, got {other:?}"),
    }
}

#[test]
fn disconnect_drops_that_peers_pending_and_keeps_the_other() {
    let sandbox = SyncSandbox::new();
    let cfg_path = sandbox.write_master_config(vec![
        slave_acl("dev-alice", ALICE, &["/src"]),
        slave_acl("backup-1", BACKUP, &["/"]),
    ]);
    let cfg = LoadedMaster::load(&cfg_path).unwrap();
    let mut master = Master::open(cfg, MemoryStorage::new(), WholeFileLater).unwrap();
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master
        .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
        .unwrap();

    let hello = b"hello";
    let hash = hash_bytes(hello);
    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o644, hash);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/alice.txt"),
                new: new.clone(),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::SignatureRequest { path, .. }) => {
            assert_eq!(path, p("/src/alice.txt"))
        }
        other => panic!("expected SignatureRequest, got {other:?}"),
    }
    match master
        .handle(
            BACKUP,
            ProtocolMessage::FileAnnounce {
                checkout_id: "bak".into(),
                path: p("/backup.txt"),
                new,
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::SignatureRequest { path, .. }) => {
            assert_eq!(path, p("/backup.txt"))
        }
        other => panic!("expected SignatureRequest, got {other:?}"),
    }

    master.disconnect(ALICE);
    match master
        .apply_bulk(
            ALICE,
            arborsync_core::protocol::BulkHeader {
                checkout_id: "src".into(),
                path: p("/src/alice.txt"),
                want_hash: hash,
                encoding: arborsync_core::protocol::BulkEncoding::Whole,
                size: hello.len() as u64,
            },
            hello,
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::Error { code, .. }) => {
            assert_eq!(code, "unknown_transfer")
        }
        other => panic!("expected unknown_transfer, got {other:?}"),
    }
    match master
        .apply_bulk(
            BACKUP,
            arborsync_core::protocol::BulkHeader {
                checkout_id: "bak".into(),
                path: p("/backup.txt"),
                want_hash: hash,
                encoding: arborsync_core::protocol::BulkEncoding::Whole,
                size: hello.len() as u64,
            },
            hello,
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept { path, .. }) => {
            assert_eq!(path, p("/backup.txt"))
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }
}

#[test]
fn write_event_reuses_content_hash_when_only_mode_changes() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
        .unwrap();
    sandbox
        .tree(&sandbox.central_root())
        .file("edit.txt", b"typed by hand");
    master
        .note_local(LocalEvent::Changed(p("/edit.txt")))
        .unwrap();
    let first = master.meta(&p("/edit.txt")).unwrap().unwrap();
    let _ = master.poll(BACKUP);
    sandbox
        .tree(&sandbox.central_root())
        .set_mode("edit.txt", first.mode | 0o111);
    sandbox
        .tree(&sandbox.central_root())
        .set_mtime_ns("edit.txt", first.mtime_ns);
    master
        .note_local(LocalEvent::Changed(p("/edit.txt")))
        .unwrap();
    let second = master.meta(&p("/edit.txt")).unwrap().unwrap();
    assert_eq!(second.content_hash, first.content_hash);
    assert_ne!(second.mode, first.mode);
    match &master.poll(BACKUP)[..] {
        [ProtocolMessage::FileAnnounce { new, .. }] => {
            assert_eq!(new.content_hash, first.content_hash);
            assert_ne!(new.mode, first.mode);
        }
        other => panic!("expected chmod FileAnnounce, got {other:?}"),
    }
}

#[test]
fn rescan_reuses_content_hash_when_only_mode_changes() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    let bytes = vec![b'x'; 8192];
    sandbox
        .tree(&sandbox.central_root())
        .file("big.bin", &bytes);
    master.rescan().unwrap();
    let first = master.meta(&p("/big.bin")).unwrap().unwrap();
    assert_eq!(first.content_hash, hash_bytes(&bytes));
    sandbox
        .tree(&sandbox.central_root())
        .set_mode("big.bin", first.mode | 0o111);
    sandbox
        .tree(&sandbox.central_root())
        .set_mtime_ns("big.bin", first.mtime_ns);
    master.rescan().unwrap();
    let second = master.meta(&p("/big.bin")).unwrap().unwrap();
    assert_eq!(second.content_hash, first.content_hash);
    assert_eq!(second.content_hash, hash_bytes(&bytes));
    assert_ne!(second.mode, first.mode);
}

#[test]
fn git_object_fanout_dir_is_indexed_as_dir_and_dir_list_does_not_read_it() {
    let sandbox = SyncSandbox::new();
    let objects = sandbox.central_root().join("src/arborsync/.git/objects/39");
    std::fs::create_dir_all(&objects).unwrap();
    std::fs::write(objects.join("deadbeef"), b"blob").unwrap();

    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master.rescan().unwrap();
    let path = p("/src/arborsync/.git/objects/39");
    let meta = master.meta(&path).unwrap().unwrap();
    assert_eq!(meta.kind, EntryKind::Dir);

    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    match master
        .handle(
            ALICE,
            ProtocolMessage::DirListRequest {
                checkout_id: "src".into(),
                path: path.clone(),
                after: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::DirListResponse { entries, .. }) => {
            assert!(
                entries
                    .iter()
                    .any(|child| child.name().as_str() == "deadbeef")
            );
        }
        other => panic!("expected DirListResponse, got {other:?}"),
    }
}

#[test]
fn signature_request_for_a_git_object_fanout_dir_is_missing_hash() {
    let sandbox = SyncSandbox::new();
    let objects = sandbox.central_root().join("src/arborsync/.git/objects/39");
    std::fs::create_dir_all(&objects).unwrap();
    std::fs::write(objects.join("deadbeef"), b"blob").unwrap();

    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master.rescan().unwrap();
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let path = p("/src/arborsync/.git/objects/39");
    match master
        .handle(
            ALICE,
            ProtocolMessage::SignatureRequest {
                checkout_id: "src".into(),
                path: path.clone(),
                want_hash: ContentHash::ZERO,
                signature: Vec::new(),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::Error { code, message }) => {
            assert_eq!(code, "missing_hash");
            assert_eq!(message, path.as_str());
        }
        other => panic!("expected missing_hash, got {other:?}"),
    }
}

#[cfg(target_os = "macos")]
#[test]
fn directory_cas_survives_when_dir_metadata_is_eperm() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let objects = sandbox.central_root().join("src/arborsync/.git/objects/39");
    std::fs::create_dir_all(&objects).unwrap();
    let flagged = std::process::Command::new("chflags")
        .args(["uchg", objects.to_str().unwrap()])
        .status()
        .expect("chflags");
    if !flagged.success() {
        let _ = std::process::Command::new("chflags")
            .args(["nouchg", objects.to_str().unwrap()])
            .status();
        panic!("chflags uchg failed; cannot synthesize EPERM on a directory");
    }

    let announced = FileMetadata::directory(MTIME, 0o040755);
    let path = p("/src/arborsync/.git/objects/39");
    let result = master.handle(
        ALICE,
        ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: path.clone(),
            new: announced.clone(),
            basis: None,
        },
    );
    let _ = std::process::Command::new("chflags")
        .args(["nouchg", objects.to_str().unwrap()])
        .status();
    match result.unwrap() {
        Reply::Send(ProtocolMessage::CasAccept {
            file_node: node, ..
        }) => assert_eq!(node, Some(file_node(&announced))),
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert!(objects.is_dir());
    assert_eq!(master.meta(&path).unwrap().unwrap().kind, EntryKind::Dir);
}

fn ready_with(
    master: &Master<MemoryStorage, MemoryContent>,
    extra: &[(&str, FileMetadata)],
) -> CentralWalk {
    let mut ready = BTreeMap::new();
    ready.insert(p("/"), master.meta(&p("/")).unwrap().unwrap());
    for (path, meta) in extra {
        ready.insert(p(path), meta.clone());
    }
    CentralWalk {
        ready,
        needs: Vec::new(),
    }
}

#[test]
fn adopt_survey_keeps_a_file_the_survey_missed() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    sandbox
        .tree(&sandbox.central_root())
        .file("keep.txt", b"keep");
    sandbox
        .tree(&sandbox.central_root())
        .file("seen.txt", b"seen");
    master.rescan().unwrap();
    let seen = master.meta(&p("/seen.txt")).unwrap().unwrap();
    master
        .adopt_survey(ready_with(&master, &[("/seen.txt", seen)]))
        .unwrap();
    assert!(master.meta(&p("/keep.txt")).unwrap().is_some());
}

#[test]
fn adopt_survey_does_not_overwrite_a_newer_row() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    sandbox
        .tree(&sandbox.central_root())
        .file("edit.txt", b"old bytes");
    master.rescan().unwrap();
    let old = master.meta(&p("/edit.txt")).unwrap().unwrap();
    sandbox
        .tree(&sandbox.central_root())
        .file("edit.txt", b"newer bytes here");
    master
        .note_local(LocalEvent::Changed(p("/edit.txt")))
        .unwrap();
    let fresh = master.meta(&p("/edit.txt")).unwrap().unwrap();
    master
        .adopt_survey(ready_with(&master, &[("/edit.txt", old)]))
        .unwrap();
    assert_eq!(
        master.meta(&p("/edit.txt")).unwrap().unwrap().content_hash,
        fresh.content_hash
    );
}

#[test]
fn adopt_survey_drops_a_file_that_is_gone() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    sandbox
        .tree(&sandbox.central_root())
        .file("gone.txt", b"gone");
    master.rescan().unwrap();
    std::fs::remove_file(sandbox.central_root().join("gone.txt")).unwrap();
    master.adopt_survey(ready_with(&master, &[])).unwrap();
    assert!(master.meta(&p("/gone.txt")).unwrap().is_none());
}

#[test]
fn directory_delete_reclaims_off_the_index_commit() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let directory = dir();
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/big"),
                new: directory.clone(),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            file_node: node, ..
        }) => {
            assert_eq!(node, Some(file_node(&directory)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }
    std::fs::create_dir(sandbox.central_root().join("src/big/leaf")).unwrap();

    let PreparedDelete::Reclaim { reply, root, path } = master
        .prepare_delete(ALICE, "src".into(), p("/src/big"), file_node(&directory))
        .unwrap()
    else {
        panic!("expected reclaim");
    };
    assert!(matches!(
        reply,
        ProtocolMessage::CasAccept {
            file_node: None,
            ..
        }
    ));
    assert!(master.meta(&p("/src/big")).unwrap().is_none());
    assert!(sandbox.central_root().join("src/big/leaf").is_dir());

    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/other"),
                new: directory.clone(),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept { .. }) => {}
        other => panic!("expected sibling CasAccept, got {other:?}"),
    }
    assert!(sandbox.central_root().join("src/other").is_dir());
    assert!(master.overlaps_wipe(&p("/src/big/leaf")));
    assert!(!master.overlaps_wipe(&p("/src/other")));
    assert!(matches!(
        master
            .prepare_delete(ALICE, "src".into(), p("/src"), file_node(&directory))
            .unwrap(),
        PreparedDelete::Later { .. }
    ));
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/big/again"),
                new: directory,
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::Error { code, .. }) => assert_eq!(code, "wiping"),
        other => panic!("expected wiping, got {other:?}"),
    }

    master.rescan().unwrap();
    assert!(master.meta(&p("/src/big")).unwrap().is_none());
    assert!(master.meta(&p("/src/big/leaf")).unwrap().is_none());
    assert!(sandbox.central_root().join("src/big/leaf").is_dir());
    assert!(master.meta(&p("/src/other")).unwrap().is_some());

    reclaim_tree(&root, &path).unwrap();
    master.finish_wipe(&path);
    assert!(!sandbox.central_root().join("src/big").exists());
    assert!(!master.overlaps_wipe(&p("/src/big")));
    assert!(master.meta(&p("/src/other")).unwrap().is_some());
}

#[test]
fn directory_rename_reindexes_only_that_subtree() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    sandbox
        .tree(&sandbox.central_root())
        .file("src/dir/child.txt", b"inside");
    sandbox
        .tree(&sandbox.central_root())
        .file("src/blocked/secret.txt", b"nope");
    master
        .note_local(LocalEvent::Changed(p("/src/dir/child.txt")))
        .unwrap();
    master
        .note_local(LocalEvent::Changed(p("/src/blocked/secret.txt")))
        .unwrap();
    let dir_meta = master.meta(&p("/src/dir")).unwrap().unwrap();

    std::fs::set_permissions(
        sandbox.central_root().join("src/blocked"),
        std::fs::Permissions::from_mode(0o000),
    )
    .unwrap();

    match master
        .handle(
            ALICE,
            ProtocolMessage::Rename {
                checkout_id: "src".into(),
                from: p("/src/dir"),
                to: p("/src/moved"),
                from_basis: file_node(&dir_meta),
                to_new: dir_meta.clone(),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept {
            path,
            file_node: node,
            ..
        }) => {
            assert_eq!(path, p("/src/moved"));
            assert_eq!(node, Some(file_node(&dir_meta)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }

    assert!(master.meta(&p("/src/moved/child.txt")).unwrap().is_some());
    let _ = std::fs::set_permissions(
        sandbox.central_root().join("src/blocked"),
        std::fs::Permissions::from_mode(0o755),
    );
}

#[test]
fn inbound_error_is_recorded_and_quiet() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    match master
        .handle(
            ALICE,
            ProtocolMessage::Error {
                code: "keep_live".into(),
                message: "/src/hello.txt".into(),
            },
        )
        .unwrap()
    {
        Reply::Quiet => {}
        other => panic!("expected Quiet, got {other:?}"),
    }
}

#[test]
fn failed_publish_disarms_inflight() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let parent = sandbox.central_root().join("src");
    std::fs::create_dir_all(&parent).unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555)).unwrap();

    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    let err = master.handle(
        ALICE,
        ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new,
            basis: None,
        },
    );
    assert!(err.is_err(), "publish into a read-only parent must fail");

    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    sandbox
        .tree(&sandbox.central_root())
        .file("src/hello.txt", hello);
    let out = master
        .plan_local(LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    assert!(
        !out.send.is_empty() || !out.hash.is_empty(),
        "failed publish must disarm inflight so a real write is planned; got {out:?}"
    );
}

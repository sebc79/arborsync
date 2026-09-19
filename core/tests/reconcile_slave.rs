use arborsync_core::LoadedSlave;
use arborsync_core::LocalEvent;
use arborsync_core::config::CheckoutConfig;
use arborsync_core::keys::format_hex_key;
use arborsync_core::merkle::{DirChild, empty_dir_node, file_node};
use arborsync_core::meta::{FileMetadata, hash_bytes};
use arborsync_core::path::{RESERVED_TMP, conflict_sidecar_path};
use arborsync_core::protocol::{CheckoutAck, ProtocolMessage};
use arborsync_core::slave::{MemoryContent, Reply, Slave};
use arborsync_core::test_support::{MemoryStorage, SyncSandbox, name, p};

const MASTER_PIN: [u8; 32] = [0x11; 32];
const MTIME: i64 = 1_700_000_000_000;

fn alice_slave(
    sandbox: &SyncSandbox,
    bodies: MemoryContent,
) -> Slave<MemoryStorage, MemoryContent> {
    let local = sandbox.add_checkout("dev-alice", "src");
    let cfg_path = sandbox.write_slave_config(
        "dev-alice",
        vec![CheckoutConfig {
            id: "src".into(),
            central: "/src".into(),
            local: local.to_string_lossy().into_owned(),
        }],
        vec![format_hex_key(&MASTER_PIN)],
    );
    let cfg = LoadedSlave::load(&cfg_path).unwrap();
    Slave::open(cfg, MemoryStorage::new(), bodies).unwrap()
}

fn subscribe_ack() -> ProtocolMessage {
    ProtocolMessage::SubscribeAck {
        checkouts: vec![CheckoutAck {
            id: "src".into(),
            central: p("/src"),
            master_root: empty_dir_node().into(),
        }],
    }
}

fn send(reply: Reply) -> Vec<ProtocolMessage> {
    match reply {
        Reply::Send(msgs) => msgs,
        other => panic!("expected Send, got {other:?}"),
    }
}

#[test]
fn subscribe_ack_on_empty_checkout_reports_empty_dir() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    match &send(slave.handle(subscribe_ack()).unwrap())[..] {
        [
            ProtocolMessage::RootReport {
                checkout_id,
                path,
                root,
            },
        ] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, &p("/src"));
            assert_eq!(*root, empty_dir_node().into());
        }
        other => panic!("expected one RootReport, got {other:?}"),
    }
}

#[test]
fn subscribe_ack_rescans_leftover_disk_file_into_meta() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("leftover.txt", b"mine");

    let first = send(slave.handle(subscribe_ack()).unwrap());
    assert!(
        first.iter().any(|msg| matches!(
            msg,
            ProtocolMessage::RootReport { path, root, .. }
                if path == &p("/src") && *root != empty_dir_node().into()
        )),
        "expected RootReport, got {first:?}"
    );
    assert!(
        first.iter().any(|msg| matches!(
            msg,
            ProtocolMessage::FileAnnounce { path, .. } if path == &p("/src/leftover.txt")
        )),
        "expected leftover FileAnnounce, got {first:?}"
    );
    assert_eq!(
        slave
            .meta("src", &p("/src/leftover.txt"))
            .unwrap()
            .unwrap()
            .content_hash,
        hash_bytes(b"mine")
    );
}

#[test]
fn root_ack_matched_true_emits_no_dir_list() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    send(slave.handle(subscribe_ack()).unwrap());
    assert_eq!(
        send(
            slave
                .handle(ProtocolMessage::RootAck {
                    checkout_id: "src".into(),
                    path: p("/src"),
                    matched: true,
                    master_root: empty_dir_node().into(),
                })
                .unwrap()
        ),
        Vec::new()
    );
}

#[test]
fn root_ack_matched_false_requests_central_listing() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    send(slave.handle(subscribe_ack()).unwrap());
    match &send(
        slave
            .handle(ProtocolMessage::RootAck {
                checkout_id: "src".into(),
                path: p("/src"),
                matched: false,
                master_root: empty_dir_node().into(),
            })
            .unwrap(),
    )[..]
    {
        [
            ProtocolMessage::DirListRequest {
                checkout_id, path, ..
            },
        ] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, &p("/src"));
        }
        other => panic!("expected DirListRequest, got {other:?}"),
    }
}

#[test]
fn dir_list_master_only_file_pulls_without_conflict_sidecar() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    send(slave.handle(subscribe_ack()).unwrap());

    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    match &send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                after: None,
                entries: vec![DirChild::File {
                    name: name("hello.txt"),
                    node: file_node(&new),
                }],
                more: false,
            })
            .unwrap(),
    )[..]
    {
        [
            ProtocolMessage::DirListRequest {
                checkout_id, path, ..
            },
        ] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, &p("/src/hello.txt"));
        }
        other => panic!("expected pull DirListRequest, got {other:?}"),
    }

    send(
        slave
            .handle(ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: new.clone(),
                basis: None,
            })
            .unwrap(),
    );

    let local = slave.checkout_local("src").unwrap().join("hello.txt");
    assert_eq!(std::fs::read(&local).unwrap(), hello);
    let sidecar = conflict_sidecar_path(
        slave.checkout_local("src").unwrap(),
        &p("/src/hello.txt"),
        &hash,
    );
    assert!(!sidecar.exists());
}

#[test]
fn dir_list_kind_change_file_to_dir_replaces_the_local_file() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    send(
        slave
            .handle(ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new,
                basis: None,
            })
            .unwrap(),
    );

    match &send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                after: None,
                entries: vec![DirChild::Directory {
                    name: name("hello.txt"),
                    node: empty_dir_node(),
                }],
                more: false,
            })
            .unwrap(),
    )[..]
    {
        [ProtocolMessage::DirListRequest { path, .. }] => {
            assert_eq!(path, &p("/src/hello.txt"));
        }
        other => panic!("expected DirListRequest, got {other:?}"),
    }

    let host = slave.checkout_local("src").unwrap().join("hello.txt");
    assert!(host.is_dir(), "kind change must replace the live file");
    assert!(std::fs::read(&host).is_err());
}

#[test]
fn dir_list_slave_only_leftover_announces_create() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("leftover.txt", b"mine");
    slave
        .note_local("src", LocalEvent::Changed(p("/src/leftover.txt")))
        .unwrap();

    match &send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                after: None,
                entries: vec![],
                more: false,
            })
            .unwrap(),
    )[..]
    {
        [
            ProtocolMessage::FileAnnounce {
                checkout_id,
                path,
                new,
                basis,
            },
        ] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, &p("/src/leftover.txt"));
            assert_eq!(new.content_hash, hash_bytes(b"mine"));
            assert_eq!(*basis, None);
        }
        other => panic!("expected create FileAnnounce, got {other:?}"),
    }
}

#[test]
fn dir_list_slave_only_nested_dir_announces_the_nested_file() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("photos/album/shot.jpg", b"img");
    send(slave.handle(subscribe_ack()).unwrap());

    match &send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                after: None,
                entries: vec![],
                more: false,
            })
            .unwrap(),
    )[..]
    {
        [
            ProtocolMessage::FileAnnounce { path, basis, .. },
            ProtocolMessage::DirListRequest {
                path: walk, after, ..
            },
        ] => {
            assert_eq!(path, &p("/src/photos"));
            assert_eq!(*basis, None);
            assert_eq!(walk, &p("/src/photos"));
            assert_eq!(*after, None);
        }
        other => panic!("expected photos announce and walk, got {other:?}"),
    }

    match &send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src/photos"),
                after: None,
                entries: vec![],
                more: false,
            })
            .unwrap(),
    )[..]
    {
        [
            ProtocolMessage::FileAnnounce { path, basis, .. },
            ProtocolMessage::DirListRequest {
                path: walk, after, ..
            },
        ] => {
            assert_eq!(path, &p("/src/photos/album"));
            assert_eq!(*basis, None);
            assert_eq!(walk, &p("/src/photos/album"));
            assert_eq!(*after, None);
        }
        other => panic!("expected album announce and walk, got {other:?}"),
    }

    match &send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src/photos/album"),
                after: None,
                entries: vec![],
                more: false,
            })
            .unwrap(),
    )[..]
    {
        [
            ProtocolMessage::FileAnnounce {
                path, new, basis, ..
            },
        ] => {
            assert_eq!(path, &p("/src/photos/album/shot.jpg"));
            assert_eq!(new.content_hash, hash_bytes(b"img"));
            assert_eq!(*basis, None);
        }
        other => panic!("expected shot.jpg FileAnnounce, got {other:?}"),
    }
}

#[test]
fn dir_list_slave_only_with_last_synced_equal_local_deletes() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    send(
        slave
            .handle(ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: new.clone(),
                basis: None,
            })
            .unwrap(),
    );
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(file_node(&new))
    );

    match &send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                after: None,
                entries: vec![],
                more: false,
            })
            .unwrap(),
    )[..]
    {
        [
            ProtocolMessage::Delete {
                checkout_id,
                path,
                basis,
            },
        ] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, &p("/src/hello.txt"));
            assert_eq!(*basis, file_node(&new));
        }
        other => panic!("expected Delete, got {other:?}"),
    }
}

#[test]
fn dir_list_slave_only_dir_with_last_synced_equal_local_deletes() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let dir = FileMetadata::directory(MTIME, 0o040755);
    send(
        slave
            .handle(ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/nested"),
                new: dir.clone(),
                basis: None,
            })
            .unwrap(),
    );
    assert_eq!(
        slave.last_synced("src", &p("/src/nested")).unwrap(),
        Some(file_node(&dir))
    );

    match &send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                after: None,
                entries: vec![],
                more: false,
            })
            .unwrap(),
    )[..]
    {
        [
            ProtocolMessage::Delete {
                checkout_id,
                path,
                basis,
            },
        ] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, &p("/src/nested"));
            assert_eq!(*basis, file_node(&dir));
        }
        other => panic!("expected Delete, got {other:?}"),
    }
}

#[test]
fn dir_list_page_with_more_does_not_delete_later_names() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    send(
        slave
            .handle(ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: new.clone(),
                basis: None,
            })
            .unwrap(),
    );

    match &send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                after: None,
                entries: vec![DirChild::File {
                    name: name("aaa"),
                    node: file_node(&new),
                }],
                more: true,
            })
            .unwrap(),
    )[..]
    {
        [
            ProtocolMessage::DirListRequest {
                path: pull,
                after: None,
                ..
            },
            ProtocolMessage::DirListRequest {
                path: again,
                after: Some(cursor),
                ..
            },
        ] => {
            assert_eq!(pull, &p("/src/aaa"));
            assert_eq!(again, &p("/src"));
            assert_eq!(cursor.as_str(), "aaa");
        }
        other => panic!("expected pull plus next page, got {other:?}"),
    }
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(file_node(&new))
    );
}

#[test]
fn subscribe_ack_reports_and_announces_before_walk_finishes() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    for i in 0..80 {
        sandbox
            .tree(&local)
            .file(&format!("f{i:02}.txt"), format!("b{i}").as_bytes());
    }

    let first = send(slave.handle(subscribe_ack()).unwrap());
    assert!(slave.crawl_pending());
    assert!(
        first
            .iter()
            .any(|msg| matches!(msg, ProtocolMessage::RootReport { .. })),
        "SubscribeAck must RootReport before the walk finishes, got {first:?}"
    );
    assert!(
        first
            .iter()
            .any(|msg| matches!(msg, ProtocolMessage::FileAnnounce { .. })),
        "one crawl step must announce leftover files before the walk finishes, got {first:?}"
    );

    let rest = slave.finish_crawl().unwrap();
    assert!(
        rest.iter().any(|msg| matches!(
            msg,
            ProtocolMessage::RootReport { path, root, .. }
                if path == &p("/src") && *root != empty_dir_node().into()
        )),
        "expected RootReport after finish_crawl, got {rest:?}"
    );
    assert!(!slave.crawl_pending());
    assert!(slave.meta("src", &p("/src/f79.txt")).unwrap().is_some());
}

#[test]
fn second_subscribe_ack_emits_root_report_again() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let first = send(slave.handle(subscribe_ack()).unwrap());
    let second = send(slave.handle(subscribe_ack()).unwrap());
    match (&first[..], &second[..]) {
        (
            [ProtocolMessage::RootReport { root: a, .. }],
            [ProtocolMessage::RootReport { root: b, path, .. }],
        ) => {
            assert_eq!(a, b);
            assert_eq!(path, &p("/src"));
        }
        other => panic!("expected RootReports, got {other:?}"),
    }
}

#[test]
fn rescan_skips_arborsync_tmp_under_checkout_local() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    let tree = sandbox.tree(&local);
    tree.file(&format!("{RESERVED_TMP}/scratch"), b"tmp");
    tree.file("keep.txt", b"keep");
    slave.rescan("src").unwrap();
    assert!(slave.meta("src", &p("/src/keep.txt")).unwrap().is_some());
    assert!(
        slave
            .meta("src", &p("/src/.arborsync-tmp/scratch"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn rescan_does_not_change_last_synced() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    send(
        slave
            .handle(ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: new.clone(),
                basis: None,
            })
            .unwrap(),
    );
    let before = slave.last_synced("src", &p("/src/hello.txt")).unwrap();
    assert_eq!(before, Some(file_node(&new)));
    slave.rescan("src").unwrap();
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        before
    );
}

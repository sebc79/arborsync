use arborsync_core::config::{CheckoutConfig, SlaveAcl};
use arborsync_core::hash::ContentHash;
use arborsync_core::keys::format_hex_key;
use arborsync_core::master::{LocalEvent, Master, MemoryContent, Reply};
use arborsync_core::merkle::{DirChild, dir_node, empty_dir_node, file_node};
use arborsync_core::meta::{EntryKind, FileMetadata, hash_bytes};
use arborsync_core::protocol::{CheckoutAck, CheckoutRef, ProtocolMessage};
use arborsync_core::reconcile::{ThreeWay, WalkAction, decide_child, decide_three_way};
use arborsync_core::slave::{Reply as SlaveReply, Slave};
use arborsync_core::test_support::{MemoryStorage, SyncSandbox, name, p};
use arborsync_core::{LoadedMaster, LoadedSlave};

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
        decide_child(Some(&link), Some(&other_link), Some(node(1)), Some(node(1))),
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
        decide_child(Some(&local), None, Some(node(1)), Some(node(1))),
        WalkAction::AnnounceDelete
    );
    assert_eq!(
        decide_child(Some(&local), None, Some(node(9)), Some(node(1))),
        WalkAction::AnnounceCas
    );
    assert_eq!(
        decide_child(Some(&local), Some(&master), None, Some(node(1))),
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

const MASTER_PIN: [u8; 32] = [0x11; 32];

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

fn slave_send(reply: SlaveReply) -> Vec<ProtocolMessage> {
    match reply {
        SlaveReply::Send(msgs) => msgs,
        other => panic!("expected Send, got {other:?}"),
    }
}

#[test]
fn subscribe_ack_reports_empty_dir_node_on_a_fresh_checkout() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    match &slave_send(slave.handle(subscribe_ack()).unwrap())[..] {
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
fn mismatch_walk_announces_a_slave_only_file_without_last_synced() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("leftover.txt", b"mine");
    slave
        .note_local("src", LocalEvent::Changed(p("/src/leftover.txt")))
        .unwrap();

    match &slave_send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                entries: vec![],
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
fn mismatch_walk_deletes_when_last_synced_matches_local() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    slave_send(
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

    match &slave_send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                entries: vec![],
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
fn root_ack_mismatch_requests_the_central_listing() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    slave_send(slave.handle(subscribe_ack()).unwrap());
    match &slave_send(
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
        [ProtocolMessage::DirListRequest { checkout_id, path }] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, &p("/src"));
        }
        other => panic!("expected DirListRequest, got {other:?}"),
    }
}

fn pump(
    master: &mut Master<MemoryStorage, MemoryContent>,
    slave: &mut Slave<MemoryStorage, MemoryContent>,
    mut to_master: Vec<ProtocolMessage>,
    mut to_slave: Vec<ProtocolMessage>,
) {
    for step in 0..32 {
        if let Some(msg) = to_slave.pop() {
            match slave.handle(msg).unwrap() {
                SlaveReply::Send(outs) => to_master.extend(outs),
                SlaveReply::Bulk(xfer) => match master
                    .apply_bulk(ALICE, xfer.header, &xfer.body)
                    .unwrap()
                {
                    Reply::Send(out) => to_slave.push(out),
                    other => panic!("unexpected master bulk reply {other:?}"),
                },
                other => panic!("unexpected slave reply {other:?}"),
            }
            continue;
        }
        if let Some(msg) = to_master.pop() {
            match master.handle(ALICE, msg).unwrap() {
                Reply::Send(out) => to_slave.push(out),
                Reply::Bulk(xfer) => match slave.apply_bulk(xfer.header, &xfer.body).unwrap() {
                    SlaveReply::Send(outs) => to_master.extend(outs),
                    other => panic!("unexpected slave bulk reply {other:?}"),
                },
                other => panic!("unexpected master reply {other:?}"),
            }
            to_slave.extend(master.poll(ALICE));
            continue;
        }
        let _ = step;
        return;
    }
    panic!("pump did not go quiet");
}

#[test]
fn pump_pulls_hello_txt_from_master_onto_a_fresh_slave() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    sandbox
        .tree(&sandbox.central_root())
        .file("src/hello.txt", b"hello");
    master
        .note_local(LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    send(master.handle(ALICE, subscribe()).unwrap());

    let hello = b"hello";
    let mut bodies = MemoryContent::new();
    bodies.offer(hash_bytes(hello), hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let reports = slave_send(slave.handle(subscribe_ack()).unwrap());
    pump(&mut master, &mut slave, reports, Vec::new());

    let local = slave.checkout_local("src").unwrap().join("hello.txt");
    assert_eq!(std::fs::read(&local).unwrap(), hello);
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(file_node(
            &slave.meta("src", &p("/src/hello.txt")).unwrap().unwrap()
        ))
    );
}

#[test]
fn mismatch_walk_pulls_when_last_synced_still_matches_local() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    sandbox
        .tree(&sandbox.central_root())
        .file("src/hello.txt", b"master");
    master
        .note_local(LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    send(master.handle(ALICE, subscribe()).unwrap());
    let master_meta = master.meta(&p("/src/hello.txt")).unwrap().unwrap();

    let hello = b"local";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    bodies.offer(master_meta.content_hash, b"master".to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let local_meta = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    slave_send(
        slave
            .handle(ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: local_meta,
                basis: None,
            })
            .unwrap(),
    );

    let pull = slave_send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                entries: vec![DirChild::File {
                    name: name("hello.txt"),
                    node: file_node(&master_meta),
                }],
            })
            .unwrap(),
    );
    match &pull[..] {
        [ProtocolMessage::DirListRequest { checkout_id, path }] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, &p("/src/hello.txt"));
        }
        other => panic!("expected pull DirListRequest, got {other:?}"),
    }

    pump(&mut master, &mut slave, pull, Vec::new());
    let local = slave.checkout_local("src").unwrap().join("hello.txt");
    assert_eq!(std::fs::read(&local).unwrap(), b"master");
}

#[test]
fn rescan_indexes_a_missed_file_then_announce_walks_it() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    send(master.handle(ALICE, subscribe()).unwrap());

    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("missed.txt", b"seen");
    let reports = slave.rescan("src").unwrap();
    match &reports[..] {
        [ProtocolMessage::RootReport { path, root, .. }] => {
            assert_eq!(path, &p("/src"));
            assert_ne!(*root, empty_dir_node().into());
        }
        other => panic!("expected RootReport from rescan, got {other:?}"),
    }
    assert_eq!(
        slave
            .meta("src", &p("/src/missed.txt"))
            .unwrap()
            .unwrap()
            .content_hash,
        hash_bytes(b"seen")
    );
    assert_eq!(slave.last_synced("src", &p("/src/missed.txt")).unwrap(), None);

    pump(&mut master, &mut slave, reports, Vec::new());
    assert_eq!(
        std::fs::read(sandbox.central_root().join("src/missed.txt")).unwrap(),
        b"seen"
    );
}

#[test]
fn second_subscribe_ack_clears_pending_and_reports_again() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let first = slave_send(slave.handle(subscribe_ack()).unwrap());
    slave_send(
        slave
            .handle(ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: FileMetadata::file(5, MTIME, 0o100644, hash_bytes(b"hello")),
                basis: None,
            })
            .unwrap(),
    );
    let second = slave_send(slave.handle(subscribe_ack()).unwrap());
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
    match slave
        .apply_bulk(
            arborsync_core::protocol::BulkHeader {
                path: p("/src/hello.txt"),
                checkout_id: "src".into(),
                want_hash: hash_bytes(b"hello"),
                encoding: arborsync_core::protocol::BulkEncoding::Whole,
                size: 5,
            },
            b"hello",
        )
        .unwrap()
    {
        SlaveReply::Send(msgs) => match &msgs[..] {
            [ProtocolMessage::Error { code, .. }] => assert_eq!(code, "unknown_transfer"),
            other => panic!("expected unknown_transfer, got {other:?}"),
        },
        other => panic!("expected Send, got {other:?}"),
    }
}


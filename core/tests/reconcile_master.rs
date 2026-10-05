use arborsync_core::LoadedMaster;
use arborsync_core::LocalEvent;
use arborsync_core::config::SlaveAcl;
use arborsync_core::keys::format_hex_key;
use arborsync_core::master::{Master, MemoryContent, Reply};
use arborsync_core::merkle::{DirChild, dir_node, empty_dir_node, file_node};
use arborsync_core::meta::{EntryKind, hash_bytes};
use arborsync_core::protocol::{CheckoutRef, ProtocolMessage, RestoreEpoch};
use arborsync_core::storage::Storage;
use arborsync_core::test_support::{MemoryStorage, SyncSandbox, name, p};

const ALICE: [u8; 32] = [0xA1; 32];

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
fn root_report_matching_empty_src_is_acked() {
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
fn root_report_wrong_root_after_file_is_unmatched() {
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
fn dir_list_request_returns_only_direct_children() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    send(master.handle(ALICE, subscribe()).unwrap());

    sandbox
        .tree(&sandbox.central_root())
        .file("src/a/b.rs", b"nested");
    master
        .note_local(LocalEvent::Changed(p("/src/a/b.rs")))
        .unwrap();

    match send(
        master
            .handle(
                ALICE,
                ProtocolMessage::DirListRequest {
                    checkout_id: "src".into(),
                    path: p("/src"),
                    after: None,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::DirListResponse {
            checkout_id,
            path,
            entries,
            more,
            ..
        } => {
            assert!(!more);
            assert_eq!(checkout_id, "src");
            assert_eq!(path, p("/src"));
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].name().as_str(), "a");
            assert!(!entries.iter().any(|child| child.name().as_str() == "b.rs"));
        }
        other => panic!("expected DirListResponse, got {other:?}"),
    }
}

#[test]
fn dir_list_request_on_a_file_returns_file_announce() {
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
                    after: None,
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

#[test]
fn subscribe_with_a_stored_epoch_sends_epochs_before_ack() {
    let sandbox = SyncSandbox::new();
    let store = MemoryStorage::new();
    store.put_restore_epoch(&p("/src"), 4).unwrap();
    let cfg_path = sandbox.write_master_config(vec![SlaveAcl {
        id: "dev-alice".into(),
        public_keys: vec![format_hex_key(&ALICE)],
        allowed_prefixes: vec!["/src".into()],
    }]);
    let mut master = Master::open(
        LoadedMaster::load(&cfg_path).unwrap(),
        store,
        MemoryContent::new(),
    )
    .unwrap();
    match master.handle(ALICE, subscribe()).unwrap() {
        Reply::SendMany(msgs) => match &msgs[..] {
            [
                ProtocolMessage::RestoreEpochs { epochs },
                ProtocolMessage::SubscribeAck { .. },
            ] => {
                assert_eq!(
                    epochs,
                    &vec![RestoreEpoch {
                        prefix: p("/src"),
                        generation: 4,
                    }]
                );
            }
            other => panic!("expected RestoreEpochs then SubscribeAck, got {other:?}"),
        },
        other => panic!("expected SendMany, got {other:?}"),
    }
}

#[test]
fn subscribe_without_an_epoch_returns_only_subscribe_ack() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    match master.handle(ALICE, subscribe()).unwrap() {
        Reply::Send(ProtocolMessage::SubscribeAck { .. }) => {}
        other => panic!("expected only SubscribeAck, got {other:?}"),
    }
}

#[test]
fn root_report_under_the_checkout_is_acked_and_a_sibling_is_rejected() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    send(master.handle(ALICE, subscribe()).unwrap());

    match send(
        master
            .handle(
                ALICE,
                ProtocolMessage::RootReport {
                    checkout_id: "src".into(),
                    path: p("/src/nested"),
                    root: empty_dir_node().into(),
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::RootAck {
            checkout_id,
            path,
            matched,
            ..
        } => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, p("/src/nested"));
            assert!(matched);
        }
        other => panic!("expected RootAck, got {other:?}"),
    }

    match send(
        master
            .handle(
                ALICE,
                ProtocolMessage::RootReport {
                    checkout_id: "src".into(),
                    path: p("/other"),
                    root: empty_dir_node().into(),
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::Error { code, .. } => assert_eq!(code, "outside_central"),
        other => panic!("expected outside_central, got {other:?}"),
    }
}

#[test]
fn root_report_without_subscribe_is_an_error() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
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
        ProtocolMessage::Error { code, .. } => assert_eq!(code, "not_subscribed"),
        other => panic!("expected not_subscribed, got {other:?}"),
    }
}

#[test]
fn dir_list_request_without_subscribe_is_an_error() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    match send(
        master
            .handle(
                ALICE,
                ProtocolMessage::DirListRequest {
                    checkout_id: "src".into(),
                    path: p("/src"),
                    after: None,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::Error { code, .. } => assert_eq!(code, "not_subscribed"),
        other => panic!("expected not_subscribed, got {other:?}"),
    }
}

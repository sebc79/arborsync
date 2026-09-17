use arborsync_core::LoadedSlave;
use arborsync_core::config::CheckoutConfig;
use arborsync_core::hash::ContentHash;
use arborsync_core::keys::format_hex_key;
use arborsync_core::merkle::file_node;
use arborsync_core::meta::{FileMetadata, hash_bytes};
use arborsync_core::path::{RESERVED_CONFLICTS, conflict_sidecar_path};
use arborsync_core::protocol::ProtocolMessage;
use arborsync_core::slave::{
    DeleteAction, LocalEvent, MemoryContent, ReplicaAction, Reply, Slave, decide_incoming,
    decide_master_won_delete,
};
use arborsync_core::test_support::{MemoryStorage, SyncSandbox, p};

const MASTER: [u8; 32] = [0x11; 32];
const MTIME: i64 = 1_700_000_000_000;

fn file(byte: u8) -> FileMetadata {
    FileMetadata::file(3, MTIME, 0o100644, ContentHash::from_bytes([byte; 32]))
}

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
        vec![format_hex_key(&MASTER)],
    );
    let cfg = LoadedSlave::load(&cfg_path).unwrap();
    Slave::open(cfg, MemoryStorage::new(), bodies).unwrap()
}

#[test]
fn incoming_announce_follows_the_replica_table() {
    let live = file(1);
    let incoming = file(2);
    let same = live.clone();

    assert_eq!(decide_incoming(None, None, &incoming), ReplicaAction::Apply);
    assert_eq!(
        decide_incoming(Some(&live), Some(file_node(&live)), &incoming),
        ReplicaAction::Apply
    );
    assert_eq!(
        decide_incoming(Some(&same), None, &same),
        ReplicaAction::NoopRefresh
    );
    assert_eq!(
        decide_incoming(Some(&live), None, &incoming),
        ReplicaAction::SidecarThenApply
    );

    let mut meta_only = live.clone();
    meta_only.mode = 0o100755;
    assert_eq!(
        decide_incoming(Some(&live), None, &meta_only),
        ReplicaAction::ApplyMetaOnly
    );
}

#[test]
fn master_won_delete_sidecars_divergent_content() {
    let live = file(1);
    assert_eq!(
        decide_master_won_delete(None, file_node(&live), None),
        DeleteAction::AlreadyGone
    );
    assert_eq!(
        decide_master_won_delete(Some(&live), file_node(&live), None),
        DeleteAction::Remove
    );
    assert_eq!(
        decide_master_won_delete(Some(&live), file_node(&file(9)), Some(file_node(&live))),
        DeleteAction::Remove
    );
    assert_eq!(
        decide_master_won_delete(Some(&live), file_node(&file(9)), Some(file_node(&file(8)))),
        DeleteAction::SidecarThenRemove
    );
}

#[test]
fn pin_miss_hangs_up_before_subscribe() {
    let sandbox = SyncSandbox::new();
    let slave = alice_slave(&sandbox, MemoryContent::new());
    match slave.pin_check([0x00; 32]) {
        Err(Reply::Hangup { reason }) => assert_eq!(reason, "master pin miss"),
        other => panic!("expected hangup, got {other:?}"),
    }
    slave.pin_check(MASTER).unwrap();
    match slave.subscribe() {
        ProtocolMessage::Subscribe {
            slave_id,
            checkouts,
        } => {
            assert_eq!(slave_id, "dev-alice");
            assert_eq!(checkouts.len(), 1);
            assert_eq!(checkouts[0].id, "src");
            assert_eq!(checkouts[0].central, p("/src"));
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
}

#[test]
fn local_edit_announces_without_moving_last_synced() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("hello.txt", b"typed");

    let out = slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    match &out[..] {
        [
            ProtocolMessage::FileAnnounce {
                checkout_id,
                path,
                new,
                basis,
            },
        ] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, &p("/src/hello.txt"));
            assert_eq!(new.content_hash, hash_bytes(b"typed"));
            assert_eq!(*basis, None);
        }
        other => panic!("expected FileAnnounce, got {other:?}"),
    }
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        None
    );

    slave
        .handle(ProtocolMessage::CasAccept {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            file_node: Some(file_node(
                &slave.meta("src", &p("/src/hello.txt")).unwrap().unwrap(),
            )),
        })
        .unwrap();
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(file_node(
            &slave.meta("src", &p("/src/hello.txt")).unwrap().unwrap()
        ))
    );
}

#[test]
fn master_announce_writes_inside_the_checkout_and_sets_last_synced() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);

    match slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: new.clone(),
            basis: None,
        })
        .unwrap()
    {
        Reply::Send(msgs) => assert!(msgs.is_empty()),
        other => panic!("expected empty send, got {other:?}"),
    }

    let local = slave.checkout_local("src").unwrap().to_path_buf();
    assert_eq!(std::fs::read(local.join("hello.txt")).unwrap(), hello);
    assert!(!local.join("src").exists());
    assert_eq!(
        slave.meta("src", &p("/src/hello.txt")).unwrap(),
        Some(new.clone())
    );
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(file_node(&new))
    );
}

#[test]
fn cas_reject_sidecars_local_bytes_then_adopts_the_winner() {
    let sandbox = SyncSandbox::new();
    let winner_bytes = b"master";
    let winner_hash = hash_bytes(winner_bytes);
    let mut bodies = MemoryContent::new();
    bodies.offer(winner_hash, winner_bytes.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("hello.txt", b"local");
    slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();

    let winner = FileMetadata::file(winner_bytes.len() as u64, MTIME, 0o100644, winner_hash);
    slave
        .handle(ProtocolMessage::CasReject {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            current: Some(winner.clone()),
        })
        .unwrap();

    let local_hash = hash_bytes(b"local");
    let sidecar = conflict_sidecar_path(&local, &p("/src/hello.txt"), &local_hash);
    assert_eq!(std::fs::read(&sidecar).unwrap(), b"local");
    assert!(sidecar.starts_with(local.join(RESERVED_CONFLICTS)));
    assert_eq!(
        std::fs::read(local.join("hello.txt")).unwrap(),
        winner_bytes
    );
    assert_eq!(
        slave.meta("src", &p("/src/hello.txt")).unwrap(),
        Some(winner)
    );
}

#[test]
fn echo_of_an_applied_announce_does_not_reannounce() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new,
            basis: None,
        })
        .unwrap();

    let out = slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    assert!(out.is_empty());
}

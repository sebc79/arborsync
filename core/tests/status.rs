use arborsync_core::LoadedMaster;
use arborsync_core::LoadedSlave;
use arborsync_core::LocalEvent;
use arborsync_core::config::CheckoutConfig;
use arborsync_core::config::SlaveAcl;
use arborsync_core::hash::{ContentHash, SubtreeRoot};
use arborsync_core::keys::format_hex_key;
use arborsync_core::master::{Master, MemoryContent, Reply};
use arborsync_core::merkle::file_node;
use arborsync_core::meta::{FileMetadata, hash_bytes};
use arborsync_core::protocol::{CheckoutRef, ProtocolMessage};
use arborsync_core::slave::Slave;
use arborsync_core::status::{Flow, Health, Queues, classify};
use arborsync_core::test_support::{MemoryStorage, SyncSandbox, p};

const ALICE: [u8; 32] = [0xA1; 32];
const BACKUP: [u8; 32] = [0xB1; 32];
const MASTER: [u8; 32] = [0x11; 32];
const MTIME: i64 = 1_700_000_000_000;

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
fn reject_only_window_is_failed_not_busy() {
    let mut flow = Flow::default();
    flow.in_msgs = 1;
    flow.out_msgs = 1;
    flow.cas_reject = 1;
    assert_eq!(classify(&flow, &Queues::default(), 1), Health::Failed);
}

#[test]
fn fresh_take_after_subscribe_and_idle_is_idle() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    let _handshake = master.take_status();
    let status = master.take_status();
    assert_eq!(status.health, Health::Idle);
    assert!(status.last_error.is_none());
    assert_eq!(status.connected, 1);
    assert_eq!(status.slaves[0].slave_id, "dev-alice");
    assert!(status.slaves[0].connected);
    assert_eq!(
        status.lines(5)[0],
        "status 5s health=idle connected=1 in=0 out=0 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=0 outbox=0 writable=true last_error=-"
    );
    assert_eq!(
        status.lines(5)[1],
        "status slave=dev-alice health=idle connected=true checkouts=1 in=0 out=0 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=0 outbox=0 writable=true last_error=-"
    );
}

#[test]
fn file_announce_cas_accept_is_busy_then_idle() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    let _handshake = master.take_status();

    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasAccept { .. }) => {}
        other => panic!("expected CasAccept, got {other:?}"),
    }

    let status = master.take_status();
    assert_eq!(status.health, Health::Busy);
    assert!(status.flow.cas_accept >= 1);
    assert_eq!(
        status.lines(5)[0],
        "status 5s health=busy connected=1 in=1 out=1 cas_ok=1 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=0 outbox=0 writable=true last_error=-"
    );

    let idle = master.take_status();
    assert_eq!(idle.health, Health::Idle);
}

#[test]
fn stale_basis_file_announce_records_cas_reject() {
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
    let _setup = master.take_status();

    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: FileMetadata::file(3, MTIME, 0o100644, ContentHash::from_bytes([9; 32])),
                basis: Some(file_node(&FileMetadata::file(
                    3,
                    MTIME,
                    0o100644,
                    ContentHash::from_bytes([2; 32]),
                ))),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::CasReject { .. }) => {}
        other => panic!("expected CasReject, got {other:?}"),
    }

    let status = master.take_status();
    assert_eq!(status.health, Health::Failed);
    let error = status.last_error.as_ref().expect("last_error");
    assert!(error.reason.starts_with("cas_reject:"), "{}", error.reason);
    assert_eq!(
        status.lines(5)[0],
        "status 5s health=failed connected=1 in=1 out=1 cas_ok=0 cas_rej=1 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=0 outbox=0 writable=true last_error=cas_reject:/src/hello.txt"
    );
}

#[test]
fn unflushed_fanout_is_stuck() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    let _handshake = master.take_status();

    sandbox
        .tree(&sandbox.central_root())
        .file("src/hello.txt", b"typed");
    master
        .note_local(LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();

    let status = master.take_status();
    assert_eq!(status.health, Health::Stuck);
    assert!(status.queues.outbox > 0);
    assert_eq!(
        status.lines(5)[0],
        "status 5s health=stuck connected=1 in=0 out=0 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=1 rescan=0 flushed=0 pending=0 outbox=1 writable=true last_error=-"
    );
}

#[test]
fn slave_dir_list_and_root_report_increment_counters() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let _ = slave.take_status(true);

    match slave
        .handle(ProtocolMessage::RootAck {
            checkout_id: "src".into(),
            path: p("/src"),
            matched: false,
            master_root: SubtreeRoot::ZERO,
        })
        .unwrap()
    {
        arborsync_core::slave::Reply::Send(msgs) => {
            assert!(
                msgs.iter()
                    .any(|msg| matches!(msg, ProtocolMessage::DirListRequest { .. }))
            );
        }
        other => panic!("expected DirListRequest, got {other:?}"),
    }

    slave
        .handle(ProtocolMessage::DirListResponse {
            checkout_id: "src".into(),
            path: p("/src"),
            after: None,
            entries: Vec::new(),
            more: false,
        })
        .unwrap();

    let status = slave.take_status(true);
    assert!(status.flow.root >= 1);
    assert!(status.flow.dir_list >= 1);
    assert_eq!(
        status.line(5),
        "status 5s health=busy connected=true in=2 out=1 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=2 root=1 local=0 rescan=0 flushed=0 pending=0 pending_pulls=0 pending_renames=0 last_error=-"
    );
}

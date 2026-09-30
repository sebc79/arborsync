use std::time::{Duration, Instant};

use arborsync_core::LoadedMaster;
use arborsync_core::LoadedSlave;
use arborsync_core::LocalEvent;
use arborsync_core::bottleneck::{Gauge, Stage};
use arborsync_core::config::CheckoutConfig;
use arborsync_core::config::SlaveAcl;
use arborsync_core::hash::{ContentHash, SubtreeRoot};
use arborsync_core::keys::format_hex_key;
use arborsync_core::master::{Master, MemoryContent, Reply};
use arborsync_core::merkle::file_node;
use arborsync_core::meta::{FileMetadata, hash_bytes};
use arborsync_core::protocol::{CheckoutRef, ProtocolMessage};
use arborsync_core::slave::{LinkState, Slave};
use arborsync_core::status::{Flow, Health, PeerLive, Queues, StatusLedger, classify};
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

/// What an idle slave sends: no fulfill stage, nothing parked or sending.
fn idle_gauge(seq: u32) -> Gauge {
    Gauge {
        seq,
        stage: None,
        age_ms: 0,
        depth: 0,
        parked: 0,
        sending: 0,
        pending: 0,
    }
}

/// What a parked slave sends: 41 s behind on the third of three asks.
fn parked_gauge(seq: u32) -> Gauge {
    Gauge {
        seq,
        stage: Some(Stage::FulfillParked),
        age_ms: 41_000,
        depth: 3,
        parked: 3,
        sending: 1,
        pending: 0,
    }
}

/// Leaves the master holding an `origin_bytes` wait on `dev-alice`.
fn ask_alice_for_bytes(master: &mut Master<MemoryStorage, MemoryContent>) {
    let body = b"bytes the master does not hold";
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/big.bin"),
                new: FileMetadata::file(body.len() as u64, MTIME, 0o100644, hash_bytes(body)),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::SignatureRequest { .. }) => {}
        other => panic!("expected SignatureRequest, got {other:?}"),
    }
}

fn online() -> LinkState {
    LinkState {
        connected: true,
        waits: Vec::new(),
        work_depth: 0,
    }
}

/// The age of a live wait is whatever the test run produced, so it is the one
/// field a literal line cannot pin.
fn without_age(line: &str) -> String {
    let (head, rest) = line.split_once(" age=").expect("age field");
    let (_, tail) = rest.split_once(' ').expect("a field after age");
    format!("{head} age=<n> {tail}")
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
        "status 5s health=idle bottleneck=none connected=1 in=0 out=0 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=0 outbox=0 writable=true fanout_dropped=0 last_error=-"
    );
    assert_eq!(
        status.lines(5)[1],
        "status slave=dev-alice health=idle bottleneck=unobserved hint=absent connected=true checkouts=1 in=0 out=0 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=0 outbox=0 writable=true fanout_dropped=0 last_error=-"
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
        "status 5s health=busy bottleneck=none connected=1 in=1 out=1 cas_ok=1 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=0 outbox=0 writable=true fanout_dropped=0 last_error=-"
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
        "status 5s health=failed bottleneck=none connected=1 in=1 out=1 cas_ok=0 cas_rej=1 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=0 outbox=0 writable=true fanout_dropped=0 last_error=cas_reject:/src/hello.txt"
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
        without_age(&status.lines(5)[0]),
        "status 5s health=stuck bottleneck=fanout@dev-alice age=<n> depth=1 via=local connected=1 in=0 out=0 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=1 rescan=0 flushed=0 pending=0 outbox=1 writable=true fanout_dropped=0 last_error=-"
    );
}

#[test]
fn dropping_unwritable_outbox_counts_fanout_dropped() {
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
    master.set_writable(ALICE, false);

    let status = master.take_status();
    assert_eq!(status.queues.outbox, 0);
    assert!(!status.queues.writable);
    assert_eq!(status.queues.fanout_dropped, 1);
    let line = status.lines(5)[0].clone();
    assert!(line.contains("writable=false fanout_dropped=1"), "{line}");
    assert!(line.contains("health=busy"), "{line}");
}

#[test]
fn master_waiting_on_slave_bytes_names_origin_bytes() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    let _handshake = master.take_status();
    ask_alice_for_bytes(&mut master);

    let status = master.take_status();
    assert_eq!(
        without_age(&status.lines(5)[0]),
        "status 5s health=stuck bottleneck=origin_bytes@dev-alice age=<n> depth=1 via=local connected=1 in=1 out=1 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=1 outbox=0 writable=true fanout_dropped=0 last_error=-"
    );
    assert_eq!(
        without_age(&status.lines(5)[1]),
        "status slave=dev-alice health=stuck bottleneck=origin_bytes age=<n> depth=1 hint=absent connected=true checkouts=1 in=1 out=1 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=1 outbox=0 writable=true fanout_dropped=0 last_error=-"
    );
}

#[test]
fn an_idle_slave_gauge_reasks_origin_bytes_older_than_one_status_interval() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    let _handshake = master.take_status();
    ask_alice_for_bytes(&mut master);
    master.observe_gauge(ALICE, &idle_gauge(1).encode());

    assert!(
        master.reask_idle(ALICE, Instant::now()).is_empty(),
        "the first ask is still inside one status interval"
    );

    // Sandbox status interval is 0, treated as a 5s period. Six seconds is past
    // one period and inside the three-period gauge window.
    let later = Instant::now() + Duration::from_secs(6);
    let asks = master.reask_idle(ALICE, later);
    assert!(
        matches!(
            asks.as_slice(),
            [ProtocolMessage::SignatureRequest { path, .. }] if path == &p("/src/big.bin")
        ),
        "idle gauge with an old origin_bytes row must reask, got {asks:?}"
    );
    assert!(
        master.reask_idle(ALICE, later).is_empty(),
        "the same instant must not ask twice"
    );
}

#[test]
fn a_parked_or_missing_gauge_does_not_reask_origin_bytes() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    let _handshake = master.take_status();
    ask_alice_for_bytes(&mut master);
    let later = Instant::now() + Duration::from_secs(6);
    assert!(master.reask_idle(ALICE, later).is_empty());

    master.observe_gauge(ALICE, &parked_gauge(1).encode());
    assert!(master.reask_idle(ALICE, later).is_empty());
}

#[test]
fn a_fresh_slave_gauge_names_the_far_end_of_the_master_wait() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    let _handshake = master.take_status();
    ask_alice_for_bytes(&mut master);

    master.observe_gauge(ALICE, &parked_gauge(1).encode());

    let status = master.take_status();
    let aggregate = &status.lines(5)[0];
    assert!(
        aggregate.contains("bottleneck=fulfill_parked@dev-alice"),
        "{aggregate}"
    );
    assert!(aggregate.contains("via=gauge"), "{aggregate}");
    assert_eq!(
        without_age(&status.lines(5)[1]),
        "status slave=dev-alice health=stuck bottleneck=fulfill_parked age=<n> depth=3 hint=fresh connected=true checkouts=1 in=1 out=1 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=1 outbox=0 writable=true fanout_dropped=0 last_error=-"
    );
}

#[test]
fn a_replayed_or_garbled_gauge_leaves_the_last_one_standing() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master.observe_gauge(ALICE, &parked_gauge(4).encode());

    master.observe_gauge(ALICE, b"not a gauge at all");
    master.observe_gauge(
        ALICE,
        &Gauge {
            stage: Some(Stage::Reconcile),
            ..parked_gauge(4)
        }
        .encode(),
    );

    let slave_line = master.take_status().lines(5)[1].clone();
    assert!(
        slave_line.contains("bottleneck=fulfill_parked"),
        "{slave_line}"
    );
    assert!(slave_line.contains("hint=fresh"), "{slave_line}");
}

#[test]
fn a_disconnect_drops_the_peer_gauge() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();
    master.observe_gauge(ALICE, &parked_gauge(1).encode());

    master.disconnect(ALICE);
    master
        .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
        .unwrap();

    let slave_line = master.take_status().lines(5)[1].clone();
    assert!(slave_line.contains("bottleneck=unobserved"), "{slave_line}");
    assert!(slave_line.contains("hint=absent"), "{slave_line}");
}

#[test]
fn a_gauge_older_than_three_status_periods_is_stale() {
    let mut ledger = StatusLedger::default();
    let live = || {
        vec![PeerLive {
            slave_id: "dev-alice".into(),
            checkouts: 1,
            queues: Queues::default(),
            waits: Vec::new(),
        }]
    };

    ledger.observe_gauge(
        "dev-alice",
        parked_gauge(1),
        Instant::now() - Duration::from_secs(10),
    );

    let believed = ledger.take_master(live(), Vec::new(), 5);
    assert!(
        believed.lines(5)[1].contains("bottleneck=fulfill_parked"),
        "{}",
        believed.lines(5)[1]
    );
    assert!(believed.lines(5)[1].contains("hint=fresh"));
    let expired = ledger.take_master(live(), Vec::new(), 1);
    assert!(
        expired.lines(5)[1].contains("bottleneck=unobserved hint=stale"),
        "{}",
        expired.lines(5)[1]
    );
    assert!(expired.lines(5)[0].contains("bottleneck=none"));
}

#[test]
fn slave_dir_list_and_root_report_increment_counters() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let _ = slave.take_status(online());

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

    let status = slave.take_status(online());
    assert!(status.flow.root >= 1);
    assert!(status.flow.dir_list >= 1);
    assert_eq!(
        status.line(5),
        "status 5s health=busy bottleneck=none connected=true in=2 out=1 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=2 root=1 local=0 rescan=0 flushed=0 pending=0 pending_pulls=0 pending_renames=0 parked=0 sending=0 work=0 last_error=-"
    );
}

#[test]
fn slave_waiting_on_master_bytes_names_origin_bytes() {
    let sandbox = SyncSandbox::new();
    let body = b"bytes the slave does not hold";
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let _ = slave.take_status(online());

    match slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/big.bin"),
            new: FileMetadata::file(body.len() as u64, MTIME, 0o100644, hash_bytes(body)),
            basis: None,
        })
        .unwrap()
    {
        arborsync_core::slave::Reply::Send(msgs)
            if matches!(msgs.as_slice(), [ProtocolMessage::SignatureRequest { .. }]) => {}
        other => panic!("expected SignatureRequest, got {other:?}"),
    }

    let status = slave.take_status(online());
    assert_eq!(
        without_age(&status.line(5)),
        "status 5s health=stuck bottleneck=origin_bytes age=<n> depth=1 connected=true in=1 out=1 cas_ok=0 cas_rej=0 apply_ok=0 apply_fail=0 bulk_in=0 bulk_out=0 bytes_in=0 bytes_out=0 dir_list=0 root=0 local=0 rescan=0 flushed=0 pending=1 pending_pulls=0 pending_renames=0 parked=0 sending=0 work=0 last_error=-"
    );
}

#[test]
fn hashing_wait_names_the_hop() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let _ = slave.take_status(online());
    let local = slave.checkout_local("src").expect("src").to_path_buf();
    sandbox.tree(&local).file("fresh.txt", b"typed");
    let plan = slave
        .plan_local("src", LocalEvent::Changed(p("/src/fresh.txt")))
        .unwrap();
    assert_eq!(plan.hash.len(), 1);
    let status = slave.take_status(online());
    assert!(
        status.line(5).contains("bottleneck=hashing"),
        "{}",
        status.line(5)
    );
    assert!(status.line(5).contains("depth=1"), "{}", status.line(5));
}

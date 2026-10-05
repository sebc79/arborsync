use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, Instant};

use arborsync_core::LoadedSlave;
use arborsync_core::bottleneck::{Stage, Wait};
use arborsync_core::config::{ReloadError, SlaveAcl};
use arborsync_core::keys::format_hex_key;
use arborsync_core::master::{Master, MemoryContent, Reply};
use arborsync_core::meta::{FileMetadata, hash_bytes};
use arborsync_core::peers::{Pace, PeerView, Presence, query_peers};
use arborsync_core::protocol::{CheckoutRef, ProtocolMessage};
use arborsync_core::slave::{self, LinkState, Slave};
use arborsync_core::status::{Queues, StatusLedger};
use arborsync_core::test_support::{MemoryStorage, SyncSandbox, p};

const ALICE: [u8; 32] = [0xA1; 32];
const BACKUP: [u8; 32] = [0xB1; 32];
const STANDBY: [u8; 32] = [0xC1; 32];
const MTIME: i64 = 1_700_000_000_000;

#[test]
fn report_and_log_watermarks_do_not_steal() {
    let mut ledger = StatusLedger::default();
    ledger.local(None);
    ledger.local(None);
    let (flow, errors) = ledger.report_delta();
    assert_eq!(flow.local, 2);
    assert_eq!(errors, 0);

    ledger.local(None);
    ledger.local(None);
    ledger.local(None);
    let status = ledger.take_slave(true, Queues::default(), &[]);
    assert_eq!(status.flow.local, 5);

    let (flow, _) = ledger.report_delta();
    assert_eq!(flow.local, 3);

    let status = ledger.take_slave(true, Queues::default(), &[]);
    assert_eq!(status.flow.local, 0);
    assert!(status.last_error.is_none());
}

#[test]
fn a_second_log_take_with_no_new_events_is_zeros() {
    let mut ledger = StatusLedger::default();
    ledger.local(None);
    let first = ledger.take_slave(true, Queues::default(), &[]);
    assert_eq!(first.flow.local, 1);
    let second = ledger.take_slave(false, Queues::default(), &[]);
    assert_eq!(second.flow.local, 0);
    assert_eq!(second.flow.in_msgs, 0);
    assert!(!second.connected);
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

fn open_master(sandbox: &SyncSandbox) -> Master<MemoryStorage, MemoryContent> {
    let cfg_path = sandbox.write_master_config(vec![
        slave_acl("dev-alice", ALICE, &["/src"]),
        slave_acl("backup-1", BACKUP, &["/"]),
        slave_acl("standby", STANDBY, &["/src"]),
    ]);
    let cfg = arborsync_core::LoadedMaster::load(&cfg_path).unwrap();
    Master::open(cfg, MemoryStorage::new(), MemoryContent::new()).unwrap()
}

fn subscribe_ok(
    master: &mut Master<MemoryStorage, MemoryContent>,
    peer: [u8; 32],
    msg: ProtocolMessage,
) {
    match master.handle(peer, msg).unwrap() {
        Reply::Send(_) => {}
        other => panic!("expected SubscribeAck, got {other:?}"),
    }
}

#[test]
fn directory_keeps_pace_across_replace_disconnect_and_fifteen_seconds() {
    let sandbox = SyncSandbox::new();
    let mut master = open_master(&sandbox);
    subscribe_ok(
        &mut master,
        ALICE,
        subscribe("dev-alice", &[("src", "/src")]),
    );
    subscribe_ok(&mut master, BACKUP, subscribe("backup-1", &[("bak", "/")]));
    assert_eq!(master.session_generation(&ALICE), Some(1));

    let first = master.poll_peer_directory(BACKUP).unwrap();
    let alice = first.get("dev-alice").unwrap();
    assert_eq!(alice.presence, Presence::Connected);
    assert_eq!(alice.pace, Pace::Idle);
    assert_eq!(alice.depth.get(), 0);
    assert!(!alice.fresh);
    let standby = first.get("standby").unwrap();
    assert_eq!(standby.presence, Presence::Disconnected);
    assert_eq!(standby.pace, Pace::Idle);
    assert_eq!(standby.depth.get(), 0);
    assert!(!standby.fresh);
    assert!(first.get("backup-1").is_none());
    assert!(first.get("carol").is_none());

    let own = master.poll_peer_directory(ALICE).unwrap();
    assert!(own.get("dev-alice").is_none());
    let backup = own.get("backup-1").unwrap();
    assert_eq!(backup.presence, Presence::Connected);
    assert_eq!(backup.pace, Pace::Idle);
    assert_eq!(backup.depth.get(), 0);
    assert!(!backup.fresh);

    master.observe_peer_report(ALICE, 1, 5, Pace::Busy, 4);
    let published = master.poll_peer_directory(BACKUP).unwrap();
    let alice = published.get("dev-alice").unwrap();
    assert_eq!(alice.presence, Presence::Connected);
    assert_eq!(alice.pace, Pace::Busy);
    assert_eq!(alice.depth.get(), 4);
    assert!(alice.fresh);

    master.observe_peer_report(ALICE, 1, 5, Pace::Idle, 0);
    assert!(master.poll_peer_directory(BACKUP).is_none());
    master.observe_peer_report(ALICE, 1, 4, Pace::Idle, 1);
    assert!(master.poll_peer_directory(BACKUP).is_none());

    master.publish_peers(Instant::now() + Duration::from_secs(15));
    let stale = master.poll_peer_directory(BACKUP).unwrap();
    let alice = stale.get("dev-alice").unwrap();
    assert_eq!(alice.pace, Pace::Busy);
    assert_eq!(alice.depth.get(), 4);
    assert!(!alice.fresh);

    subscribe_ok(
        &mut master,
        ALICE,
        subscribe("dev-alice", &[("src", "/src")]),
    );
    assert_eq!(master.session_generation(&ALICE), Some(2));
    master.observe_peer_report(ALICE, 1, 9, Pace::Idle, 1);
    let after_old = master.poll_peer_directory(BACKUP);
    if let Some(view) = after_old {
        let alice = view.get("dev-alice").unwrap();
        assert_eq!(alice.pace, Pace::Busy);
        assert_eq!(alice.depth.get(), 4);
    }

    master.observe_peer_report(ALICE, 2, 1, Pace::Idle, 0);
    let replaced = master.poll_peer_directory(BACKUP).unwrap();
    let alice = replaced.get("dev-alice").unwrap();
    assert_eq!(alice.presence, Presence::Connected);
    assert_eq!(alice.pace, Pace::Idle);
    assert_eq!(alice.depth.get(), 0);
    assert!(alice.fresh);

    master.observe_peer_report(ALICE, 2, 2, Pace::Busy, 3);
    let _ = master.poll_peer_directory(BACKUP);
    master.disconnect(ALICE);
    let gone = master.poll_peer_directory(BACKUP).unwrap();
    let alice = gone.get("dev-alice").unwrap();
    assert_eq!(alice.presence, Presence::Disconnected);
    assert_eq!(alice.pace, Pace::Busy);
    assert_eq!(alice.depth.get(), 3);
    assert!(!alice.fresh);

    subscribe_ok(
        &mut master,
        ALICE,
        subscribe("dev-alice", &[("src", "/src")]),
    );
    assert_eq!(master.session_generation(&ALICE), Some(3));
    master.observe_peer_report(ALICE, 2, 8, Pace::Idle, 9);
    let late = master.poll_peer_directory(BACKUP);
    if let Some(view) = late {
        let alice = view.get("dev-alice").unwrap();
        assert_eq!(alice.pace, Pace::Busy);
        assert_eq!(alice.depth.get(), 3);
        assert_eq!(alice.presence, Presence::Connected);
    }
}

#[test]
fn a_long_file_batch_does_not_drop_the_directory() {
    let sandbox = SyncSandbox::new();
    let mut master = open_master(&sandbox);
    subscribe_ok(
        &mut master,
        ALICE,
        subscribe("dev-alice", &[("src", "/src")]),
    );
    subscribe_ok(&mut master, BACKUP, subscribe("backup-1", &[("bak", "/")]));
    master.set_writable(BACKUP, false);
    assert!(master.poll(BACKUP).is_empty());
    let view = master.poll_peer_directory(BACKUP).unwrap();
    assert!(matches!(view, PeerView::Current(_)));
    assert!(view.get("dev-alice").is_some());
    assert!(view.get("backup-1").is_none());
}

#[test]
fn sample_report_depth_is_ten_and_failed_health_stays_on_the_log() {
    let sandbox = SyncSandbox::new();
    let local = sandbox.add_checkout("dev-alice", "src");
    let cfg_path = sandbox.write_slave_config(
        "dev-alice",
        vec![arborsync_core::config::CheckoutConfig {
            id: "src".into(),
            central: "/src".into(),
            local: local.to_string_lossy().into_owned(),
        }],
        vec![format_hex_key(&[0x11; 32])],
    );
    let mut slave = Slave::open(
        arborsync_core::LoadedSlave::load(&cfg_path).unwrap(),
        MemoryStorage::new(),
        MemoryContent::new(),
    )
    .unwrap();
    assert_eq!(slave.served_peers(), PeerView::Waiting);
    assert_eq!(
        slave.peer_socket(),
        sandbox
            .slave_db("dev-alice")
            .parent()
            .unwrap()
            .join("peers.sock")
    );

    let quiet = LinkState {
        connected: true,
        waits: Vec::new(),
        work_depth: 0,
    };
    let _ = slave.sample_report(quiet.clone(), 1);
    let _ = slave.take_status(quiet.clone());
    slave.note_status_error(None, "disk full");
    let (pace, _) = slave.sample_report(quiet.clone(), 2);
    assert_eq!(pace, Pace::Stuck);
    let status = slave.take_status(quiet);
    assert!(
        status.line(5).contains("health=failed"),
        "{}",
        status.line(5)
    );

    let body = b"bytes the slave does not hold";
    match slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/big.bin"),
            new: FileMetadata::file(body.len() as u64, MTIME, 0o100644, hash_bytes(body)),
            basis: None,
        })
        .unwrap()
    {
        arborsync_core::slave::Reply::Send(_) => {}
        other => panic!("expected a reply, got {other:?}"),
    }
    let (pace, depth) = slave.sample_report(
        LinkState {
            connected: true,
            waits: vec![
                Wait::new(Stage::FulfillParked, Instant::now(), 2),
                Wait::new(Stage::FulfillRead, Instant::now(), 3),
            ],
            work_depth: 4,
        },
        1,
    );
    assert_eq!(pace, Pace::Stuck);
    assert_eq!(depth.get(), 10);
}

#[tokio::test]
async fn query_peers_on_the_slave_socket_returns_a_literal_view() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("peers.sock");
    let (tx, rx) = tokio::sync::watch::channel(PeerView::Waiting);
    let _serve = slave::serve_peers(sock.clone(), rx).unwrap();
    let mode = std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);

    let sock_waiting = sock.clone();
    let waiting = tokio::task::spawn_blocking(move || query_peers(&sock_waiting).unwrap())
        .await
        .unwrap();
    assert_eq!(waiting, PeerView::Waiting);

    let sandbox = SyncSandbox::new();
    let mut master = open_master(&sandbox);
    subscribe_ok(
        &mut master,
        ALICE,
        subscribe("dev-alice", &[("src", "/src")]),
    );
    let served = master.poll_peer_directory(ALICE).unwrap();
    tx.send(served).unwrap();

    let sock_current = sock.clone();
    let current = tokio::task::spawn_blocking(move || query_peers(&sock_current).unwrap())
        .await
        .unwrap();
    let PeerView::Current(peers) = current else {
        panic!("expected Current");
    };
    assert_eq!(peers.len(), 2);
    assert_eq!(peers[0].name.as_str(), "backup-1");
    assert_eq!(peers[0].presence, Presence::Disconnected);
    assert_eq!(peers[0].pace, Pace::Idle);
    assert_eq!(peers[0].depth.get(), 0);
    assert!(!peers[0].fresh);
    assert_eq!(peers[1].name.as_str(), "standby");
    assert_eq!(peers[1].presence, Presence::Disconnected);
    assert_eq!(peers[1].pace, Pace::Idle);
    assert_eq!(peers[1].depth.get(), 0);
    assert!(!peers[1].fresh);
}

#[test]
fn omitted_peer_socket_is_beside_the_db_and_a_change_restarts() {
    let current = LoadedSlave::parse(
        r#"
slave_id = "dev-alice"
master_addr = "master.example.com:8443"
slave_key_path = "/etc/arborsync/slave.key"
master_public_keys = ["hex:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
db_path = "/var/cache/arborsync/cache.redb"
watcher_debounce_ms = 200
checkouts = []
"#,
    )
    .unwrap();
    assert_eq!(
        current.peer_socket(),
        std::path::Path::new("/var/cache/arborsync/peers.sock")
    );
    let next = LoadedSlave::parse(
        r#"
slave_id = "dev-alice"
master_addr = "master.example.com:8443"
slave_key_path = "/etc/arborsync/slave.key"
master_public_keys = ["hex:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
db_path = "/var/cache/arborsync/cache.redb"
peer_socket = "/var/cache/arborsync/other.sock"
watcher_debounce_ms = 200
checkouts = []
"#,
    )
    .unwrap();
    match current.plan_reload(&next) {
        Err(ReloadError::RestartRequired { fields }) => {
            assert!(fields.contains(&"peer_socket".to_string()), "{fields:?}");
        }
        other => panic!("expected RestartRequired, got {other:?}"),
    }
}

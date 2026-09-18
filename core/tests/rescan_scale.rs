use std::fs;
use std::path::Path;
use std::time::Instant;

use arborsync_core::config::CheckoutConfig;
use arborsync_core::keys::format_hex_key;
use arborsync_core::merkle::empty_dir_node;
use arborsync_core::path::CanonicalPath;
use arborsync_core::protocol::ProtocolMessage;
use arborsync_core::slave::{MemoryContent, Reply, Slave};
use arborsync_core::test_support::SyncSandbox;
use arborsync_core::{LoadedSlave, RedbStorage, Storage};

const MASTER_PIN: [u8; 32] = [0x11; 32];

fn sizes() -> Vec<usize> {
    match std::env::var("ARBORSYNC_RESCAN_NS") {
        Ok(raw) => raw
            .split(',')
            .filter_map(|part| part.trim().parse().ok())
            .collect(),
        Err(_) => vec![250, 500, 1000],
    }
}

fn send(reply: Reply) -> Vec<ProtocolMessage> {
    match reply {
        Reply::Send(msgs) => msgs,
        other => panic!("expected Send, got {other:?}"),
    }
}

fn write_files(local: &Path, n: usize) {
    for i in 0..n {
        fs::write(local.join(format!("f{i:05}")), [b'x']).unwrap();
    }
}

fn open_slave(sandbox: &SyncSandbox) -> Slave<RedbStorage, MemoryContent> {
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
    let store = RedbStorage::open(&sandbox.slave_db("dev-alice")).unwrap();
    Slave::open(cfg, store, MemoryContent::new()).unwrap()
}

fn measure(n: usize) {
    let sandbox = SyncSandbox::new();
    let mut slave = open_slave(&sandbox);
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    let created = Instant::now();
    write_files(&local, n);
    let create_ms = created.elapsed().as_millis();

    let first = Instant::now();
    let first_msgs = slave.rescan("src").unwrap();
    let first_ms = first.elapsed().as_millis();
    assert_eq!(first_msgs.len(), 1);

    let second = Instant::now();
    let second_msgs = slave.rescan("src").unwrap();
    let second_ms = second.elapsed().as_millis();
    assert_eq!(second_msgs.len(), 1);

    let ack = Instant::now();
    let requests = send(
        slave
            .handle(ProtocolMessage::RootAck {
                checkout_id: "src".into(),
                path: CanonicalPath::parse("/src").unwrap(),
                matched: false,
                master_root: empty_dir_node().into(),
            })
            .unwrap(),
    );
    let ack_ms = ack.elapsed().as_millis();
    assert_eq!(requests.len(), 1);

    let walk = Instant::now();
    let announces = send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: CanonicalPath::parse("/src").unwrap(),
                entries: vec![],
            })
            .unwrap(),
    );
    let walk_ms = walk.elapsed().as_millis();
    let announce_count = announces
        .iter()
        .filter(|msg| matches!(msg, ProtocolMessage::FileAnnounce { .. }))
        .count();

    let again = Instant::now();
    let again_announces = send(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: CanonicalPath::parse("/src").unwrap(),
                entries: vec![],
            })
            .unwrap(),
    );
    let again_ms = again.elapsed().as_millis();
    let again_count = again_announces
        .iter()
        .filter(|msg| matches!(msg, ProtocolMessage::FileAnnounce { .. }))
        .count();

    println!(
        "n={n} create_ms={create_ms} first_rescan_ms={first_ms} second_rescan_ms={second_ms} root_ack_ms={ack_ms} dirlist_ms={walk_ms} announces={announce_count} second_dirlist_ms={again_ms} second_announces={again_count} first_us_per_file={} dirlist_us_per_file={}",
        (first_ms * 1000) / n.max(1) as u128,
        (walk_ms * 1000) / n.max(1) as u128
    );
}

#[test]
#[ignore]
fn rescan_scale_slave_only_files() {
    for n in sizes() {
        measure(n);
    }
}

use std::time::Instant;

use arborsync_core::config::SlaveAcl;
use arborsync_core::hash::ContentHash;
use arborsync_core::keys::format_hex_key;
use arborsync_core::master::{Master, Reply, WholeFileLater};
use arborsync_core::merkle::file_node;
use arborsync_core::meta::{hash_bytes, FileMetadata};
use arborsync_core::path::CanonicalPath;
use arborsync_core::protocol::ProtocolMessage;
use arborsync_core::storage::CheckoutId;
use arborsync_core::test_support::SyncSandbox;
use arborsync_core::transfer::fulfill;
use arborsync_core::{LoadedMaster, RedbStorage, Storage, WriteBatch};

const ALICE: [u8; 32] = [0xA1; 32];
const MTIME: i64 = 1_700_000_000_000;

fn sizes() -> Vec<usize> {
    match std::env::var("ARBORSYNC_APPLY_NS") {
        Ok(raw) => raw
            .split(',')
            .filter_map(|part| part.trim().parse().ok())
            .collect(),
        Err(_) => vec![100, 250, 500],
    }
}

fn subscribe() -> ProtocolMessage {
    ProtocolMessage::Subscribe {
        slave_id: "dev-alice".into(),
        checkouts: vec![arborsync_core::protocol::CheckoutRef {
            id: "src".into(),
            central: CanonicalPath::parse("/src").unwrap(),
        }],
    }
}

fn open_master(sandbox: &SyncSandbox) -> Master<RedbStorage, WholeFileLater> {
    let cfg_path = sandbox.write_master_config(vec![SlaveAcl {
        id: "dev-alice".into(),
        public_keys: vec![format_hex_key(&ALICE)],
        allowed_prefixes: vec!["/src".into()],
    }]);
    let cfg = LoadedMaster::load(&cfg_path).unwrap();
    let store = RedbStorage::open(&sandbox.master_db()).unwrap();
    Master::open(cfg, store, WholeFileLater).unwrap()
}

fn apply_one(master: &mut Master<RedbStorage, WholeFileLater>, path: CanonicalPath, body: &[u8]) {
    let hash = hash_bytes(body);
    let new = FileMetadata::file(body.len() as u64, MTIME, 0o100644, hash);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: path.clone(),
                new: new.clone(),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::SignatureRequest { want_hash, .. }) => {
            assert_eq!(want_hash, hash);
        }
        other => panic!("expected SignatureRequest, got {other:?}"),
    }
    let xfer = fulfill("src", path.clone(), hash, body, &[]).unwrap();
    match master.apply_bulk(ALICE, xfer.header, &xfer.body).unwrap() {
        Reply::Send(ProtocolMessage::CasAccept {
            file_node: node, ..
        }) => {
            assert_eq!(node, Some(file_node(&new)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }
}

fn measure_shape(shape: &str, n: usize, path_at: impl Fn(usize) -> CanonicalPath) {
    let sandbox = SyncSandbox::new();
    let mut master = open_master(&sandbox);
    master.handle(ALICE, subscribe()).unwrap();

    let body = b"x";
    let mid = n / 2;
    let first = Instant::now();
    for i in 0..mid {
        apply_one(&mut master, path_at(i), body);
    }
    let first_ms = first.elapsed().as_millis();
    let second = Instant::now();
    for i in mid..n {
        apply_one(&mut master, path_at(i), body);
    }
    let second_ms = second.elapsed().as_millis();
    let apply_ms = first_ms + second_ms;
    println!(
        "shape={shape} n={n} apply_ms={apply_ms} first_half_ms={first_ms} second_half_ms={second_ms} us_per_file={} second_us_per_file={}",
        (apply_ms * 1000) / n.max(1) as u128,
        (second_ms * 1000) / (n - mid).max(1) as u128
    );
}

fn measure_range_scan(n: usize) {
    let sandbox = SyncSandbox::new();
    let store = RedbStorage::open(&sandbox.master_db()).unwrap();
    let ck = CheckoutId::master();
    let meta = FileMetadata::file(1, MTIME, 0o100644, ContentHash::from_bytes([1; 32]));
    let mut batch = store.begin_write().unwrap();
    for i in 0..n {
        let path = CanonicalPath::parse(&format!("/src/f{i:05}")).unwrap();
        batch.put_meta(&ck, &path, &meta).unwrap();
    }
    batch.commit().unwrap();

    let once = Instant::now();
    let under_src = store
        .range_meta(&ck, &CanonicalPath::parse("/src").unwrap())
        .unwrap();
    let src_ms = once.elapsed().as_millis();
    let root = Instant::now();
    let under_root = store
        .range_meta(&ck, &CanonicalPath::parse("/").unwrap())
        .unwrap();
    let root_ms = root.elapsed().as_millis();
    assert_eq!(under_src.len(), n);
    assert_eq!(under_root.len(), n);
    println!("shape=range_meta n={n} src_ms={src_ms} root_ms={root_ms}");
}

#[test]
#[ignore]
fn apply_scale_slave_to_master() {
    for n in sizes() {
        measure_shape("flat", n, |i| {
            CanonicalPath::parse(&format!("/src/f{i:05}")).unwrap()
        });
        measure_shape("nested50", n, |i| {
            let dir = i / 50;
            CanonicalPath::parse(&format!("/src/d{dir:03}/f{i:05}")).unwrap()
        });
        measure_range_scan(n);
    }
    measure_after_seed(2000, 200);
}

fn measure_after_seed(seed: usize, n: usize) {
    let sandbox = SyncSandbox::new();
    let mut master = open_master(&sandbox);
    master.handle(ALICE, subscribe()).unwrap();
    let body = b"x";
    for i in 0..seed {
        apply_one(
            &mut master,
            CanonicalPath::parse(&format!("/src/s{i:05}")).unwrap(),
            body,
        );
    }
    let started = Instant::now();
    for i in 0..n {
        apply_one(
            &mut master,
            CanonicalPath::parse(&format!("/src/n{i:05}")).unwrap(),
            body,
        );
    }
    let apply_ms = started.elapsed().as_millis();
    println!(
        "shape=after_seed seed={seed} n={n} apply_ms={apply_ms} us_per_file={}",
        (apply_ms * 1000) / n.max(1) as u128
    );
}

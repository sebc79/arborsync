use arborsync_core::LoadedMaster;
use arborsync_core::config::{CheckoutConfig, SlaveAcl};
use arborsync_core::keys::format_hex_key;
use arborsync_core::master::{Master, MemoryContent, Reply};
use arborsync_core::merkle::file_node;
use arborsync_core::meta::{EntryKind, FileMetadata, hash_bytes};
use arborsync_core::protocol::{BulkEncoding, ProtocolMessage};
use arborsync_core::slave::Slave;
use arborsync_core::test_support::{MemoryStorage, SyncSandbox, p};
use arborsync_core::transfer::{
    MIN_DELTA_BASIS, ask_kind, delta_bytes, fulfill, patch_bytes, signature_bytes,
};

const ALICE: [u8; 32] = [0xA1; 32];
const MTIME: i64 = 1_700_000_000_000;
const MASTER: [u8; 32] = [0x11; 32];

fn slave_acl() -> SlaveAcl {
    SlaveAcl {
        id: "dev-alice".into(),
        public_keys: vec![format_hex_key(&ALICE)],
        allowed_prefixes: vec!["/src".into()],
    }
}

fn subscribe() -> ProtocolMessage {
    ProtocolMessage::Subscribe {
        slave_id: "dev-alice".into(),
        checkouts: vec![arborsync_core::protocol::CheckoutRef {
            id: "src".into(),
            central: p("/src"),
        }],
    }
}

fn alice_master(sandbox: &SyncSandbox) -> Master<MemoryStorage, MemoryContent> {
    let cfg_path = sandbox.write_master_config(vec![slave_acl()]);
    let cfg = LoadedMaster::load(&cfg_path).unwrap();
    Master::open(cfg, MemoryStorage::new(), MemoryContent::new()).unwrap()
}

fn alice_slave(sandbox: &SyncSandbox) -> Slave<MemoryStorage, MemoryContent> {
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
    Slave::open(
        arborsync_core::LoadedSlave::load(&cfg_path).unwrap(),
        MemoryStorage::new(),
        MemoryContent::new(),
    )
    .unwrap()
}

#[test]
fn ask_kind_is_whole_unless_the_live_file_is_at_least_4_kib() {
    assert_eq!(
        ask_kind(EntryKind::File, None),
        arborsync_core::transfer::AskKind::Whole
    );
    assert_eq!(
        ask_kind(EntryKind::File, Some(MIN_DELTA_BASIS - 1)),
        arborsync_core::transfer::AskKind::Whole
    );
    assert_eq!(
        ask_kind(EntryKind::File, Some(MIN_DELTA_BASIS)),
        arborsync_core::transfer::AskKind::Delta
    );
    assert_eq!(
        ask_kind(EntryKind::Symlink, Some(MIN_DELTA_BASIS)),
        arborsync_core::transfer::AskKind::Whole
    );
}

#[test]
fn copia_delta_reconstructs_the_source_bytes() {
    let basis = vec![b'a'; 8 * 1024];
    let mut source = basis.clone();
    source[100..120].copy_from_slice(&[b'z'; 20]);
    let signature = signature_bytes(&basis).unwrap();
    let delta = delta_bytes(&source, &signature).unwrap();
    assert_eq!(patch_bytes(&basis, &delta).unwrap(), source);
}

#[test]
fn fulfill_empty_signature_sends_whole_bytes() {
    let source = b"hello-world";
    let hash = hash_bytes(source);
    let xfer = fulfill("src", p("/src/hello.txt"), hash, source, &[]).unwrap();
    assert_eq!(xfer.header.encoding, BulkEncoding::Whole);
    assert_eq!(xfer.header.want_hash, hash);
    assert_eq!(xfer.header.size, source.len() as u64);
    assert_eq!(xfer.body, source);
}

#[test]
fn fulfill_with_a_signature_sends_a_delta_that_patches_back() {
    let basis = vec![b'a'; 8 * 1024];
    let mut source = basis.clone();
    source[0] = b'b';
    let hash = hash_bytes(&source);
    let signature = signature_bytes(&basis).unwrap();
    let xfer = fulfill("src", p("/src/hello.txt"), hash, &source, &signature).unwrap();
    assert_eq!(xfer.header.encoding, BulkEncoding::Delta);
    assert_eq!(patch_bytes(&basis, &xfer.body).unwrap(), source.as_slice());
}

#[test]
fn master_asks_then_applies_a_whole_bulk_stream() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    master.handle(ALICE, subscribe()).unwrap();

    let hello = b"hello";
    let hash = hash_bytes(hello);
    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    match master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: new.clone(),
                basis: None,
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::SignatureRequest {
            checkout_id,
            path,
            want_hash,
            signature,
        }) => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, p("/src/hello.txt"));
            assert_eq!(want_hash, hash);
            assert!(signature.is_empty());
        }
        other => panic!("expected SignatureRequest, got {other:?}"),
    }
    assert!(!sandbox.central_root().join("src/hello.txt").exists());

    let xfer = fulfill("src", p("/src/hello.txt"), hash, hello, &[]).unwrap();
    match master.apply_bulk(ALICE, xfer.header, &xfer.body).unwrap() {
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
}

#[test]
fn master_fulfills_a_signature_request_from_central_root() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    master.handle(ALICE, subscribe()).unwrap();

    let hello = b"hello";
    let hash = hash_bytes(hello);
    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    let xfer = fulfill("src", p("/src/hello.txt"), hash, hello, &[]).unwrap();
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
    master.apply_bulk(ALICE, xfer.header, &xfer.body).unwrap();

    match master
        .handle(
            ALICE,
            ProtocolMessage::SignatureRequest {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                want_hash: hash,
                signature: Vec::new(),
            },
        )
        .unwrap()
    {
        Reply::Bulk(out) => {
            assert_eq!(out.header.encoding, BulkEncoding::Whole);
            assert_eq!(out.body, hello);
        }
        other => panic!("expected Bulk, got {other:?}"),
    }
}

#[test]
fn master_missing_want_hash_replies_error() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    master.handle(ALICE, subscribe()).unwrap();
    match master
        .handle(
            ALICE,
            ProtocolMessage::SignatureRequest {
                checkout_id: "src".into(),
                path: p("/src/missing.txt"),
                want_hash: hash_bytes(b"nope"),
                signature: Vec::new(),
            },
        )
        .unwrap()
    {
        Reply::Send(ProtocolMessage::Error { code, .. }) => assert_eq!(code, "missing_hash"),
        other => panic!("expected Error, got {other:?}"),
    }
}

#[test]
fn corrupt_delta_retries_whole_then_keeps_the_live_file() {
    let sandbox = SyncSandbox::new();
    let mut master = alice_master(&sandbox);
    master.handle(ALICE, subscribe()).unwrap();

    let basis = vec![b'a'; MIN_DELTA_BASIS as usize];
    let basis_hash = hash_bytes(&basis);
    let basis_meta = FileMetadata::file(basis.len() as u64, MTIME, 0o100644, basis_hash);
    let first = fulfill("src", p("/src/hello.txt"), basis_hash, &basis, &[]).unwrap();
    master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: basis_meta,
                basis: None,
            },
        )
        .unwrap();
    master.apply_bulk(ALICE, first.header, &first.body).unwrap();

    let mut source = basis.clone();
    source[0] = b'z';
    let want = hash_bytes(&source);
    let new = FileMetadata::file(source.len() as u64, MTIME, 0o100644, want);
    master
        .handle(
            ALICE,
            ProtocolMessage::FileAnnounce {
                checkout_id: "src".into(),
                path: p("/src/hello.txt"),
                new: new.clone(),
                basis: Some(file_node(&FileMetadata::file(
                    basis.len() as u64,
                    MTIME,
                    0o100644,
                    basis_hash,
                ))),
            },
        )
        .unwrap();

    let junk = delta_junk(&new, b"not-a-delta");
    match master.apply_bulk(ALICE, junk.0, &junk.1).unwrap() {
        Reply::Send(ProtocolMessage::SignatureRequest { signature, .. }) => {
            assert!(signature.is_empty());
        }
        other => panic!("expected whole-file retry, got {other:?}"),
    }
    assert_eq!(
        std::fs::read(sandbox.central_root().join("src/hello.txt")).unwrap(),
        basis
    );

    let mut header = fulfill(
        "src",
        p("/src/hello.txt"),
        hash_bytes(b"wrong-bytes"),
        b"wrong-bytes",
        &[],
    )
    .unwrap()
    .header;
    header.want_hash = want;
    match master.apply_bulk(ALICE, header, b"wrong-bytes").unwrap() {
        Reply::Send(ProtocolMessage::Error { code, .. }) => assert_eq!(code, "keep_live"),
        other => panic!("expected keep_live, got {other:?}"),
    }
    assert_eq!(
        std::fs::read(sandbox.central_root().join("src/hello.txt")).unwrap(),
        basis
    );
    assert_eq!(
        master
            .meta(&p("/src/hello.txt"))
            .unwrap()
            .unwrap()
            .content_hash,
        basis_hash
    );
}

fn delta_junk(new: &FileMetadata, body: &[u8]) -> (arborsync_core::protocol::BulkHeader, Vec<u8>) {
    (
        arborsync_core::protocol::BulkHeader {
            path: p("/src/hello.txt"),
            checkout_id: "src".into(),
            want_hash: new.content_hash,
            encoding: BulkEncoding::Delta,
            size: body.len() as u64,
        },
        body.to_vec(),
    )
}

#[test]
fn slave_asks_then_applies_a_whole_bulk_stream() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox);
    let hello = b"hello";
    let hash = hash_bytes(hello);
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
        arborsync_core::slave::Reply::Send(msgs) => match &msgs[..] {
            [
                ProtocolMessage::SignatureRequest {
                    want_hash,
                    signature,
                    ..
                },
            ] => {
                assert_eq!(*want_hash, hash);
                assert!(signature.is_empty());
            }
            other => panic!("expected SignatureRequest, got {other:?}"),
        },
        other => panic!("expected Send, got {other:?}"),
    }

    let xfer = fulfill("src", p("/src/hello.txt"), hash, hello, &[]).unwrap();
    slave.apply_bulk(xfer.header, &xfer.body).unwrap();
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    assert_eq!(std::fs::read(local.join("hello.txt")).unwrap(), hello);
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(file_node(&new))
    );
}

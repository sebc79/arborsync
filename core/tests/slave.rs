use std::os::unix::fs::PermissionsExt;

use arborsync_core::LoadedSlave;
use arborsync_core::LocalEvent;
use arborsync_core::config::{CheckoutConfig, ReloadError};
use arborsync_core::hash::ContentHash;
use arborsync_core::hashing::HashPlan;
use arborsync_core::keys::format_hex_key;
use arborsync_core::merkle::file_node;
use arborsync_core::meta::{FileMetadata, hash_bytes};
use arborsync_core::path::{RESERVED_CONFLICTS, conflict_sidecar_path};
use arborsync_core::protocol::ProtocolMessage;
use arborsync_core::slave::{
    DeleteAction, MemoryContent, ReplicaAction, Reply, Slave, SlaveError, decide_incoming,
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

    let hash = ContentHash::from_bytes([7; 32]);
    let as_file = FileMetadata::file(3, MTIME, 0o100644, hash);
    let as_link = FileMetadata::symlink(3, MTIME, 0o120777, hash);
    assert_eq!(
        decide_incoming(Some(&as_file), None, &as_link),
        ReplicaAction::SidecarThenApply
    );
}

#[test]
fn master_won_delete_sidecars_divergent_content() {
    let live = file(1);
    assert_eq!(
        decide_master_won_delete(None, file_node(&live), None, None),
        DeleteAction::AlreadyGone
    );
    assert_eq!(
        decide_master_won_delete(Some(&live), file_node(&live), None, None),
        DeleteAction::Remove
    );
    assert_eq!(
        decide_master_won_delete(
            Some(&live),
            file_node(&file(9)),
            Some(file_node(&live)),
            None
        ),
        DeleteAction::Remove
    );
    assert_eq!(
        decide_master_won_delete(
            Some(&live),
            file_node(&file(9)),
            Some(file_node(&file(8))),
            None
        ),
        DeleteAction::SidecarThenRemove
    );
    let mut chmodded = live.clone();
    chmodded.mode = 0o100755;
    assert_eq!(
        decide_master_won_delete(
            Some(&chmodded),
            file_node(&live),
            Some(file_node(&live)),
            Some(live.content_hash)
        ),
        DeleteAction::Remove
    );
}

#[test]
fn pin_miss_hangs_up_before_subscribe() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
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
fn incoming_type_change_file_to_dir_replaces_the_live_file() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let previous = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: previous.clone(),
            basis: None,
        })
        .unwrap();

    let new = FileMetadata::directory(MTIME, 0o040755);
    match slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: new.clone(),
            basis: Some(file_node(&previous)),
        })
        .unwrap()
    {
        Reply::Send(msgs) => assert!(msgs.is_empty()),
        other => panic!("expected empty send, got {other:?}"),
    }

    let host = slave.checkout_local("src").unwrap().join("hello.txt");
    assert!(host.is_dir());
    assert_eq!(
        slave.meta("src", &p("/src/hello.txt")).unwrap(),
        Some(new.clone())
    );
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(file_node(&new))
    );
    assert!(
        !conflict_sidecar_path(
            slave.checkout_local("src").unwrap(),
            &p("/src/hello.txt"),
            &previous.content_hash,
        )
        .exists()
    );

    let out = slave
        .note_local("src", LocalEvent::Removed(p("/src/hello.txt")))
        .unwrap();
    assert!(out.is_empty());
    assert!(host.is_dir());
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(file_node(&new))
    );
}

#[test]
fn incoming_type_change_dir_to_file_replaces_the_directory() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let previous = FileMetadata::directory(MTIME, 0o040755);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: previous.clone(),
            basis: None,
        })
        .unwrap();
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt/child.txt"),
            new: FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash),
            basis: None,
        })
        .unwrap();

    let new = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    match slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: new.clone(),
            basis: Some(file_node(&previous)),
        })
        .unwrap()
    {
        Reply::Send(msgs) => assert!(msgs.is_empty()),
        other => panic!("expected empty send, got {other:?}"),
    }

    let local = slave.checkout_local("src").unwrap().to_path_buf();
    let host = local.join("hello.txt");
    assert_eq!(std::fs::read(&host).unwrap(), hello);
    assert_eq!(
        slave.meta("src", &p("/src/hello.txt")).unwrap(),
        Some(new.clone())
    );
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(file_node(&new))
    );
    assert_eq!(
        slave.meta("src", &p("/src/hello.txt/child.txt")).unwrap(),
        None
    );
    assert!(!local.join("hello.txt/child.txt").exists());
    assert!(!conflict_sidecar_path(&local, &p("/src/hello.txt"), &previous.content_hash).exists());
}

#[test]
fn incoming_type_change_file_to_symlink_replaces_the_live_file() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let target = b"somewhere";
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    bodies.offer(hash_bytes(target), target.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let previous = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: previous.clone(),
            basis: None,
        })
        .unwrap();

    let new = FileMetadata::symlink(target.len() as u64, MTIME, 0o120777, hash_bytes(target));
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: new.clone(),
            basis: Some(file_node(&previous)),
        })
        .unwrap();

    let host = slave.checkout_local("src").unwrap().join("hello.txt");
    assert!(host.symlink_metadata().unwrap().file_type().is_symlink());
    assert_eq!(
        std::fs::read_link(&host).unwrap(),
        std::path::Path::new("somewhere")
    );
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(file_node(&new))
    );
    assert!(
        !conflict_sidecar_path(
            slave.checkout_local("src").unwrap(),
            &p("/src/hello.txt"),
            &previous.content_hash,
        )
        .exists()
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

#[test]
fn reload_adds_a_checkout_and_subscribe_lists_it() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let src = sandbox.add_checkout("dev-alice", "src");
    let docs = sandbox.add_checkout("dev-alice", "docs");
    let next = LoadedSlave::load(&sandbox.write_slave_config(
        "dev-alice",
        vec![
            CheckoutConfig {
                id: "src".into(),
                central: "/src".into(),
                local: src.to_string_lossy().into_owned(),
            },
            CheckoutConfig {
                id: "docs".into(),
                central: "/docs".into(),
                local: docs.to_string_lossy().into_owned(),
            },
        ],
        vec![format_hex_key(&MASTER)],
    ))
    .unwrap();

    let plan = slave.reload(next).unwrap();
    assert_eq!(plan.added, vec!["docs".to_string()]);
    assert_eq!(plan.resubscribe, true);
    let docs_local = docs.canonicalize().unwrap();
    assert_eq!(slave.checkout_local("docs"), Some(docs_local.as_path()));

    match slave.subscribe() {
        ProtocolMessage::Subscribe { checkouts, .. } => {
            let mut ids: Vec<_> = checkouts.iter().map(|c| c.id.as_str()).collect();
            ids.sort();
            assert_eq!(ids, ["docs", "src"]);
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
}

#[test]
fn reload_removes_a_checkout_and_drops_its_index() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("hello.txt", b"keep");
    slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    assert!(slave.meta("src", &p("/src/hello.txt")).unwrap().is_some());

    let next = LoadedSlave::load(&sandbox.write_slave_config(
        "dev-alice",
        vec![],
        vec![format_hex_key(&MASTER)],
    ))
    .unwrap();
    let plan = slave.reload(next).unwrap();
    assert_eq!(plan.removed, vec!["src".to_string()]);
    assert_eq!(slave.checkout_local("src"), None);
    match slave.meta("src", &p("/src/hello.txt")) {
        Err(SlaveError::UnknownCheckout(id)) => assert_eq!(id, "src"),
        other => panic!("expected UnknownCheckout, got {other:?}"),
    }
    assert_eq!(std::fs::read(local.join("hello.txt")).unwrap(), b"keep");
}

#[test]
fn reload_rejects_slave_id_change_and_keeps_checkouts() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let src = slave.checkout_local("src").unwrap().to_path_buf();
    let next = LoadedSlave::load(&sandbox.write_slave_config(
        "dev-bob",
        vec![CheckoutConfig {
            id: "src".into(),
            central: "/src".into(),
            local: src.to_string_lossy().into_owned(),
        }],
        vec![format_hex_key(&MASTER)],
    ))
    .unwrap();
    match slave.reload(next) {
        Err(SlaveError::Reload(ReloadError::RestartRequired { fields })) => {
            assert!(fields.contains(&"slave_id".to_string()));
        }
        other => panic!("expected RestartRequired, got {other:?}"),
    }
    assert_eq!(slave.checkout_local("src"), Some(src.as_path()));
}

fn backup_slave(
    sandbox: &SyncSandbox,
    bodies: MemoryContent,
) -> Slave<MemoryStorage, MemoryContent> {
    let overlap = sandbox.overlap_backup_slave();
    let cfg_path = sandbox.write_slave_config(
        "backup-1",
        vec![
            CheckoutConfig {
                id: "src".into(),
                central: "/src".into(),
                local: overlap.src.to_string_lossy().into_owned(),
            },
            CheckoutConfig {
                id: "bak".into(),
                central: "/".into(),
                local: overlap.bak.to_string_lossy().into_owned(),
            },
        ],
        vec![format_hex_key(&MASTER)],
    );
    let cfg = LoadedSlave::load(&cfg_path).unwrap();
    Slave::open(cfg, MemoryStorage::new(), bodies).unwrap()
}

#[test]
fn symlink_resolved_local_overlap_fails_at_open() {
    let sandbox = SyncSandbox::new();
    let real = sandbox.add_checkout("overlap", "real");
    let link_parent = sandbox.slave_root("overlap").join("checkouts");
    let link = link_parent.join("via-link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let cfg_path = sandbox.write_slave_config(
        "overlap",
        vec![
            CheckoutConfig {
                id: "a".into(),
                central: "/src".into(),
                local: real.to_string_lossy().into_owned(),
            },
            CheckoutConfig {
                id: "b".into(),
                central: "/docs".into(),
                local: link.to_string_lossy().into_owned(),
            },
        ],
        vec![format_hex_key(&MASTER)],
    );
    match LoadedSlave::load(&cfg_path) {
        Err(arborsync_core::ConfigError::LocalOverlap { a, b }) => {
            let a_path = std::path::PathBuf::from(&a);
            let b_path = std::path::PathBuf::from(&b);
            assert!(a_path.ends_with("real") || b_path.ends_with("real"));
        }
        Ok(cfg) => match Slave::open(cfg, MemoryStorage::new(), MemoryContent::new()) {
            Err(SlaveError::Config(arborsync_core::ConfigError::LocalOverlap { a, b })) => {
                assert_ne!(a, b);
            }
            Err(err) => panic!("expected LocalOverlap at open, got {err}"),
            Ok(_) => panic!("expected LocalOverlap at open, got Ok"),
        },
        other => panic!("expected LocalOverlap or a loadable config, got {other:?}"),
    }
}

#[test]
fn incoming_delete_same_content_different_mode_writes_no_sidecar() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let applied = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: applied.clone(),
            basis: None,
        })
        .unwrap();
    let mut changed = applied.clone();
    changed.mode = 0o100755;
    match slave
        .handle(ProtocolMessage::Delete {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            basis: file_node(&changed),
        })
        .unwrap()
    {
        Reply::Send(msgs) => assert!(msgs.is_empty()),
        other => panic!("expected empty send, got {other:?}"),
    }
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    let sidecar = conflict_sidecar_path(&local, &p("/src/hello.txt"), &hash);
    assert!(!sidecar.exists());
    assert!(!local.join("hello.txt").exists());
}

#[test]
fn incoming_delete_after_local_chmod_writes_no_sidecar() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let applied = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: applied.clone(),
            basis: None,
        })
        .unwrap();
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    assert!(
        slave
            .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
            .unwrap()
            .is_empty()
    );
    std::fs::set_permissions(
        local.join("hello.txt"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    slave
        .note_local("src", LocalEvent::Metadata(p("/src/hello.txt")))
        .unwrap();
    assert_ne!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        slave
            .meta("src", &p("/src/hello.txt"))
            .unwrap()
            .as_ref()
            .map(file_node)
    );
    match slave
        .handle(ProtocolMessage::Delete {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            basis: file_node(&applied),
        })
        .unwrap()
    {
        Reply::Send(msgs) => assert!(msgs.is_empty()),
        other => panic!("expected empty send, got {other:?}"),
    }
    let sidecar = conflict_sidecar_path(&local, &p("/src/hello.txt"), &hash);
    assert!(!sidecar.exists());
    assert!(!local.join("hello.txt").exists());
}

#[test]
fn directory_delete_removes_nested_file_then_parent() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("nested/child.txt", b"inside");
    slave
        .note_local("src", LocalEvent::Changed(p("/src/nested")))
        .unwrap();
    slave
        .note_local("src", LocalEvent::Changed(p("/src/nested/child.txt")))
        .unwrap();
    let dir_meta = slave.meta("src", &p("/src/nested")).unwrap().unwrap();
    assert_eq!(
        std::fs::read(local.join("nested/child.txt")).unwrap(),
        b"inside"
    );
    slave
        .handle(ProtocolMessage::Delete {
            checkout_id: "src".into(),
            path: p("/src/nested"),
            basis: file_node(&dir_meta),
        })
        .unwrap();
    assert!(!local.join("nested/child.txt").exists());
    assert!(!local.join("nested").exists());
}

#[test]
fn subscribe_reject_filters_denied_central_and_hangs_up_when_none_remain() {
    let sandbox = SyncSandbox::new();
    let mut slave = backup_slave(&sandbox, MemoryContent::new());
    match slave.subscribe() {
        ProtocolMessage::Subscribe { checkouts, .. } => {
            let mut centrals: Vec<_> = checkouts.iter().map(|c| c.central.as_str()).collect();
            centrals.sort();
            assert_eq!(centrals, ["/", "/src"]);
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
    match slave
        .handle(ProtocolMessage::SubscribeReject {
            reason: "central is outside allowed_prefixes".into(),
            denied_centrals: vec![p("/")],
        })
        .unwrap()
    {
        Reply::Send(msgs) => match &msgs[..] {
            [ProtocolMessage::Subscribe { checkouts, .. }] => {
                assert_eq!(checkouts.len(), 1);
                assert_eq!(checkouts[0].id, "src");
                assert_eq!(checkouts[0].central, p("/src"));
            }
            other => panic!("expected filtered Subscribe, got {other:?}"),
        },
        other => panic!("expected Send, got {other:?}"),
    }
    match slave.subscribe() {
        ProtocolMessage::Subscribe { checkouts, .. } => {
            assert_eq!(checkouts.len(), 1);
            assert_eq!(checkouts[0].central, p("/src"));
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
    match slave
        .handle(ProtocolMessage::SubscribeReject {
            reason: "central is outside allowed_prefixes".into(),
            denied_centrals: vec![p("/src")],
        })
        .unwrap()
    {
        Reply::Hangup { reason } => {
            assert_eq!(reason, "central is outside allowed_prefixes")
        }
        other => panic!("expected Hangup, got {other:?}"),
    }
}

#[test]
fn echo_of_an_applied_dir_or_meta_only_does_not_reannounce() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let dir = FileMetadata::directory(MTIME, 0o040755);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/nested"),
            new: dir.clone(),
            basis: None,
        })
        .unwrap();
    assert_eq!(
        slave.last_synced("src", &p("/src/nested")).unwrap(),
        Some(file_node(&dir))
    );
    assert!(
        slave
            .note_local("src", LocalEvent::Changed(p("/src/nested")))
            .unwrap()
            .is_empty()
    );

    let local = slave.checkout_local("src").unwrap().to_path_buf();
    std::fs::set_permissions(local.join("nested"), std::fs::Permissions::from_mode(0o700)).unwrap();
    match &slave
        .note_local("src", LocalEvent::Changed(p("/src/nested")))
        .unwrap()[..]
    {
        [
            ProtocolMessage::FileAnnounce {
                path, new, basis, ..
            },
        ] => {
            assert_eq!(path, &p("/src/nested"));
            assert_eq!(*basis, Some(file_node(&dir)));
            assert_ne!(file_node(new), file_node(&dir));
        }
        other => panic!("expected chmod FileAnnounce, got {other:?}"),
    }

    let file = FileMetadata::file(hello.len() as u64, MTIME, 0o100644, hash);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: file.clone(),
            basis: None,
        })
        .unwrap();
    let mut meta_only = file;
    meta_only.mode = 0o100755;
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            new: meta_only,
            basis: Some(file_node(&FileMetadata::file(
                hello.len() as u64,
                MTIME,
                0o100644,
                hash,
            ))),
        })
        .unwrap();
    assert!(
        slave
            .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn local_file_rename_announces_rename_and_cas_accepts_advance_last_synced() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("old.txt", b"moved");
    let created = slave
        .note_local("src", LocalEvent::Changed(p("/src/old.txt")))
        .unwrap();
    let ProtocolMessage::FileAnnounce { new, .. } = &created[0] else {
        panic!("expected FileAnnounce, got {created:?}");
    };
    let node = file_node(new);
    slave
        .handle(ProtocolMessage::CasAccept {
            checkout_id: "src".into(),
            path: p("/src/old.txt"),
            file_node: Some(node),
        })
        .unwrap();
    assert_eq!(
        slave.last_synced("src", &p("/src/old.txt")).unwrap(),
        Some(node)
    );

    std::fs::rename(local.join("old.txt"), local.join("new.txt")).unwrap();
    let out = slave
        .note_local(
            "src",
            LocalEvent::Renamed {
                from: p("/src/old.txt"),
                to: p("/src/new.txt"),
            },
        )
        .unwrap();
    match &out[..] {
        [
            ProtocolMessage::Rename {
                checkout_id,
                from,
                to,
                from_basis,
                to_new,
            },
        ] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(from, &p("/src/old.txt"));
            assert_eq!(to, &p("/src/new.txt"));
            assert_eq!(*from_basis, node);
            assert_eq!(to_new.content_hash, hash_bytes(b"moved"));
        }
        other => panic!("expected Rename, got {other:?}"),
    }
    assert_eq!(
        slave.last_synced("src", &p("/src/old.txt")).unwrap(),
        Some(node)
    );
    assert_eq!(slave.last_synced("src", &p("/src/new.txt")).unwrap(), None);

    let to_node = file_node(&slave.meta("src", &p("/src/new.txt")).unwrap().unwrap());
    slave
        .handle(ProtocolMessage::CasAccept {
            checkout_id: "src".into(),
            path: p("/src/new.txt"),
            file_node: Some(to_node),
        })
        .unwrap();
    slave
        .handle(ProtocolMessage::CasAccept {
            checkout_id: "src".into(),
            path: p("/src/old.txt"),
            file_node: None,
        })
        .unwrap();
    assert_eq!(slave.last_synced("src", &p("/src/old.txt")).unwrap(), None);
    assert_eq!(
        slave.last_synced("src", &p("/src/new.txt")).unwrap(),
        Some(to_node)
    );
}

#[test]
fn incoming_rename_moves_the_file_and_sets_last_synced_on_both_paths() {
    let sandbox = SyncSandbox::new();
    let body = b"hello";
    let hash = hash_bytes(body);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, body.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let new = FileMetadata::file(body.len() as u64, MTIME, 0o100644, hash);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/old.txt"),
            new: new.clone(),
            basis: None,
        })
        .unwrap();

    match slave
        .handle(ProtocolMessage::Rename {
            checkout_id: "src".into(),
            from: p("/src/old.txt"),
            to: p("/src/new.txt"),
            from_basis: file_node(&new),
            to_new: new.clone(),
        })
        .unwrap()
    {
        Reply::Send(msgs) => assert!(msgs.is_empty()),
        other => panic!("expected empty send, got {other:?}"),
    }

    let local = slave.checkout_local("src").unwrap().to_path_buf();
    assert_eq!(std::fs::read(local.join("new.txt")).unwrap(), body);
    assert!(!local.join("old.txt").exists());
    assert_eq!(slave.last_synced("src", &p("/src/old.txt")).unwrap(), None);
    assert_eq!(
        slave.last_synced("src", &p("/src/new.txt")).unwrap(),
        Some(file_node(&new))
    );
}

#[test]
fn incoming_rename_sidecars_a_destination_whose_content_differs() {
    let sandbox = SyncSandbox::new();
    let from_body = b"from-bytes";
    let to_body = b"dest-bytes";
    let from_hash = hash_bytes(from_body);
    let to_hash = hash_bytes(to_body);
    let mut bodies = MemoryContent::new();
    bodies.offer(from_hash, from_body.to_vec());
    bodies.offer(to_hash, to_body.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let from_meta = FileMetadata::file(from_body.len() as u64, MTIME, 0o100644, from_hash);
    let to_meta = FileMetadata::file(to_body.len() as u64, MTIME, 0o100644, to_hash);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/old.txt"),
            new: from_meta.clone(),
            basis: None,
        })
        .unwrap();
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/new.txt"),
            new: to_meta,
            basis: None,
        })
        .unwrap();

    slave
        .handle(ProtocolMessage::Rename {
            checkout_id: "src".into(),
            from: p("/src/old.txt"),
            to: p("/src/new.txt"),
            from_basis: file_node(&from_meta),
            to_new: from_meta,
        })
        .unwrap();

    let local = slave.checkout_local("src").unwrap().to_path_buf();
    assert_eq!(std::fs::read(local.join("new.txt")).unwrap(), from_body);
    let sidecar = conflict_sidecar_path(&local, &p("/src/new.txt"), &to_hash);
    assert!(
        sidecar.exists(),
        "destination bytes were replaced without a sidecar"
    );
    assert_eq!(std::fs::read(&sidecar).unwrap(), to_body);
}

#[test]
fn unpaired_remove_and_change_still_delete_plus_announce() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("old.txt", b"moved");
    slave
        .note_local("src", LocalEvent::Changed(p("/src/old.txt")))
        .unwrap();
    std::fs::rename(local.join("old.txt"), local.join("new.txt")).unwrap();

    let removed = slave
        .note_local("src", LocalEvent::Removed(p("/src/old.txt")))
        .unwrap();
    match &removed[..] {
        [ProtocolMessage::Delete { path, .. }] => assert_eq!(path, &p("/src/old.txt")),
        other => panic!("expected Delete, got {other:?}"),
    }
    let created = slave
        .note_local("src", LocalEvent::Changed(p("/src/new.txt")))
        .unwrap();
    match &created[..] {
        [ProtocolMessage::FileAnnounce { path, new, .. }] => {
            assert_eq!(path, &p("/src/new.txt"));
            assert_eq!(new.content_hash, hash_bytes(b"moved"));
        }
        other => panic!("expected FileAnnounce, got {other:?}"),
    }
}

#[test]
fn write_event_reuses_hash_when_only_mode_changes() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("hello.txt", b"typed");
    let created = slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    let ProtocolMessage::FileAnnounce { new, .. } = &created[0] else {
        panic!("expected FileAnnounce, got {created:?}");
    };
    let hash = new.content_hash;
    let mtime = new.mtime_ns;
    std::fs::set_permissions(
        local.join("hello.txt"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    sandbox.tree(&local).set_mtime_ns("hello.txt", mtime);
    match &slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap()[..]
    {
        [ProtocolMessage::FileAnnounce { new, .. }] => {
            assert_eq!(new.content_hash, hash);
            assert_ne!(new.mode & 0o777, 0o644);
        }
        other => panic!("expected chmod FileAnnounce, got {other:?}"),
    }
}

#[test]
fn metadata_chmod_announces_new_mode_and_keeps_the_content_hash() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("hello.txt", b"typed");
    let created = slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    let ProtocolMessage::FileAnnounce { new, .. } = &created[0] else {
        panic!("expected FileAnnounce, got {created:?}");
    };
    let hash = new.content_hash;
    std::fs::set_permissions(
        local.join("hello.txt"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    match &slave
        .note_local("src", LocalEvent::Metadata(p("/src/hello.txt")))
        .unwrap()[..]
    {
        [ProtocolMessage::FileAnnounce { new, .. }] => {
            assert_eq!(new.content_hash, hash);
            assert_ne!(new.mode & 0o777, 0o644);
        }
        other => panic!("expected chmod FileAnnounce, got {other:?}"),
    }
}

#[test]
fn echo_of_an_applied_rename_does_not_reannounce() {
    let sandbox = SyncSandbox::new();
    let body = b"hello";
    let hash = hash_bytes(body);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, body.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let new = FileMetadata::file(body.len() as u64, MTIME, 0o100644, hash);
    slave
        .handle(ProtocolMessage::FileAnnounce {
            checkout_id: "src".into(),
            path: p("/src/old.txt"),
            new: new.clone(),
            basis: None,
        })
        .unwrap();
    slave
        .handle(ProtocolMessage::Rename {
            checkout_id: "src".into(),
            from: p("/src/old.txt"),
            to: p("/src/new.txt"),
            from_basis: file_node(&new),
            to_new: new,
        })
        .unwrap();
    assert!(
        slave
            .note_local("src", LocalEvent::Changed(p("/src/new.txt")))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn hashed_siblings_announce_with_matching_index_rows() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    let tree = sandbox.tree(&local);
    tree.file("a.txt", b"aaa");
    tree.file("b.txt", b"bbb");
    tree.file("nested/c.txt", b"ccc");

    let mut plan = HashPlan::default();
    for path in ["/src/a.txt", "/src/b.txt", "/src/nested/c.txt"] {
        plan.append(
            slave
                .plan_local("src", LocalEvent::Changed(p(path)))
                .unwrap(),
        );
    }
    assert!(
        plan.send.is_empty(),
        "new files should need a hash job, got {:?}",
        plan.send
    );
    let mut dones = Vec::new();
    for need in plan.hash {
        slave.start_hashed(&need.key);
        dones.push(need.run());
    }
    let out = slave.commit_hashed_batch(dones).unwrap();
    let hashes: Vec<(&str, ContentHash)> = out
        .iter()
        .map(|msg| match msg {
            ProtocolMessage::FileAnnounce {
                checkout_id,
                path,
                new,
                basis,
            } => {
                assert_eq!(checkout_id, "src");
                assert_eq!(*basis, None);
                (path.as_str(), new.content_hash)
            }
            other => panic!("expected FileAnnounce, got {other:?}"),
        })
        .collect();
    assert_eq!(
        hashes,
        [
            ("/src/a.txt", hash_bytes(b"aaa")),
            ("/src/b.txt", hash_bytes(b"bbb")),
            ("/src/nested/c.txt", hash_bytes(b"ccc")),
        ]
    );
    assert_eq!(
        slave
            .meta("src", &p("/src/a.txt"))
            .unwrap()
            .unwrap()
            .content_hash,
        hash_bytes(b"aaa")
    );
    assert_eq!(
        slave
            .meta("src", &p("/src/b.txt"))
            .unwrap()
            .unwrap()
            .content_hash,
        hash_bytes(b"bbb")
    );
    assert_eq!(
        slave
            .meta("src", &p("/src/nested/c.txt"))
            .unwrap()
            .unwrap()
            .content_hash,
        hash_bytes(b"ccc")
    );
    assert_eq!(slave.last_synced("src", &p("/src/a.txt")).unwrap(), None);
}

#[test]
fn reserved_tmp_rename_produces_no_message() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file(".arborsync-tmp/x", b"tmp");
    assert!(
        slave
            .note_local(
                "src",
                LocalEvent::Renamed {
                    from: p("/src/.arborsync-tmp/x"),
                    to: p("/src/.arborsync-tmp/y"),
                },
            )
            .unwrap()
            .is_empty()
    );
}

#[test]
fn cas_reject_adopts_a_winner_of_a_different_kind() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("hello.txt", b"local");
    slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();

    let winner = FileMetadata::directory(MTIME, 0o040755);
    slave
        .handle(ProtocolMessage::CasReject {
            checkout_id: "src".into(),
            path: p("/src/hello.txt"),
            current: Some(winner.clone()),
        })
        .unwrap();

    let sidecar = conflict_sidecar_path(&local, &p("/src/hello.txt"), &hash_bytes(b"local"));
    assert_eq!(std::fs::read(&sidecar).unwrap(), b"local");
    assert!(local.join("hello.txt").is_dir());
    assert_eq!(
        slave.meta("src", &p("/src/hello.txt")).unwrap(),
        Some(winner)
    );
}

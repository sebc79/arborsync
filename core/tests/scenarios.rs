use arborsync_core::LoadedMaster;
use arborsync_core::LoadedSlave;
use arborsync_core::LocalEvent;
use arborsync_core::config::{CheckoutConfig, SlaveAcl};
use arborsync_core::keys::format_hex_key;
use arborsync_core::master::{Master, MemoryContent, Reply as MasterReply};
use arborsync_core::merkle::{empty_dir_node, file_node};
use arborsync_core::meta::{EntryKind, FileMetadata, collect_from_path, hash_bytes};
use arborsync_core::path::{RESERVED_CONFLICTS, RESERVED_TMP, conflict_sidecar_path};
use arborsync_core::protocol::{CheckoutRef, ProtocolMessage};
use arborsync_core::slave::{Reply as SlaveReply, Slave};
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

fn file_meta(bytes: &[u8], mode: u32) -> FileMetadata {
    FileMetadata::file(bytes.len() as u64, MTIME, mode, hash_bytes(bytes))
}

fn master_msg(reply: MasterReply) -> ProtocolMessage {
    match reply {
        MasterReply::Send(msg) => msg,
        other => panic!("expected Send, got {other:?}"),
    }
}

fn slave_msgs(reply: SlaveReply) -> Vec<ProtocolMessage> {
    match reply {
        SlaveReply::Send(msgs) => msgs,
        other => panic!("expected Send, got {other:?}"),
    }
}

fn tmp_contains_body(tmp: &std::path::Path, body: &[u8]) -> bool {
    let Ok(entries) = std::fs::read_dir(tmp) else {
        return false;
    };
    entries.filter_map(|e| e.ok()).any(|e| {
        std::fs::read(e.path())
            .ok()
            .is_some_and(|bytes| bytes == body)
    })
}

#[test]
fn reserved_sidecar_dirs_stay_off_the_index() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    let mut slave = alice_slave(&sandbox, bodies);
    master_msg(
        master
            .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
            .unwrap(),
    );

    let central = master.central_root().to_path_buf();
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    for root in [&central, &local] {
        let tree = sandbox.tree(root);
        tree.file(&format!("{RESERVED_TMP}/scratch"), b"tmp");
        tree.file(&format!("{RESERVED_CONFLICTS}/noise"), b"noise");
        tree.file("keep.txt", b"keep");
    }

    master.rescan().unwrap();
    slave.rescan("src").unwrap();

    assert_eq!(
        master.meta(&p("/keep.txt")).unwrap().unwrap().content_hash,
        hash_bytes(b"keep")
    );
    assert_eq!(master.meta(&p("/.arborsync-tmp/scratch")).unwrap(), None);
    assert_eq!(
        master.meta(&p("/.arborsync-conflicts/noise")).unwrap(),
        None
    );
    assert_eq!(
        slave
            .meta("src", &p("/src/keep.txt"))
            .unwrap()
            .unwrap()
            .content_hash,
        hash_bytes(b"keep")
    );
    assert_eq!(
        slave
            .meta("src", &p("/src/.arborsync-tmp/scratch"))
            .unwrap(),
        None
    );
    assert_eq!(
        slave
            .meta("src", &p("/src/.arborsync-conflicts/noise"))
            .unwrap(),
        None
    );

    let _ = master.poll(BACKUP);
    master
        .note_local(LocalEvent::Changed(p("/.arborsync-tmp/scratch")))
        .unwrap();
    assert!(master.poll(BACKUP).is_empty());
    assert!(
        slave
            .note_local("src", LocalEvent::Changed(p("/src/.arborsync-tmp/scratch")))
            .unwrap()
            .is_empty()
    );

    assert!(
        slave_msgs(
            slave
                .handle(ProtocolMessage::FileAnnounce {
                    checkout_id: "src".into(),
                    path: p("/src/hello.txt"),
                    new: file_meta(hello, 0o100644),
                    basis: None,
                })
                .unwrap()
        )
        .is_empty()
    );
    assert_eq!(std::fs::read(local.join("hello.txt")).unwrap(), hello);
    assert!(!tmp_contains_body(&local.join(RESERVED_TMP), hello));
}

#[test]
fn same_slave_src_and_root_apply_independently() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut master_bodies = MemoryContent::new();
    master_bodies.offer(hash, hello.to_vec());
    let mut slave_bodies = MemoryContent::new();
    slave_bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, master_bodies);
    let mut slave = backup_slave(&sandbox, slave_bodies);
    master_msg(
        master
            .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
            .unwrap(),
    );
    master_msg(
        master
            .handle(
                BACKUP,
                subscribe("backup-1", &[("src", "/src"), ("bak", "/")]),
            )
            .unwrap(),
    );

    let src = slave.checkout_local("src").unwrap().to_path_buf();
    let bak = slave.checkout_local("bak").unwrap().to_path_buf();
    sandbox.tree(&src).file("hello.txt", b"src-only");
    let announced = slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    match &announced[..] {
        [ProtocolMessage::FileAnnounce { path, new, .. }] => {
            assert_eq!(path, &p("/src/hello.txt"));
            assert_eq!(new.content_hash, hash_bytes(b"src-only"));
        }
        other => panic!("expected FileAnnounce, got {other:?}"),
    }
    assert_eq!(std::fs::read(src.join("hello.txt")).unwrap(), b"src-only");
    assert!(!bak.join("src/hello.txt").exists());
    assert_eq!(slave.meta("bak", &p("/src/hello.txt")).unwrap(), None);

    let new = file_meta(hello, 0o100644);
    match master_msg(
        master
            .handle(
                ALICE,
                ProtocolMessage::FileAnnounce {
                    checkout_id: "src".into(),
                    path: p("/src/other.txt"),
                    new: new.clone(),
                    basis: None,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::CasAccept {
            checkout_id,
            path,
            file_node: node,
        } => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, p("/src/other.txt"));
            assert_eq!(node, Some(file_node(&new)));
        }
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert!(master.poll(ALICE).is_empty());

    let pushed = master.poll(BACKUP);
    assert_eq!(pushed.len(), 2);
    let mut ids: Vec<String> = pushed
        .iter()
        .map(|msg| match msg {
            ProtocolMessage::FileAnnounce {
                checkout_id,
                path,
                new: announced,
                basis,
            } => {
                assert_eq!(path, &p("/src/other.txt"));
                assert_eq!(announced, &new);
                assert_eq!(*basis, None);
                checkout_id.clone()
            }
            other => panic!("expected FileAnnounce, got {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, ["bak", "src"]);

    for msg in pushed {
        assert!(slave_msgs(slave.handle(msg).unwrap()).is_empty());
    }
    assert_eq!(std::fs::read(src.join("other.txt")).unwrap(), hello);
    assert_eq!(std::fs::read(bak.join("src/other.txt")).unwrap(), hello);
    assert_eq!(
        slave.last_synced("src", &p("/src/other.txt")).unwrap(),
        Some(file_node(&new))
    );
    assert_eq!(
        slave.last_synced("bak", &p("/src/other.txt")).unwrap(),
        Some(file_node(&new))
    );
}

#[test]
fn cas_reject_sidecars_loser_then_live_path_is_the_winner() {
    let sandbox = SyncSandbox::new();
    sandbox
        .tree(&sandbox.central_root())
        .file("src/hello.txt", b"master");
    let winner = b"master";
    let winner_hash = hash_bytes(winner);
    let mut slave_bodies = MemoryContent::new();
    slave_bodies.offer(winner_hash, winner.to_vec());
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    let mut slave = alice_slave(&sandbox, slave_bodies);
    master_msg(
        master
            .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
            .unwrap(),
    );

    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("hello.txt", b"local");
    let announce = slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    let ProtocolMessage::FileAnnounce {
        checkout_id,
        path,
        new,
        basis,
    } = announce.into_iter().next().expect("one announce")
    else {
        panic!("expected FileAnnounce");
    };

    match master_msg(
        master
            .handle(
                ALICE,
                ProtocolMessage::FileAnnounce {
                    checkout_id,
                    path,
                    new,
                    basis,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::CasReject { current, .. } => {
            assert_eq!(current.unwrap().content_hash, winner_hash)
        }
        other => panic!("expected CasReject, got {other:?}"),
    }

    let winner_meta = master.meta(&p("/src/hello.txt")).unwrap().unwrap();
    assert!(
        slave_msgs(
            slave
                .handle(ProtocolMessage::CasReject {
                    checkout_id: "src".into(),
                    path: p("/src/hello.txt"),
                    current: Some(winner_meta),
                })
                .unwrap()
        )
        .is_empty()
    );

    let local_hash = hash_bytes(b"local");
    let sidecar = conflict_sidecar_path(&local, &p("/src/hello.txt"), &local_hash);
    assert_eq!(std::fs::read(&sidecar).unwrap(), b"local");
    assert!(sidecar.starts_with(local.join(RESERVED_CONFLICTS)));
    assert_eq!(std::fs::read(local.join("hello.txt")).unwrap(), winner);
    assert_eq!(
        std::fs::read(sandbox.central_root().join("src/hello.txt")).unwrap(),
        winner
    );
}

#[test]
fn incoming_announce_sidecars_when_content_differs_and_meta_only_does_not() {
    let sandbox = SyncSandbox::new();
    let remote = b"remote";
    let remote_hash = hash_bytes(remote);
    let mut bodies = MemoryContent::new();
    bodies.offer(remote_hash, remote.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("hello.txt", b"local");
    slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();

    assert!(
        slave_msgs(
            slave
                .handle(ProtocolMessage::FileAnnounce {
                    checkout_id: "src".into(),
                    path: p("/src/hello.txt"),
                    new: file_meta(remote, 0o100644),
                    basis: None,
                })
                .unwrap()
        )
        .is_empty()
    );
    let local_hash = hash_bytes(b"local");
    let sidecar = conflict_sidecar_path(&local, &p("/src/hello.txt"), &local_hash);
    assert_eq!(std::fs::read(&sidecar).unwrap(), b"local");
    assert_eq!(std::fs::read(local.join("hello.txt")).unwrap(), remote);

    let live = slave.meta("src", &p("/src/hello.txt")).unwrap().unwrap();
    let mut incoming = live.clone();
    incoming.mode = 0o100755;
    assert!(
        slave_msgs(
            slave
                .handle(ProtocolMessage::FileAnnounce {
                    checkout_id: "src".into(),
                    path: p("/src/hello.txt"),
                    new: incoming,
                    basis: None,
                })
                .unwrap()
        )
        .is_empty()
    );
    assert!(!conflict_sidecar_path(&local, &p("/src/hello.txt"), &remote_hash).exists());
    assert_eq!(std::fs::read(local.join("hello.txt")).unwrap(), remote);
    assert_eq!(
        slave
            .meta("src", &p("/src/hello.txt"))
            .unwrap()
            .unwrap()
            .mode,
        0o100755
    );
}

#[test]
fn inflight_echo_survives_mtime_only_then_a_real_edit_announces() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut bodies = MemoryContent::new();
    bodies.offer(hash, hello.to_vec());
    let mut slave = alice_slave(&sandbox, bodies);
    let applied = file_meta(hello, 0o100644);
    let applied_node = file_node(&applied);
    assert!(
        slave_msgs(
            slave
                .handle(ProtocolMessage::FileAnnounce {
                    checkout_id: "src".into(),
                    path: p("/src/hello.txt"),
                    new: applied.clone(),
                    basis: None,
                })
                .unwrap()
        )
        .is_empty()
    );

    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox
        .tree(&local)
        .set_mtime_ns("hello.txt", MTIME + 1_000_000_000);
    let disk = collect_from_path(&local.join("hello.txt"))
        .unwrap()
        .unwrap();
    assert_eq!(disk.content_hash, hash);
    assert_ne!(file_node(&disk), applied_node);
    assert!(
        slave
            .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
            .unwrap()
            .is_empty()
    );

    sandbox.tree(&local).file("hello.txt", b"edited");
    match &slave
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap()[..]
    {
        [
            ProtocolMessage::FileAnnounce {
                path, new, basis, ..
            },
        ] => {
            assert_eq!(path, &p("/src/hello.txt"));
            assert_eq!(new.content_hash, hash_bytes(b"edited"));
            assert_eq!(*basis, Some(applied_node));
        }
        other => panic!("expected FileAnnounce, got {other:?}"),
    }
    assert_eq!(
        slave.last_synced("src", &p("/src/hello.txt")).unwrap(),
        Some(applied_node)
    );
}

#[test]
fn restricted_slave_cannot_subscribe_root_or_announce_outside_src() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    let mut slave = alice_slave(&sandbox, MemoryContent::new());

    match master_msg(
        master
            .handle(ALICE, subscribe("dev-alice", &[("root", "/")]))
            .unwrap(),
    ) {
        ProtocolMessage::SubscribeReject {
            denied_centrals, ..
        } => assert_eq!(denied_centrals, vec![p("/")]),
        other => panic!("expected SubscribeReject, got {other:?}"),
    }

    match master_msg(
        master
            .handle(
                ALICE,
                subscribe("dev-alice", &[("src", "/src"), ("root", "/")]),
            )
            .unwrap(),
    ) {
        ProtocolMessage::SubscribeReject {
            denied_centrals, ..
        } => assert_eq!(denied_centrals, vec![p("/")]),
        other => panic!("expected SubscribeReject, got {other:?}"),
    }

    match slave
        .handle(ProtocolMessage::SubscribeReject {
            reason: "central is outside allowed_prefixes".into(),
            denied_centrals: vec![p("/")],
        })
        .unwrap()
    {
        SlaveReply::Send(msgs) => match &msgs[..] {
            [ProtocolMessage::Subscribe { checkouts, .. }] => {
                assert_eq!(checkouts.len(), 1);
                assert_eq!(checkouts[0].central, p("/src"));
            }
            other => panic!("expected filtered Subscribe, got {other:?}"),
        },
        other => panic!("expected Send, got {other:?}"),
    }

    match master_msg(
        master
            .handle(
                ALICE,
                ProtocolMessage::FileAnnounce {
                    checkout_id: "src".into(),
                    path: p("/src/hello.txt"),
                    new: file_meta(b"hello", 0o100644),
                    basis: None,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::Error { code, .. } => assert_eq!(code, "not_subscribed"),
        other => panic!("expected Error not_subscribed, got {other:?}"),
    }

    match master
        .handle([0xEE; 32], subscribe("dev-alice", &[("src", "/src")]))
        .unwrap()
    {
        MasterReply::Hangup { reason, rate_limit } => {
            assert_eq!(reason, "unknown static key");
            assert!(rate_limit);
        }
        other => panic!("expected Hangup, got {other:?}"),
    }

    master_msg(
        master
            .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
            .unwrap(),
    );
    match master_msg(
        master
            .handle(
                ALICE,
                ProtocolMessage::FileAnnounce {
                    checkout_id: "src".into(),
                    path: p("/docs/secret.txt"),
                    new: file_meta(b"secret", 0o100644),
                    basis: None,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::Error { code, .. } => assert_eq!(code, "outside_central"),
        other => panic!("expected Error outside_central, got {other:?}"),
    }
    assert!(!sandbox.central_root().join("docs/secret.txt").exists());
}

#[test]
fn master_rescan_fans_out_a_file_the_watcher_never_saw() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    master_msg(
        master
            .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
            .unwrap(),
    );

    sandbox
        .tree(&sandbox.central_root())
        .file("src/missed.txt", b"missed");
    master.rescan().unwrap();

    let pushed = master.poll(BACKUP);
    let announced = pushed.iter().find_map(|msg| match msg {
        ProtocolMessage::FileAnnounce { path, new, .. } if path == &p("/src/missed.txt") => {
            Some(new.content_hash)
        }
        _ => None,
    });
    assert_eq!(announced, Some(hash_bytes(b"missed")));
    let disk = collect_from_path(&sandbox.central_root().join("src/missed.txt"))
        .unwrap()
        .unwrap();
    assert_eq!(master.meta(&p("/src/missed.txt")).unwrap(), Some(disk));

    std::fs::remove_file(sandbox.central_root().join("src/missed.txt")).unwrap();
    master.rescan().unwrap();
    let pushed = master.poll(BACKUP);
    assert!(pushed.iter().any(|msg| matches!(
        msg,
        ProtocolMessage::Delete { path, .. } if path == &p("/src/missed.txt")
    )));
    assert_eq!(master.meta(&p("/src/missed.txt")).unwrap(), None);
}

#[test]
fn slave_rescan_then_reconcile_announces_a_file_the_watcher_never_saw() {
    let sandbox = SyncSandbox::new();
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("leftover.txt", b"mine");

    match &slave.rescan("src").unwrap()[..] {
        [ProtocolMessage::RootReport { path, root, .. }] => {
            assert_eq!(path, &p("/src"));
            assert_ne!(*root, empty_dir_node().into());
        }
        other => panic!("expected RootReport, got {other:?}"),
    }
    assert_eq!(
        slave
            .meta("src", &p("/src/leftover.txt"))
            .unwrap()
            .unwrap()
            .content_hash,
        hash_bytes(b"mine")
    );
    assert_eq!(
        slave.last_synced("src", &p("/src/leftover.txt")).unwrap(),
        None
    );

    match &slave_msgs(
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
        [
            ProtocolMessage::DirListRequest {
                checkout_id, path, ..
            },
        ] => {
            assert_eq!(checkout_id, "src");
            assert_eq!(path, &p("/src"));
        }
        other => panic!("expected DirListRequest, got {other:?}"),
    }

    match &slave_msgs(
        slave
            .handle(ProtocolMessage::DirListResponse {
                checkout_id: "src".into(),
                path: p("/src"),
                after: None,
                entries: vec![],
                more: false,
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
fn slave_can_cas_a_new_directory_and_later_delete_it() {
    let sandbox = SyncSandbox::new();
    let mut master = two_slave_master(&sandbox, MemoryContent::new());
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    master_msg(
        master
            .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
            .unwrap(),
    );

    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).mkdir("nested");
    let announce = slave
        .note_local("src", LocalEvent::Changed(p("/src/nested")))
        .unwrap();
    let ProtocolMessage::FileAnnounce {
        checkout_id,
        path,
        new,
        basis,
    } = announce.into_iter().next().expect("one announce")
    else {
        panic!("expected FileAnnounce");
    };
    assert_eq!(path, p("/src/nested"));
    assert_eq!(new.kind, EntryKind::Dir);
    assert_eq!(basis, None);

    match master_msg(
        master
            .handle(
                ALICE,
                ProtocolMessage::FileAnnounce {
                    checkout_id: checkout_id.clone(),
                    path: path.clone(),
                    new: new.clone(),
                    basis,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::CasAccept {
            file_node: node, ..
        } => assert_eq!(node, Some(file_node(&new))),
        other => panic!("expected CasAccept, got {other:?}"),
    }
    slave_msgs(
        slave
            .handle(ProtocolMessage::CasAccept {
                checkout_id: checkout_id.clone(),
                path: path.clone(),
                file_node: Some(file_node(&new)),
            })
            .unwrap(),
    );
    assert_eq!(
        slave.last_synced("src", &path).unwrap(),
        Some(file_node(&new))
    );

    std::fs::remove_dir(local.join("nested")).unwrap();
    let removed = slave
        .note_local("src", LocalEvent::Removed(p("/src/nested")))
        .unwrap();
    let ProtocolMessage::Delete {
        checkout_id,
        path,
        basis,
    } = removed.into_iter().next().expect("one delete")
    else {
        panic!("expected Delete");
    };
    assert_eq!(basis, file_node(&new));

    match master_msg(
        master
            .handle(
                ALICE,
                ProtocolMessage::Delete {
                    checkout_id,
                    path: path.clone(),
                    basis,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::CasAccept {
            file_node: node, ..
        } => assert_eq!(node, None),
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert!(!sandbox.central_root().join("src/nested").exists());
}

#[test]
fn same_window_rename_is_one_message_and_moves_central() {
    let sandbox = SyncSandbox::new();
    let mut bodies = MemoryContent::new();
    bodies.offer(hash_bytes(b"moved"), b"moved".to_vec());
    let mut master = two_slave_master(&sandbox, bodies);
    let mut slave = alice_slave(&sandbox, MemoryContent::new());
    master_msg(
        master
            .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
            .unwrap(),
    );
    master_msg(
        master
            .handle(BACKUP, subscribe("backup-1", &[("bak", "/")]))
            .unwrap(),
    );

    let local = slave.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("old.txt", b"moved");
    let created = slave
        .note_local("src", LocalEvent::Changed(p("/src/old.txt")))
        .unwrap();
    let ProtocolMessage::FileAnnounce {
        checkout_id,
        path,
        new,
        basis,
    } = created.into_iter().next().expect("one announce")
    else {
        panic!("expected FileAnnounce");
    };
    match master_msg(
        master
            .handle(
                ALICE,
                ProtocolMessage::FileAnnounce {
                    checkout_id,
                    path: path.clone(),
                    new: new.clone(),
                    basis,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::CasAccept {
            file_node: node, ..
        } => assert_eq!(node, Some(file_node(&new))),
        other => panic!("expected CasAccept, got {other:?}"),
    }
    slave_msgs(
        slave
            .handle(ProtocolMessage::CasAccept {
                checkout_id: "src".into(),
                path: path.clone(),
                file_node: Some(file_node(&new)),
            })
            .unwrap(),
    );
    let _ = master.poll(BACKUP);

    std::fs::rename(local.join("old.txt"), local.join("new.txt")).unwrap();
    let announced = slave
        .note_local(
            "src",
            LocalEvent::Renamed {
                from: p("/src/old.txt"),
                to: p("/src/new.txt"),
            },
        )
        .unwrap();
    let ProtocolMessage::Rename {
        checkout_id,
        from,
        to,
        from_basis,
        to_new,
    } = announced.into_iter().next().expect("one rename")
    else {
        panic!("expected Rename");
    };
    assert_eq!(from, p("/src/old.txt"));
    assert_eq!(to, p("/src/new.txt"));
    assert_eq!(from_basis, file_node(&new));
    assert_eq!(to_new.content_hash, hash_bytes(b"moved"));

    match master_msg(
        master
            .handle(
                ALICE,
                ProtocolMessage::Rename {
                    checkout_id,
                    from: from.clone(),
                    to: to.clone(),
                    from_basis,
                    to_new: to_new.clone(),
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::CasAccept {
            path,
            file_node: node,
            ..
        } => {
            assert_eq!(path, p("/src/new.txt"));
            assert_eq!(node, Some(file_node(&to_new)));
        }
        other => panic!("expected CasAccept on to, got {other:?}"),
    }
    assert!(sandbox.central_root().join("src/new.txt").exists());
    assert!(!sandbox.central_root().join("src/old.txt").exists());

    match &master.poll(BACKUP)[..] {
        [ProtocolMessage::Rename { from, to, .. }] => {
            assert_eq!(from, &p("/src/old.txt"));
            assert_eq!(to, &p("/src/new.txt"));
        }
        other => panic!("expected Rename fan-out, got {other:?}"),
    }
}

#[test]
fn alice_replaces_a_synced_file_with_a_directory_and_backup_applies() {
    let sandbox = SyncSandbox::new();
    let hello = b"hello";
    let hash = hash_bytes(hello);
    let mut master_bodies = MemoryContent::new();
    master_bodies.offer(hash, hello.to_vec());
    let mut backup_bodies = MemoryContent::new();
    backup_bodies.offer(hash, hello.to_vec());
    let mut master = two_slave_master(&sandbox, master_bodies);
    let mut alice = alice_slave(&sandbox, MemoryContent::new());
    let mut backup = backup_slave(&sandbox, backup_bodies);
    master_msg(
        master
            .handle(ALICE, subscribe("dev-alice", &[("src", "/src")]))
            .unwrap(),
    );
    master_msg(
        master
            .handle(
                BACKUP,
                subscribe("backup-1", &[("src", "/src"), ("bak", "/")]),
            )
            .unwrap(),
    );

    let local = alice.checkout_local("src").unwrap().to_path_buf();
    sandbox.tree(&local).file("hello.txt", hello);
    let created = alice
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    let ProtocolMessage::FileAnnounce {
        checkout_id,
        path,
        new,
        basis,
    } = created.into_iter().next().expect("one announce")
    else {
        panic!("expected FileAnnounce");
    };
    match master_msg(
        master
            .handle(
                ALICE,
                ProtocolMessage::FileAnnounce {
                    checkout_id: checkout_id.clone(),
                    path: path.clone(),
                    new: new.clone(),
                    basis,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::CasAccept {
            file_node: node, ..
        } => assert_eq!(node, Some(file_node(&new))),
        other => panic!("expected CasAccept, got {other:?}"),
    }
    slave_msgs(
        alice
            .handle(ProtocolMessage::CasAccept {
                checkout_id: checkout_id.clone(),
                path: path.clone(),
                file_node: Some(file_node(&new)),
            })
            .unwrap(),
    );
    for msg in master.poll(BACKUP) {
        assert!(slave_msgs(backup.handle(msg).unwrap()).is_empty());
    }
    let backup_src = backup.checkout_local("src").unwrap().to_path_buf();
    assert_eq!(std::fs::read(backup_src.join("hello.txt")).unwrap(), hello);

    std::fs::remove_file(local.join("hello.txt")).unwrap();
    std::fs::create_dir(local.join("hello.txt")).unwrap();
    let changed = alice
        .note_local("src", LocalEvent::Changed(p("/src/hello.txt")))
        .unwrap();
    let ProtocolMessage::FileAnnounce {
        checkout_id,
        path,
        new: as_dir,
        basis,
    } = changed.into_iter().next().expect("one announce")
    else {
        panic!("expected FileAnnounce");
    };
    assert_eq!(as_dir.kind, EntryKind::Dir);
    assert_eq!(basis, Some(file_node(&new)));

    match master_msg(
        master
            .handle(
                ALICE,
                ProtocolMessage::FileAnnounce {
                    checkout_id: checkout_id.clone(),
                    path: path.clone(),
                    new: as_dir.clone(),
                    basis,
                },
            )
            .unwrap(),
    ) {
        ProtocolMessage::CasAccept {
            file_node: node, ..
        } => assert_eq!(node, Some(file_node(&as_dir))),
        other => panic!("expected CasAccept, got {other:?}"),
    }
    assert!(sandbox.central_root().join("src/hello.txt").is_dir());
    assert!(!sandbox.central_root().join(RESERVED_CONFLICTS).exists());
    slave_msgs(
        alice
            .handle(ProtocolMessage::CasAccept {
                checkout_id,
                path: path.clone(),
                file_node: Some(file_node(&as_dir)),
            })
            .unwrap(),
    );

    let pushed = master.poll(BACKUP);
    assert!(!pushed.is_empty());
    for msg in &pushed {
        match msg {
            ProtocolMessage::FileAnnounce {
                path: announced,
                new: fanout,
                basis,
                ..
            } => {
                assert_eq!(announced, &path);
                assert_eq!(fanout, &as_dir);
                assert_eq!(*basis, Some(file_node(&new)));
            }
            other => panic!("expected FileAnnounce, got {other:?}"),
        }
    }
    assert_eq!(
        backup.meta("src", &path).unwrap().as_ref().map(file_node),
        Some(file_node(&new))
    );
    for msg in pushed {
        assert!(slave_msgs(backup.handle(msg).unwrap()).is_empty());
    }
    assert!(backup_src.join("hello.txt").is_dir());
    assert_eq!(
        backup.last_synced("src", &path).unwrap(),
        Some(file_node(&as_dir))
    );
    assert!(!conflict_sidecar_path(&backup_src, &path, &new.content_hash).exists());
}

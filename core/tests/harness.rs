use std::time::Duration;

use arborsync_core::config::{CheckoutConfig, SlaveAcl};
use arborsync_core::hash::{ContentHash, DirNode, FileNode};
use arborsync_core::meta::FileMetadata;
use arborsync_core::path::{RESERVED_CONFLICTS, RESERVED_TMP, is_reserved_root_entry};
use arborsync_core::protocol::ProtocolMessage;
use arborsync_core::storage::{CheckoutId, Storage, WriteBatch};
use arborsync_core::test_support::{MemoryStorage, SyncSandbox, TempTree, memory_link, p};

fn meta(content_byte: u8) -> FileMetadata {
    FileMetadata::file(1, 0, 0o100644, ContentHash::from_bytes([content_byte; 32]))
}

#[test]
fn temp_tree_writes_files_dirs_and_symlinks() {
    let tree = TempTree::new();
    let b = tree.builder();
    b.file("src/foo.rs", b"fn main() {}");
    b.mkdir("docs");
    b.symlink("src/link", "foo.rs");

    assert_eq!(
        std::fs::read(tree.path().join("src/foo.rs")).unwrap(),
        b"fn main() {}"
    );
    assert!(tree.path().join("docs").is_dir());
    assert_eq!(
        std::fs::read_link(tree.path().join("src/link"))
            .unwrap()
            .to_string_lossy(),
        "foo.rs"
    );
}

#[test]
fn sandbox_overlap_layout_has_independent_locals_and_reserved_dirs() {
    let box_ = SyncSandbox::new();
    let overlap = box_.overlap_backup_slave();
    assert_ne!(overlap.src, overlap.bak);
    assert!(overlap.src.join(RESERVED_TMP).is_dir());
    assert!(overlap.bak.join(RESERVED_CONFLICTS).is_dir());
    assert!(!overlap.bak.starts_with(&overlap.src));
    assert!(!overlap.src.starts_with(&overlap.bak));
}

#[test]
fn sandbox_writes_round_trippable_toml() {
    let box_ = SyncSandbox::new();
    let src = box_.add_checkout("backup-1", "src");
    let bak = box_.add_checkout("backup-1", "bak");
    box_.write_master_config(vec![SlaveAcl {
        id: "backup-1".into(),
        public_keys: vec!["hex:".to_string() + &"aa".repeat(32)],
        allowed_prefixes: vec!["/".into()],
    }]);
    let slave_cfg = box_.write_slave_config(
        "backup-1",
        vec![
            CheckoutConfig {
                id: "src".into(),
                central: "/src".into(),
                local: src.to_string_lossy().into_owned(),
            },
            CheckoutConfig {
                id: "bak".into(),
                central: "/".into(),
                local: bak.to_string_lossy().into_owned(),
            },
        ],
        vec!["hex:".to_string() + &"bb".repeat(32)],
    );
    assert!(box_.master_config_path().is_file());
    assert!(slave_cfg.is_file());
}

#[test]
fn memory_storage_commit_is_atomic_and_ranges_respect_interest() {
    let store = MemoryStorage::new();
    let master = CheckoutId::master();
    let src = CheckoutId::new("src");

    let mut batch = store.begin_write().unwrap();
    batch
        .put_meta(&master, &p("/src/foo.rs"), &meta(1))
        .unwrap();
    batch
        .put_meta(&master, &p("/src2/bar.rs"), &meta(2))
        .unwrap();
    batch.put_meta(&src, &p("/src/foo.rs"), &meta(3)).unwrap();
    drop(batch);
    assert!(
        store
            .get_meta(&master, &p("/src/foo.rs"))
            .unwrap()
            .is_none()
    );

    let mut batch = store.begin_write().unwrap();
    batch
        .put_meta(&master, &p("/src/foo.rs"), &meta(1))
        .unwrap();
    batch
        .put_meta(&master, &p("/src2/bar.rs"), &meta(2))
        .unwrap();
    batch
        .put_dir_node(&master, &p("/src"), DirNode::from_bytes([9; 32]))
        .unwrap();
    batch.commit().unwrap();

    let under_src = store.range_meta(&master, &p("/src")).unwrap();
    assert_eq!(under_src.len(), 1);
    assert_eq!(under_src[0].0, p("/src/foo.rs"));
    assert!(store.get_meta(&src, &p("/src/foo.rs")).unwrap().is_none());

    store.delete_checkout(&master).unwrap();
    assert!(store.range_meta(&master, &p("/")).unwrap().is_empty());
}

#[test]
fn memory_storage_purge_prefix_spares_the_sorting_sibling() {
    let store = MemoryStorage::new();
    let master = CheckoutId::master();

    let mut batch = store.begin_write().unwrap();
    batch
        .put_meta(&master, &p("/src/foo.rs"), &meta(1))
        .unwrap();
    batch
        .put_dir_node(&master, &p("/src"), DirNode::from_bytes([9; 32]))
        .unwrap();
    batch
        .put_last_synced(&master, &p("/src/foo.rs"), FileNode::from_bytes([8; 32]))
        .unwrap();
    batch
        .put_last_synced(&master, &p("/src2/bar.rs"), FileNode::from_bytes([6; 32]))
        .unwrap();
    batch.commit().unwrap();

    let mut batch = store.begin_write().unwrap();
    batch.purge_prefix(&master, &p("/src")).unwrap();
    batch.commit().unwrap();

    assert!(
        store
            .get_meta(&master, &p("/src/foo.rs"))
            .unwrap()
            .is_none()
    );
    assert!(store.get_dir_node(&master, &p("/src")).unwrap().is_none());
    assert!(
        store
            .get_last_synced(&master, &p("/src/foo.rs"))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.get_last_synced(&master, &p("/src2/bar.rs")).unwrap(),
        Some(FileNode::from_bytes([6; 32]))
    );
}

#[test]
fn memory_link_is_bidirectional() {
    let (master, slave) = memory_link();
    slave.send(ProtocolMessage::Disconnect {
        reason: "from-slave".into(),
    });
    match master.recv_timeout(Duration::from_secs(1)) {
        Some(ProtocolMessage::Disconnect { reason }) => assert_eq!(reason, "from-slave"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn reserved_root_entries_are_the_sidecar_dirs() {
    assert!(is_reserved_root_entry(RESERVED_TMP));
    assert!(is_reserved_root_entry(RESERVED_CONFLICTS));
}

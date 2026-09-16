use arborsync_core::hash::{ContentHash, DirNode, FileNode};
use arborsync_core::meta::FileMetadata;
use arborsync_core::storage::{CheckoutId, RedbStorage, Storage, WriteBatch};
use arborsync_core::test_support::p;

fn open_tmp() -> (tempfile::TempDir, RedbStorage) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = RedbStorage::open(&dir.path().join("index.redb")).expect("open");
    (dir, store)
}

fn meta(content_byte: u8) -> FileMetadata {
    FileMetadata::file(1, 0, 0o100644, ContentHash::from_bytes([content_byte; 32]))
}

#[test]
fn write_is_invisible_until_commit() {
    let (_dir, store) = open_tmp();
    let ck = CheckoutId::master();

    let mut batch = store.begin_write().unwrap();
    batch.put_meta(&ck, &p("/src/foo.rs"), &meta(1)).unwrap();
    drop(batch);
    assert!(store.get_meta(&ck, &p("/src/foo.rs")).unwrap().is_none());

    let mut batch = store.begin_write().unwrap();
    batch.put_meta(&ck, &p("/src/foo.rs"), &meta(1)).unwrap();
    batch
        .put_dir_node(&ck, &p("/src"), DirNode::from_bytes([9; 32]))
        .unwrap();
    batch
        .put_last_synced(&ck, &p("/src/foo.rs"), FileNode::from_bytes([8; 32]))
        .unwrap();
    batch.commit().unwrap();

    assert_eq!(
        store.get_meta(&ck, &p("/src/foo.rs")).unwrap(),
        Some(meta(1))
    );
    assert_eq!(
        store.get_dir_node(&ck, &p("/src")).unwrap(),
        Some(DirNode::from_bytes([9; 32]))
    );
    assert_eq!(
        store.get_last_synced(&ck, &p("/src/foo.rs")).unwrap(),
        Some(FileNode::from_bytes([8; 32]))
    );
}

#[test]
fn range_follows_interest_and_isolates_checkouts() {
    let (_dir, store) = open_tmp();
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
    batch
        .put_dir_node(&master, &p("/src"), DirNode::from_bytes([9; 32]))
        .unwrap();
    batch
        .put_dir_node(&master, &p("/src2"), DirNode::from_bytes([7; 32]))
        .unwrap();
    batch.commit().unwrap();

    let under_src = store.range_meta(&master, &p("/src")).unwrap();
    assert_eq!(under_src.len(), 1);
    assert_eq!(under_src[0].0, p("/src/foo.rs"));
    assert_eq!(
        under_src[0].1.content_hash,
        ContentHash::from_bytes([1; 32])
    );

    let dirs = store.range_dir_nodes(&master, &p("/src")).unwrap();
    assert_eq!(dirs, vec![(p("/src"), DirNode::from_bytes([9; 32]))]);

    assert_eq!(
        store
            .get_meta(&src, &p("/src/foo.rs"))
            .unwrap()
            .unwrap()
            .content_hash,
        ContentHash::from_bytes([3; 32])
    );
}

#[test]
fn range_and_prefix_delete_keep_descendants_past_sorting_siblings() {
    let (_dir, store) = open_tmp();
    let master = CheckoutId::master();
    let mut batch = store.begin_write().unwrap();
    batch.put_meta(&master, &p("/src.foo"), &meta(4)).unwrap();
    batch
        .put_meta(&master, &p("/src/foo.rs"), &meta(5))
        .unwrap();
    batch
        .put_dir_node(&master, &p("/src"), DirNode::from_bytes([6; 32]))
        .unwrap();
    batch
        .put_dir_node(&master, &p("/src.foo"), DirNode::from_bytes([7; 32]))
        .unwrap();
    batch.commit().unwrap();

    let under_src = store.range_meta(&master, &p("/src")).unwrap();
    assert_eq!(
        under_src
            .iter()
            .map(|(path, _)| path.as_str())
            .collect::<Vec<_>>(),
        ["/src/foo.rs"]
    );
    let dirs = store.range_dir_nodes(&master, &p("/src")).unwrap();
    assert_eq!(dirs, vec![(p("/src"), DirNode::from_bytes([6; 32]))]);

    let mut batch = store.begin_write().unwrap();
    batch.del_meta_prefix(&master, &p("/src")).unwrap();
    batch.del_dir_prefix(&master, &p("/src")).unwrap();
    batch.commit().unwrap();

    assert!(
        store
            .get_meta(&master, &p("/src/foo.rs"))
            .unwrap()
            .is_none()
    );
    assert!(store.get_dir_node(&master, &p("/src")).unwrap().is_none());
    assert_eq!(
        store.get_meta(&master, &p("/src.foo")).unwrap(),
        Some(meta(4))
    );
    assert_eq!(
        store.get_dir_node(&master, &p("/src.foo")).unwrap(),
        Some(DirNode::from_bytes([7; 32]))
    );
}

#[test]
fn prefix_delete_and_checkout_delete() {
    let (_dir, store) = open_tmp();
    let master = CheckoutId::master();
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
    batch
        .put_dir_node(&master, &p("/src2"), DirNode::from_bytes([7; 32]))
        .unwrap();
    batch
        .put_last_synced(&master, &p("/src/foo.rs"), FileNode::from_bytes([8; 32]))
        .unwrap();
    batch.commit().unwrap();

    let mut batch = store.begin_write().unwrap();
    batch.del_meta_prefix(&master, &p("/src")).unwrap();
    batch.del_dir_prefix(&master, &p("/src")).unwrap();
    batch.del_last_synced(&master, &p("/src/foo.rs")).unwrap();
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
    assert!(
        store
            .get_meta(&master, &p("/src2/bar.rs"))
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store.get_dir_node(&master, &p("/src2")).unwrap(),
        Some(DirNode::from_bytes([7; 32]))
    );

    store.delete_checkout(&master).unwrap();
    assert!(store.range_meta(&master, &p("/")).unwrap().is_empty());
    assert!(store.range_dir_nodes(&master, &p("/")).unwrap().is_empty());
}

#[test]
fn purge_prefix_clears_all_three_tables_and_spares_the_sibling() {
    let (_dir, store) = open_tmp();
    let master = CheckoutId::master();
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
    batch
        .put_dir_node(&master, &p("/src2"), DirNode::from_bytes([7; 32]))
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
        store.get_meta(&master, &p("/src2/bar.rs")).unwrap(),
        Some(meta(2))
    );
    assert_eq!(
        store.get_dir_node(&master, &p("/src2")).unwrap(),
        Some(DirNode::from_bytes([7; 32]))
    );
    assert_eq!(
        store.get_last_synced(&master, &p("/src2/bar.rs")).unwrap(),
        Some(FileNode::from_bytes([6; 32]))
    );
}

#[test]
fn del_entry_clears_one_path_from_all_three_tables() {
    let (_dir, store) = open_tmp();
    let master = CheckoutId::master();
    let mut batch = store.begin_write().unwrap();
    batch.put_meta(&master, &p("/src"), &meta(1)).unwrap();
    batch
        .put_dir_node(&master, &p("/src"), DirNode::from_bytes([9; 32]))
        .unwrap();
    batch
        .put_last_synced(&master, &p("/src"), FileNode::from_bytes([8; 32]))
        .unwrap();
    batch
        .put_meta(&master, &p("/src/foo.rs"), &meta(2))
        .unwrap();
    batch.commit().unwrap();

    let mut batch = store.begin_write().unwrap();
    batch.del_entry(&master, &p("/src")).unwrap();
    batch.commit().unwrap();

    assert!(store.get_meta(&master, &p("/src")).unwrap().is_none());
    assert!(store.get_dir_node(&master, &p("/src")).unwrap().is_none());
    assert!(
        store
            .get_last_synced(&master, &p("/src"))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.get_meta(&master, &p("/src/foo.rs")).unwrap(),
        Some(meta(2))
    );
}

#[test]
fn reopen_sees_committed_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("index.redb");
    let ck = CheckoutId::new("bak");
    let meta = FileMetadata::directory(12, 0o040755);

    {
        let store = RedbStorage::open(&path).unwrap();
        let mut batch = store.begin_write().unwrap();
        batch.put_meta(&ck, &p("/"), &meta).unwrap();
        batch
            .put_dir_node(&ck, &p("/"), DirNode::from_bytes([4; 32]))
            .unwrap();
        batch.commit().unwrap();
    }

    let store = RedbStorage::open(&path).unwrap();
    assert_eq!(store.get_meta(&ck, &p("/")).unwrap(), Some(meta));
    assert_eq!(
        store.get_dir_node(&ck, &p("/")).unwrap(),
        Some(DirNode::from_bytes([4; 32]))
    );
    assert!(
        store
            .get_meta(&CheckoutId::master(), &p("/"))
            .unwrap()
            .is_none()
    );
}

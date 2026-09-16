//! Spec §13: redb storage — same contract as the in-memory double.

use arborsync_core::meta::FileMetadata;
use arborsync_core::storage::{CheckoutId, RedbStorage, Storage, WriteBatch};

fn open_tmp() -> (tempfile::TempDir, RedbStorage) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = RedbStorage::open(&dir.path().join("index.redb")).expect("open");
    (dir, store)
}

#[test]
fn write_is_invisible_until_commit() {
    let (_dir, store) = open_tmp();
    let ck = CheckoutId::master();
    let meta = FileMetadata::file(1, 0, 0o100644, [1; 32]);

    let mut batch = store.begin_write().unwrap();
    batch.put_meta(&ck, "/src/foo.rs", &meta).unwrap();
    drop(batch);
    assert!(store.get_meta(&ck, "/src/foo.rs").unwrap().is_none());

    let mut batch = store.begin_write().unwrap();
    batch.put_meta(&ck, "/src/foo.rs", &meta).unwrap();
    batch.put_dir_node(&ck, "/src", [9; 32]).unwrap();
    batch.put_last_synced(&ck, "/src/foo.rs", [8; 32]).unwrap();
    batch.commit().unwrap();

    assert_eq!(store.get_meta(&ck, "/src/foo.rs").unwrap(), Some(meta));
    assert_eq!(store.get_dir_node(&ck, "/src").unwrap(), Some([9; 32]));
    assert_eq!(
        store.get_last_synced(&ck, "/src/foo.rs").unwrap(),
        Some([8; 32])
    );
}

#[test]
fn range_follows_interest_and_isolates_checkouts() {
    let (_dir, store) = open_tmp();
    let master = CheckoutId::master();
    let src = CheckoutId::new("src");
    let mut batch = store.begin_write().unwrap();
    batch
        .put_meta(
            &master,
            "/src/foo.rs",
            &FileMetadata::file(1, 0, 0o100644, [1; 32]),
        )
        .unwrap();
    batch
        .put_meta(
            &master,
            "/src2/bar.rs",
            &FileMetadata::file(1, 0, 0o100644, [2; 32]),
        )
        .unwrap();
    batch
        .put_meta(
            &src,
            "/src/foo.rs",
            &FileMetadata::file(1, 0, 0o100644, [3; 32]),
        )
        .unwrap();
    batch.put_dir_node(&master, "/src", [9; 32]).unwrap();
    batch.put_dir_node(&master, "/src2", [7; 32]).unwrap();
    batch.commit().unwrap();

    let under_src = store.range_meta(&master, "/src").unwrap();
    assert_eq!(under_src.len(), 1);
    assert_eq!(under_src[0].0, "/src/foo.rs");
    assert_eq!(under_src[0].1.content_hash, [1; 32]);

    let dirs = store.range_dir_nodes(&master, "/src").unwrap();
    assert_eq!(dirs.len(), 1);
    assert_eq!(dirs[0], ("/src".into(), [9; 32]));

    assert_eq!(
        store
            .get_meta(&src, "/src/foo.rs")
            .unwrap()
            .unwrap()
            .content_hash,
        [3; 32]
    );
}

#[test]
fn prefix_delete_and_checkout_delete() {
    let (_dir, store) = open_tmp();
    let master = CheckoutId::master();
    let mut batch = store.begin_write().unwrap();
    batch
        .put_meta(
            &master,
            "/src/foo.rs",
            &FileMetadata::file(1, 0, 0o100644, [1; 32]),
        )
        .unwrap();
    batch
        .put_meta(
            &master,
            "/src2/bar.rs",
            &FileMetadata::file(1, 0, 0o100644, [2; 32]),
        )
        .unwrap();
    batch.put_dir_node(&master, "/src", [9; 32]).unwrap();
    batch.put_dir_node(&master, "/src2", [7; 32]).unwrap();
    batch
        .put_last_synced(&master, "/src/foo.rs", [8; 32])
        .unwrap();
    batch.commit().unwrap();

    let mut batch = store.begin_write().unwrap();
    batch.del_meta_prefix(&master, "/src").unwrap();
    batch.del_dir_prefix(&master, "/src").unwrap();
    batch.del_last_synced(&master, "/src/foo.rs").unwrap();
    batch.commit().unwrap();

    assert!(store.get_meta(&master, "/src/foo.rs").unwrap().is_none());
    assert!(store.get_dir_node(&master, "/src").unwrap().is_none());
    assert!(
        store
            .get_last_synced(&master, "/src/foo.rs")
            .unwrap()
            .is_none()
    );
    assert!(store.get_meta(&master, "/src2/bar.rs").unwrap().is_some());
    assert_eq!(store.get_dir_node(&master, "/src2").unwrap(), Some([7; 32]));

    store.delete_checkout(&master).unwrap();
    assert!(store.range_meta(&master, "/").unwrap().is_empty());
    assert!(store.range_dir_nodes(&master, "/").unwrap().is_empty());
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
        batch.put_meta(&ck, "/", &meta).unwrap();
        batch.put_dir_node(&ck, "/", [4; 32]).unwrap();
        batch.commit().unwrap();
    }

    let store = RedbStorage::open(&path).unwrap();
    assert_eq!(store.get_meta(&ck, "/").unwrap(), Some(meta));
    assert_eq!(store.get_dir_node(&ck, "/").unwrap(), Some([4; 32]));
    assert!(
        store
            .get_meta(&CheckoutId::master(), "/")
            .unwrap()
            .is_none()
    );
}

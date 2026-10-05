use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use arborsync_core::hash::ContentHash;
use arborsync_core::meta::{
    EntryKind, collect_for_rescan, collect_from_path, hash_bytes, hash_file,
};
use arborsync_core::test_support::TempTree;

#[test]
fn hash_file_does_not_follow_a_symlink() {
    let tree = TempTree::new();
    let target = tree.builder().file("target.bin", b"secret-bytes");
    let link = tree.path().join("alias");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    match hash_file(&link) {
        Ok(hash) => panic!(
            "followed the symlink and hashed the target ({hash:?} == {:?})",
            hash_bytes(b"secret-bytes")
        ),
        Err(err) => assert_ne!(
            err.kind(),
            std::io::ErrorKind::NotFound,
            "the symlink path exists"
        ),
    }
    let row = collect_from_path(&link).unwrap().unwrap();
    let target_bytes = std::fs::read_link(&link).unwrap();
    assert_eq!(row.kind, EntryKind::Symlink);
    assert_eq!(
        row.content_hash,
        hash_bytes(target_bytes.as_os_str().as_bytes())
    );
}

#[test]
fn collect_skips_fifo_and_unreadable_file() {
    let tree = TempTree::new();
    let fifo = tree.path().join("pipe");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(collect_from_path(&fifo).unwrap(), None);
    assert_eq!(collect_for_rescan(&fifo, None).unwrap(), None);

    let gated = tree.builder().mkdir("gated");
    let nested = gated.join("file");
    std::fs::write(&nested, b"hidden").unwrap();
    std::fs::set_permissions(&gated, std::fs::Permissions::from_mode(0o000)).unwrap();
    assert_eq!(collect_from_path(&nested).unwrap(), None);
    assert_eq!(collect_for_rescan(&nested, None).unwrap(), None);
    std::fs::set_permissions(&gated, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn collect_for_rescan_reuses_hash_when_only_mode_changes() {
    let tree = TempTree::new();
    let path = tree.builder().file("note.txt", b"same");
    let first = collect_from_path(&path).unwrap().unwrap();
    tree.builder().set_mode("note.txt", first.mode | 0o111);
    tree.builder().set_mtime_ns("note.txt", first.mtime_ns);
    let second = collect_for_rescan(&path, Some(&first)).unwrap().unwrap();
    assert_eq!(second.content_hash, first.content_hash);
    assert_eq!(second.size, first.size);
    assert_eq!(second.mtime_ns, first.mtime_ns);
    assert_ne!(second.mode, first.mode);
}

#[test]
fn collect_for_rescan_reuses_hash_when_bytes_change_but_size_and_mtime_match() {
    let tree = TempTree::new();
    let path = tree.builder().file("note.txt", b"same");
    let first = collect_from_path(&path).unwrap().unwrap();
    std::fs::write(&path, b"diff").unwrap();
    tree.builder().set_mtime_ns("note.txt", first.mtime_ns);
    let second = collect_for_rescan(&path, Some(&first)).unwrap().unwrap();
    assert_eq!(second.content_hash, first.content_hash);
    assert_ne!(
        second.content_hash,
        collect_from_path(&path).unwrap().unwrap().content_hash
    );
}

#[test]
fn collect_for_rescan_hashes_when_kind_changes_at_the_same_size_and_mtime() {
    let tree = TempTree::new();
    let path = tree.builder().file("empty", b"");
    let first = collect_from_path(&path).unwrap().unwrap();
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    tree.builder().set_mtime_ns("empty", first.mtime_ns);
    let second = collect_for_rescan(&path, Some(&first)).unwrap().unwrap();
    assert_eq!(second.kind, EntryKind::Dir);
    assert_eq!(second.content_hash, ContentHash::ZERO);
    assert_ne!(second.content_hash, first.content_hash);
}

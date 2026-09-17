use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use arborsync_core::meta::{collect_for_rescan, collect_from_path};
use arborsync_core::test_support::TempTree;

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

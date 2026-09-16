//! Spec §5: FileNode / DirNode encodings.

use arborsync_core::merkle::{DirChild, dir_node, empty_dir_node, file_node};
use arborsync_core::meta::{EntryKind, FileMetadata};
use arborsync_core::test_support::{expected_dir_node, expected_file_node};

fn sample_file() -> FileMetadata {
    FileMetadata::file(
        11,
        1_700_000_000_000_000_000,
        0o100644,
        *blake3::hash(b"hello world").as_bytes(),
    )
}

#[test]
fn empty_directory_is_blake3_of_empty_bytes() {
    assert_eq!(empty_dir_node(), *blake3::hash(b"").as_bytes());
    assert_eq!(empty_dir_node(), dir_node(&[]));
}

#[test]
fn file_node_matches_spec_byte_layout() {
    let meta = sample_file();
    assert_eq!(file_node(&meta), expected_file_node(&meta));
}

#[test]
fn file_node_does_not_hash_the_path() {
    let meta = sample_file();
    let a = file_node(&meta);
    let b = file_node(&meta);
    assert_eq!(a, b);
}

#[test]
fn file_node_changes_when_mtime_or_mode_changes() {
    let mut meta = sample_file();
    let base = file_node(&meta);
    meta.mtime_ns += 1;
    assert_ne!(file_node(&meta), base);
    meta.mtime_ns -= 1;
    meta.mode = 0o100755;
    assert_ne!(file_node(&meta), base);
}

#[test]
fn file_node_allows_negative_mtime() {
    let meta = FileMetadata::file(0, -1, 0o100644, [7; 32]);
    assert_eq!(file_node(&meta), expected_file_node(&meta));
}

#[test]
fn dir_node_sorts_children_as_raw_utf8() {
    let h1 = [1u8; 32];
    let h2 = [2u8; 32];
    let unsorted = [
        DirChild {
            kind: EntryKind::File as u8,
            name: "ζed".into(),
            node_hash: h2,
        },
        DirChild {
            kind: EntryKind::File as u8,
            name: "alpha".into(),
            node_hash: h1,
        },
    ];
    let expected = expected_dir_node(vec![
        (EntryKind::File as u8, "ζed".into(), h2),
        (EntryKind::File as u8, "alpha".into(), h1),
    ]);
    assert_eq!(dir_node(&unsorted), expected);
}

#[test]
fn dir_metadata_content_hash_is_not_the_dir_node() {
    let meta = FileMetadata::directory(0, 0o040755);
    assert_eq!(meta.content_hash, [0u8; 32]);
    assert_ne!(empty_dir_node(), [0u8; 32]);
}

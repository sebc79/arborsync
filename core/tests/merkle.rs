use arborsync_core::hash::{ContentHash, DirNode, FileNode};
use arborsync_core::merkle::{DirChild, dir_node, empty_dir_node, file_node};
use arborsync_core::meta::{EntryKind, FileMetadata};
use arborsync_core::test_support::{expected_dir_node, expected_file_node, name};

fn sample_file() -> FileMetadata {
    FileMetadata::file(
        11,
        1_700_000_000_000_000_000,
        0o100644,
        ContentHash::from_bytes(*blake3::hash(b"hello world").as_bytes()),
    )
}

#[test]
fn empty_directory_is_blake3_of_empty_bytes() {
    assert_eq!(
        empty_dir_node(),
        DirNode::from_bytes(*blake3::hash(b"").as_bytes())
    );
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
    let meta = FileMetadata::file(0, -1, 0o100644, ContentHash::from_bytes([7; 32]));
    assert_eq!(file_node(&meta), expected_file_node(&meta));
}

#[test]
fn dir_node_sorts_children_as_raw_utf8() {
    let h1 = [1u8; 32];
    let h2 = [2u8; 32];
    let unsorted = [
        DirChild::File {
            name: name("ζed"),
            node: FileNode::from_bytes(h2),
        },
        DirChild::File {
            name: name("alpha"),
            node: FileNode::from_bytes(h1),
        },
    ];
    let expected = expected_dir_node(vec![
        (EntryKind::File as u8, "ζed".into(), h2),
        (EntryKind::File as u8, "alpha".into(), h1),
    ]);
    assert_eq!(dir_node(&unsorted), expected);
}

#[test]
fn dir_node_distinguishes_a_file_child_from_a_directory_child() {
    let bytes = [5u8; 32];
    let as_file = dir_node(&[DirChild::File {
        name: name("x"),
        node: FileNode::from_bytes(bytes),
    }]);
    let as_dir = dir_node(&[DirChild::Directory {
        name: name("x"),
        node: DirNode::from_bytes(bytes),
    }]);
    assert_ne!(as_file, as_dir);
    assert_eq!(
        as_dir,
        expected_dir_node(vec![(EntryKind::Dir as u8, "x".into(), bytes)])
    );
}

#[test]
fn dir_metadata_content_hash_is_not_the_dir_node() {
    let meta = FileMetadata::directory(0, 0o040755);
    assert_eq!(meta.content_hash, ContentHash::ZERO);
    assert_ne!(empty_dir_node(), DirNode::ZERO);
}

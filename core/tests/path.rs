//! Spec §1–§3, §6: interest, reserved names, canonical paths, overlap.

use std::path::Path;

use arborsync_core::path::{
    RESERVED_CONFLICTS, RESERVED_TMP, conflict_sidecar_path, host_to_canonical, is_interested,
    is_reserved_root_entry, join_central, local_paths_overlap, normalize_canonical,
};

#[test]
fn root_central_matches_every_canonical_path() {
    assert!(is_interested("/", "/"));
    assert!(is_interested("/", "/src"));
    assert!(is_interested("/", "/src/foo.rs"));
}

#[test]
fn prefix_matches_self_and_descendants_not_siblings() {
    assert!(is_interested("/src", "/src"));
    assert!(is_interested("/src", "/src/foo.rs"));
    assert!(is_interested("/src", "/src/project1/file.txt"));
    assert!(!is_interested("/src", "/src2"));
    assert!(!is_interested("/src", "/docs"));
    assert!(!is_interested("/src", "/"));
}

#[test]
fn child_mapping_does_not_receive_parent_files() {
    assert!(!is_interested(
        "/src/project1/subdir",
        "/src/project1/file.txt"
    ));
}

#[test]
fn reserved_names_only_the_two_sidecar_dirs() {
    assert!(is_reserved_root_entry(RESERVED_TMP));
    assert!(is_reserved_root_entry(RESERVED_CONFLICTS));
    assert!(!is_reserved_root_entry(".arborsync-tmp-backup"));
    assert!(!is_reserved_root_entry("arborsync-tmp"));
    assert!(!is_reserved_root_entry(".git"));
}

#[test]
fn host_path_under_central_root_strips_the_root() {
    let canonical =
        host_to_canonical(Path::new("/central"), Path::new("/central/src/foo.rs")).unwrap();
    assert_eq!(canonical, "/src/foo.rs");
}

#[test]
fn host_path_equal_to_root_is_hierarchy_root() {
    let canonical = host_to_canonical(Path::new("/central"), Path::new("/central")).unwrap();
    assert_eq!(canonical, "/");
}

#[test]
fn slave_join_central_builds_logical_path() {
    assert_eq!(join_central("/src", "foo.rs").unwrap(), "/src/foo.rs");
    assert_eq!(join_central("/", "src/foo.rs").unwrap(), "/src/foo.rs");
    assert_eq!(join_central("/src", "").unwrap(), "/src");
}

#[test]
fn normalize_rejects_dot_and_relative() {
    assert!(normalize_canonical("src/foo").is_err());
    assert!(normalize_canonical("/src/../etc").is_err());
    assert!(normalize_canonical("/src/./foo").is_err());
    assert_eq!(normalize_canonical("/src/foo").unwrap(), "/src/foo");
}

#[test]
fn local_overlap_is_parent_or_equal_not_string_prefix() {
    assert!(local_paths_overlap(
        Path::new("/opt/a"),
        Path::new("/opt/a")
    ));
    assert!(local_paths_overlap(
        Path::new("/opt/a"),
        Path::new("/opt/a/b")
    ));
    assert!(local_paths_overlap(
        Path::new("/opt/a/b"),
        Path::new("/opt/a")
    ));
    assert!(!local_paths_overlap(
        Path::new("/opt/a"),
        Path::new("/opt/ab")
    ));
}

#[test]
fn sidecar_uses_reserved_dir_and_hash_prefix() {
    let hash = [0xab; 32];
    let path = conflict_sidecar_path(Path::new("/opt/src"), "/src/foo.rs", &hash);
    let text = path.to_string_lossy();
    assert!(text.contains("/.arborsync-conflicts/"));
    assert!(text.contains("src/foo.rs--abababababababab"));
    assert!(!text.contains("/opt/src/foo.rs--"));
}

use std::path::Path;

use arborsync_core::hash::ContentHash;
use arborsync_core::path::{
    CanonicalPath, EntryName, PathError, RESERVED_CONFLICTS, RESERVED_TMP, canonical_to_host,
    conflict_sidecar_path, host_to_canonical, is_interested, is_reserved_root_entry, join_central,
    local_paths_overlap,
};
use arborsync_core::test_support::p;

#[test]
fn root_central_matches_every_canonical_path() {
    assert!(is_interested(&p("/"), &p("/")));
    assert!(is_interested(&p("/"), &p("/src")));
    assert!(is_interested(&p("/"), &p("/src/foo.rs")));
}

#[test]
fn prefix_matches_self_and_descendants_not_siblings() {
    assert!(is_interested(&p("/src"), &p("/src")));
    assert!(is_interested(&p("/src"), &p("/src/foo.rs")));
    assert!(is_interested(&p("/src"), &p("/src/project1/file.txt")));
    assert!(!is_interested(&p("/src"), &p("/src2")));
    assert!(!is_interested(&p("/src"), &p("/docs")));
    assert!(!is_interested(&p("/src"), &p("/")));
}

#[test]
fn child_mapping_does_not_receive_parent_files() {
    assert!(!is_interested(
        &p("/src/project1/subdir"),
        &p("/src/project1/file.txt")
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
    assert_eq!(canonical.as_str(), "/src/foo.rs");
}

#[test]
fn host_path_equal_to_root_is_hierarchy_root() {
    let canonical = host_to_canonical(Path::new("/central"), Path::new("/central")).unwrap();
    assert_eq!(canonical.as_str(), "/");
}

#[test]
fn slave_join_central_builds_logical_path() {
    assert_eq!(
        join_central(&p("/src"), "foo.rs").unwrap().as_str(),
        "/src/foo.rs"
    );
    assert_eq!(
        join_central(&p("/"), "src/foo.rs").unwrap().as_str(),
        "/src/foo.rs"
    );
    assert_eq!(join_central(&p("/src"), "").unwrap().as_str(), "/src");
}

#[test]
fn parse_rejects_dot_and_relative() {
    assert!(matches!(
        CanonicalPath::parse("src/foo"),
        Err(PathError::NotAbsolute(_))
    ));
    assert!(matches!(
        CanonicalPath::parse("/src/../etc"),
        Err(PathError::DotComponent(_))
    ));
    assert!(matches!(
        CanonicalPath::parse("/src/./foo"),
        Err(PathError::DotComponent(_))
    ));
    assert_eq!(
        CanonicalPath::parse("/src/foo").unwrap().as_str(),
        "/src/foo"
    );
    assert_eq!(CanonicalPath::parse("/").unwrap().as_str(), "/");
}

#[test]
fn parse_rejects_a_non_canonical_spelling_instead_of_rewriting_it() {
    assert_eq!(
        CanonicalPath::parse("//src").unwrap_err(),
        PathError::NonCanonical {
            value: "//src".into(),
            normalized: "/src".into(),
        }
    );
    assert_eq!(
        CanonicalPath::parse("/src/").unwrap_err(),
        PathError::NonCanonical {
            value: "/src/".into(),
            normalized: "/src".into(),
        }
    );
}

#[test]
fn entry_name_is_one_component() {
    assert_eq!(EntryName::parse("foo.rs").unwrap().as_str(), "foo.rs");
    assert_eq!(EntryName::parse("ζed").unwrap().as_str(), "ζed");
    for bad in ["", ".", "..", "a/b", "/"] {
        assert_eq!(
            EntryName::parse(bad).unwrap_err(),
            PathError::BadEntryName(bad.into()),
            "{bad}"
        );
    }
}

#[test]
fn host_path_outside_root_is_error() {
    assert_eq!(
        host_to_canonical(Path::new("/central"), Path::new("/other/foo")).unwrap_err(),
        PathError::EscapesRoot
    );
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
fn parent_stops_at_the_hierarchy_root() {
    assert_eq!(p("/src/a/b").parent(), Some(p("/src/a")));
    assert_eq!(p("/src").parent(), Some(p("/")));
    assert_eq!(p("/").parent(), None);
}

#[test]
fn ancestors_walk_root_ward_and_exclude_self() {
    assert_eq!(
        p("/src/a/b").ancestors().collect::<Vec<_>>(),
        vec![p("/src/a"), p("/src"), p("/")]
    );
    assert_eq!(p("/src").ancestors().collect::<Vec<_>>(), vec![p("/")]);
    assert_eq!(p("/").ancestors().collect::<Vec<_>>(), Vec::new());
}

#[test]
fn canonical_to_host_rejoins_the_central_root() {
    let root = Path::new("/central");
    assert_eq!(
        canonical_to_host(root, &p("/src/foo.rs")),
        Path::new("/central/src/foo.rs")
    );
    assert_eq!(canonical_to_host(root, &p("/")), Path::new("/central"));
}

#[test]
fn canonical_to_host_round_trips_host_to_canonical() {
    let root = Path::new("/central");
    let host = canonical_to_host(root, &p("/src/foo.rs"));
    assert_eq!(host_to_canonical(root, &host).unwrap(), p("/src/foo.rs"));
}

#[test]
fn sidecar_uses_reserved_dir_and_hash_prefix() {
    let hash = ContentHash::from_bytes([0xab; 32]);
    let path = conflict_sidecar_path(Path::new("/opt/src"), &p("/src/foo.rs"), &hash);
    let text = path.to_string_lossy();
    assert!(text.contains("/.arborsync-conflicts/"));
    assert!(text.contains("src/foo.rs--abababababababab"));
    assert!(!text.contains("/opt/src/foo.rs--"));
}

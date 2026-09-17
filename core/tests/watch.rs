use std::path::{Path, PathBuf};

use arborsync_core::master::LocalEvent;
use arborsync_core::test_support::p;
use arborsync_core::watch::{WatchEvent, WatchKind, to_local_events};

fn canon(host: &Path) -> Option<arborsync_core::CanonicalPath> {
    match host.to_str()? {
        "/src/old.txt" => Some(p("/src/old.txt")),
        "/src/new.txt" => Some(p("/src/new.txt")),
        "/src/file.txt" => Some(p("/src/file.txt")),
        _ => None,
    }
}

#[test]
fn both_path_rename_is_one_renamed() {
    let events = [WatchEvent {
        path: PathBuf::from("/src/old.txt"),
        kind: WatchKind::Rename {
            to: Some(PathBuf::from("/src/new.txt")),
        },
    }];
    assert_eq!(
        to_local_events(events, canon),
        vec![LocalEvent::Renamed {
            from: p("/src/old.txt"),
            to: p("/src/new.txt"),
        }]
    );
}

#[test]
fn from_only_rename_is_removed() {
    let events = [WatchEvent {
        path: PathBuf::from("/src/old.txt"),
        kind: WatchKind::Rename { to: None },
    }];
    assert_eq!(
        to_local_events(events, canon),
        vec![LocalEvent::Removed(p("/src/old.txt"))]
    );
}

#[test]
fn to_only_rename_is_changed() {
    let events = [WatchEvent {
        path: PathBuf::from("/outside/old.txt"),
        kind: WatchKind::Rename {
            to: Some(PathBuf::from("/src/new.txt")),
        },
    }];
    assert_eq!(
        to_local_events(events, canon),
        vec![LocalEvent::Changed(p("/src/new.txt"))]
    );
}

#[test]
fn metadata_stays_metadata() {
    let events = [WatchEvent {
        path: PathBuf::from("/src/file.txt"),
        kind: WatchKind::Metadata,
    }];
    assert_eq!(
        to_local_events(events, canon),
        vec![LocalEvent::Metadata(p("/src/file.txt"))]
    );
}

#[test]
fn create_and_write_are_changed() {
    let events = [
        WatchEvent {
            path: PathBuf::from("/src/file.txt"),
            kind: WatchKind::Create,
        },
        WatchEvent {
            path: PathBuf::from("/src/file.txt"),
            kind: WatchKind::Write,
        },
    ];
    assert_eq!(
        to_local_events(events, canon),
        vec![
            LocalEvent::Changed(p("/src/file.txt")),
            LocalEvent::Changed(p("/src/file.txt")),
        ]
    );
}

#[test]
fn outside_map_is_dropped() {
    let events = [WatchEvent {
        path: PathBuf::from("/other/file.txt"),
        kind: WatchKind::Write,
    }];
    assert!(to_local_events(events, canon).is_empty());
}

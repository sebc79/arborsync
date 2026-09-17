use std::path::{Path, PathBuf};

use crate::path::CanonicalPath;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalEvent {
    Changed(CanonicalPath),
    Metadata(CanonicalPath),
    Removed(CanonicalPath),
    Renamed {
        from: CanonicalPath,
        to: CanonicalPath,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchKind {
    Create,
    Write,
    Metadata,
    Remove,
    Rename { to: Option<PathBuf> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchEvent {
    pub path: PathBuf,
    pub kind: WatchKind,
}

pub fn to_local_events(
    events: impl IntoIterator<Item = WatchEvent>,
    mut to_canonical: impl FnMut(&Path) -> Option<CanonicalPath>,
) -> Vec<LocalEvent> {
    let mut out = Vec::new();
    for event in events {
        match event.kind {
            WatchKind::Create | WatchKind::Write => {
                if let Some(path) = to_canonical(&event.path) {
                    out.push(LocalEvent::Changed(path));
                }
            }
            WatchKind::Metadata => {
                if let Some(path) = to_canonical(&event.path) {
                    out.push(LocalEvent::Metadata(path));
                }
            }
            WatchKind::Remove => {
                if let Some(path) = to_canonical(&event.path) {
                    out.push(LocalEvent::Removed(path));
                }
            }
            WatchKind::Rename { to: Some(to) } => {
                let from = to_canonical(&event.path);
                let to = to_canonical(&to);
                match (from, to) {
                    (Some(from), Some(to)) => out.push(LocalEvent::Renamed { from, to }),
                    (Some(from), None) => out.push(LocalEvent::Removed(from)),
                    (None, Some(to)) => out.push(LocalEvent::Changed(to)),
                    (None, None) => {}
                }
            }
            WatchKind::Rename { to: None } => {
                if let Some(from) = to_canonical(&event.path) {
                    out.push(LocalEvent::Removed(from));
                }
            }
        }
    }
    out
}

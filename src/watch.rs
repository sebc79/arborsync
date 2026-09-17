use arborsync_core::watch::{WatchEvent, WatchKind};
use notify::event::{ModifyKind, RenameMode};
use notify::{Event, EventKind};

pub fn from_notify(event: &Event) -> Option<WatchEvent> {
    if event.need_rescan() {
        return None;
    }
    let path = event.paths.first()?.clone();
    let kind = match event.kind {
        EventKind::Create(_) => WatchKind::Create,
        EventKind::Modify(ModifyKind::Data(_)) => WatchKind::Write,
        EventKind::Modify(ModifyKind::Metadata(_)) => WatchKind::Metadata,
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => WatchKind::Rename {
            to: event.paths.get(1).cloned(),
        },
        EventKind::Modify(ModifyKind::Name(RenameMode::From)) => WatchKind::Rename { to: None },
        EventKind::Modify(ModifyKind::Name(RenameMode::To)) => WatchKind::Create,
        EventKind::Modify(ModifyKind::Name(RenameMode::Any | RenameMode::Other)) => {
            if event.paths.len() >= 2 {
                WatchKind::Rename {
                    to: event.paths.get(1).cloned(),
                }
            } else {
                WatchKind::Write
            }
        }
        EventKind::Remove(_) => WatchKind::Remove,
        EventKind::Modify(ModifyKind::Any | ModifyKind::Other) | EventKind::Any => WatchKind::Write,
        EventKind::Access(_) | EventKind::Other => return None,
    };
    Some(WatchEvent { path, kind })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use notify::event::{CreateKind, DataChange, MetadataKind, ModifyKind, RemoveKind, RenameMode};
    use notify::{Event, EventKind};

    use super::from_notify;
    use arborsync_core::watch::WatchKind;

    fn event(kind: EventKind, paths: &[&str]) -> Event {
        let mut event = Event::new(kind);
        for path in paths {
            event = event.add_path(PathBuf::from(path));
        }
        event
    }

    #[test]
    fn maps_notify_kinds() {
        let create = from_notify(&event(EventKind::Create(CreateKind::File), &["/a"])).unwrap();
        assert_eq!(create.path, PathBuf::from("/a"));
        assert_eq!(create.kind, WatchKind::Create);

        let write = from_notify(&event(
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            &["/w"],
        ))
        .unwrap();
        assert_eq!(write.kind, WatchKind::Write);

        let meta = from_notify(&event(
            EventKind::Modify(ModifyKind::Metadata(MetadataKind::Permissions)),
            &["/m"],
        ))
        .unwrap();
        assert_eq!(meta.kind, WatchKind::Metadata);

        let both = from_notify(&event(
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            &["/old", "/new"],
        ))
        .unwrap();
        assert_eq!(both.path, PathBuf::from("/old"));
        assert_eq!(
            both.kind,
            WatchKind::Rename {
                to: Some(PathBuf::from("/new"))
            }
        );

        let from_only = from_notify(&event(
            EventKind::Modify(ModifyKind::Name(RenameMode::From)),
            &["/old"],
        ))
        .unwrap();
        assert_eq!(from_only.kind, WatchKind::Rename { to: None });

        let to_only = from_notify(&event(
            EventKind::Modify(ModifyKind::Name(RenameMode::To)),
            &["/new"],
        ))
        .unwrap();
        assert_eq!(to_only.kind, WatchKind::Create);

        let any_pair = from_notify(&event(
            EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
            &["/old", "/new"],
        ))
        .unwrap();
        assert_eq!(
            any_pair.kind,
            WatchKind::Rename {
                to: Some(PathBuf::from("/new"))
            }
        );

        let any_one = from_notify(&event(
            EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
            &["/x"],
        ))
        .unwrap();
        assert_eq!(any_one.kind, WatchKind::Write);

        let remove = from_notify(&event(EventKind::Remove(RemoveKind::File), &["/gone"])).unwrap();
        assert_eq!(remove.kind, WatchKind::Remove);

        let any = from_notify(&event(EventKind::Any, &["/q"])).unwrap();
        assert_eq!(any.kind, WatchKind::Write);

        assert!(
            from_notify(&event(
                EventKind::Access(notify::event::AccessKind::Any),
                &["/a"]
            ))
            .is_none()
        );
        assert!(from_notify(&event(EventKind::Other, &["/a"])).is_none());
    }
}

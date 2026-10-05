use arborsync_core::watch::{WatchEvent, WatchKind};
use notify::event::{ModifyKind, RenameMode};
use notify::{Event, EventKind};

pub fn classify(
    events: impl IntoIterator<Item = impl std::ops::Deref<Target = Event>>,
) -> (bool, Vec<WatchEvent>) {
    let mut need_rescan = false;
    let mut mapped = Vec::new();
    for event in events {
        let event = &*event;
        if event.need_rescan() {
            need_rescan = true;
            continue;
        }
        if let Some(watch) = from_notify(event) {
            mapped.push(watch);
        }
    }
    (need_rescan, mapped)
}

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
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use notify::event::{CreateKind, DataChange, MetadataKind, ModifyKind, RemoveKind, RenameMode};
    use notify::{Event, EventKind, RecursiveMode};
    use notify_debouncer_full::new_debouncer;

    use super::{classify, from_notify};
    use arborsync_core::config::{CheckoutConfig, SlaveConfig};
    use arborsync_core::master::MemoryContent;
    use arborsync_core::meta::hash_bytes;
    use arborsync_core::path::{CanonicalPath, local_to_canonical};
    use arborsync_core::protocol::ProtocolMessage;
    use arborsync_core::slave::Slave;
    use arborsync_core::test_support::{MemoryStorage, SyncSandbox};
    use arborsync_core::tune::TuneSpec;
    use arborsync_core::watch::{WatchKind, to_local_events};
    use arborsync_core::{LoadedSlave, format_hex_key};

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

    #[test]
    fn notify_thread_announces_file_written_after_arm() {
        let sandbox = SyncSandbox::new();
        let local = sandbox.add_checkout("dev-alice", "src");
        let root = sandbox.slave_root("dev-alice");
        fs::create_dir_all(&root).expect("slave root");
        let cfg = SlaveConfig {
            slave_id: "dev-alice".into(),
            master_addr: "127.0.0.1:8443".into(),
            slave_key_path: root.join("slave.key").to_string_lossy().into_owned(),
            master_public_keys: vec![format_hex_key(&[0x11; 32])],
            db_path: sandbox.slave_db("dev-alice").to_string_lossy().into_owned(),
            log_level: "debug".into(),
            max_checkouts_per_slave: 100,
            watcher_debounce_ms: 200,
            rescan_interval_seconds: 3600,
            status_interval_seconds: 0,
            peer_socket: None,
            peer_socket_mode: None,
            checkouts: vec![CheckoutConfig {
                id: "src".into(),
                central: "/src".into(),
                local: local.to_string_lossy().into_owned(),
            }],
            tune: TuneSpec::default(),
        };
        let cfg_path = root.join("slave.toml");
        fs::write(&cfg_path, cfg.to_toml().expect("toml")).expect("write slave.toml");
        fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o600))
            .expect("chmod slave.toml");

        let loaded = LoadedSlave::load(&cfg_path).expect("load slave.toml");
        let mut slave =
            Slave::open(loaded, MemoryStorage::new(), MemoryContent::new()).expect("open slave");
        let checkout = slave
            .checkout_local("src")
            .expect("src checkout")
            .to_path_buf();
        let central = CanonicalPath::parse("/src").expect("central");

        let (tx, rx) = mpsc::channel();
        let mut debouncer = new_debouncer(Duration::from_millis(200), None, tx).expect("debouncer");
        debouncer
            .watch(&checkout, RecursiveMode::Recursive)
            .expect("watch armed");

        let live = checkout.join("hello.txt");
        fs::write(&live, b"notify-hello").expect("write after arm");

        let deadline = Instant::now() + Duration::from_secs(4);
        let mut announced = None;
        while Instant::now() < deadline {
            match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(Ok(events)) => {
                    let (_need_rescan, mapped) = classify(events);
                    let locals = to_local_events(mapped, |host| {
                        local_to_canonical(&checkout, &central, host).ok()
                    });
                    for event in locals {
                        for msg in slave
                            .note_local("src", event)
                            .expect("note_local after notify")
                        {
                            if let ProtocolMessage::FileAnnounce { path, new, .. } = msg {
                                if path.as_str() == "/src/hello.txt" {
                                    announced = Some(new.content_hash);
                                }
                            }
                        }
                    }
                    if announced.is_some() {
                        break;
                    }
                }
                Ok(Err(errs)) => panic!("watch errors after writing hello.txt: {errs:?}"),
                Err(_) => break,
            }
        }

        assert_eq!(
            announced,
            Some(hash_bytes(b"notify-hello")),
            "notify + note_local should FileAnnounce /src/hello.txt"
        );
    }
}

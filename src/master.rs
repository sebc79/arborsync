use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

use anyhow::Context;
use arborsync_core::keys::read_static_key;
use arborsync_core::master::{LocalEvent, Master, WholeFileLater};
use arborsync_core::path::host_to_canonical;
use arborsync_core::storage::Storage;
use arborsync_core::{LoadedMaster, RedbStorage};
use notify_debouncer_mini::notify::RecursiveMode;
use notify_debouncer_mini::{DebounceEventResult, new_debouncer};

const DEFAULT_CONFIG: &str = "/etc/arborsync/master.toml";

type CentralMaster = Master<RedbStorage, WholeFileLater>;

pub fn run(config: Option<PathBuf>) -> anyhow::Result<()> {
    let config_path = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG));
    let cfg = LoadedMaster::load(&config_path)
        .with_context(|| format!("load {}", config_path.display()))?;

    read_static_key(cfg.master_key_path())
        .with_context(|| format!("read {}", cfg.master_key_path().display()))?;

    if let Some(parent) = cfg.db_path().parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let store = RedbStorage::open(cfg.db_path())
        .with_context(|| format!("open index {}", cfg.db_path().display()))?;

    let debounce = Duration::from_millis(cfg.watcher_debounce_ms());
    let rescan_every = Duration::from_secs(cfg.rescan_interval_seconds());
    let mut master = Master::open(cfg, store, WholeFileLater)?;
    log::info!("master watching {}", master.central_root().display());
    log::warn!("QUIC listener is not wired yet; running the local watch only");

    loop {
        watch_central(&mut master, debounce, rescan_every)?;
        log::warn!("filesystem watcher stopped; rescanning and re-arming");
        master.rescan()?;
    }
}

fn watch_central(
    master: &mut CentralMaster,
    debounce: Duration,
    rescan_every: Duration,
) -> anyhow::Result<()> {
    let root = master.central_root().to_path_buf();
    let (tx, rx) = mpsc::channel::<DebounceEventResult>();
    let mut debouncer = new_debouncer(debounce, tx)?;
    debouncer
        .watcher()
        .watch(&root, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", root.display()))?;
    master.rescan()?;

    loop {
        match rx.recv_timeout(rescan_every) {
            Ok(Ok(events)) => {
                for event in events {
                    note(master, &root, &event.path)?;
                }
            }
            Ok(Err(err)) => {
                log::warn!("watch error, rescanning: {err}");
                master.rescan()?;
            }
            Err(RecvTimeoutError::Timeout) => master.rescan()?,
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

fn note(master: &mut CentralMaster, root: &Path, host: &Path) -> anyhow::Result<()> {
    match host_to_canonical(root, host) {
        Ok(path) => Ok(master.note_local(LocalEvent::Changed(path))?),
        Err(err) => {
            log::warn!("skipping {}: {err}", host.display());
            Ok(())
        }
    }
}

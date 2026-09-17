use std::path::{Path, PathBuf};
use std::time::Duration;

use notify_debouncer_mini::new_debouncer;
use notify_debouncer_mini::notify::RecursiveMode;
use tokio::sync::mpsc::UnboundedSender;

pub fn apply_file_log_level(level: &str) {
    if std::env::var_os("ARBORSYNC_LOG_LEVEL").is_some() {
        return;
    }
    if let Some(filter) = arborsync_core::log_level_filter(level) {
        log::set_max_level(filter);
    }
}

pub fn spawn_config_watch(path: PathBuf, tx: UnboundedSender<()>) {
    std::thread::spawn(move || {
        if let Err(err) = watch_config(&path, &tx) {
            log::warn!("config watch stopped: {err}");
        }
    });
}

fn watch_config(path: &Path, tx: &UnboundedSender<()>) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("config path {} has no parent", path.display()))?;
    let name = path.file_name().map(|name| name.to_owned());
    let (notify_tx, rx) = std::sync::mpsc::channel();
    let mut debouncer = new_debouncer(Duration::from_millis(200), notify_tx)?;
    debouncer
        .watcher()
        .watch(parent, RecursiveMode::NonRecursive)?;
    while let Ok(result) = rx.recv() {
        match result {
            Ok(events) => {
                if events
                    .iter()
                    .any(|event| event.path.file_name() == name.as_deref())
                {
                    let _ = tx.send(());
                }
            }
            Err(err) => log::warn!("config watch: {err}"),
        }
    }
    Ok(())
}

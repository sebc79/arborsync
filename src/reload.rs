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
        let mut delay = config_watch_backoff(Duration::ZERO);
        loop {
            match watch_config(&path, &tx) {
                Ok(()) => {
                    log::warn!("config watch stopped");
                    return;
                }
                Err(err) => {
                    log::warn!(
                        "config watch setup failed: {err}; retrying in {}ms",
                        delay.as_millis()
                    );
                    std::thread::sleep(delay);
                    delay = config_watch_backoff(delay);
                }
            }
        }
    });
}

/// Backoff for a failed config-watch setup. Starts at 200 ms, doubles, caps at 30 s.
fn config_watch_backoff(prev: Duration) -> Duration {
    const INITIAL: Duration = Duration::from_millis(200);
    const MAX: Duration = Duration::from_secs(30);
    if prev.is_zero() {
        INITIAL
    } else {
        prev.saturating_mul(2).min(MAX)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;

    #[test]
    fn config_watch_backoff_doubles_then_caps() {
        let first = config_watch_backoff(Duration::ZERO);
        assert_eq!(first, Duration::from_millis(200));
        let second = config_watch_backoff(first);
        assert_eq!(second, Duration::from_millis(400));
        let mut delay = Duration::from_secs(20);
        delay = config_watch_backoff(delay);
        assert_eq!(delay, Duration::from_secs(30));
        assert_eq!(config_watch_backoff(delay), Duration::from_secs(30));
    }

    #[test]
    fn config_watch_retries_until_parent_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let parent = tmp.path().join("later");
        let cfg = parent.join("cfg.toml");
        let (tx, mut rx) = mpsc::unbounded_channel();
        spawn_config_watch(cfg.clone(), tx);

        // First setups fail: parent is missing. Create it after the first backoff window.
        std::thread::sleep(Duration::from_millis(250));
        fs::create_dir_all(&parent).unwrap();
        fs::write(&cfg, "v1\n").unwrap();

        // Wait for a successful arm, then rewrite so notify fires.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut armed = false;
        while Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
            // Touch after parent exists so a later successful watch can see it.
            fs::write(&cfg, format!("v{}\n", Instant::now().elapsed().as_millis())).unwrap();
            match rx.try_recv() {
                Ok(()) => {
                    armed = true;
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    panic!("config watch sender dropped before delivering an event")
                }
            }
        }
        assert!(
            armed,
            "expected a config-change event after the parent appeared and the watch retried"
        );
    }
}

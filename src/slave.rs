use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use arborsync_core::keys::read_static_key;
use arborsync_core::path::local_to_canonical;
use arborsync_core::slave::{LocalEvent, Reply, Slave, WholeFileLater};
use arborsync_core::storage::Storage;
use arborsync_core::transport::{
    client_endpoint, connect, open_control, peer_static_key, read_control, write_control,
};
use arborsync_core::{CanonicalPath, LoadedSlave, RedbStorage};
use notify_debouncer_mini::notify::RecursiveMode;
use notify_debouncer_mini::{DebounceEventResult, new_debouncer};

type SharedSlave = Arc<Mutex<Slave<RedbStorage, WholeFileLater>>>;

enum Work {
    Local { checkout: String, event: LocalEvent },
}

pub fn run(config: Option<PathBuf>) -> anyhow::Result<()> {
    tokio::runtime::Runtime::new()?.block_on(run_async(config))
}

async fn run_async(config: Option<PathBuf>) -> anyhow::Result<()> {
    let config_path = config.unwrap_or_else(default_config_path);
    let cfg = LoadedSlave::load(&config_path)
        .with_context(|| format!("load {}", config_path.display()))?;

    let secret = read_static_key(cfg.slave_key_path())
        .with_context(|| format!("read {}", cfg.slave_key_path().display()))?;

    if let Some(parent) = cfg.db_path().parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let store = RedbStorage::open(cfg.db_path())
        .with_context(|| format!("open cache {}", cfg.db_path().display()))?;

    let debounce = Duration::from_millis(cfg.watcher_debounce_ms());
    let rescan_every = Duration::from_secs(cfg.rescan_interval_seconds());
    let slave = Arc::new(Mutex::new(Slave::open(cfg, store, WholeFileLater)?));
    let watched = {
        let guard = slave.lock().expect("slave");
        log::info!("slave {} ready", guard.slave_id());
        guard.watched_checkouts()
    };

    let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel();
    for (id, local, central) in watched {
        let tx = work_tx.clone();
        std::thread::spawn(move || {
            loop {
                if let Err(err) = watch_checkout(&id, &local, &central, debounce, rescan_every, &tx)
                {
                    log::warn!("watch {id} stopped: {err}");
                }
            }
        });
    }

    let mut backoff = Duration::from_secs(1);
    loop {
        match session(&secret, &slave, &mut work_rx).await {
            Ok(()) => backoff = Duration::from_secs(1),
            Err(err) => {
                log::warn!("{err:#}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }
}

async fn session(
    secret: &[u8; 32],
    slave: &SharedSlave,
    work: &mut tokio::sync::mpsc::UnboundedReceiver<Work>,
) -> anyhow::Result<()> {
    let (addr_text, subscribe) = {
        let guard = slave.lock().expect("slave");
        (guard.master_addr().to_string(), guard.subscribe())
    };
    let addr = addr_text
        .to_socket_addrs()
        .with_context(|| format!("resolve {addr_text}"))?
        .next()
        .with_context(|| format!("no address for {addr_text}"))?;

    let endpoint = client_endpoint(secret)?;
    let conn = connect(&endpoint, addr).await?;
    let peer = peer_static_key(&conn)?;
    if let Err(Reply::Hangup { reason }) = slave.lock().expect("slave").pin_check(peer) {
        anyhow::bail!("{reason}");
    }

    let (mut send, mut recv) = open_control(&conn).await?;
    write_control(&mut send, &subscribe).await?;
    match slave
        .lock()
        .expect("slave")
        .handle(read_control(&mut recv).await?)?
    {
        Reply::Hangup { reason } => anyhow::bail!("{reason}"),
        Reply::Send(outs) => {
            for out in outs {
                write_control(&mut send, &out).await?;
            }
        }
    }
    log::info!("connected to {addr_text}");

    loop {
        tokio::select! {
            msg = read_control(&mut recv) => {
                let msg = msg?;
                let reply = slave.lock().expect("slave").handle(msg)?;
                match reply {
                    Reply::Hangup { reason } => anyhow::bail!("{reason}"),
                    Reply::Send(outs) => {
                        for out in outs {
                            write_control(&mut send, &out).await?;
                        }
                    }
                }
            }
            work = work.recv() => {
                let Some(Work::Local { checkout, event }) = work else {
                    anyhow::bail!("watch channel closed");
                };
                let outs = slave.lock().expect("slave").note_local(&checkout, event)?;
                for out in outs {
                    write_control(&mut send, &out).await?;
                }
            }
        }
    }
}

fn watch_checkout(
    checkout: &str,
    local: &Path,
    central: &CanonicalPath,
    debounce: Duration,
    rescan_every: Duration,
    tx: &tokio::sync::mpsc::UnboundedSender<Work>,
) -> anyhow::Result<()> {
    let (notify_tx, rx) = mpsc::channel::<DebounceEventResult>();
    let mut debouncer = new_debouncer(debounce, notify_tx)?;
    debouncer
        .watcher()
        .watch(local, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", local.display()))?;

    loop {
        match rx.recv_timeout(rescan_every) {
            Ok(Ok(events)) => {
                for event in events {
                    note(checkout, local, central, &event.path, tx)?;
                }
            }
            Ok(Err(err)) => log::warn!("watch {checkout}: {err}"),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

fn note(
    checkout: &str,
    local: &Path,
    central: &CanonicalPath,
    host: &Path,
    tx: &tokio::sync::mpsc::UnboundedSender<Work>,
) -> anyhow::Result<()> {
    match local_to_canonical(local, central, host) {
        Ok(path) => tx
            .send(Work::Local {
                checkout: checkout.into(),
                event: LocalEvent::Changed(path),
            })
            .context("session dropped"),
        Err(err) => {
            log::warn!("skipping {}: {err}", host.display());
            Ok(())
        }
    }
}

fn default_config_path() -> PathBuf {
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(".config/arborsync/slave.toml"),
        None => PathBuf::from("/nonexistent/slave.toml"),
    }
}

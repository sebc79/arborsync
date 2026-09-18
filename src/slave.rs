use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use arborsync_core::LocalEvent;
use arborsync_core::keys::{format_hex_key, public_from_secret, read_static_key};
use arborsync_core::path::local_to_canonical;
use arborsync_core::slave::{Reply, Slave, SlaveError, WholeFileLater};
use arborsync_core::storage::Storage;
use arborsync_core::transport::{Transport, client_endpoint, connect};
use arborsync_core::watch::to_local_events;
use arborsync_core::{CanonicalPath, LoadedSlave, RedbStorage, ReloadError, SlaveReload};
use notify::RecursiveMode;
use notify_debouncer_full::{DebounceEventResult, new_debouncer};
use quinn::Connection;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::mpsc::UnboundedSender;

use crate::reload::{apply_file_log_level, spawn_config_watch};

type SharedSlave = Arc<Mutex<Slave<RedbStorage, WholeFileLater>>>;

enum Work {
    Local { checkout: String, event: LocalEvent },
    Rescan { checkout: String },
}

enum WatchStop {
    Restart,
    Removed,
}

pub fn run(config: Option<PathBuf>) -> anyhow::Result<()> {
    tokio::runtime::Runtime::new()?.block_on(run_async(config))
}

async fn run_async(config: Option<PathBuf>) -> anyhow::Result<()> {
    let config_path = config.unwrap_or_else(default_config_path);
    let cfg = LoadedSlave::load(&config_path)
        .with_context(|| format!("load {}", config_path.display()))?;
    apply_file_log_level(cfg.log_level());

    let secret = read_static_key(cfg.slave_key_path())
        .with_context(|| format!("read {}", cfg.slave_key_path().display()))?;
    let pin = format_hex_key(&public_from_secret(&secret));
    log::info!("slave public key {pin}");

    if let Some(parent) = cfg.db_path().parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let store = RedbStorage::open(cfg.db_path())
        .with_context(|| format!("open cache {}", cfg.db_path().display()))?;

    let slave = Arc::new(Mutex::new(Slave::open(cfg, store, WholeFileLater)?));
    let watched = {
        let guard = slave.lock().expect("slave");
        log::info!("slave {} ready", guard.slave_id());
        guard.watched_checkouts()
    };

    let watch_gen = Arc::new(AtomicU64::new(0));
    let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel();
    spawn_checkout_watchers(
        watched.into_iter().map(|(id, _, _)| id),
        &slave,
        &work_tx,
        &watch_gen,
    );

    let mut hangup = signal(SignalKind::hangup())?;
    let (cfg_tx, mut cfg_rx) = tokio::sync::mpsc::unbounded_channel();
    spawn_config_watch(config_path.clone(), cfg_tx);

    let mut backoff = Duration::from_secs(1);
    loop {
        match session(
            &secret,
            &slave,
            &mut work_rx,
            &mut hangup,
            &mut cfg_rx,
            &config_path,
            &work_tx,
            &watch_gen,
        )
        .await
        {
            Ok(()) => backoff = Duration::from_secs(1),
            Err(err) => {
                log::warn!("{err:#}");
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = hangup.recv() => {
                        reload_slave_from_disk(&config_path, &slave, &work_tx, &watch_gen);
                    }
                    Some(()) = cfg_rx.recv() => {
                        reload_slave_from_disk(&config_path, &slave, &work_tx, &watch_gen);
                    }
                }
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }
}

fn reload_slave_from_disk(
    path: &Path,
    slave: &SharedSlave,
    work_tx: &UnboundedSender<Work>,
    watch_gen: &Arc<AtomicU64>,
) -> Option<SlaveReload> {
    let next = match LoadedSlave::load(path) {
        Ok(cfg) => cfg,
        Err(err) => {
            log::warn!("reload {}: {err}", path.display());
            return None;
        }
    };
    let (old_debounce, old_rescan) = {
        let guard = slave.lock().expect("slave");
        (guard.watcher_debounce_ms(), guard.rescan_interval_seconds())
    };
    let mut guard = slave.lock().expect("slave");
    match guard.reload(next) {
        Ok(plan) => {
            apply_file_log_level(&plan.log_level);
            if plan.watcher_debounce_ms != old_debounce
                || plan.rescan_interval_seconds != old_rescan
            {
                watch_gen.fetch_add(1, Ordering::Relaxed);
            }
            drop(guard);
            spawn_checkout_watchers(plan.added.iter().cloned(), slave, work_tx, watch_gen);
            Some(plan)
        }
        Err(SlaveError::Reload(ReloadError::RestartRequired { fields })) => {
            log::warn!("reload requires restart: {}", fields.join(", "));
            None
        }
        Err(err) => {
            log::warn!("reload {}: {err}", path.display());
            None
        }
    }
}

fn spawn_checkout_watchers(
    ids: impl IntoIterator<Item = String>,
    slave: &SharedSlave,
    work_tx: &UnboundedSender<Work>,
    watch_gen: &Arc<AtomicU64>,
) {
    let watched = slave.lock().expect("slave").watched_checkouts();
    for id in ids {
        let Some((_, local, central)) = watched.iter().find(|(cid, ..)| cid == &id) else {
            continue;
        };
        let local = local.clone();
        let central = central.clone();
        let slave = slave.clone();
        let work_tx = work_tx.clone();
        let watch_gen = watch_gen.clone();
        std::thread::spawn(move || {
            loop {
                let (debounce, rescan_every, start_gen) = {
                    let guard = slave.lock().expect("slave");
                    (
                        Duration::from_millis(guard.watcher_debounce_ms()),
                        Duration::from_secs(guard.rescan_interval_seconds()),
                        watch_gen.load(Ordering::Relaxed),
                    )
                };
                match watch_checkout(
                    &id,
                    &local,
                    &central,
                    debounce,
                    rescan_every,
                    &work_tx,
                    &slave,
                    &watch_gen,
                    start_gen,
                ) {
                    Ok(WatchStop::Removed) => return,
                    Ok(WatchStop::Restart) => {}
                    Err(err) => log::warn!("watch {id} stopped: {err}"),
                }
            }
        });
    }
}

async fn session(
    secret: &[u8; 32],
    slave: &SharedSlave,
    work: &mut tokio::sync::mpsc::UnboundedReceiver<Work>,
    hangup: &mut Signal,
    config_rx: &mut tokio::sync::mpsc::UnboundedReceiver<()>,
    config_path: &Path,
    work_tx: &UnboundedSender<Work>,
    watch_gen: &Arc<AtomicU64>,
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
    let peer = conn.peer_static_key()?;
    if let Err(Reply::Hangup { reason }) = slave.lock().expect("slave").pin_check(peer) {
        anyhow::bail!("{reason}");
    }

    let (mut send, mut recv) = conn.open_control().await?;
    Connection::write_control(&mut send, &subscribe).await?;
    dispatch_slave(
        &conn,
        &mut send,
        slave
            .lock()
            .expect("slave")
            .handle(Connection::read_control(&mut recv).await?)?,
    )
    .await?;
    log::info!("connected to {addr_text}");

    loop {
        tokio::select! {
            msg = Connection::read_control(&mut recv) => {
                let msg = msg?;
                dispatch_slave(&conn, &mut send, slave.lock().expect("slave").handle(msg)?).await?;
            }
            incoming = conn.accept_bulk() => {
                let (header, body) = incoming?;
                dispatch_slave(
                    &conn,
                    &mut send,
                    slave.lock().expect("slave").apply_bulk(header, &body)?,
                )
                .await?;
            }
            work = work.recv() => {
                let Some(work) = work else {
                    anyhow::bail!("watch channel closed");
                };
                match work {
                    Work::Local { checkout, event } => {
                        match slave.lock().expect("slave").note_local(&checkout, event) {
                            Ok(outs) => {
                                for out in outs {
                                    Connection::write_control(&mut send, &out).await?;
                                }
                            }
                            Err(SlaveError::UnknownCheckout(_)) => {}
                            Err(err) => return Err(err.into()),
                        }
                    }
                    Work::Rescan { checkout } => {
                        match slave.lock().expect("slave").rescan(&checkout) {
                            Ok(outs) => {
                                for out in outs {
                                    Connection::write_control(&mut send, &out).await?;
                                }
                            }
                            Err(SlaveError::UnknownCheckout(_)) => {}
                            Err(err) => return Err(err.into()),
                        }
                    }
                }
            }
            _ = hangup.recv() => {
                apply_live_slave_reload(
                    config_path,
                    slave,
                    work_tx,
                    watch_gen,
                    peer,
                    &mut send,
                )
                .await?;
            }
            Some(()) = config_rx.recv() => {
                apply_live_slave_reload(
                    config_path,
                    slave,
                    work_tx,
                    watch_gen,
                    peer,
                    &mut send,
                )
                .await?;
            }
        }
    }
}

async fn apply_live_slave_reload(
    config_path: &Path,
    slave: &SharedSlave,
    work_tx: &UnboundedSender<Work>,
    watch_gen: &Arc<AtomicU64>,
    peer: [u8; 32],
    send: &mut quinn::SendStream,
) -> anyhow::Result<()> {
    let Some(plan) = reload_slave_from_disk(config_path, slave, work_tx, watch_gen) else {
        return Ok(());
    };
    if let Err(Reply::Hangup { reason }) = slave.lock().expect("slave").pin_check(peer) {
        anyhow::bail!("{reason}");
    }
    if plan.resubscribe {
        let subscribe = slave.lock().expect("slave").subscribe();
        Connection::write_control(send, &subscribe).await?;
    }
    Ok(())
}

async fn dispatch_slave(
    conn: &quinn::Connection,
    send: &mut quinn::SendStream,
    reply: Reply,
) -> anyhow::Result<()> {
    match reply {
        Reply::Hangup { reason } => anyhow::bail!("{reason}"),
        Reply::Send(outs) => {
            for out in outs {
                Connection::write_control(send, &out).await?;
            }
            Ok(())
        }
        Reply::Bulk(xfer) => {
            conn.write_bulk(&xfer).await?;
            Ok(())
        }
    }
}

fn watch_checkout(
    checkout: &str,
    local: &Path,
    central: &CanonicalPath,
    debounce: Duration,
    rescan_every: Duration,
    tx: &UnboundedSender<Work>,
    slave: &SharedSlave,
    watch_gen: &AtomicU64,
    start_gen: u64,
) -> anyhow::Result<WatchStop> {
    if !still_this_checkout(slave, checkout, local, central) {
        return Ok(WatchStop::Removed);
    }
    let (notify_tx, rx) = mpsc::channel::<DebounceEventResult>();
    let mut debouncer = new_debouncer(debounce, None, notify_tx)?;
    debouncer
        .watch(local, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", local.display()))?;

    loop {
        if !still_this_checkout(slave, checkout, local, central) {
            return Ok(WatchStop::Removed);
        }
        if watch_gen.load(Ordering::Relaxed) != start_gen {
            return Ok(WatchStop::Restart);
        }
        match rx.recv_timeout(rescan_every) {
            Ok(Ok(events)) => {
                let (need_rescan, mapped) = crate::watch::classify(events);
                if need_rescan {
                    tx.send(Work::Rescan {
                        checkout: checkout.into(),
                    })
                    .context("session dropped")?;
                }
                let locals =
                    to_local_events(mapped, |host| local_to_canonical(local, central, host).ok());
                for event in locals {
                    tx.send(Work::Local {
                        checkout: checkout.into(),
                        event,
                    })
                    .context("session dropped")?;
                }
            }
            Ok(Err(errs)) => {
                log::warn!("watch {checkout}: {errs:?}");
                tx.send(Work::Rescan {
                    checkout: checkout.into(),
                })
                .context("session dropped")?;
            }
            Err(RecvTimeoutError::Timeout) => {
                tx.send(Work::Rescan {
                    checkout: checkout.into(),
                })
                .context("session dropped")?;
            }
            Err(RecvTimeoutError::Disconnected) => return Ok(WatchStop::Restart),
        }
    }
}

fn still_this_checkout(
    slave: &SharedSlave,
    checkout: &str,
    local: &Path,
    central: &CanonicalPath,
) -> bool {
    slave.lock().expect("slave").watched_checkouts().iter().any(
        |(id, watched_local, watched_central)| {
            id == checkout && watched_local == local && watched_central == central
        },
    )
}

fn default_config_path() -> PathBuf {
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(".config/arborsync/slave.toml"),
        None => PathBuf::from("/nonexistent/slave.toml"),
    }
}

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use arborsync_core::ReloadError;
use arborsync_core::keys::{format_hex_key, public_from_secret, read_static_key};
use arborsync_core::master::{Master, Reply, WholeFileLater};
use arborsync_core::path::host_to_canonical;
use arborsync_core::protocol::ProtocolMessage;
use arborsync_core::storage::Storage;
use arborsync_core::transport::{AttemptLimiter, Transport, listen, read_bulk, stream_err};
use arborsync_core::watch::to_local_events;
use arborsync_core::{LoadedMaster, RedbStorage};
use notify::RecursiveMode;
use notify_debouncer_full::{DebounceEventResult, new_debouncer};
use quinn::{Connection, Incoming};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::reload::{apply_file_log_level, spawn_config_watch};

const DEFAULT_CONFIG: &str = "/etc/arborsync/master.toml";
const OUTBOX_BACKPRESSURE: usize = 32;

type SharedMaster = Arc<Mutex<Master<RedbStorage, WholeFileLater>>>;

struct SessionHandle {
    replaced: tokio::sync::watch::Sender<bool>,
    conn: quinn::Connection,
}

pub fn run(config: Option<PathBuf>) -> anyhow::Result<()> {
    tokio::runtime::Runtime::new()?.block_on(run_async(config))
}

async fn run_async(config: Option<PathBuf>) -> anyhow::Result<()> {
    let config_path = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG));
    let cfg = LoadedMaster::load(&config_path)
        .with_context(|| format!("load {}", config_path.display()))?;
    apply_file_log_level(cfg.log_level());

    let secret = read_static_key(cfg.master_key_path())
        .with_context(|| format!("read {}", cfg.master_key_path().display()))?;
    let pin = format_hex_key(&public_from_secret(&secret));
    log::info!("master public key {pin}");

    if let Some(parent) = cfg.db_path().parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let store = RedbStorage::open(cfg.db_path())
        .with_context(|| format!("open index {}", cfg.db_path().display()))?;

    let listen_addr = cfg.listen_addr();
    let max_attempts = cfg.max_connection_attempts_per_minute();

    let master = Arc::new(Mutex::new(Master::open(cfg, store, WholeFileLater)?));
    let endpoint = listen(listen_addr, &secret)?;
    log::info!(
        "master watching {} and listening on {}",
        master.lock().expect("master").central_root().display(),
        endpoint.local_addr()?
    );

    let watch_gen = Arc::new(AtomicU64::new(0));
    let watched = master.clone();
    let watched_gen = watch_gen.clone();
    std::thread::spawn(move || {
        loop {
            let (debounce, rescan_every, start_gen) = {
                let guard = watched.lock().expect("master");
                (
                    Duration::from_millis(guard.watcher_debounce_ms()),
                    Duration::from_secs(guard.rescan_interval_seconds()),
                    watched_gen.load(Ordering::Relaxed),
                )
            };
            if let Err(err) =
                watch_central(&watched, debounce, rescan_every, &watched_gen, start_gen)
            {
                log::warn!("filesystem watcher stopped: {err}");
            }
            if let Err(err) = watched.lock().expect("master").rescan() {
                log::warn!("rescan failed: {err}");
            }
            log::warn!("rescanning and re-arming");
        }
    });

    let limiter = Arc::new(Mutex::new(AttemptLimiter::new(max_attempts)));
    let sessions = Arc::new(Mutex::new(HashMap::<String, SessionHandle>::new()));
    let mut hangup = signal(SignalKind::hangup())?;
    let (cfg_tx, mut cfg_rx) = tokio::sync::mpsc::unbounded_channel();
    spawn_config_watch(config_path.clone(), cfg_tx);
    let mut status_clock = crate::status::Clock::new();

    loop {
        let status_every = master.lock().expect("master").status_interval_seconds();
        tokio::select! {
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else {
                    break;
                };
                let master = master.clone();
                let limiter = limiter.clone();
                let sessions = sessions.clone();
                tokio::spawn(async move {
                    if let Err(err) = accept_session(incoming, master.clone(), limiter, sessions).await {
                        log::warn!("{err:#}");
                        master
                            .lock()
                            .expect("master")
                            .note_status_error(None, format!("{err:#}"));
                    }
                });
            }
            _ = hangup.recv() => {
                reload_master_from_disk(&config_path, &master, &limiter, &sessions, &watch_gen);
            }
            Some(()) = cfg_rx.recv() => {
                reload_master_from_disk(&config_path, &master, &limiter, &sessions, &watch_gen);
            }
            _ = status_clock.wait(status_every) => {
                emit_master_status(&master);
            }
        }
    }
    Ok(())
}

fn reload_master_from_disk(
    path: &Path,
    master: &SharedMaster,
    limiter: &Mutex<AttemptLimiter>,
    sessions: &Mutex<HashMap<String, SessionHandle>>,
    watch_gen: &AtomicU64,
) {
    let next = match LoadedMaster::load(path) {
        Ok(cfg) => cfg,
        Err(err) => {
            log::warn!("reload {}: {err}", path.display());
            return;
        }
    };
    let mut guard = master.lock().expect("master");
    let old_debounce = guard.watcher_debounce_ms();
    let old_rescan = guard.rescan_interval_seconds();
    match guard.reload(next) {
        Ok(plan) => {
            limiter
                .lock()
                .expect("limiter")
                .set_max(plan.max_connection_attempts_per_minute);
            apply_file_log_level(&plan.log_level);
            if plan.watcher_debounce_ms != old_debounce
                || plan.rescan_interval_seconds != old_rescan
            {
                watch_gen.fetch_add(1, Ordering::Relaxed);
            }
            drop(guard);
            let mut live = sessions.lock().expect("sessions");
            for id in &plan.drop_slave_ids {
                if let Some(handle) = live.remove(id) {
                    handle.conn.close(0u32.into(), b"acl reload");
                }
            }
        }
        Err(ReloadError::RestartRequired { fields }) => {
            log::warn!("reload requires restart: {}", fields.join(", "));
        }
    }
}

fn emit_master_status(master: &SharedMaster) {
    let mut guard = master.lock().expect("master");
    let period = guard.status_interval_seconds();
    if period == 0 {
        return;
    }
    for line in guard.take_status().lines(period) {
        log::info!("{line}");
    }
}

async fn accept_session(
    incoming: Incoming,
    master: SharedMaster,
    limiter: Arc<Mutex<AttemptLimiter>>,
    sessions: Arc<Mutex<HashMap<String, SessionHandle>>>,
) -> anyhow::Result<()> {
    let ip = incoming.remote_address().ip();
    if limiter.lock().expect("limiter").limited(ip, Instant::now()) {
        incoming.ignore();
        return Ok(());
    }
    let conn = incoming.await.context("handshake")?;
    let peer = conn.peer_static_key().context("peer static key")?;
    let slave_id = master
        .lock()
        .expect("master")
        .authorize_peer(&peer)
        .map(str::to_owned);
    let Some(slave_id) = slave_id else {
        let allowed = limiter.lock().expect("limiter").allow(ip, Instant::now());
        if allowed {
            log::warn!("unknown static key from {ip}");
        } else {
            log::warn!("rate-limited unknown key from {ip}");
        }
        conn.close(0u32.into(), b"unknown static key");
        return Ok(());
    };

    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    {
        let mut live = sessions.lock().expect("sessions");
        let max = master.lock().expect("master").max_connections() as usize;
        if live.len() >= max && !live.contains_key(&slave_id) {
            conn.close(0u32.into(), b"max connections");
            return Ok(());
        }
        if let Some(previous) = live.insert(
            slave_id.clone(),
            SessionHandle {
                replaced: stop_tx,
                conn: conn.clone(),
            },
        ) {
            previous.conn.close(0u32.into(), b"replaced");
            let _ = previous.replaced.send(true);
        }
    }

    let (mut send, mut recv) = conn.accept_control().await?;
    let (write_tx, mut write_rx) = unbounded_channel::<Vec<ProtocolMessage>>();
    let (write_err_tx, mut write_err_rx) = unbounded_channel();
    let writer_master = master.clone();
    tokio::spawn(async move {
        while let Some(batch) = write_rx.recv().await {
            let large = batch.len() > OUTBOX_BACKPRESSURE;
            for msg in &batch {
                if let Err(err) = Connection::write_control(&mut send, msg).await {
                    let _ = write_err_tx.send(err);
                    return;
                }
            }
            if large {
                writer_master
                    .lock()
                    .expect("master")
                    .set_writable(peer, true);
            }
        }
    });
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    break;
                }
            }
            msg = Connection::read_control(&mut recv) => {
                let msg = msg?;
                let reply = master.lock().expect("master").handle(peer, msg)?;
                if dispatch_master(&master, peer, &slave_id, &conn, &write_tx, reply, &limiter, ip)? {
                    break;
                }
            }
            incoming = conn.accept_uni() => {
                let mut recv = incoming.map_err(stream_err)?;
                let (header, body) = read_bulk(&mut recv).await?;
                let reply = master.lock().expect("master").apply_bulk(peer, header, &body)?;
                if dispatch_master(&master, peer, &slave_id, &conn, &write_tx, reply, &limiter, ip)? {
                    break;
                }
            }
            Some(err) = write_err_rx.recv() => return Err(err.into()),
            _ = tick.tick() => flush_outbox(&master, peer, &write_tx)?,
        }
    }

    if !*stop_rx.borrow() {
        sessions.lock().expect("sessions").remove(&slave_id);
        master.lock().expect("master").disconnect(peer);
    }
    Ok(())
}

fn dispatch_master(
    master: &SharedMaster,
    peer: [u8; 32],
    slave_id: &str,
    conn: &quinn::Connection,
    write_tx: &UnboundedSender<Vec<ProtocolMessage>>,
    reply: Reply,
    limiter: &Mutex<AttemptLimiter>,
    ip: std::net::IpAddr,
) -> anyhow::Result<bool> {
    match reply {
        Reply::Hangup { reason, rate_limit } => {
            if rate_limit {
                limiter.lock().expect("limiter").allow(ip, Instant::now());
            }
            log::info!("hangup {slave_id}: {reason}");
            master.lock().expect("master").disconnect(peer);
            Ok(true)
        }
        Reply::Send(out) => {
            enqueue_control(write_tx, vec![out])?;
            flush_outbox(master, peer, write_tx)?;
            Ok(false)
        }
        Reply::Bulk(xfer) => {
            let conn = conn.clone();
            tokio::spawn(async move {
                if let Err(err) = conn.write_bulk(&xfer).await {
                    log::warn!("master bulk send: {err:#}");
                }
            });
            flush_outbox(master, peer, write_tx)?;
            Ok(false)
        }
    }
}

fn flush_outbox(
    master: &SharedMaster,
    peer: [u8; 32],
    write_tx: &UnboundedSender<Vec<ProtocolMessage>>,
) -> anyhow::Result<()> {
    let pending = master.lock().expect("master").poll(peer);
    if pending.is_empty() {
        return Ok(());
    }
    if pending.len() > OUTBOX_BACKPRESSURE {
        master.lock().expect("master").set_writable(peer, false);
    }
    enqueue_control(write_tx, pending)
}

fn enqueue_control(
    write_tx: &UnboundedSender<Vec<ProtocolMessage>>,
    msgs: Vec<ProtocolMessage>,
) -> anyhow::Result<()> {
    if msgs.is_empty() {
        return Ok(());
    }
    write_tx
        .send(msgs)
        .map_err(|_| anyhow::anyhow!("control writer closed"))
}

fn watch_central(
    master: &SharedMaster,
    debounce: Duration,
    rescan_every: Duration,
    watch_gen: &AtomicU64,
    start_gen: u64,
) -> anyhow::Result<()> {
    let root = master.lock().expect("master").central_root().to_path_buf();
    let (tx, rx) = mpsc::channel::<DebounceEventResult>();
    let mut debouncer = new_debouncer(debounce, None, tx)?;
    debouncer
        .watch(&root, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", root.display()))?;
    master.lock().expect("master").rescan()?;

    loop {
        if watch_gen.load(Ordering::Relaxed) != start_gen {
            return Ok(());
        }
        match rx.recv_timeout(rescan_every) {
            Ok(Ok(events)) => {
                let (need_rescan, mapped) = crate::watch::classify(events);
                if need_rescan {
                    master.lock().expect("master").rescan()?;
                }
                let locals = to_local_events(mapped, |host| host_to_canonical(&root, host).ok());
                for event in locals {
                    master.lock().expect("master").note_local(event)?;
                }
            }
            Ok(Err(errs)) => {
                log::warn!("watch error, rescanning: {errs:?}");
                master.lock().expect("master").rescan()?;
            }
            Err(RecvTimeoutError::Timeout) => master.lock().expect("master").rescan()?,
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

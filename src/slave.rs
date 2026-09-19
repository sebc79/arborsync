use std::collections::{HashSet, VecDeque};
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use arborsync_core::LocalEvent;
use arborsync_core::hash::ContentHash;
use arborsync_core::keys::{format_hex_key, public_from_secret, read_static_key};
use arborsync_core::path::local_to_canonical;
use arborsync_core::protocol::ProtocolMessage;
use arborsync_core::slave::{Reply, Slave, SlaveError, WholeFileLater, fulfill_from_host};
use arborsync_core::storage::Storage;
use arborsync_core::transport::{Transport, client_endpoint, connect, read_bulk, stream_err};
use arborsync_core::watch::to_local_events;
use arborsync_core::{CanonicalPath, LoadedSlave, RedbStorage, ReloadError, SlaveReload};
use notify::RecursiveMode;
use notify_debouncer_full::{DebounceEventResult, new_debouncer};
use quinn::Connection;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

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
                slave
                    .lock()
                    .expect("slave")
                    .note_status_error(None, format!("{err:#}"));
                let deadline = std::time::Instant::now() + backoff;
                let mut status_clock = crate::status::Clock::new();
                loop {
                    let remain = deadline.saturating_duration_since(std::time::Instant::now());
                    if remain.is_zero() {
                        break;
                    }
                    let status_every = slave.lock().expect("slave").status_interval_seconds();
                    tokio::select! {
                        _ = tokio::time::sleep(remain) => break,
                        _ = status_clock.wait(status_every) => {
                            emit_slave_status(&slave, false);
                        }
                        _ = hangup.recv() => {
                            reload_slave_from_disk(&config_path, &slave, &work_tx, &watch_gen);
                            break;
                        }
                        Some(()) = cfg_rx.recv() => {
                            reload_slave_from_disk(&config_path, &slave, &work_tx, &watch_gen);
                            break;
                        }
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
        let mut guard = slave.lock().expect("slave");
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
    let (urgent_tx, mut urgent_rx) = unbounded_channel::<ProtocolMessage>();
    let (walk_tx, mut walk_rx) = unbounded_channel::<ProtocolMessage>();
    let (write_err_tx, mut write_err_rx) = unbounded_channel();
    tokio::spawn(async move {
        while let Some(msg) = next_outbound(&mut urgent_rx, &mut walk_rx).await {
            if let Err(err) = Connection::write_control(&mut send, &msg).await {
                let _ = write_err_tx.send(err);
                return;
            }
        }
    });
    let (bulk_tx, mut bulk_rx) = unbounded_channel();
    let mut outbound = Outbound::new(urgent_tx, walk_tx.clone(), bulk_tx);
    outbound.ingest(slave, &conn, Connection::read_control(&mut recv).await?)?;
    log::info!("connected to {addr_text}");

    let mut status_clock = crate::status::Clock::new();
    loop {
        let status_every = slave.lock().expect("slave").status_interval_seconds();
        tokio::select! {
            msg = Connection::read_control(&mut recv) => {
                outbound.ingest(slave, &conn, msg?)?;
            }
            incoming = conn.accept_uni() => {
                let mut recv = incoming.map_err(stream_err)?;
                let (header, body) = read_bulk(&mut recv).await?;
                outbound.push_reply(
                    &conn,
                    slave.lock().expect("slave").apply_bulk(header, &body)?,
                )?;
            }
            Some(result) = bulk_rx.recv() => {
                outbound.on_bulk_done(slave, &conn, result)?;
            }
            Some(err) = write_err_rx.recv() => return Err(err.into()),
            _ = wait_if_should_crawl(slave, outbound.serving()) => {
                match slave.lock().expect("slave").crawl_step() {
                    Ok(outs) => outbound.enqueue(outs)?,
                    Err(SlaveError::UnknownCheckout(_)) => {}
                    Err(err) => return Err(err.into()),
                }
            }
            work = work.recv() => {
                let Some(work) = work else {
                    anyhow::bail!("watch channel closed");
                };
                match work {
                    Work::Local { checkout, event } => {
                        match slave.lock().expect("slave").note_local(&checkout, event) {
                            Ok(outs) => outbound.enqueue(outs)?,
                            Err(SlaveError::UnknownCheckout(_)) => {}
                            Err(err) => return Err(err.into()),
                        }
                    }
                    Work::Rescan { checkout } => {
                        match slave.lock().expect("slave").request_rescan(&checkout) {
                            Ok(()) => {}
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
                    &outbound.walk_tx,
                )?;
            }
            Some(()) = config_rx.recv() => {
                apply_live_slave_reload(
                    config_path,
                    slave,
                    work_tx,
                    watch_gen,
                    peer,
                    &outbound.walk_tx,
                )?;
            }
            _ = status_clock.wait(status_every) => {
                emit_slave_status(slave, true);
            }
        }
    }
}

async fn wait_if_should_crawl(slave: &SharedSlave, serving: bool) {
    if serving {
        std::future::pending::<()>().await;
    }
    if slave.lock().expect("slave").crawl_pending() {
        return;
    }
    std::future::pending::<()>().await;
}

async fn next_outbound(
    urgent: &mut tokio::sync::mpsc::UnboundedReceiver<ProtocolMessage>,
    walk: &mut tokio::sync::mpsc::UnboundedReceiver<ProtocolMessage>,
) -> Option<ProtocolMessage> {
    tokio::select! {
        biased;
        msg = urgent.recv() => match msg {
            Some(msg) => Some(msg),
            None => walk.recv().await,
        },
        msg = walk.recv() => match msg {
            Some(msg) => Some(msg),
            None => urgent.recv().await,
        },
    }
}

fn emit_slave_status(slave: &SharedSlave, connected: bool) {
    let mut guard = slave.lock().expect("slave");
    let period = guard.status_interval_seconds();
    if period == 0 {
        return;
    }
    log::info!("{}", guard.take_status(connected).line(period));
}

fn apply_live_slave_reload(
    config_path: &Path,
    slave: &SharedSlave,
    work_tx: &UnboundedSender<Work>,
    watch_gen: &Arc<AtomicU64>,
    peer: [u8; 32],
    write_tx: &UnboundedSender<ProtocolMessage>,
) -> anyhow::Result<()> {
    let Some(plan) = reload_slave_from_disk(config_path, slave, work_tx, watch_gen) else {
        return Ok(());
    };
    if let Err(Reply::Hangup { reason }) = slave.lock().expect("slave").pin_check(peer) {
        anyhow::bail!("{reason}");
    }
    if plan.resubscribe {
        let subscribe = slave.lock().expect("slave").subscribe();
        write_tx
            .send(subscribe)
            .map_err(|_| anyhow::anyhow!("control writer closed"))?;
    }
    Ok(())
}

const MAX_BULK_INFLIGHT: usize = 4;
const LARGE_BULK_BYTES: u64 = 16 * 1024 * 1024;

struct BulkDone {
    result: anyhow::Result<()>,
    large: bool,
    control: Vec<ProtocolMessage>,
}

struct Outbound {
    urgent_tx: UnboundedSender<ProtocolMessage>,
    walk_tx: UnboundedSender<ProtocolMessage>,
    parked: VecDeque<ProtocolMessage>,
    fulfilled_asks: HashSet<(String, CanonicalPath, ContentHash)>,
    bulk_tx: UnboundedSender<BulkDone>,
    inflight: usize,
    large_inflight: usize,
}

impl Outbound {
    fn new(
        urgent_tx: UnboundedSender<ProtocolMessage>,
        walk_tx: UnboundedSender<ProtocolMessage>,
        bulk_tx: UnboundedSender<BulkDone>,
    ) -> Self {
        Self {
            urgent_tx,
            walk_tx,
            parked: VecDeque::new(),
            fulfilled_asks: HashSet::new(),
            bulk_tx,
            inflight: 0,
            large_inflight: 0,
        }
    }

    fn serving(&self) -> bool {
        self.inflight > 0 || !self.parked.is_empty()
    }

    fn ingest(
        &mut self,
        slave: &SharedSlave,
        conn: &Connection,
        msg: ProtocolMessage,
    ) -> anyhow::Result<()> {
        if let Some(key) = signature_request_key(&msg) {
            slave.lock().expect("slave").note_inbound(&msg);
            if self.fulfilled_asks.contains(&key)
                || self
                    .parked
                    .iter()
                    .any(|queued| signature_request_key(queued).as_ref() == Some(&key))
            {
                return Ok(());
            }
            if !self.can_start(slave, &msg) {
                self.parked.push_back(msg);
                return Ok(());
            }
            return self.start_ask(slave, conn, msg);
        }
        let reply = slave.lock().expect("slave").handle(msg)?;
        self.push_reply(conn, reply)
    }

    fn enqueue(&mut self, msgs: Vec<ProtocolMessage>) -> anyhow::Result<()> {
        for msg in msgs {
            let tx = if matches!(msg, ProtocolMessage::SignatureRequest { .. }) {
                &self.urgent_tx
            } else {
                &self.walk_tx
            };
            tx.send(msg)
                .map_err(|_| anyhow::anyhow!("control writer closed"))?;
        }
        Ok(())
    }

    fn push_reply(&mut self, _conn: &Connection, reply: Reply) -> anyhow::Result<()> {
        match reply {
            Reply::Hangup { reason } => anyhow::bail!("{reason}"),
            Reply::Send(msgs) => self.enqueue(msgs),
            Reply::Bulk(_) => anyhow::bail!("bulk reply must start via start_ask"),
        }
    }

    fn can_start(&self, slave: &SharedSlave, msg: &ProtocolMessage) -> bool {
        if self.inflight >= MAX_BULK_INFLIGHT {
            return false;
        }
        !(self.is_large(slave, msg) && self.large_inflight > 0)
    }

    fn is_large(&self, slave: &SharedSlave, msg: &ProtocolMessage) -> bool {
        let Some((checkout_id, path, _)) = signature_request_key(msg) else {
            return true;
        };
        slave
            .lock()
            .expect("slave")
            .announced_size(&checkout_id, &path)
            .ok()
            .flatten()
            .is_some_and(|size| size >= LARGE_BULK_BYTES)
    }

    fn start_ask(
        &mut self,
        slave: &SharedSlave,
        conn: &Connection,
        msg: ProtocolMessage,
    ) -> anyhow::Result<()> {
        let ProtocolMessage::SignatureRequest {
            checkout_id,
            path,
            want_hash,
            signature,
        } = msg
        else {
            return Ok(());
        };
        let (host, large) = {
            let guard = slave.lock().expect("slave");
            let host = match guard.bulk_host(&checkout_id, &path) {
                Ok(host) => host,
                Err(SlaveError::UnknownCheckout(_)) => return Ok(()),
                Err(err) => return Err(err.into()),
            };
            let large = match guard.announced_size(&checkout_id, &path) {
                Ok(size) => size.is_some_and(|size| size >= LARGE_BULK_BYTES),
                Err(SlaveError::UnknownCheckout(_)) => return Ok(()),
                Err(err) => return Err(err.into()),
            };
            (host, large)
        };
        self.fulfilled_asks
            .insert((checkout_id.clone(), path.clone(), want_hash));
        self.inflight += 1;
        if large {
            self.large_inflight += 1;
        }
        let conn = conn.clone();
        let tx = self.bulk_tx.clone();
        let status = slave.clone();
        tokio::spawn(async move {
            let reply = tokio::task::spawn_blocking(move || {
                fulfill_from_host(checkout_id, path, want_hash, &signature, &host)
            })
            .await;
            let done = match reply {
                Ok(Ok(Reply::Bulk(xfer))) => {
                    let bytes = xfer.body.len() as u64;
                    status.lock().expect("slave").note_bulk_out(bytes);
                    let result = conn
                        .write_bulk(&xfer)
                        .await
                        .map_err(|err| anyhow::anyhow!("{err:#}"));
                    BulkDone {
                        result,
                        large,
                        control: Vec::new(),
                    }
                }
                Ok(Ok(Reply::Send(control))) => BulkDone {
                    result: Ok(()),
                    large,
                    control,
                },
                Ok(Ok(Reply::Hangup { reason })) => BulkDone {
                    result: Err(anyhow::anyhow!("{reason}")),
                    large,
                    control: Vec::new(),
                },
                Ok(Err(err)) => BulkDone {
                    result: Err(anyhow::anyhow!("{err}")),
                    large,
                    control: Vec::new(),
                },
                Err(err) => BulkDone {
                    result: Err(anyhow::anyhow!("{err}")),
                    large,
                    control: Vec::new(),
                },
            };
            let _ = tx.send(done);
        });
        Ok(())
    }

    fn on_bulk_done(
        &mut self,
        slave: &SharedSlave,
        conn: &Connection,
        done: BulkDone,
    ) -> anyhow::Result<()> {
        done.result?;
        self.inflight = self.inflight.saturating_sub(1);
        if done.large {
            self.large_inflight = self.large_inflight.saturating_sub(1);
        }
        for msg in &done.control {
            slave.lock().expect("slave").note_outbound(msg);
        }
        self.enqueue(done.control)?;
        self.kick(slave, conn)
    }

    fn kick(&mut self, slave: &SharedSlave, conn: &Connection) -> anyhow::Result<()> {
        loop {
            let idx = self
                .parked
                .iter()
                .position(|msg| self.can_start(slave, msg));
            let Some(idx) = idx else {
                return Ok(());
            };
            let msg = self.parked.remove(idx).expect("parked ask");
            self.start_ask(slave, conn, msg)?;
        }
    }
}

fn signature_request_key(msg: &ProtocolMessage) -> Option<(String, CanonicalPath, ContentHash)> {
    match msg {
        ProtocolMessage::SignatureRequest {
            checkout_id,
            path,
            want_hash,
            ..
        } => Some((checkout_id.clone(), path.clone(), *want_hash)),
        _ => None,
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

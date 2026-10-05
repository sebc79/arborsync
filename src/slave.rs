use std::collections::VecDeque;
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use arborsync_core::LocalEvent;
use arborsync_core::bottleneck::{Stage, Wait, Waiting};
use arborsync_core::hash::ContentHash;
use arborsync_core::hashing::{HashDone, HashNeed};
use arborsync_core::keys::{format_hex_key, public_from_secret, read_static_key};
use arborsync_core::path::local_to_canonical;
use arborsync_core::peers::{PeerView, report_period};
use arborsync_core::protocol::{FrameError, ProtocolMessage};
use arborsync_core::slave::{
    ApplyBulkPlan, LinkState, Reply, RescanStat, RescanStated, Slave, SlaveError, WholeFileLater,
    fulfill_from_host, serve_peers,
};
use arborsync_core::status::SlaveStatus;
use arborsync_core::storage::Storage;
use arborsync_core::transport::{
    SessionFrame, Transport, TransportError, client_endpoint, connect, copy_bulk_body,
    read_bulk_header, stream_err,
};
use arborsync_core::tune::FulfillAdmission;
use arborsync_core::watch::to_local_events;
use arborsync_core::{CanonicalPath, LoadedSlave, RedbStorage, ReloadError, SlaveReload};
use notify::RecursiveMode;
use notify_debouncer_full::{DebounceEventResult, new_debouncer};
use quinn::Connection;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::reload::{apply_file_log_level, spawn_config_watch};

type SharedSlave = Arc<Mutex<Slave<RedbStorage, WholeFileLater>>>;

const HASH_DONE_BATCH: usize = 64;

enum Work {
    Local { checkout: String, event: LocalEvent },
    Rescan { checkout: String },
}

enum WatchStop {
    Restart,
    Removed,
}

struct HashPump {
    inflight: usize,
    queued: VecDeque<HashNeed>,
    held: Vec<HashDone>,
    done_tx: UnboundedSender<HashDone>,
}

impl HashPump {
    fn offer(&mut self, slave: &SharedSlave, needs: Vec<HashNeed>) {
        self.queued.extend(needs);
        self.kick(slave);
    }

    fn kick(&mut self, slave: &SharedSlave) {
        let cap = slave.lock().expect("slave").tune().hashing_workers().get();
        while self.inflight < cap {
            let Some(need) = self.queued.pop_front() else {
                break;
            };
            slave.lock().expect("slave").start_hashed(&need.key);
            self.inflight += 1;
            let tx = self.done_tx.clone();
            tokio::task::spawn_blocking(move || {
                if tx.send(need.run()).is_err() {
                    log::warn!("hash done channel closed");
                }
            });
        }
    }

    fn absorb_ready(&mut self, first: HashDone, rx: &mut UnboundedReceiver<HashDone>) {
        self.held.push(first);
        self.inflight = self.inflight.saturating_sub(1);
        while self.held.len() < HASH_DONE_BATCH {
            match rx.try_recv() {
                Ok(more) => {
                    self.held.push(more);
                    self.inflight = self.inflight.saturating_sub(1);
                }
                Err(_) => break,
            }
        }
    }
}

fn offer_pending_hashes(slave: &SharedSlave, hasher: &mut HashPump) {
    let needs = slave.lock().expect("slave").take_hash_jobs();
    hasher.offer(slave, needs);
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
    {
        let socket = slave.lock().expect("slave").peer_socket().to_path_buf();
        let (peer_tx, peer_rx) = tokio::sync::watch::channel(PeerView::Waiting);
        slave.lock().expect("slave").bind_served_peers(peer_tx);
        if let Err(err) = serve_peers(socket.clone(), peer_rx) {
            log::warn!("peer socket {}: {err}", socket.display());
        }
    }
    let watched = {
        let guard = slave.lock().expect("slave");
        log::info!(
            "tune hashing.workers={} from={}",
            guard.tune().hashing_workers().get(),
            guard.tune().hashing_workers_spec()
        );
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
            Ok(()) => {
                slave
                    .lock()
                    .expect("slave")
                    .set_served_peers(PeerView::Waiting);
                backoff = Duration::from_secs(1);
            }
            Err(err) => {
                log::warn!("{err:#}");
                slave
                    .lock()
                    .expect("slave")
                    .set_served_peers(PeerView::Waiting);
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
                            let _ = emit_slave_status(&slave, LinkState::offline(work_rx.len()));
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
                    Ok(WatchStop::Restart) => {
                        let _ = work_tx.send(Work::Rescan {
                            checkout: id.clone(),
                        });
                    }
                    Err(err) => {
                        log::warn!("watch {id} stopped: {err}");
                        let _ = work_tx.send(Work::Rescan {
                            checkout: id.clone(),
                        });
                        std::thread::sleep(Duration::from_secs(1));
                    }
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
    let (report_tx, mut report_rx) = tokio::sync::watch::channel(None::<DueReport>);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                changed = report_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let due = report_rx.borrow_and_update().clone();
                    if let Some(due) = due {
                        if let Err(err) = Connection::write_frame(
                            &mut send,
                            &SessionFrame::Report {
                                seq: due.seq,
                                pace: due.pace,
                                depth: due.depth,
                            },
                        )
                        .await
                        {
                            let _ = write_err_tx.send(err);
                            return;
                        }
                    }
                }
                msg = next_outbound(&mut urgent_rx, &mut walk_rx) => {
                    let Some(msg) = msg else {
                        return;
                    };
                    if let Err(err) =
                        Connection::write_frame(&mut send, &SessionFrame::File(msg)).await
                    {
                        let _ = write_err_tx.send(err);
                        return;
                    }
                }
            }
        }
    });
    let (bulk_tx, mut bulk_rx) = unbounded_channel();
    let (hash_tx, mut hash_rx) = unbounded_channel();
    let (stat_tx, mut stat_rx) = unbounded_channel::<Vec<RescanStated>>();
    let mut hasher = HashPump {
        inflight: 0,
        queued: VecDeque::new(),
        held: Vec::new(),
        done_tx: hash_tx,
    };
    let mut outbound = Outbound::new(urgent_tx, walk_tx.clone(), bulk_tx);
    slave
        .lock()
        .expect("slave")
        .set_served_peers(PeerView::Waiting);
    let ack = loop {
        match Connection::read_frame(&mut recv).await {
            Ok(SessionFrame::File(msg)) => break msg,
            Ok(SessionFrame::Directory(view)) => {
                slave.lock().expect("slave").set_served_peers(view);
            }
            Ok(SessionFrame::Report { .. }) => log::warn!("slave ignored a peer report"),
            Err(err) if bad_peer(&err) => log::warn!("skipped peer frame: {err}"),
            Err(err) => return Err(err.into()),
        }
    };
    outbound.ingest(slave, &conn, ack)?;
    offer_pending_hashes(slave, &mut hasher);
    let mut stat_busy = false;
    kick_crawl(slave, &mut outbound, &mut hasher, &mut stat_busy, &stat_tx)?;
    log::info!("connected to {addr_text}");

    let mut status_clock = crate::status::Clock::new();
    let mut report_clock = tokio::time::interval(report_period());
    report_clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut report_seq = 0u64;
    loop {
        let status_every = slave.lock().expect("slave").status_interval_seconds();
        let work_depth = work.len();
        tokio::select! {
            frame = Connection::read_frame(&mut recv) => {
                match frame {
                    Ok(SessionFrame::File(msg)) => {
                        outbound.ingest(slave, &conn, msg)?;
                        offer_pending_hashes(slave, &mut hasher);
                    }
                    Ok(SessionFrame::Directory(view)) => {
                        slave.lock().expect("slave").set_served_peers(view);
                    }
                    Ok(SessionFrame::Report { .. }) => {
                        log::warn!("slave ignored a peer report");
                    }
                    Err(err) if bad_peer(&err) => {
                        log::warn!("skipped peer frame: {err}");
                    }
                    Err(err) => return Err(err.into()),
                }
            }
            incoming = conn.accept_uni() => {
                let mut recv = incoming.map_err(stream_err)?;
                let header = read_bulk_header(&mut recv).await?;
                let plan = {
                    let mut guard = slave.lock().expect("slave");
                    guard.begin_apply_bulk(header.size);
                    guard.prepare_apply_bulk(header.clone())?
                };
                let reply = match plan {
                    ApplyBulkPlan::Done(reply) => {
                        copy_bulk_body(&mut recv, header.size, &mut std::io::sink()).await?;
                        reply
                    }
                    ApplyBulkPlan::Reconstruct(job) => {
                        let outcome = if job.is_delta() {
                            let mut body = Vec::new();
                            copy_bulk_body(&mut recv, header.size, &mut body).await?;
                            tokio::task::spawn_blocking(move || job.complete_delta(body))
                                .await
                                .map_err(|err| anyhow::anyhow!("apply join: {err}"))?
                        } else {
                            let mut stage = job.open_stage()?;
                            copy_bulk_body(&mut recv, header.size, &mut stage).await?;
                            tokio::task::spawn_blocking(move || job.complete_whole(stage))
                                .await
                                .map_err(|err| anyhow::anyhow!("apply join: {err}"))?
                        };
                        slave.lock().expect("slave").finish_apply_bulk(outcome)?
                    }
                };
                slave.lock().expect("slave").end_apply_bulk(&reply);
                outbound.push_reply(&conn, reply)?;
            }
            Some(result) = bulk_rx.recv() => {
                outbound.on_bulk_done(slave, &conn, result)?;
            }
            Some(err) = write_err_rx.recv() => return Err(err.into()),
            Some(done) = hash_rx.recv() => {
                hasher.absorb_ready(done, &mut hash_rx);
                hasher.kick(slave);
                offer_pending_hashes(slave, &mut hasher);
                if !hasher.held.is_empty()
                    && (hasher.held.len() >= HASH_DONE_BATCH || hasher.inflight == 0)
                {
                    let dones = std::mem::take(&mut hasher.held);
                    let outs = slave.lock().expect("slave").commit_hashed_batch(dones)?;
                    outbound.enqueue(outs)?;
                    offer_pending_hashes(slave, &mut hasher);
                    kick_crawl(slave, &mut outbound, &mut hasher, &mut stat_busy, &stat_tx)?;
                }
            }
            Some(stated) = stat_rx.recv() => {
                stat_busy = false;
                let outs = slave.lock().expect("slave").apply_rescan_stats(stated)?;
                outbound.enqueue(outs)?;
                offer_pending_hashes(slave, &mut hasher);
                kick_crawl(slave, &mut outbound, &mut hasher, &mut stat_busy, &stat_tx)?;
            }
            _ = wait_if_should_crawl(slave, outbound.serving(), stat_busy) => {
                kick_crawl(slave, &mut outbound, &mut hasher, &mut stat_busy, &stat_tx)?;
            }
            work = work.recv() => {
                let Some(work) = work else {
                    anyhow::bail!("watch channel closed");
                };
                match work {
                    Work::Local { checkout, event } => {
                        let planned = slave.lock().expect("slave").plan_local(&checkout, event);
                        match planned {
                            Ok(plan) => {
                                outbound.enqueue(plan.send)?;
                                hasher.offer(slave, plan.hash);
                            }
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
                let link = LinkState {
                    connected: true,
                    waits: outbound.waits(),
                    work_depth,
                };
                outbound.report_status(slave, &conn, link);
            }
            _ = report_clock.tick() => {
                report_seq = report_seq.wrapping_add(1);
                let link = LinkState {
                    connected: true,
                    waits: outbound.waits(),
                    work_depth,
                };
                let (pace, depth) = slave.lock().expect("slave").sample_report(link, report_seq);
                let _ = report_tx.send(Some(DueReport {
                    seq: report_seq,
                    pace,
                    depth,
                }));
            }
        }
    }
}

fn kick_crawl(
    slave: &SharedSlave,
    outbound: &mut Outbound,
    hasher: &mut HashPump,
    stat_busy: &mut bool,
    stat_tx: &UnboundedSender<Vec<RescanStated>>,
) -> anyhow::Result<()> {
    let serving = outbound.serving();
    let mut guard = slave.lock().expect("slave");
    if guard.crawl_has_pages() && guard.should_step_crawl(serving) {
        match guard.crawl_step() {
            Ok(outs) => {
                drop(guard);
                outbound.enqueue(outs)?;
                offer_pending_hashes(slave, hasher);
            }
            Err(SlaveError::UnknownCheckout(_)) => {}
            Err(err) => return Err(err.into()),
        }
        return Ok(());
    }
    if *stat_busy || (serving && guard.crawl_has_pages()) {
        return Ok(());
    }
    if guard.rescan_wants_stat() {
        let batch = match guard.take_rescan_stats() {
            Ok(batch) => batch,
            Err(SlaveError::UnknownCheckout(_)) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        if batch.is_empty() {
            return Ok(());
        }
        *stat_busy = true;
        drop(guard);
        let tx = stat_tx.clone();
        tokio::spawn(async move {
            let stated = tokio::task::spawn_blocking(move || {
                batch
                    .into_iter()
                    .map(RescanStat::inspect)
                    .collect::<Vec<_>>()
            })
            .await;
            match stated {
                Ok(stated) => {
                    if tx.send(stated).is_err() {
                        log::warn!("rescan stat channel closed");
                    }
                }
                Err(err) => log::warn!("rescan stat join: {err}"),
            }
        });
        return Ok(());
    }
    if !guard.should_step_crawl(serving) {
        return Ok(());
    }
    match guard.crawl_step() {
        Ok(outs) => {
            drop(guard);
            outbound.enqueue(outs)?;
            offer_pending_hashes(slave, hasher);
        }
        Err(SlaveError::UnknownCheckout(_)) => {}
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

async fn wait_if_should_crawl(slave: &SharedSlave, serving: bool, stat_busy: bool) {
    let guard = slave.lock().expect("slave");
    if guard.crawl_has_pages() && guard.should_step_crawl(serving) {
        return;
    }
    if stat_busy || (serving && guard.crawl_has_pages()) {
        drop(guard);
        std::future::pending::<()>().await;
        return;
    }
    if guard.should_step_crawl(serving) || guard.rescan_wants_stat() {
        return;
    }
    drop(guard);
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

fn emit_slave_status(slave: &SharedSlave, link: LinkState) -> Option<SlaveStatus> {
    let mut guard = slave.lock().expect("slave");
    let period = guard.status_interval_seconds();
    if period == 0 {
        return None;
    }
    let status = guard.take_status(link);
    log::info!("{}", status.line(period));
    Some(status)
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
        log::info!("resubscribe after reload");
        let subscribe = slave.lock().expect("slave").subscribe();
        write_tx
            .send(subscribe)
            .map_err(|_| anyhow::anyhow!("control writer closed"))?;
    }
    Ok(())
}

type AskKey = (String, CanonicalPath, ContentHash);
type IsLarge = bool;

struct ParkedAsk {
    msg: ProtocolMessage,
    size: Option<u64>,
}

#[derive(Clone)]
struct DueReport {
    seq: u64,
    pace: arborsync_core::peers::Pace,
    depth: arborsync_core::peers::QueueDepth,
}

fn bad_peer(err: &TransportError) -> bool {
    matches!(err, TransportError::Frame(FrameError::BadPeer))
}

struct BulkDone {
    result: anyhow::Result<()>,
    key: AskKey,
    control: Vec<ProtocolMessage>,
}

struct Outbound {
    urgent_tx: UnboundedSender<ProtocolMessage>,
    walk_tx: UnboundedSender<ProtocolMessage>,
    parked: Waiting<AskKey, ParkedAsk>,
    sending: Waiting<AskKey, IsLarge>,
    bulk_tx: UnboundedSender<BulkDone>,
    gauge_seq: u32,
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
            parked: Waiting::new(Stage::FulfillParked),
            sending: Waiting::new(Stage::FulfillRead),
            bulk_tx,
            gauge_seq: 0,
        }
    }

    fn report_status(&mut self, slave: &SharedSlave, conn: &Connection, link: LinkState) {
        let Some(status) = emit_slave_status(slave, link) else {
            return;
        };
        Transport::send_datagram(conn, &status.gauge(self.gauge_seq).encode());
        self.gauge_seq += 1;
    }

    fn serving(&self) -> bool {
        !self.sending.is_empty() || !self.parked.is_empty()
    }

    fn waits(&self) -> Vec<Wait> {
        [self.parked.oldest(), self.sending.oldest()]
            .into_iter()
            .flatten()
            .collect()
    }

    fn ingest(
        &mut self,
        slave: &SharedSlave,
        conn: &Connection,
        msg: ProtocolMessage,
    ) -> anyhow::Result<()> {
        if let Some(key) = signature_request_key(&msg) {
            let (admission, size) = {
                let mut guard = slave.lock().expect("slave");
                guard.note_inbound(&msg);
                let size = guard.announced_size(&key.0, &key.1).ok().flatten();
                let admission = guard.tune().fulfill_admission().expect("slave role");
                (admission, size)
            };
            if self.suppress_ask(&key) {
                return Ok(());
            }
            if !self.admits(admission, size) {
                self.parked
                    .insert(key, ParkedAsk { msg, size }, Instant::now());
                return Ok(());
            }
            return self.start_ask(slave, conn, msg, size);
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

    /// A second ask for the same key is dropped only while that ask is parked
    /// or on the wire. After `on_bulk_done` removes it, a later ask fulfills
    /// again. Remembering every finished ask left the master holding
    /// `origin_bytes` after a send the slave would not repeat.
    fn suppress_ask(&self, key: &AskKey) -> bool {
        self.sending.get(key).is_some() || self.parked.get(key).is_some()
    }

    fn admits(&self, admission: FulfillAdmission, size: Option<u64>) -> bool {
        admission.admits(
            self.sending.len(),
            self.sending.values().any(|large| *large),
            size,
        )
    }

    fn start_ask(
        &mut self,
        slave: &SharedSlave,
        conn: &Connection,
        msg: ProtocolMessage,
        size: Option<u64>,
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
            let large = size.is_some_and(FulfillAdmission::is_large);
            (host, large)
        };
        let key = (checkout_id.clone(), path.clone(), want_hash);
        self.sending.insert(key.clone(), large, Instant::now());
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
                        key,
                        control: Vec::new(),
                    }
                }
                Ok(Ok(Reply::Send(control))) => BulkDone {
                    result: Ok(()),
                    key,
                    control,
                },
                Ok(Ok(Reply::Hangup { reason })) => BulkDone {
                    result: Err(anyhow::anyhow!("{reason}")),
                    key,
                    control: Vec::new(),
                },
                Ok(Err(err)) => BulkDone {
                    result: Err(anyhow::anyhow!("{err}")),
                    key,
                    control: Vec::new(),
                },
                Err(err) => BulkDone {
                    result: Err(anyhow::anyhow!("{err}")),
                    key,
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
        self.sending.remove(&done.key);
        done.result?;
        for msg in &done.control {
            slave.lock().expect("slave").note_outbound(msg);
        }
        self.enqueue(done.control)?;
        self.kick(slave, conn)
    }

    fn kick(&mut self, slave: &SharedSlave, conn: &Connection) -> anyhow::Result<()> {
        let admission = slave
            .lock()
            .expect("slave")
            .tune()
            .fulfill_admission()
            .expect("slave role");
        loop {
            let Some(key) = next_parked_key(
                &self.parked,
                self.sending.len(),
                self.sending.values().any(|large| *large),
                admission,
            ) else {
                return Ok(());
            };
            let ask = self.parked.remove(&key).expect("oldest parked ask");
            self.start_ask(slave, conn, ask.msg, ask.size)?;
        }
    }
}

fn next_parked_key(
    parked: &Waiting<AskKey, ParkedAsk>,
    sending: usize,
    any_large: bool,
    admission: FulfillAdmission,
) -> Option<AskKey> {
    parked
        .oldest_key_where(|_, ask| admission.admits(sending, any_large, ask.size))
        .cloned()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_finished_ask_is_not_suppressed() {
        let (urgent_tx, _urgent_rx) = unbounded_channel();
        let (walk_tx, _walk_rx) = unbounded_channel();
        let (bulk_tx, _bulk_rx) = unbounded_channel();
        let mut outbound = Outbound::new(urgent_tx, walk_tx, bulk_tx);
        let key = (
            "src".into(),
            CanonicalPath::parse("/src/a.txt").unwrap(),
            ContentHash::ZERO,
        );
        assert!(!outbound.suppress_ask(&key));
        outbound.sending.insert(key.clone(), false, Instant::now());
        assert!(outbound.suppress_ask(&key));
        outbound.sending.remove(&key);
        assert!(
            !outbound.suppress_ask(&key),
            "a send that finished must accept the master's re-ask"
        );
        outbound.parked.insert(
            key.clone(),
            ParkedAsk {
                msg: ProtocolMessage::SignatureRequest {
                    checkout_id: "src".into(),
                    path: key.1.clone(),
                    want_hash: key.2,
                    signature: Vec::new(),
                },
                size: None,
            },
            Instant::now(),
        );
        assert!(outbound.suppress_ask(&key));
    }
}

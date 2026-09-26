use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use arborsync_core::ReloadError;
use arborsync_core::hashing::HashNeed;
use arborsync_core::keys::{format_hex_key, public_from_secret, read_static_key};
use arborsync_core::master::{
    ApplyBulkPlan, ContentHook, FulfillPlan, Master, PreparedDelete, Reply, WholeFileLater,
    reclaim_tree, survey_central,
};
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
use tokio::sync::Notify;
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
    {
        let guard = master.lock().expect("master");
        log::info!(
            "tune hashing.workers={} from={}",
            guard.tune().hashing_workers().get(),
            guard.tune().hashing_workers_spec()
        );
    }
    let endpoint = listen(listen_addr, &secret)?;
    log::info!(
        "master watching {} and listening on {}",
        master.lock().expect("master").central_root().display(),
        endpoint.local_addr()?
    );

    let watch_gen = Arc::new(AtomicU64::new(0));
    let watched = master.clone();
    let watched_gen = watch_gen.clone();
    let rt = tokio::runtime::Handle::current();
    let hash_inflight = Arc::new(AtomicU64::new(0));
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
            if let Err(err) = watch_central(
                &watched,
                debounce,
                rescan_every,
                &watched_gen,
                start_gen,
                &rt,
                &hash_inflight,
            ) {
                log::warn!("filesystem watcher stopped: {err}");
            }
            match rescan_off_lock(&watched) {
                Ok(plan) => submit_master_hashes(&rt, &watched, &hash_inflight, plan.hash),
                Err(err) => log::warn!("rescan failed: {err}"),
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
    let wipe_ready = Arc::new(Notify::new());
    let mut parked: VecDeque<ProtocolMessage> = VecDeque::new();
    let drive = SessionDrive {
        master: &master,
        peer,
        slave_id: &slave_id,
        conn: &conn,
        write_tx: &write_tx,
        limiter: &limiter,
        ip,
        wipe_ready: &wipe_ready,
    };
    let session = async {
        loop {
            if drain_parked(&drive, &mut parked).await? {
                break;
            }
            tokio::select! {
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        break;
                    }
                }
                msg = Connection::read_control(&mut recv) => {
                    let msg = msg?;
                    if control_waits(&master, &msg) {
                        parked.push_back(msg);
                    } else if drive_control(&drive, &mut parked, msg).await? {
                        break;
                    }
                }
                incoming = conn.accept_uni() => {
                    let mut recv = incoming.map_err(stream_err)?;
                    let (header, body) = read_bulk(&mut recv).await?;
                    let plan = {
                        let mut guard = master.lock().expect("master");
                        guard.begin_apply_bulk(peer, body.len() as u64);
                        guard.prepare_apply_bulk(peer, header)?
                    };
                    let reply = match plan {
                        ApplyBulkPlan::Done(reply) => reply,
                        ApplyBulkPlan::Reconstruct(job) => {
                            let outcome =
                                tokio::task::spawn_blocking(move || job.reconstruct(&body))
                                    .await
                                    .map_err(|err| anyhow::anyhow!("apply join: {err}"))?;
                            master
                                .lock()
                                .expect("master")
                                .finish_apply_bulk(peer, outcome)?
                        }
                    };
                    master
                        .lock()
                        .expect("master")
                        .end_apply_bulk(peer, &reply);
                    if dispatch_master(&master, peer, &slave_id, &conn, &write_tx, reply, &limiter, ip)? {
                        break;
                    }
                }
                frame = conn.recv_datagram() => {
                    master.lock().expect("master").observe_gauge(peer, &frame);
                }
                Some(err) = write_err_rx.recv() => return Err(err.into()),
                _ = wipe_ready.notified() => {}
                _ = tick.tick() => flush_outbox(&master, peer, &write_tx)?,
            }
        }
        Ok(())
    }
    .await;

    release_session(
        &sessions,
        &*master,
        &slave_id,
        peer,
        *stop_rx.borrow(),
    );
    session
}

fn release_session<S: Storage, C: ContentHook>(
    sessions: &Mutex<HashMap<String, SessionHandle>>,
    master: &Mutex<Master<S, C>>,
    slave_id: &str,
    peer: [u8; 32],
    replaced: bool,
) {
    if replaced {
        master.lock().expect("master").drop_pending(peer);
        return;
    }
    sessions.lock().expect("sessions").remove(slave_id);
    master.lock().expect("master").disconnect(peer);
}

struct SessionDrive<'a> {
    master: &'a SharedMaster,
    peer: [u8; 32],
    slave_id: &'a str,
    conn: &'a quinn::Connection,
    write_tx: &'a UnboundedSender<Vec<ProtocolMessage>>,
    limiter: &'a Mutex<AttemptLimiter>,
    ip: std::net::IpAddr,
    wipe_ready: &'a Arc<Notify>,
}

fn control_waits(master: &SharedMaster, msg: &ProtocolMessage) -> bool {
    let guard = master.lock().expect("master");
    match msg {
        ProtocolMessage::FileAnnounce { path, .. } | ProtocolMessage::Delete { path, .. } => {
            guard.overlaps_wipe(path)
        }
        ProtocolMessage::Rename { from, to, .. } => {
            guard.overlaps_wipe(from) || guard.overlaps_wipe(to)
        }
        _ => false,
    }
}

async fn drain_parked(
    drive: &SessionDrive<'_>,
    parked: &mut VecDeque<ProtocolMessage>,
) -> anyhow::Result<bool> {
    let mut index = 0;
    while index < parked.len() {
        if control_waits(drive.master, &parked[index]) {
            index += 1;
            continue;
        }
        let msg = parked.remove(index).expect("parked index");
        if drive_control(drive, parked, msg).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn drive_control(
    drive: &SessionDrive<'_>,
    parked: &mut VecDeque<ProtocolMessage>,
    msg: ProtocolMessage,
) -> anyhow::Result<bool> {
    if control_waits(drive.master, &msg) {
        parked.push_back(msg);
        return Ok(false);
    }
    if let ProtocolMessage::SignatureRequest {
        checkout_id,
        path,
        want_hash,
        signature,
    } = msg
    {
        let plan = drive.master.lock().expect("master").plan_fulfill(
            drive.peer,
            checkout_id,
            path,
            want_hash,
            signature,
        )?;
        return match plan {
            FulfillPlan::Send(out) => dispatch_master(
                drive.master,
                drive.peer,
                drive.slave_id,
                drive.conn,
                drive.write_tx,
                Reply::Send(out),
                drive.limiter,
                drive.ip,
            ),
            job @ FulfillPlan::BulkHost { .. } => {
                let conn = drive.conn.clone();
                let write_tx = drive.write_tx.clone();
                tokio::spawn(async move {
                    match tokio::task::spawn_blocking(move || job.run()).await {
                        Ok(Ok(Reply::Bulk(xfer))) => {
                            if let Err(err) = conn.write_bulk(&xfer).await {
                                log::warn!("master bulk send: {err:#}");
                            }
                        }
                        Ok(Ok(Reply::Send(out))) => {
                            if let Err(err) = fulfill_control(&write_tx, out) {
                                log::warn!("master fulfill: {err:#}");
                            }
                        }
                        Ok(Ok(Reply::Quiet)) => {}
                        Ok(Ok(Reply::Hangup { reason, .. })) => {
                            log::warn!("master fulfill hangup: {reason}");
                        }
                        Ok(Err(err)) => log::warn!("master fulfill: {err}"),
                        Err(err) => log::warn!("master fulfill join: {err}"),
                    }
                });
                Ok(false)
            }
        };
    }

    if let ProtocolMessage::Delete {
        checkout_id,
        path,
        basis,
    } = msg
    {
        let prepared = drive.master.lock().expect("master").prepare_delete(
            drive.peer,
            checkout_id,
            path,
            basis,
        )?;
        return match prepared {
            PreparedDelete::Later {
                checkout_id,
                path,
                basis,
            } => {
                parked.push_back(ProtocolMessage::Delete {
                    checkout_id,
                    path,
                    basis,
                });
                Ok(false)
            }
            PreparedDelete::Reply(out) => dispatch_master(
                drive.master,
                drive.peer,
                drive.slave_id,
                drive.conn,
                drive.write_tx,
                Reply::Send(out),
                drive.limiter,
                drive.ip,
            ),
            PreparedDelete::Reclaim { reply, root, path } => {
                let hangup = dispatch_master(
                    drive.master,
                    drive.peer,
                    drive.slave_id,
                    drive.conn,
                    drive.write_tx,
                    Reply::Send(reply),
                    drive.limiter,
                    drive.ip,
                )?;
                let master = drive.master.clone();
                let ready = drive.wipe_ready.clone();
                let logged = path.clone();
                tokio::spawn(async move {
                    match tokio::task::spawn_blocking(move || reclaim_tree(&root, &path)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => log::warn!("reclaim {}: {err}", logged.as_str()),
                        Err(err) => log::warn!("reclaim {}: {err}", logged.as_str()),
                    }
                    master.lock().expect("master").finish_wipe(&logged);
                    ready.notify_waiters();
                });
                Ok(hangup)
            }
        };
    }

    let reply = drive
        .master
        .lock()
        .expect("master")
        .handle(drive.peer, msg)?;
    dispatch_master(
        drive.master,
        drive.peer,
        drive.slave_id,
        drive.conn,
        drive.write_tx,
        reply,
        drive.limiter,
        drive.ip,
    )
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
        Reply::Quiet => {
            flush_outbox(master, peer, write_tx)?;
            Ok(false)
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

fn fulfill_control(
    write_tx: &UnboundedSender<Vec<ProtocolMessage>>,
    out: ProtocolMessage,
) -> anyhow::Result<()> {
    enqueue_control(write_tx, vec![out])
}

fn rescan_off_lock(master: &SharedMaster) -> anyhow::Result<arborsync_core::HashPlan> {
    let (root, store) = {
        let guard = master.lock().expect("master");
        (guard.central_root().to_path_buf(), guard.storage_handle())
    };
    let walk = survey_central(&root, &store)?;
    Ok(master.lock().expect("master").adopt_survey(walk)?)
}

fn watch_central(
    master: &SharedMaster,
    debounce: Duration,
    rescan_every: Duration,
    watch_gen: &AtomicU64,
    start_gen: u64,
    rt: &tokio::runtime::Handle,
    hash_inflight: &Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let root = master.lock().expect("master").central_root().to_path_buf();
    let (tx, rx) = mpsc::channel::<DebounceEventResult>();
    let mut debouncer = new_debouncer(debounce, None, tx)?;
    debouncer
        .watch(&root, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", root.display()))?;
    submit_master_hashes(rt, master, hash_inflight, rescan_off_lock(master)?.hash);

    loop {
        if watch_gen.load(Ordering::Relaxed) != start_gen {
            return Ok(());
        }
        match rx.recv_timeout(rescan_every) {
            Ok(Ok(events)) => {
                let (need_rescan, mapped) = crate::watch::classify(events);
                if need_rescan {
                    submit_master_hashes(rt, master, hash_inflight, rescan_off_lock(master)?.hash);
                }
                let locals = to_local_events(mapped, |host| host_to_canonical(&root, host).ok());
                for event in locals {
                    let plan = master.lock().expect("master").plan_local(event)?;
                    submit_master_hashes(rt, master, hash_inflight, plan.hash);
                }
            }
            Ok(Err(errs)) => {
                log::warn!("watch error, rescanning: {errs:?}");
                submit_master_hashes(rt, master, hash_inflight, rescan_off_lock(master)?.hash);
            }
            Err(RecvTimeoutError::Timeout) => {
                submit_master_hashes(rt, master, hash_inflight, rescan_off_lock(master)?.hash)
            }
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

fn submit_master_hashes(
    rt: &tokio::runtime::Handle,
    master: &SharedMaster,
    inflight: &Arc<AtomicU64>,
    needs: Vec<HashNeed>,
) {
    for need in needs {
        let master = master.clone();
        let inflight = inflight.clone();
        rt.spawn(async move {
            let cap = master
                .lock()
                .expect("master")
                .tune()
                .hashing_workers()
                .get() as u64;
            while inflight.load(Ordering::Relaxed) >= cap {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            inflight.fetch_add(1, Ordering::Relaxed);
            master.lock().expect("master").start_hashed(&need.key);
            let done = tokio::task::spawn_blocking(move || need.run()).await;
            inflight.fetch_sub(1, Ordering::Relaxed);
            match done {
                Ok(mut done) => loop {
                    let follow = {
                        let mut guard = master.lock().expect("master");
                        match guard.commit_hashed(done) {
                            Ok(plan) if plan.hash.is_empty() => None,
                            Ok(mut plan) => Some(Ok(plan.hash.remove(0))),
                            Err(err) => Some(Err(err)),
                        }
                    };
                    match follow {
                        None => break,
                        Some(Ok(need)) => {
                            match tokio::task::spawn_blocking(move || need.run()).await {
                                Ok(next) => done = next,
                                Err(err) => {
                                    log::warn!("hash join: {err}");
                                    break;
                                }
                            }
                        }
                        Some(Err(err)) => {
                            log::warn!("commit hashed: {err}");
                            break;
                        }
                    }
                },
                Err(err) => log::warn!("hash join: {err}"),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{fulfill_control, release_session};
    use arborsync_core::LoadedMaster;
    use arborsync_core::config::SlaveAcl;
    use arborsync_core::keys::format_hex_key;
    use arborsync_core::master::{Master, MemoryContent, Reply};
    use arborsync_core::meta::{hash_bytes, FileMetadata};
    use arborsync_core::protocol::{BulkEncoding, BulkHeader, CheckoutRef, ProtocolMessage};
    use arborsync_core::test_support::{MemoryStorage, SyncSandbox, p};
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn a_failed_fulfill_is_written_on_the_control_stream() {
        let (tx, mut rx) = unbounded_channel();
        let msg = ProtocolMessage::Error {
            code: "missing_hash".into(),
            message: "/src/gone".into(),
        };
        fulfill_control(&tx, msg.clone()).unwrap();
        assert_eq!(
            rx.try_recv().ok(),
            Some(vec![msg]),
            "the asker was not told the fulfill failed"
        );
    }

    const ALICE: [u8; 32] = [0xA1; 32];

    fn subscribed_master() -> Mutex<Master<MemoryStorage, MemoryContent>> {
        let sandbox = SyncSandbox::new();
        let cfg_path = sandbox.write_master_config(vec![SlaveAcl {
            id: "dev-alice".into(),
            public_keys: vec![format_hex_key(&ALICE)],
            allowed_prefixes: vec!["/src".into()],
        }]);
        let cfg = LoadedMaster::load(&cfg_path).unwrap();
        let mut master = Master::open(cfg, MemoryStorage::new(), MemoryContent::new()).unwrap();
        master
            .handle(
                ALICE,
                ProtocolMessage::Subscribe {
                    slave_id: "dev-alice".into(),
                    checkouts: vec![CheckoutRef {
                        id: "src".into(),
                        central: p("/src"),
                    }],
                },
            )
            .unwrap();
        Mutex::new(master)
    }

    #[test]
    fn ending_a_session_drops_the_slave_from_the_roster() {
        let master = subscribed_master();
        let sessions = Mutex::new(std::collections::HashMap::new());
        release_session(&sessions, &master, "dev-alice", ALICE, false);
        assert_eq!(
            master.lock().expect("master").take_status().connected,
            0,
            "a finished session still counted as connected"
        );
    }

    #[test]
    fn a_replaced_session_keeps_the_new_roster_entry() {
        let master = subscribed_master();
        let sessions = Mutex::new(std::collections::HashMap::new());
        release_session(&sessions, &master, "dev-alice", ALICE, true);
        assert_eq!(
            master.lock().expect("master").take_status().connected,
            1,
            "the replacement was dropped with the old task"
        );
    }

    #[test]
    fn a_replaced_session_drops_that_peers_pending() {
        let sandbox = SyncSandbox::new();
        let cfg_path = sandbox.write_master_config(vec![SlaveAcl {
            id: "dev-alice".into(),
            public_keys: vec![format_hex_key(&ALICE)],
            allowed_prefixes: vec!["/src".into()],
        }]);
        let mut master = Master::open(
            LoadedMaster::load(&cfg_path).unwrap(),
            MemoryStorage::new(),
            MemoryContent::new(),
        )
        .unwrap();
        master
            .handle(
                ALICE,
                ProtocolMessage::Subscribe {
                    slave_id: "dev-alice".into(),
                    checkouts: vec![CheckoutRef {
                        id: "src".into(),
                        central: p("/src"),
                    }],
                },
            )
            .unwrap();
        let body = b"pending-bytes";
        let new = FileMetadata::file(body.len() as u64, 1_700_000_000_000, 0o100644, hash_bytes(body));
        match master
            .handle(
                ALICE,
                ProtocolMessage::FileAnnounce {
                    checkout_id: "src".into(),
                    path: p("/src/hello.txt"),
                    new,
                    basis: None,
                },
            )
            .unwrap()
        {
            Reply::Send(ProtocolMessage::SignatureRequest { .. }) => {}
            other => panic!("expected SignatureRequest, got {other:?}"),
        }
        let master = Mutex::new(master);
        let sessions = Mutex::new(std::collections::HashMap::new());
        release_session(&sessions, &master, "dev-alice", ALICE, true);
        assert_eq!(master.lock().expect("master").take_status().connected, 1);
        match master
            .lock()
            .expect("master")
            .apply_bulk(
                ALICE,
                BulkHeader {
                    path: p("/src/hello.txt"),
                    checkout_id: "src".into(),
                    want_hash: hash_bytes(body),
                    encoding: BulkEncoding::Whole,
                    size: body.len() as u64,
                },
                body,
            )
            .unwrap()
        {
            Reply::Send(ProtocolMessage::Error { code, .. }) => {
                assert_eq!(code, "unknown_transfer")
            }
            other => panic!("expected unknown_transfer, got {other:?}"),
        }
        assert!(!sandbox.central_root().join("src/hello.txt").exists());
    }
}

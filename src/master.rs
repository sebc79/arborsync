use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use arborsync_core::keys::read_static_key;
use arborsync_core::master::{LocalEvent, Master, Reply, WholeFileLater};
use arborsync_core::path::host_to_canonical;
use arborsync_core::storage::Storage;
use arborsync_core::transport::{
    AttemptLimiter, accept_control, listen, peer_static_key, read_control, write_control,
};
use arborsync_core::{LoadedMaster, RedbStorage};
use notify_debouncer_mini::notify::RecursiveMode;
use notify_debouncer_mini::{DebounceEventResult, new_debouncer};
use quinn::Incoming;

const DEFAULT_CONFIG: &str = "/etc/arborsync/master.toml";

type SharedMaster = Arc<Mutex<Master<RedbStorage, WholeFileLater>>>;

pub fn run(config: Option<PathBuf>) -> anyhow::Result<()> {
    tokio::runtime::Runtime::new()?.block_on(run_async(config))
}

async fn run_async(config: Option<PathBuf>) -> anyhow::Result<()> {
    let config_path = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG));
    let cfg = LoadedMaster::load(&config_path)
        .with_context(|| format!("load {}", config_path.display()))?;

    let secret = read_static_key(cfg.master_key_path())
        .with_context(|| format!("read {}", cfg.master_key_path().display()))?;

    if let Some(parent) = cfg.db_path().parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let store = RedbStorage::open(cfg.db_path())
        .with_context(|| format!("open index {}", cfg.db_path().display()))?;

    let debounce = Duration::from_millis(cfg.watcher_debounce_ms());
    let rescan_every = Duration::from_secs(cfg.rescan_interval_seconds());
    let listen_addr = cfg.listen_addr();
    let max_attempts = cfg.max_connection_attempts_per_minute();

    let master = Arc::new(Mutex::new(Master::open(cfg, store, WholeFileLater)?));
    let endpoint = listen(listen_addr, &secret)?;
    log::info!(
        "master watching {} and listening on {}",
        master.lock().expect("master").central_root().display(),
        endpoint.local_addr()?
    );

    let watched = master.clone();
    std::thread::spawn(move || {
        loop {
            if let Err(err) = watch_central(&watched, debounce, rescan_every) {
                log::warn!("filesystem watcher stopped: {err}");
            }
            if let Err(err) = watched.lock().expect("master").rescan() {
                log::warn!("rescan failed: {err}");
            }
            log::warn!("rescanning and re-arming");
        }
    });

    let limiter = Arc::new(Mutex::new(AttemptLimiter::new(max_attempts)));
    let sessions = Arc::new(Mutex::new(HashMap::<
        String,
        tokio::sync::watch::Sender<bool>,
    >::new()));

    while let Some(incoming) = endpoint.accept().await {
        let master = master.clone();
        let limiter = limiter.clone();
        let sessions = sessions.clone();
        tokio::spawn(async move {
            if let Err(err) = accept_session(incoming, master, limiter, sessions).await {
                log::warn!("{err:#}");
            }
        });
    }
    Ok(())
}

async fn accept_session(
    incoming: Incoming,
    master: SharedMaster,
    limiter: Arc<Mutex<AttemptLimiter>>,
    sessions: Arc<Mutex<HashMap<String, tokio::sync::watch::Sender<bool>>>>,
) -> anyhow::Result<()> {
    let conn = incoming.await.context("handshake")?;
    let peer = peer_static_key(&conn).context("peer static key")?;
    let ip = conn.remote_address().ip();
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
        if let Some(previous) = live.insert(slave_id.clone(), stop_tx) {
            let _ = previous.send(true);
        }
    }

    let (mut send, mut recv) = accept_control(&conn).await?;
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    break;
                }
            }
            msg = read_control(&mut recv) => {
                let msg = msg?;
                let reply = master.lock().expect("master").handle(peer, msg)?;
                match reply {
                    Reply::Hangup { reason } => {
                        log::info!("hangup {slave_id}: {reason}");
                        master.lock().expect("master").disconnect(peer);
                        break;
                    }
                    Reply::Send(out) => {
                        write_control(&mut send, &out).await?;
                        flush_outbox(&master, peer, &mut send).await?;
                    }
                }
            }
            _ = tick.tick() => flush_outbox(&master, peer, &mut send).await?,
        }
    }

    if !*stop_rx.borrow() {
        sessions.lock().expect("sessions").remove(&slave_id);
        master.lock().expect("master").disconnect(peer);
    }
    Ok(())
}

async fn flush_outbox(
    master: &SharedMaster,
    peer: [u8; 32],
    send: &mut quinn::SendStream,
) -> anyhow::Result<()> {
    let pending = master.lock().expect("master").poll(peer);
    for msg in pending {
        write_control(send, &msg).await?;
    }
    Ok(())
}

fn watch_central(
    master: &SharedMaster,
    debounce: Duration,
    rescan_every: Duration,
) -> anyhow::Result<()> {
    let root = master.lock().expect("master").central_root().to_path_buf();
    let (tx, rx) = mpsc::channel::<DebounceEventResult>();
    let mut debouncer = new_debouncer(debounce, tx)?;
    debouncer
        .watcher()
        .watch(&root, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", root.display()))?;
    master.lock().expect("master").rescan()?;

    loop {
        match rx.recv_timeout(rescan_every) {
            Ok(Ok(events)) => {
                for event in events {
                    note(master, &root, &event.path)?;
                }
            }
            Ok(Err(err)) => {
                log::warn!("watch error, rescanning: {err}");
                master.lock().expect("master").rescan()?;
            }
            Err(RecvTimeoutError::Timeout) => master.lock().expect("master").rescan()?,
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

fn note(master: &SharedMaster, root: &Path, host: &Path) -> anyhow::Result<()> {
    match host_to_canonical(root, host) {
        Ok(path) => Ok(master
            .lock()
            .expect("master")
            .note_local(LocalEvent::Changed(path))?),
        Err(err) => {
            log::warn!("skipping {}: {err}", host.display());
            Ok(())
        }
    }
}

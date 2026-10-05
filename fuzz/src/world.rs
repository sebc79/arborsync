use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use arborsync_core::config::{CheckoutConfig, MasterConfig, SlaveAcl, SlaveConfig};
use arborsync_core::meta::hash_bytes;
use arborsync_core::tune::TuneSpec;

use crate::oracle::{
    ActorView, EntryKey, Health, LocalSnap, Observed, Residue, SnapBody, StatusSnap, TreeSnap,
};
use crate::schedule::{Actor, Bin, DiskOp, Layout, MtimeNs, RelPath, SecondView, SpecialFile, UnixMode, WorldOp};
use crate::types::{Finding, HarnessError};

const SETTLE_LIMIT: Duration = Duration::from_secs(30);
const SIGKILL: i32 = 9;
const SIGCONT: i32 = 18;
const SIGSTOP: i32 = 19;
const O_WRONLY: i32 = 1;
const O_CREAT: i32 = 0x40;
const O_APPEND: i32 = 0x400;
const O_CLOEXEC: i32 = 0x80000;
const AT_FDCWD: i32 = -100;
const WNOHANG: i32 = 1;

unsafe extern "C" {
    fn open(path: *const i8, flags: i32, mode: u32) -> i32;
    fn write(fd: i32, buf: *const u8, count: usize) -> isize;
    fn close(fd: i32) -> i32;
    fn getpid() -> i32;
    fn kill(pid: i32, sig: i32) -> i32;
    fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
    fn utimensat(fd: i32, path: *const i8, times: *const Timespec, flag: i32) -> i32;
    fn mkfifo(path: *const i8, mode: u32) -> i32;
    fn mknod(path: *const i8, mode: u32, dev: u64) -> i32;
}

#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

/// Directory this process created for one run.
pub(crate) struct PrivateRoot {
    path: PathBuf,
}

impl PrivateRoot {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

/// `127.0.0.1` and a port the kernel assigned. Port 0 cannot be represented.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BoundAddr {
    addr: SocketAddr,
}

impl BoundAddr {
    pub(crate) fn addr(self) -> SocketAddr {
        self.addr
    }
}

/// `listening on <ip>:<port>` from the master log.
/// `None` when the line is not that sentence, the port is 0, or the
/// address is not `127.0.0.1`.
pub(crate) fn parse_listening(line: &str) -> Option<BoundAddr> {
    let rest = line.split_once("listening on ")?.1;
    let token = rest.split_whitespace().next()?.trim().trim_end_matches('.');
    let addr: SocketAddr = token.parse().ok()?;
    if addr.port() == 0 {
        return None;
    }
    if addr.ip() != IpAddr::V4(Ipv4Addr::LOCALHOST) {
        return None;
    }
    Some(BoundAddr { addr })
}

/// One daemon status line. `None` when the line is not a status line.
pub(crate) fn parse_status(line: &str) -> Option<StatusSnap> {
    if !line.contains("status ") || !line.contains("health=") || line.contains("status slave=") {
        return None;
    }
    let health = token_after(line, "health=")?;
    let pending = token_after(line, "pending=")?.parse().ok()?;
    let rescan = token_after(line, "rescan=")?.parse().ok()?;
    let last_error = line.split_once("last_error=")?.1.trim().to_string();
    Some(StatusSnap {
        health: parse_health(&health),
        pending,
        rescan,
        last_error,
    })
}

fn token_after(line: &str, key: &str) -> Option<String> {
    let rest = line.split_once(key)?.1;
    Some(rest.split_whitespace().next()?.to_string())
}

fn parse_health(token: &str) -> Health {
    match token {
        "idle" => Health::Idle,
        "busy" => Health::Busy,
        "stuck" => Health::Stuck,
        "failed" => Health::Failed,
        other => Health::Other(other.to_string()),
    }
}

pub(crate) fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let rc = unsafe { kill(pid as i32, 0) };
    if rc == 0 {
        return true;
    }
    // EPERM means the pid exists and is owned by someone else.
    std::io::Error::last_os_error().raw_os_error() == Some(1)
}

/// Kills every recorded daemon whose `owner` pid is dead, then deletes
/// that root. A live owner is a concurrent run and is skipped. Safe to
/// call twice. Never returns a root for the caller to reuse.
pub(crate) fn reap_abandoned() -> Result<(), HarnessError> {
    let temp = std::env::temp_dir();
    let entries = fs::read_dir(&temp).map_err(|err| {
        HarnessError::Io(format!("read {}: {err}", temp.display()))
    })?;
    for entry in entries {
        let entry = entry.map_err(|err| HarnessError::Io(err.to_string()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("arborsync-fuzz-") {
            continue;
        }
        let path = entry.path();
        let meta = match fs::symlink_metadata(&path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(HarnessError::Io(err.to_string())),
        };
        if !meta.is_dir() {
            continue;
        }
        reap_one(&path)?;
    }
    Ok(())
}

fn reap_one(root: &Path) -> Result<(), HarnessError> {
    let text = match fs::read_to_string(root.join("manifest")) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(HarnessError::Io(err.to_string())),
    };
    let Some(owner) = manifest_owner(&text) else {
        return Ok(());
    };
    if pid_alive(owner) {
        return Ok(());
    }
    for pid in manifest_pids(&text) {
        reap_pid(pid);
    }
    match fs::remove_dir_all(root) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(HarnessError::Io(format!("remove {}: {err}", root.display()))),
    }
}

fn manifest_owner(text: &str) -> Option<u32> {
    for line in text.lines() {
        if let Some(pid) = line.trim().strip_prefix("owner ") {
            return pid.trim().parse().ok();
        }
    }
    None
}

fn manifest_pids(text: &str) -> Vec<u32> {
    text.lines()
        .filter_map(|line| line.trim().strip_prefix("pid "))
        .filter_map(|pid| pid.trim().parse().ok())
        .collect()
}

fn reap_pid(pid: u32) {
    if pid == 0 {
        return;
    }
    unsafe { kill(pid as i32, SIGKILL) };
    let mut status = 0;
    for _ in 0..50 {
        let rc = unsafe { waitpid(pid as i32, &mut status, WNOHANG) };
        if rc != 0 || !pid_alive(pid) {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Claimed,
    MasterBound,
    Live,
    Closed,
}

/// Capability returned by [`World::freeze`]. Not a path.
pub(crate) struct FreezeToken {
    actor: Actor,
}

struct Daemon {
    actor: Actor,
    pid: u32,
    log_path: PathBuf,
    log_offset: u64,
    rescan_sum: u64,
    status: Option<StatusSnap>,
    child: Child,
}

struct SlaveDraft {
    id: String,
    index: u8,
    key_path: PathBuf,
    public: String,
    db_path: PathBuf,
    checkout: PathBuf,
    central: String,
    peer_socket: PathBuf,
    toml_path: PathBuf,
    log_path: PathBuf,
    prefix: String,
}

pub(crate) struct World {
    root: PrivateRoot,
    layout: Layout,
    phase: Phase,
    addr: Option<BoundAddr>,
    yard: [u8; 32],
    yard_path: PathBuf,
    central_tree: PathBuf,
    central_root: PathBuf,
    master_toml: PathBuf,
    master_log: PathBuf,
    manifest: PathBuf,
    child_tmp: PathBuf,
    bin: PathBuf,
    owner: u32,
    master_public: String,
    daemons: Vec<Daemon>,
    slaves: Vec<SlaveDraft>,
    frozen: BTreeSet<Actor>,
}

/// The product came up, or it already failed and the world is still closable.
pub(crate) enum Boot {
    Up,
    Failed(Finding),
}

pub(crate) enum Wait {
    Idle(Observed),
    Failed(Finding),
}

impl World {
    /// Mint a new root, write keys with `arborsync keygen`, and write the
    /// master TOML. Does not spawn and does not reopen an existing root.
    pub(crate) fn claim(bin: &Bin, layout: &Layout) -> Result<Self, HarnessError> {
        let root = create_root()?;
        let owner = std::process::id();
        let manifest = root.path().join("manifest");
        write_mode(
            &manifest,
            &format!("owner {owner}\n"),
            0o600,
        )?;
        let child_tmp = root.path().join("tmp");
        fs::create_dir_all(&child_tmp).map_err(ioe)?;
        let central_root = root.path().join("central");
        let central_tree = central_root.join("src");
        fs::create_dir_all(&central_tree).map_err(ioe)?;
        if matches!(
            layout,
            Layout::Two {
                second: SecondView::Nested
            }
        ) {
            fs::create_dir_all(central_tree.join("nested")).map_err(ioe)?;
        }
        let yard_dir = root.path().join("yard");
        fs::create_dir_all(&yard_dir).map_err(ioe)?;
        let yard_path = yard_dir.join("marker");
        fs::write(&yard_path, b"yard-marker-v1").map_err(ioe)?;
        let yard = hash_bytes(b"yard-marker-v1").into_bytes();
        fs::create_dir_all(root.path().join("epochs")).map_err(ioe)?;

        let master_key = root.path().join("master.key");
        let master_public = keygen(bin.path(), &master_key)?;
        guard_config_path(root.path(), &master_key)?;
        let master_db = root.path().join("master.redb");
        let master_toml = root.path().join("master.toml");
        let master_log = root.path().join("master.log");
        File::create(&master_log).map_err(ioe)?;

        let mut slaves = Vec::new();
        let mut acls = Vec::new();
        for index in 0..layout.slave_count().get() {
            let id = slave_id(index);
            let dir = root.path().join("slaves").join(id);
            fs::create_dir_all(dir.join("checkout")).map_err(ioe)?;
            let key_path = dir.join("slave.key");
            let public = keygen(bin.path(), &key_path)?;
            let prefix = central_prefix(layout, index);
            let draft = SlaveDraft {
                id: id.into(),
                index,
                key_path,
                public: public.clone(),
                db_path: dir.join("slave.redb"),
                checkout: dir.join("checkout"),
                central: prefix.into(),
                peer_socket: dir.join("peers.sock"),
                toml_path: dir.join("slave.toml"),
                log_path: dir.join("slave.log"),
                prefix: prefix.into(),
            };
            File::create(&draft.log_path).map_err(ioe)?;
            guard_config_path(root.path(), &draft.key_path)?;
            guard_config_path(root.path(), &draft.db_path)?;
            guard_config_path(root.path(), &draft.checkout)?;
            guard_config_path(root.path(), &draft.peer_socket)?;
            guard_config_path(root.path(), &draft.toml_path)?;
            acls.push(SlaveAcl {
                id: draft.id.clone(),
                public_keys: vec![draft.public.clone()],
                allowed_prefixes: vec![draft.prefix.clone()],
            });
            slaves.push(draft);
        }

        for path in [&central_root, &master_db, &master_toml, &master_log, &child_tmp, &yard_path] {
            guard_config_path(root.path(), path)?;
        }
        let master_cfg = MasterConfig {
            central_root: central_root.to_string_lossy().into_owned(),
            listen_addr: "127.0.0.1:0".into(),
            master_key_path: master_key.to_string_lossy().into_owned(),
            db_path: master_db.to_string_lossy().into_owned(),
            log_level: "info".into(),
            watcher_debounce_ms: 200,
            rescan_interval_seconds: 2,
            status_interval_seconds: 1,
            max_checkouts_per_slave: 100,
            max_connections: 100,
            max_connection_attempts_per_minute: 60,
            slaves: acls,
            tune: TuneSpec::default(),
        };
        let toml = master_cfg
            .to_toml()
            .map_err(|err| HarnessError::Io(err.to_string()))?;
        write_mode(&master_toml, &toml, 0o600)?;

        Ok(Self {
            root,
            layout: layout.clone(),
            phase: Phase::Claimed,
            addr: None,
            yard,
            yard_path,
            central_tree,
            central_root,
            master_toml,
            master_log,
            manifest,
            child_tmp,
            bin: bin.path().to_path_buf(),
            owner,
            master_public,
            daemons: Vec::new(),
            slaves,
            frozen: BTreeSet::new(),
        })
    }

    pub(crate) fn layout(&self) -> &Layout {
        &self.layout
    }

    pub(crate) fn yard(&self) -> [u8; 32] {
        self.yard
    }

    pub(crate) fn tree_root(&self, actor: Actor) -> &Path {
        match actor {
            Actor::Master => &self.central_tree,
            Actor::Slave(ix) => &self.slaves[ix.get() as usize].checkout,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn log_path(&self, actor: Actor) -> &Path {
        match actor {
            Actor::Master => &self.master_log,
            Actor::Slave(ix) => &self.slaves[ix.get() as usize].log_path,
        }
    }

    /// Spawn the master, parse [`BoundAddr`], write each slave TOML, spawn the slaves.
    pub(crate) fn spawn(&mut self) -> Result<Boot, HarnessError> {
        if self.phase != Phase::Claimed {
            return Err(HarnessError::Sandbox("spawn requires a claimed world".into()));
        }
        let mut master = self.spawn_daemon(Actor::Master, &self.master_toml.clone(), &self.master_log.clone())?;
        let ready = wait_line(&mut master, "listening on ", Duration::from_secs(15))?;
        let master = match ready {
            LineWait::Found(text) => {
                let addr = parse_listening(&text).ok_or_else(|| {
                    HarnessError::Sandbox(format!("master log has no bound address: {text}"))
                })?;
                self.addr = Some(addr);
                self.daemons.push(master);
                addr
            }
            LineWait::Died(note) => {
                self.daemons.push(master);
                return Ok(Boot::Failed(Finding::Crash {
                    actor: "master".into(),
                    note,
                }));
            }
            LineWait::TimedOut(note) => {
                self.daemons.push(master);
                return Ok(Boot::Failed(Finding::Hang {
                    actor: "master".into(),
                    last_status: note,
                }));
            }
        };
        self.phase = Phase::MasterBound;
        let addr = master;
        for index in 0..self.slaves.len() {
            self.write_slave_toml(index, addr)?;
            let toml = self.slaves[index].toml_path.clone();
            let log = self.slaves[index].log_path.clone();
            let actor = Actor::Slave(crate::schedule::SlaveIx::new(index as u8, self.layout.slave_count())?);
            let mut daemon = self.spawn_daemon(actor, &toml, &log)?;
            let ready = wait_line(&mut daemon, "connected to ", Duration::from_secs(15))?;
            match ready {
                LineWait::Found(_) => self.daemons.push(daemon),
                LineWait::Died(note) => {
                    self.daemons.push(daemon);
                    return Ok(Boot::Failed(Finding::Crash {
                        actor: actor.label(),
                        note,
                    }));
                }
                LineWait::TimedOut(note) => {
                    self.daemons.push(daemon);
                    return Ok(Boot::Failed(Finding::Hang {
                        actor: actor.label(),
                        last_status: note,
                    }));
                }
            }
        }
        self.phase = Phase::Live;
        Ok(Boot::Up)
    }

    pub(crate) fn freeze(&mut self, actor: Actor) -> Result<FreezeToken, HarnessError> {
        self.require_live()?;
        if self.frozen.contains(&actor) {
            return Err(HarnessError::Sandbox(format!(
                "{} is already frozen",
                actor.label()
            )));
        }
        let pid = self.daemon(actor)?.pid;
        signal(pid, SIGSTOP)?;
        wait_proc_state(pid, true)?;
        self.frozen.insert(actor);
        Ok(FreezeToken { actor })
    }

    pub(crate) fn apply(
        &mut self,
        token: &FreezeToken,
        op: &DiskOp,
    ) -> Result<LocalSnap, HarnessError> {
        self.require_live()?;
        if token.actor != op.actor() || !self.frozen.contains(&token.actor) {
            return Err(HarnessError::Sandbox(
                "apply requires the freeze token for that actor".into(),
            ));
        }
        let root = self.tree_root(token.actor).to_path_buf();
        let yard = self.yard_path.clone();
        apply_disk(&root, &yard, op)
    }

    pub(crate) fn thaw(&mut self, token: FreezeToken) -> Result<(), HarnessError> {
        self.require_live()?;
        if !self.frozen.remove(&token.actor) {
            return Err(HarnessError::Sandbox(format!(
                "{} is not frozen",
                token.actor.label()
            )));
        }
        let pid = self.daemon(token.actor)?.pid;
        signal(pid, SIGCONT)?;
        wait_proc_state(pid, false)?;
        Ok(())
    }

    pub(crate) fn probe(&mut self, op: &WorldOp) -> Result<Boot, HarnessError> {
        self.require_live()?;
        match op {
            WorldOp::Grammar(_) | WorldOp::BadBulk { .. } => Err(HarnessError::Sandbox(
                "grammar and bad bulk are not injected in this unit".into(),
            )),
            WorldOp::StruckReload => self.struck_reload(),
            WorldOp::Restore { epoch } => self.restore(*epoch),
        }
    }

    /// Block until each thawed daemon has logged a rescan after entry and
    /// `pending=0`, or 30s. The slave line must also be `health=idle`.
    /// The master line may stay `health=busy` while that window counts a
    /// rescan or a root report.
    pub(crate) fn wait_idle(&mut self) -> Result<Wait, HarnessError> {
        self.require_live()?;
        let _ = self.pump_logs()?;
        let thawed = self.thawed_actors();
        let mut quiet: BTreeMap<Actor, StatusSnap> = BTreeMap::new();
        let start: Vec<(Actor, u64)> = thawed
            .iter()
            .filter_map(|actor| {
                self.daemons
                    .iter()
                    .find(|daemon| daemon.actor == *actor)
                    .map(|daemon| (*actor, daemon.rescan_sum))
            })
            .collect();
        if thawed.is_empty() {
            return Ok(Wait::Idle(self.observe()?));
        }
        let deadline = Instant::now() + SETTLE_LIMIT;
        loop {
            if let Some(actor) = self.dead_thawed(&thawed)? {
                return Ok(Wait::Failed(Finding::Crash {
                    actor: actor.label(),
                    note: "process exited".into(),
                }));
            }
            for (actor, sum, status) in self.pump_logs()? {
                if matches!(status.health, Health::Stuck | Health::Failed) {
                    return Ok(Wait::Failed(Finding::Protocol {
                        actor: actor.label(),
                        token: status.last_error,
                    }));
                }
                let Some((_, baseline)) = start.iter().find(|(name, _)| *name == actor) else {
                    continue;
                };
                // Root reports and rescans share the master's status window, so
                // `health=busy` with `pending=0` is the master's drained line.
                // The slave does go `health=idle` between those windows.
                let drained = status.pending == 0
                    && !matches!(status.health, Health::Stuck | Health::Failed);
                let settled = match actor {
                    Actor::Master => drained,
                    Actor::Slave(_) => drained && status.health == Health::Idle,
                };
                if sum > *baseline && settled {
                    quiet.insert(actor, status);
                }
            }
            let ready = start.iter().all(|(actor, _)| quiet.contains_key(actor));
            if ready {
                for daemon in &mut self.daemons {
                    if let Some(status) = quiet.get(&daemon.actor) {
                        daemon.status = Some(status.clone());
                    }
                }
                return Ok(Wait::Idle(self.observe()?));
            }
            if Instant::now() >= deadline {
                let (actor, status) = self
                    .daemons
                    .iter()
                    .find(|daemon| {
                        thawed.contains(&daemon.actor) && !quiet.contains_key(&daemon.actor)
                    })
                    .map(|daemon| {
                        (
                            daemon.actor,
                            daemon
                                .status
                                .as_ref()
                                .map(|status| format!("{:?}", status.health))
                                .unwrap_or_else(|| "no status".into()),
                        )
                    })
                    .unwrap_or((Actor::Master, "no status".into()));
                let stuck = self.daemons.iter().any(|daemon| {
                    thawed.contains(&daemon.actor)
                        && matches!(
                            daemon.status.as_ref().map(|status| &status.health),
                            Some(Health::Stuck | Health::Failed)
                        )
                });
                if stuck {
                    return Ok(Wait::Failed(Finding::Protocol {
                        actor: actor.label(),
                        token: status,
                    }));
                }
                return Ok(Wait::Failed(Finding::Hang {
                    actor: actor.label(),
                    last_status: status,
                }));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub(crate) fn seal_epoch(&mut self, epoch: u32) -> Result<(), HarnessError> {
        let dest = self.root.path().join("epochs").join(epoch.to_string());
        if dest.exists() {
            fs::remove_dir_all(&dest).map_err(ioe)?;
        }
        copy_tree(&self.central_tree, &dest)?;
        Ok(())
    }

    /// SIGKILL every recorded pid, fsync the closed mark, then stay closed.
    /// A second call is a no-op.
    pub(crate) fn close(&mut self) {
        if self.phase == Phase::Closed {
            return;
        }
        let daemons = std::mem::take(&mut self.daemons);
        for mut daemon in daemons {
            reap_pid(daemon.pid);
            let _ = daemon.child.kill();
            let _ = daemon.child.wait();
        }
        if let Ok(mut file) = File::create(self.root.path().join("closed")) {
            let _ = file.write_all(b"closed\n");
            let _ = file.sync_all();
        }
        self.frozen.clear();
        self.phase = Phase::Closed;
    }

    fn require_live(&self) -> Result<(), HarnessError> {
        if self.phase == Phase::Live {
            Ok(())
        } else {
            Err(HarnessError::Sandbox("world is not live".into()))
        }
    }

    fn daemon(&self, actor: Actor) -> Result<&Daemon, HarnessError> {
        self.daemons
            .iter()
            .find(|daemon| daemon.actor == actor)
            .ok_or_else(|| HarnessError::Sandbox(format!("no daemon for {}", actor.label())))
    }

    fn thawed_actors(&self) -> Vec<Actor> {
        self.daemons
            .iter()
            .map(|daemon| daemon.actor)
            .filter(|actor| !self.frozen.contains(actor))
            .collect()
    }

    fn dead_thawed(&mut self, thawed: &[Actor]) -> Result<Option<Actor>, HarnessError> {
        for daemon in &mut self.daemons {
            if !thawed.contains(&daemon.actor) {
                continue;
            }
            if let Some(status) = daemon
                .child
                .try_wait()
                .map_err(|err| HarnessError::Io(err.to_string()))?
            {
                let _ = status;
                return Ok(Some(daemon.actor));
            }
        }
        Ok(None)
    }

    fn pump_logs(&mut self) -> Result<Vec<(Actor, u64, StatusSnap)>, HarnessError> {
        let mut seen = Vec::new();
        for daemon in &mut self.daemons {
            let mut file = match File::open(&daemon.log_path) {
                Ok(file) => file,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(HarnessError::Io(err.to_string())),
            };
            let len = file.metadata().map_err(ioe)?.len();
            if len < daemon.log_offset {
                daemon.log_offset = 0;
            }
            file.seek(SeekFrom::Start(daemon.log_offset)).map_err(ioe)?;
            let mut buf = String::new();
            file.read_to_string(&mut buf).map_err(ioe)?;
            daemon.log_offset = len;
            for line in buf.lines() {
                if let Some(status) = parse_status(line) {
                    // The product logs rescan as a per-window delta, so the
                    // post-mutation bump is the sum of windows after entry.
                    daemon.rescan_sum = daemon.rescan_sum.saturating_add(status.rescan);
                    daemon.status = Some(status.clone());
                    seen.push((daemon.actor, daemon.rescan_sum, status));
                }
            }
        }
        Ok(seen)
    }

    fn observe(&self) -> Result<Observed, HarnessError> {
        let master = self.actor_view(Actor::Master, "src/")?;
        let mut slaves = Vec::new();
        for draft in &self.slaves {
            let actor = Actor::Slave(
                crate::schedule::SlaveIx::new(draft.index, self.layout.slave_count())
                    .expect("draft index"),
            );
            let prefix = strip_prefix_for(&draft.central);
            slaves.push(self.actor_view(actor, &prefix)?);
        }
        let yard = hash_bytes(&fs::read(&self.yard_path).unwrap_or_default()).into_bytes();
        let mut dead = Vec::new();
        for daemon in &self.daemons {
            if !pid_alive(daemon.pid) {
                dead.push(daemon.actor);
            }
        }
        Ok(Observed {
            master,
            slaves,
            yard,
            dead,
        })
    }

    fn actor_view(&self, actor: Actor, strip: &str) -> Result<ActorView, HarnessError> {
        let tree = walk_tree(self.tree_root(actor))?;
        let conflicts = match actor {
            Actor::Master => self.central_root.join(".arborsync-conflicts"),
            Actor::Slave(ix) => self.slaves[ix.get() as usize]
                .checkout
                .join(".arborsync-conflicts"),
        };
        let sidecars = read_sidecars(&conflicts, strip)?;
        let status = self
            .daemons
            .iter()
            .find(|daemon| daemon.actor == actor)
            .and_then(|daemon| daemon.status.clone())
            .unwrap_or(StatusSnap {
                health: Health::Other("absent".into()),
                pending: u64::MAX,
                rescan: 0,
                last_error: "-".into(),
            });
        Ok(ActorView {
            tree,
            sidecars,
            status,
        })
    }

    fn spawn_daemon(&self, actor: Actor, config: &Path, log: &Path) -> Result<Daemon, HarnessError> {
        let start = fs::metadata(log).map(|meta| meta.len()).unwrap_or(0);
        let manifest = std::ffi::CString::new(self.manifest.as_os_str().as_bytes())
            .map_err(|err| HarnessError::Io(err.to_string()))?;
        let role = if actor == Actor::Master { "master" } else { "slave" };
        let mut cmd = Command::new(&self.bin);
        cmd.arg(role)
            .arg("--config")
            .arg(config)
            .current_dir(self.root.path())
            .env("TMPDIR", &self.child_tmp)
            .env("ARBORSYNC_LOG_LEVEL", "info")
            .env_remove("ARBORSYNC_CONFIG")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log_file(log)?);
        // pre_exec runs after fork, so the pid record has to be async-signal-safe.
        unsafe {
            cmd.pre_exec(move || append_pid_raw(&manifest));
        }
        let child = cmd.spawn().map_err(|err| {
            HarnessError::Io(format!("spawn {role}: {err}"))
        })?;
        Ok(Daemon {
            actor,
            pid: child.id(),
            log_path: log.to_path_buf(),
            log_offset: start,
            rescan_sum: 0,
            status: None,
            child,
        })
    }

    fn write_slave_toml(&self, index: usize, addr: BoundAddr) -> Result<(), HarnessError> {
        let draft = &self.slaves[index];
        guard_config_path(self.root.path(), &draft.toml_path)?;
        guard_config_path(self.root.path(), &draft.key_path)?;
        guard_config_path(self.root.path(), &draft.db_path)?;
        guard_config_path(self.root.path(), &draft.checkout)?;
        guard_config_path(self.root.path(), &draft.peer_socket)?;
        let cfg = SlaveConfig {
            slave_id: draft.id.clone(),
            master_addr: addr.addr().to_string(),
            slave_key_path: draft.key_path.to_string_lossy().into_owned(),
            master_public_keys: vec![self.master_public.clone()],
            db_path: draft.db_path.to_string_lossy().into_owned(),
            log_level: "info".into(),
            max_checkouts_per_slave: 100,
            watcher_debounce_ms: 200,
            rescan_interval_seconds: 2,
            status_interval_seconds: 1,
            peer_socket: Some(draft.peer_socket.to_string_lossy().into_owned()),
            peer_socket_mode: Some("660".into()),
            checkouts: vec![CheckoutConfig {
                id: "src".into(),
                central: draft.central.clone(),
                local: draft.checkout.to_string_lossy().into_owned(),
            }],
            tune: TuneSpec::default(),
        };
        let toml = cfg
            .to_toml()
            .map_err(|err| HarnessError::Io(err.to_string()))?;
        write_mode(&draft.toml_path, &toml, 0o600)
    }

    fn struck_reload(&mut self) -> Result<Boot, HarnessError> {
        let mut text = fs::read_to_string(&self.master_toml).map_err(ioe)?;
        text.insert_str(0, "quic_idle_timeout_ms = 1\n");
        write_mode(&self.master_toml, &text, 0o600)?;
        let pid = self.daemon(Actor::Master)?.pid;
        if !pid_alive(pid) {
            return Ok(Boot::Failed(Finding::Crash {
                actor: "master".into(),
                note: "exited during struck reload".into(),
            }));
        }
        Ok(Boot::Up)
    }

    fn restore(&mut self, epoch: u32) -> Result<Boot, HarnessError> {
        let source = self.root.path().join("epochs").join(epoch.to_string());
        if !source.is_dir() {
            return Err(HarnessError::Sandbox(format!(
                "epoch {} was not sealed",
                epoch
            )));
        }
        self.stop_actor(Actor::Master)?;
        let output = Command::new(&self.bin)
            .args(["restore", "--config"])
            .arg(&self.master_toml)
            .arg("--source")
            .arg(&source)
            .arg("--prefix")
            .arg("/src")
            .current_dir(self.root.path())
            .env("TMPDIR", &self.child_tmp)
            .env_remove("ARBORSYNC_CONFIG")
            .output()
            .map_err(ioe)?;
        if !output.status.success() {
            return Ok(Boot::Failed(Finding::Protocol {
                actor: "master".into(),
                token: format!(
                    "restore: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
            }));
        }
        let mut master = self.spawn_daemon(Actor::Master, &self.master_toml.clone(), &self.master_log.clone())?;
        let ready = wait_line(&mut master, "listening on ", Duration::from_secs(15))?;
        let addr = match ready {
            LineWait::Found(text) => {
                let addr = parse_listening(&text).ok_or_else(|| {
                    HarnessError::Sandbox(format!("restored master has no address: {text}"))
                })?;
                self.daemons.push(master);
                addr
            }
            LineWait::Died(note) => {
                self.daemons.push(master);
                return Ok(Boot::Failed(Finding::Crash {
                    actor: "master".into(),
                    note,
                }));
            }
            LineWait::TimedOut(note) => {
                self.daemons.push(master);
                return Ok(Boot::Failed(Finding::Hang {
                    actor: "master".into(),
                    last_status: note,
                }));
            }
        };
        self.addr = Some(addr);
        // master_addr is RestartRequired, so the slave process has to be replaced.
        let count = self.slaves.len();
        for index in 0..count {
            let actor = Actor::Slave(
                crate::schedule::SlaveIx::new(index as u8, self.layout.slave_count())?,
            );
            self.stop_actor(actor)?;
            self.write_slave_toml(index, addr)?;
            let toml = self.slaves[index].toml_path.clone();
            let log = self.slaves[index].log_path.clone();
            let mut daemon = self.spawn_daemon(actor, &toml, &log)?;
            let ready = wait_line(&mut daemon, "connected to ", Duration::from_secs(15))?;
            match ready {
                LineWait::Found(_) => self.daemons.push(daemon),
                LineWait::Died(note) => {
                    self.daemons.push(daemon);
                    return Ok(Boot::Failed(Finding::Crash {
                        actor: actor.label(),
                        note,
                    }));
                }
                LineWait::TimedOut(note) => {
                    self.daemons.push(daemon);
                    return Ok(Boot::Failed(Finding::Hang {
                        actor: actor.label(),
                        last_status: note,
                    }));
                }
            }
        }
        Ok(Boot::Up)
    }

    fn stop_actor(&mut self, actor: Actor) -> Result<(), HarnessError> {
        let Some(pos) = self.daemons.iter().position(|daemon| daemon.actor == actor) else {
            return Ok(());
        };
        let mut daemon = self.daemons.remove(pos);
        reap_pid(daemon.pid);
        let _ = daemon.child.kill();
        let _ = daemon.child.wait();
        self.frozen.remove(&actor);
        self.rewrite_manifest()
    }

    fn rewrite_manifest(&self) -> Result<(), HarnessError> {
        let mut body = format!("owner {}\n", self.owner);
        for daemon in &self.daemons {
            body.push_str(&format!("pid {}\n", daemon.pid));
        }
        write_mode(&self.manifest, &body, 0o600)
    }
}

impl Drop for World {
    fn drop(&mut self) {
        self.close();
    }
}

enum LineWait {
    Found(String),
    Died(String),
    TimedOut(String),
}

fn wait_line(daemon: &mut Daemon, needle: &str, timeout: Duration) -> Result<LineWait, HarnessError> {
    let start = Instant::now();
    let begin = daemon.log_offset as usize;
    loop {
        if daemon.child.try_wait().map_err(ioe)?.is_some() {
            let text = fs::read_to_string(&daemon.log_path).unwrap_or_default();
            return Ok(LineWait::Died(tail(&text)));
        }
        let text = fs::read_to_string(&daemon.log_path).unwrap_or_default();
        let slice = if begin < text.len() { &text[begin..] } else { "" };
        if let Some(pos) = slice.find(needle) {
            let line = slice[pos..].lines().next().unwrap_or(slice);
            return Ok(LineWait::Found(line.to_string()));
        }
        if start.elapsed() > timeout {
            return Ok(LineWait::TimedOut(tail(&text)));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn tail(text: &str) -> String {
    let start = text.len().saturating_sub(800);
    text[start..].to_string()
}

fn log_file(path: &Path) -> Result<Stdio, HarnessError> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(ioe)?;
    Ok(Stdio::from(file))
}

fn append_pid_raw(manifest: &std::ffi::CString) -> std::io::Result<()> {
    let pid = unsafe { getpid() };
    let mut buf = [0u8; 32];
    let line = format_pid_line(pid as u32, &mut buf);
    let fd = unsafe { open(manifest.as_ptr(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0o600) };
    if fd < 0 {
        return Err(std::io::Error::other("pid record failed"));
    }
    let mut off = 0;
    while off < line.len() {
        let n = unsafe { write(fd, line[off..].as_ptr(), line.len() - off) };
        if n < 0 {
            unsafe { close(fd) };
            return Err(std::io::Error::other("pid record failed"));
        }
        off += n as usize;
    }
    unsafe { close(fd) };
    Ok(())
}

fn format_pid_line(pid: u32, buf: &mut [u8; 32]) -> &[u8] {
    buf[0] = b'p';
    buf[1] = b'i';
    buf[2] = b'd';
    buf[3] = b' ';
    let mut n = pid;
    let mut tmp = [0u8; 10];
    let mut len = 0;
    if n == 0 {
        tmp[0] = b'0';
        len = 1;
    } else {
        while n > 0 {
            tmp[len] = b'0' + (n % 10) as u8;
            n /= 10;
            len += 1;
        }
    }
    let mut pos = 4;
    for digit in tmp[..len].iter().rev() {
        buf[pos] = *digit;
        pos += 1;
    }
    buf[pos] = b'\n';
    &buf[..pos + 1]
}

fn create_root() -> Result<PrivateRoot, HarnessError> {
    let temp = std::env::temp_dir();
    let temp = temp.canonicalize().map_err(ioe)?;
    let mut n = 0u32;
    loop {
        let path = temp.join(format!(
            "arborsync-fuzz-{}-{}-{n}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        match fs::create_dir(&path) {
            Ok(()) => {
                let canon = path.canonicalize().map_err(ioe)?;
                if !canon.starts_with(&temp) {
                    let _ = fs::remove_dir_all(&canon);
                    return Err(HarnessError::Sandbox(
                        "private root escaped the temp dir".into(),
                    ));
                }
                let name = canon.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !name.starts_with("arborsync-fuzz-") {
                    let _ = fs::remove_dir_all(&canon);
                    return Err(HarnessError::Sandbox("private root has the wrong name".into()));
                }
                return Ok(PrivateRoot { path: canon });
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                n += 1;
                if n > 100 {
                    return Err(HarnessError::Io("could not mint a private root".into()));
                }
            }
            Err(err) => return Err(HarnessError::Io(format!("create temp root: {err}"))),
        }
    }
}

fn guard_config_path(root: &Path, path: &Path) -> Result<(), HarnessError> {
    if ends_with_test_master(path) || forbidden(path) {
        return Err(HarnessError::Sandbox(format!(
            "refusing config path {}",
            path.display()
        )));
    }
    let canon = canonicalize_file(path)?;
    if ends_with_test_master(&canon) || forbidden(&canon) || !canon.starts_with(root) {
        return Err(HarnessError::Sandbox(format!(
            "refusing config path {}",
            canon.display()
        )));
    }
    Ok(())
}

fn ends_with_test_master(path: &Path) -> bool {
    let mut parts = path.components();
    let file = parts.next_back();
    let dir = parts.next_back();
    matches!(
        (dir, file),
        (
            Some(std::path::Component::Normal(dir)),
            Some(std::path::Component::Normal(file))
        ) if dir == "nix" && file == "test-master.toml"
    )
}

fn forbidden(path: &Path) -> bool {
    let mut banned = vec![PathBuf::from("/etc/arborsync")];
    if let Some(home) = std::env::var_os("HOME") {
        banned.push(PathBuf::from(home).join(".config/arborsync"));
    }
    banned.iter().any(|dir| path == dir || path.starts_with(dir))
}

fn canonicalize_file(path: &Path) -> Result<PathBuf, HarnessError> {
    if path.exists() {
        return path.canonicalize().map_err(ioe);
    }
    let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let name = path.file_name().ok_or_else(|| {
        HarnessError::Sandbox(format!("config path {} has no name", path.display()))
    })?;
    let parent = parent.canonicalize().map_err(ioe)?;
    Ok(parent.join(name))
}

fn keygen(bin: &Path, out: &Path) -> Result<String, HarnessError> {
    let output = Command::new(bin)
        .args(["keygen", "--out"])
        .arg(out)
        .env_remove("ARBORSYNC_CONFIG")
        .output()
        .map_err(ioe)?;
    if !output.status.success() {
        return Err(HarnessError::Bin(format!(
            "keygen: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let mode = fs::metadata(out).map_err(ioe)?.permissions().mode() & 0o777;
    if mode != 0o600 {
        let mut perms = fs::metadata(out).map_err(ioe)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(out, perms).map_err(ioe)?;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !text.starts_with("hex:") || text.len() != 68 {
        return Err(HarnessError::Bin(format!("keygen stdout {text}")));
    }
    Ok(text)
}

fn write_mode(path: &Path, text: &str, mode: u32) -> Result<(), HarnessError> {
    fs::write(path, text).map_err(ioe)?;
    let mut perms = fs::metadata(path).map_err(ioe)?.permissions();
    perms.set_mode(mode);
    fs::set_permissions(path, perms).map_err(ioe)?;
    Ok(())
}

fn slave_id(index: u8) -> &'static str {
    match index {
        0 => "dev-alice",
        _ => "dev-bob",
    }
}

fn central_prefix(layout: &Layout, index: u8) -> &'static str {
    if matches!(
        layout,
        Layout::Two {
            second: SecondView::Nested
        }
    ) && index == 1
    {
        "/src/nested"
    } else {
        "/src"
    }
}

fn strip_prefix_for(central: &str) -> String {
    format!("{}/", central.trim_start_matches('/'))
}

fn signal(pid: u32, sig: i32) -> Result<(), HarnessError> {
    let rc = unsafe { kill(pid as i32, sig) };
    if rc == 0 {
        Ok(())
    } else {
        Err(HarnessError::Io(format!(
            "signal {sig} to {pid}: {}",
            std::io::Error::last_os_error()
        )))
    }
}

fn wait_proc_state(pid: u32, stopped: bool) -> Result<(), HarnessError> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let state = proc_state(pid);
        let is_stopped = state == Some('T') || state == Some('t');
        if is_stopped == stopped {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(HarnessError::Sandbox(format!(
                "pid {pid} did not reach stopped={stopped} (state {state:?})"
            )));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn proc_state(pid: u32) -> Option<char> {
    let text = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("State:") {
            return rest.trim().chars().next();
        }
    }
    None
}

fn apply_disk(root: &Path, yard: &Path, op: &DiskOp) -> Result<LocalSnap, HarnessError> {
    match op {
        DiskOp::Put {
            path,
            bytes,
            mode,
            mtime,
            ..
        } => {
            let host = host_path(root, path)?;
            if let Some(parent) = host.parent() {
                fs::create_dir_all(parent).map_err(ioe)?;
            }
            fs::write(&host, bytes).map_err(ioe)?;
            set_mode(&host, mode.bits())?;
            set_mtime(&host, mtime.get())?;
            Ok(LocalSnap::Applied(snap_file(&host)?))
        }
        DiskOp::RetouchSameStamp { path, bytes, .. } => {
            let host = host_path(root, path)?;
            let meta = match fs::symlink_metadata(&host) {
                Ok(meta) if meta.is_file() => meta,
                _ => return Ok(LocalSnap::Skipped),
            };
            if meta.len() as usize != bytes.len() {
                return Ok(LocalSnap::Skipped);
            }
            let stamp = mtime_of(&meta);
            fs::write(&host, bytes).map_err(ioe)?;
            set_mode(&host, meta.mode() & 0o7777)?;
            set_mtime(&host, stamp.get())?;
            Ok(LocalSnap::Applied(snap_file(&host)?))
        }
        DiskOp::Chmod { path, mode, .. } => {
            let host = host_path(root, path)?;
            if !host.exists() {
                return Ok(LocalSnap::Skipped);
            }
            set_mode(&host, mode.bits())?;
            Ok(LocalSnap::Applied(snap_any(&host)?))
        }
        DiskOp::Symlink { path, target, .. } => {
            let host = host_path(root, path)?;
            if let Some(parent) = host.parent() {
                fs::create_dir_all(parent).map_err(ioe)?;
            }
            if host.symlink_metadata().is_ok() {
                fs::remove_file(&host).or_else(|_| fs::remove_dir_all(&host)).map_err(ioe)?;
            }
            let target = std::ffi::OsStr::from_bytes(target);
            std::os::unix::fs::symlink(target, &host).map_err(ioe)?;
            Ok(LocalSnap::Applied(snap_any(&host)?))
        }
        DiskOp::Unlink { path, .. } => {
            let host = host_path(root, path)?;
            if fs::symlink_metadata(&host).is_err() {
                return Ok(LocalSnap::Skipped);
            }
            if host.is_dir() && !host.is_symlink() {
                fs::remove_dir_all(&host).map_err(ioe)?;
            } else {
                fs::remove_file(&host).map_err(ioe)?;
            }
            Ok(LocalSnap::Applied(SnapBody::Absent))
        }
        DiskOp::Rename { from, to, .. } => {
            let src = host_path(root, from)?;
            let dst = host_path(root, to)?;
            if fs::symlink_metadata(&src).is_err() {
                return Ok(LocalSnap::Skipped);
            }
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent).map_err(ioe)?;
            }
            fs::rename(&src, &dst).map_err(ioe)?;
            Ok(LocalSnap::Applied(snap_any(&dst)?))
        }
        DiskOp::Mkdir { path, mode, .. } => {
            let host = host_path(root, path)?;
            fs::create_dir_all(&host).map_err(ioe)?;
            set_mode(&host, mode.bits())?;
            let mut body = snap_any(&host)?;
            if let SnapBody::Dir {
                meta_authoritative, ..
            } = &mut body
            {
                *meta_authoritative = true;
            }
            Ok(LocalSnap::Applied(body))
        }
        DiskOp::Special { path, kind, .. } => apply_special(root, path, *kind),
        DiskOp::NonUtf8 { parent, name, .. } => {
            let dir = host_path(root, parent)?;
            fs::create_dir_all(&dir).map_err(ioe)?;
            let host = dir.join(std::ffi::OsStr::from_bytes(name));
            fs::write(&host, b"x").map_err(ioe)?;
            Ok(LocalSnap::Applied(SnapBody::Residue(Residue::NonUtf8)))
        }
        DiskOp::HardLink { from, to, .. } => {
            let src = host_path(root, from)?;
            let dst = host_path(root, to)?;
            if !src.is_file() {
                return Ok(LocalSnap::Skipped);
            }
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent).map_err(ioe)?;
            }
            if dst.symlink_metadata().is_ok() {
                fs::remove_file(&dst).map_err(ioe)?;
            }
            fs::hard_link(&src, &dst).map_err(ioe)?;
            Ok(LocalSnap::Applied(snap_file(&dst)?))
        }
        DiskOp::EscapeLink { path, .. } => {
            let host = host_path(root, path)?;
            if let Some(parent) = host.parent() {
                fs::create_dir_all(parent).map_err(ioe)?;
            }
            if host.symlink_metadata().is_ok() {
                fs::remove_file(&host).or_else(|_| fs::remove_dir_all(&host)).map_err(ioe)?;
            }
            std::os::unix::fs::symlink(yard, &host).map_err(ioe)?;
            Ok(LocalSnap::Applied(snap_any(&host)?))
        }
        DiskOp::Deny { path, .. } => {
            let host = host_path(root, path)?;
            let Ok(meta) = fs::symlink_metadata(&host) else {
                return Ok(LocalSnap::Skipped);
            };
            let next = (meta.mode() & 0o7777) & !0o500;
            set_mode(&host, next)?;
            Ok(LocalSnap::Applied(snap_any(&host).unwrap_or(SnapBody::Absent)))
        }
    }
}

fn apply_special(root: &Path, path: &RelPath, kind: SpecialFile) -> Result<LocalSnap, HarnessError> {
    let host = host_path(root, path)?;
    if let Some(parent) = host.parent() {
        fs::create_dir_all(parent).map_err(ioe)?;
    }
    if host.symlink_metadata().is_ok() {
        return Ok(LocalSnap::Skipped);
    }
    let c_path = std::ffi::CString::new(host.as_os_str().as_bytes())
        .map_err(|err| HarnessError::Io(err.to_string()))?;
    let rc = match kind {
        SpecialFile::Fifo => unsafe { mkfifo(c_path.as_ptr(), 0o644) },
        SpecialFile::Socket => {
            match std::os::unix::net::UnixListener::bind(&host) {
                Ok(listener) => {
                    drop(listener);
                    0
                }
                Err(err) if err.raw_os_error() == Some(1) => return Ok(LocalSnap::Skipped),
                Err(err) => return Err(HarnessError::Io(err.to_string())),
            }
        }
        SpecialFile::Device => unsafe { mknod(c_path.as_ptr(), 0o020000 | 0o666, 1 << 8 | 3) },
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(1) {
            return Ok(LocalSnap::Skipped);
        }
        return Err(HarnessError::Io(err.to_string()));
    }
    let residue = match kind {
        SpecialFile::Fifo => Residue::Fifo,
        SpecialFile::Socket => Residue::Socket,
        SpecialFile::Device => Residue::Device,
    };
    Ok(LocalSnap::Applied(SnapBody::Residue(residue)))
}

fn host_path(root: &Path, path: &RelPath) -> Result<PathBuf, HarnessError> {
    let mut host = root.to_path_buf();
    for part in path.parts() {
        if part.contains('/') || part == "." || part == ".." {
            return Err(HarnessError::Sandbox(format!("bad path {path}")));
        }
        host.push(part);
    }
    Ok(host)
}

fn snap_file(path: &Path) -> Result<SnapBody, HarnessError> {
    let meta = fs::symlink_metadata(path).map_err(ioe)?;
    let bytes = fs::read(path).map_err(ioe)?;
    Ok(SnapBody::File {
        bytes,
        mode: UnixMode::new(meta.mode() & 0o7777).expect("mode mask"),
        mtime: mtime_of(&meta),
    })
}

fn snap_any(path: &Path) -> Result<SnapBody, HarnessError> {
    let meta = fs::symlink_metadata(path).map_err(ioe)?;
    let ft = meta.file_type();
    if ft.is_symlink() {
        let target = fs::read_link(path).map_err(ioe)?;
        return Ok(SnapBody::Symlink {
            target: target.as_os_str().as_bytes().to_vec(),
        });
    }
    if ft.is_dir() {
        return Ok(SnapBody::Dir {
            mode: UnixMode::new(meta.mode() & 0o7777).expect("mode mask"),
            mtime: mtime_of(&meta),
            meta_authoritative: false,
        });
    }
    if ft.is_fifo() {
        return Ok(SnapBody::Residue(Residue::Fifo));
    }
    if ft.is_socket() {
        return Ok(SnapBody::Residue(Residue::Socket));
    }
    if ft.is_char_device() || ft.is_block_device() {
        return Ok(SnapBody::Residue(Residue::Device));
    }
    snap_file(path)
}

fn mtime_of(meta: &fs::Metadata) -> MtimeNs {
    MtimeNs::new(meta.mtime() * 1_000_000_000 + meta.mtime_nsec())
}

fn set_mode(path: &Path, mode: u32) -> Result<(), HarnessError> {
    let mut perms = fs::symlink_metadata(path).map_err(ioe)?.permissions();
    perms.set_mode(mode);
    fs::set_permissions(path, perms).map_err(ioe)?;
    Ok(())
}

fn set_mtime(path: &Path, ns: i64) -> Result<(), HarnessError> {
    let sec = ns.div_euclid(1_000_000_000);
    let nsec = ns.rem_euclid(1_000_000_000);
    let times = [
        Timespec {
            tv_sec: sec,
            tv_nsec: nsec,
        },
        Timespec {
            tv_sec: sec,
            tv_nsec: nsec,
        },
    ];
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|err| HarnessError::Io(err.to_string()))?;
    let rc = unsafe { utimensat(AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) };
    if rc == 0 {
        Ok(())
    } else {
        Err(HarnessError::Io(std::io::Error::last_os_error().to_string()))
    }
}

fn walk_tree(root: &Path) -> Result<TreeSnap, HarnessError> {
    let mut nodes = std::collections::BTreeMap::new();
    walk_into(root, root, &mut nodes)?;
    Ok(TreeSnap { nodes })
}

fn walk_into(
    root: &Path,
    dir: &Path,
    nodes: &mut std::collections::BTreeMap<EntryKey, SnapBody>,
) -> Result<(), HarnessError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
        Err(err) => return Err(HarnessError::Io(err.to_string())),
    };
    for entry in entries {
        let entry = entry.map_err(ioe)?;
        let name = entry.file_name();
        let rel_dir = dir.strip_prefix(root).unwrap_or(Path::new(""));
        if rel_dir.as_os_str().is_empty()
            && (name == ".arborsync-tmp" || name == ".arborsync-conflicts")
        {
            continue;
        }
        let host = entry.path();
        if name.to_str().is_none() {
            if let Some(parent) = rel_path(rel_dir) {
                nodes.insert(
                    EntryKey::Raw {
                        parent,
                        name: name.as_bytes().to_vec(),
                    },
                    SnapBody::Residue(Residue::NonUtf8),
                );
            }
            continue;
        }
        let meta = match fs::symlink_metadata(&host) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => continue,
            Err(err) => return Err(HarnessError::Io(err.to_string())),
        };
        let rel = {
            let mut parts = Vec::new();
            for component in host.strip_prefix(root).unwrap_or(&host).components() {
                if let std::path::Component::Normal(part) = component {
                    parts.push(part.to_string_lossy().into_owned());
                }
            }
            RelPath::new(parts).map_err(|err| HarnessError::Io(err.to_string()))?
        };
        let ft = meta.file_type();
        if ft.is_symlink() {
            let target = fs::read_link(&host).map_err(ioe)?;
            nodes.insert(
                EntryKey::Path(rel),
                SnapBody::Symlink {
                    target: target.as_os_str().as_bytes().to_vec(),
                },
            );
            continue;
        }
        if ft.is_dir() {
            nodes.insert(
                EntryKey::Path(rel),
                SnapBody::Dir {
                    mode: UnixMode::new(meta.mode() & 0o7777).expect("mode mask"),
                    mtime: mtime_of(&meta),
                    meta_authoritative: false,
                },
            );
            walk_into(root, &host, nodes)?;
            continue;
        }
        if ft.is_fifo() {
            nodes.insert(EntryKey::Path(rel), SnapBody::Residue(Residue::Fifo));
            continue;
        }
        if ft.is_socket() {
            nodes.insert(EntryKey::Path(rel), SnapBody::Residue(Residue::Socket));
            continue;
        }
        if ft.is_char_device() || ft.is_block_device() {
            nodes.insert(EntryKey::Path(rel), SnapBody::Residue(Residue::Device));
            continue;
        }
        match fs::read(&host) {
            Ok(bytes) => {
                nodes.insert(
                    EntryKey::Path(rel),
                    SnapBody::File {
                        bytes,
                        mode: UnixMode::new(meta.mode() & 0o7777).expect("mode mask"),
                        mtime: mtime_of(&meta),
                    },
                );
            }
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {}
            Err(err) => return Err(HarnessError::Io(err.to_string())),
        }
    }
    Ok(())
}

fn rel_path(path: &Path) -> Option<RelPath> {
    let parts: Vec<String> = path
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect();
    RelPath::new(parts).ok()
}

fn read_sidecars(dir: &Path, strip: &str) -> Result<Vec<crate::oracle::Sidecar>, HarnessError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in fs::read_dir(&current).map_err(ioe)? {
            let entry = entry.map_err(ioe)?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let rel = path.strip_prefix(dir).unwrap_or(&path);
            let Some((canonical, _)) = split_sidecar(rel) else {
                let bytes = fs::read(&path).unwrap_or_default();
                if let Ok(canonical) = RelPath::new(vec!["unknown".into()]) {
                    out.push(crate::oracle::Sidecar {
                        canonical,
                        losing_hash: hash_bytes(&bytes).into_bytes(),
                        bytes,
                    });
                }
                continue;
            };
            let trimmed = canonical
                .strip_prefix(strip)
                .unwrap_or(canonical.as_str());
            let Ok(canonical) = RelPath::new(trimmed.split('/').map(|p| p.to_string()).collect()) else {
                continue;
            };
            let bytes = fs::read(&path).map_err(ioe)?;
            out.push(crate::oracle::Sidecar {
                canonical,
                losing_hash: hash_bytes(&bytes).into_bytes(),
                bytes,
            });
        }
    }
    Ok(out)
}

fn split_sidecar(rel: &Path) -> Option<(String, String)> {
    let name = rel.file_name()?.to_str()?;
    let (stem, hex) = name.rsplit_once("--")?;
    if hex.len() != 16 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let parent = rel.parent().unwrap_or(Path::new(""));
    let canonical = if parent.as_os_str().is_empty() {
        stem.to_string()
    } else {
        format!("{}/{}", parent.to_string_lossy(), stem)
    };
    Some((canonical, hex.to_string()))
}

fn copy_tree(src: &Path, dst: &Path) -> Result<(), HarnessError> {
    fs::create_dir_all(dst).map_err(ioe)?;
    for entry in fs::read_dir(src).map_err(ioe)? {
        let entry = entry.map_err(ioe)?;
        let name = entry.file_name();
        if name == ".arborsync-tmp" || name == ".arborsync-conflicts" {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        let meta = fs::symlink_metadata(&from).map_err(ioe)?;
        let ft = meta.file_type();
        if ft.is_symlink() {
            let target = fs::read_link(&from).map_err(ioe)?;
            std::os::unix::fs::symlink(target, &to).map_err(ioe)?;
        } else if ft.is_dir() {
            copy_tree(&from, &to)?;
        } else if ft.is_file() {
            fs::copy(&from, &to).map_err(ioe)?;
            let mut perms = fs::metadata(&to).map_err(ioe)?.permissions();
            perms.set_mode(meta.mode() & 0o7777);
            fs::set_permissions(&to, perms).map_err(ioe)?;
        }
    }
    Ok(())
}

fn ioe(err: impl std::fmt::Display) -> HarnessError {
    HarnessError::Io(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reap_abandoned_kills_a_dead_owners_child_and_is_idempotent() {
        let mut sleep = Command::new("sleep").arg("120").spawn().expect("sleep");
        let pid = sleep.id();
        let dir = std::env::temp_dir().join(format!(
            "arborsync-fuzz-reap-{}-{}",
            std::process::id(),
            pid
        ));
        fs::create_dir(&dir).expect("reap dir");
        let dead_owner = 2_000_000_000u32;
        assert!(!pid_alive(dead_owner), "fake owner must be dead");
        fs::write(
            dir.join("manifest"),
            format!("owner {dead_owner}\npid {pid}\n"),
        )
        .expect("manifest");
        reap_abandoned().expect("reap");
        let _ = sleep.kill();
        let _ = sleep.wait();
        assert!(!pid_alive(pid), "sleep pid still alive");
        assert!(!dir.exists(), "reaped directory still exists");
        reap_abandoned().expect("second reap");
    }

    #[test]
    fn parse_listening_accepts_loopback_and_rejects_port_zero_and_wildcard() {
        let addr = parse_listening("listening on 127.0.0.1:12345").expect("loopback");
        assert_eq!(addr.addr(), "127.0.0.1:12345".parse().unwrap());
        assert!(parse_listening("listening on 127.0.0.1:0").is_none());
        assert!(parse_listening("listening on 0.0.0.0:9").is_none());
    }
}

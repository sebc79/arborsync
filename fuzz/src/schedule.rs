use std::collections::BTreeSet;
use std::num::{NonZeroU16, NonZeroU8};
use std::path::{Path, PathBuf};

use crate::types::HarnessError;

/// Path to the `arborsync` binary this run will exec.
/// Invariant: the path was executable when [`Bin::new`] returned.
pub struct Bin {
    path: PathBuf,
}

impl Bin {
    /// Rejects a missing or non-executable path. A bare name is resolved on `PATH`.
    pub fn new(path: PathBuf) -> Result<Self, HarnessError> {
        let path = resolve_bin(path)?;
        let meta = std::fs::metadata(&path).map_err(|err| {
            HarnessError::Bin(format!("{}: {err}", path.display()))
        })?;
        if !meta.is_file() {
            return Err(HarnessError::Bin(format!("{} is not a file", path.display())));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o111 == 0 {
                return Err(HarnessError::Bin(format!(
                    "{} is not executable",
                    path.display()
                )));
            }
        }
        Ok(Self { path })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

fn resolve_bin(path: PathBuf) -> Result<PathBuf, HarnessError> {
    if path.components().count() != 1 {
        return Ok(path);
    }
    if path.exists() {
        return Ok(path);
    }
    let Some(path_env) = std::env::var_os("PATH") else {
        return Ok(path);
    };
    for dir in std::env::split_paths(&path_env) {
        let candidate = dir.join(&path);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Ok(path)
}

/// How long one generated run is.
/// Invariant: `steps >= 1` and `slaves` is 1 or 2.
#[derive(Clone, Debug)]
pub struct Limits {
    steps: NonZeroU16,
    slaves: SlaveCount,
}

/// Invariant: the inner value is 1 or 2.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SlaveCount(NonZeroU8);

impl Limits {
    /// `slaves` outside `1..=2`, or `steps == 0`, is [`HarnessError::Limits`].
    pub fn new(steps: u16, slaves: u8) -> Result<Self, HarnessError> {
        let steps = NonZeroU16::new(steps).ok_or_else(|| {
            HarnessError::Limits("steps must be at least 1".into())
        })?;
        if !(1..=2).contains(&slaves) {
            return Err(HarnessError::Limits("slaves must be 1 or 2".into()));
        }
        let slaves = SlaveCount(NonZeroU8::new(slaves).expect("1 or 2"));
        Ok(Self { steps, slaves })
    }

    pub(crate) fn steps(&self) -> u16 {
        self.steps.get()
    }

    pub(crate) fn slaves(&self) -> SlaveCount {
        self.slaves
    }
}

impl SlaveCount {
    pub(crate) fn get(self) -> u8 {
        self.0.get()
    }
}

/// What one run will do. Paths, ports, keys, and pids are not in here.
/// Invariant: [`Schedule::from_seed`] and [`Schedule::from_bytes`] return only
/// values that pass [`Schedule::validate`]. Freeze frames are balanced.
/// A `Disk` step names a frozen actor. `Settle` happens only when every
/// actor is thawed. A `Deny` step shares its epoch with no other `Disk`
/// step. `Restore`'s epoch index is a `Settle` earlier in the schedule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schedule {
    seed: u64,
    layout: Layout,
    steps: Vec<Step>,
}

/// Slave 0 always mirrors `/src`. A second slave is either `/src` or `/src/nested`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Layout {
    One,
    Two { second: SecondView },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SecondView {
    Whole,
    Nested,
}

impl Layout {
    pub(crate) fn slave_count(&self) -> SlaveCount {
        match self {
            Layout::One => SlaveCount(NonZeroU8::new(1).expect("1")),
            Layout::Two { .. } => SlaveCount(NonZeroU8::new(2).expect("2")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    Freeze(Actor),
    Thaw(Actor),
    Disk(DiskOp),
    World(WorldOp),
    /// Wait until every thawed daemon is idle. Does not judge.
    Quiesce,
    /// Quiesce, then compare the disks to [`crate::oracle::project`].
    Settle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Actor {
    Master,
    Slave(SlaveIx),
}

impl Actor {
    pub(crate) fn label(self) -> String {
        match self {
            Actor::Master => "master".into(),
            Actor::Slave(ix) => format!("slave:{}", ix.get()),
        }
    }
}

/// Invariant: the index is less than the schedule's slave count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SlaveIx(u8);

impl SlaveIx {
    pub(crate) fn new(index: u8, slaves: SlaveCount) -> Result<Self, HarnessError> {
        if index >= slaves.get() {
            return Err(HarnessError::Limits(format!(
                "slave index {index} is outside 0..{}",
                slaves.get()
            )));
        }
        Ok(Self(index))
    }

    pub(crate) fn get(self) -> u8 {
        self.0
    }

    pub(crate) fn from_raw(index: u8) -> Self {
        Self(index)
    }
}

/// A relative path inside one tree.
/// Invariant: non-empty; each component is UTF-8, not empty, not `.` or
/// `..`, and contains no `/`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RelPath {
    parts: Vec<String>,
}

impl RelPath {
    pub(crate) fn new(parts: Vec<String>) -> Result<Self, HarnessError> {
        if parts.is_empty() {
            return Err(HarnessError::Artifact("path is empty".into()));
        }
        for part in &parts {
            if part.is_empty() || part == "." || part == ".." || part.contains('/') {
                return Err(HarnessError::Artifact(format!("bad path component {part}")));
            }
        }
        Ok(Self { parts })
    }

    pub(crate) fn parts(&self) -> &[String] {
        &self.parts
    }

    pub(crate) fn join(&self) -> String {
        self.parts.join("/")
    }
}

impl std::fmt::Display for RelPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.join())
    }
}

/// `st_mode & 0o7777`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UnixMode(u32);

impl UnixMode {
    pub(crate) fn new(bits: u32) -> Result<Self, HarnessError> {
        if bits & !0o7777 != 0 {
            return Err(HarnessError::Artifact(format!("mode {bits:o} exceeds 07777")));
        }
        Ok(Self(bits))
    }

    pub(crate) fn bits(self) -> u32 {
        self.0
    }
}

/// Nanoseconds since the Unix epoch. Negative stamps are values, not errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MtimeNs(i64);

impl MtimeNs {
    pub(crate) fn new(ns: i64) -> Self {
        Self(ns)
    }

    pub(crate) fn get(self) -> i64 {
        self.0
    }
}

/// Filesystem writes. The actor must be frozen before the step runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DiskOp {
    Put {
        actor: Actor,
        path: RelPath,
        bytes: Vec<u8>,
        mode: UnixMode,
        mtime: MtimeNs,
    },
    /// New bytes, same length and mtime as the live file.
    /// Spec: other trees keep the previous bytes.
    RetouchSameStamp {
        actor: Actor,
        path: RelPath,
        bytes: Vec<u8>,
    },
    Chmod {
        actor: Actor,
        path: RelPath,
        mode: UnixMode,
    },
    /// `target` is the link text. A missing target is a valid dangling symlink.
    Symlink {
        actor: Actor,
        path: RelPath,
        target: Vec<u8>,
    },
    Unlink { actor: Actor, path: RelPath },
    Rename {
        actor: Actor,
        from: RelPath,
        to: RelPath,
    },
    Mkdir {
        actor: Actor,
        path: RelPath,
        mode: UnixMode,
    },
    Special {
        actor: Actor,
        path: RelPath,
        kind: SpecialFile,
    },
    NonUtf8 {
        actor: Actor,
        parent: RelPath,
        name: Vec<u8>,
    },
    HardLink {
        actor: Actor,
        from: RelPath,
        to: RelPath,
    },
    /// Intermediate symlink whose target is the yard: inside the private
    /// root, outside every replicated tree.
    EscapeLink { actor: Actor, path: RelPath },
    /// Clear user read and execute. The epoch that contains this op is
    /// [`crate::oracle::Projection::UnspecifiedIo`].
    Deny { actor: Actor, path: RelPath },
}

impl DiskOp {
    pub(crate) fn actor(&self) -> Actor {
        match self {
            DiskOp::Put { actor, .. }
            | DiskOp::RetouchSameStamp { actor, .. }
            | DiskOp::Chmod { actor, .. }
            | DiskOp::Symlink { actor, .. }
            | DiskOp::Unlink { actor, .. }
            | DiskOp::Rename { actor, .. }
            | DiskOp::Mkdir { actor, .. }
            | DiskOp::Special { actor, .. }
            | DiskOp::NonUtf8 { actor, .. }
            | DiskOp::HardLink { actor, .. }
            | DiskOp::EscapeLink { actor, .. }
            | DiskOp::Deny { actor, .. } => *actor,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpecialFile {
    Fifo,
    Socket,
    Device,
}

/// Steps that are not a write into a frozen tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WorldOp {
    Grammar(GrammarFault),
    /// One real QUIC transfer of a body whose hash will not match `path`.
    BadBulk { path: RelPath },
    /// Rewrite the live master TOML with a struck key. Mode stays `0600`.
    /// The process must stay on the old root.
    StruckReload,
    /// `arborsync restore` without `--pretend`. The master is stopped.
    /// The source is the fuzzer-owned copy sealed at `Settle` number `epoch`.
    Restore { epoch: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GrammarFault {
    TruncatedLength,
    ControlOver1MiB,
    TrailingBytes,
    BadVersion,
    UnknownVariant,
    BulkChunkOver16MiB,
    BodySizeMismatch,
    UnknownBulkEncoding,
    BadAsp1,
}

impl Schedule {
    /// Deterministic. The result passes [`Schedule::validate`].
    /// One weighted draw over the op enums, not a catalog of named scenarios.
    /// Does not emit [`WorldOp::Grammar`] or [`WorldOp::BadBulk`].
    pub fn from_seed(seed: u64, limits: Limits) -> Self {
        let schedule = generate(seed, &limits);
        schedule
            .validate()
            .expect("from_seed emits a schedule that validates");
        schedule
    }

    pub(crate) fn try_from_parts(
        seed: u64,
        layout: Layout,
        steps: Vec<Step>,
    ) -> Result<Self, HarnessError> {
        let schedule = Self {
            seed,
            layout,
            steps,
        };
        schedule.validate()?;
        Ok(schedule)
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, HarnessError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|err| HarnessError::Artifact(format!("artifact is not utf-8: {err}")))?;
        parse_artifact(text)
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut steps = String::new();
        for (i, step) in self.steps.iter().enumerate() {
            if i > 0 {
                steps.push(',');
            }
            steps.push('"');
            steps.push_str(&step_token(step));
            steps.push('"');
        }
        let layout = match &self.layout {
            Layout::One => "one",
            Layout::Two {
                second: SecondView::Whole,
            } => "two-whole",
            Layout::Two {
                second: SecondView::Nested,
            } => "two-nested",
        };
        format!(
            "{{\"seed\":{},\"layout\":\"{layout}\",\"steps\":[{steps}]}}",
            self.seed
        )
        .into_bytes()
    }

    pub(crate) fn seed(&self) -> u64 {
        self.seed
    }

    pub(crate) fn layout(&self) -> &Layout {
        &self.layout
    }

    pub(crate) fn steps(&self) -> &[Step] {
        &self.steps
    }

    pub(crate) fn prefix(&self, len: usize) -> Result<Self, HarnessError> {
        Self::try_from_parts(self.seed, self.layout.clone(), self.steps[..len].to_vec())
    }

    /// Structural invariants listed on [`Schedule`]. Both constructors call this.
    pub(crate) fn validate(&self) -> Result<(), HarnessError> {
        let slaves = self.layout.slave_count();
        let mut frozen = BTreeSet::new();
        let mut disk = false;
        let mut deny = false;
        let mut settles = 0u32;
        for step in &self.steps {
            match step {
                Step::Freeze(actor) => {
                    check_actor(*actor, slaves)?;
                    if !frozen.insert(*actor) {
                        return Err(HarnessError::Artifact(format!(
                            "{} is already frozen",
                            actor.label()
                        )));
                    }
                }
                Step::Thaw(actor) => {
                    check_actor(*actor, slaves)?;
                    if !frozen.remove(actor) {
                        return Err(HarnessError::Artifact(format!(
                            "{} is not frozen",
                            actor.label()
                        )));
                    }
                }
                Step::Disk(op) => {
                    check_actor(op.actor(), slaves)?;
                    if !frozen.contains(&op.actor()) {
                        return Err(HarnessError::Artifact(format!(
                            "disk op on thawed {}",
                            op.actor().label()
                        )));
                    }
                    if matches!(op, DiskOp::Deny { .. }) {
                        if disk || deny {
                            return Err(HarnessError::Artifact(
                                "deny shares its epoch with another disk op".into(),
                            ));
                        }
                        deny = true;
                        disk = true;
                    } else if deny {
                        return Err(HarnessError::Artifact(
                            "disk op shares an epoch with deny".into(),
                        ));
                    } else {
                        disk = true;
                    }
                }
                Step::World(WorldOp::Restore { epoch }) => {
                    if *epoch >= settles {
                        return Err(HarnessError::Artifact(format!(
                            "restore epoch {epoch} is not a prior settle"
                        )));
                    }
                }
                Step::World(_) | Step::Quiesce => {}
                Step::Settle => {
                    if !frozen.is_empty() {
                        return Err(HarnessError::Artifact(
                            "settle while an actor is frozen".into(),
                        ));
                    }
                    settles += 1;
                    disk = false;
                    deny = false;
                }
            }
        }
        if !frozen.is_empty() {
            return Err(HarnessError::Artifact(
                "schedule ends with a frozen actor".into(),
            ));
        }
        Ok(())
    }
}

fn check_actor(actor: Actor, slaves: SlaveCount) -> Result<(), HarnessError> {
    if let Actor::Slave(ix) = actor {
        if ix.get() >= slaves.get() {
            return Err(HarnessError::Artifact(format!(
                "slave index {} outside layout",
                ix.get()
            )));
        }
    }
    Ok(())
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0xA5A5_A5A5_1234_5678
        } else {
            seed
        })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: usize) -> usize {
        debug_assert!(n > 0);
        (self.next_u64() as usize) % n
    }

    fn pick<T: Copy>(&mut self, items: &[(u32, T)]) -> T {
        let total: u32 = items.iter().map(|item| item.0).sum();
        let mut x = (self.next_u64() as u32) % total.max(1);
        for (weight, value) in items {
            if x < *weight {
                return *value;
            }
            x -= *weight;
        }
        items[0].1
    }
}

#[derive(Clone, Copy)]
enum Gen {
    Freeze,
    Thaw,
    Put,
    Retouch,
    Chmod,
    Symlink,
    Unlink,
    Rename,
    Mkdir,
    Special,
    NonUtf8,
    HardLink,
    Escape,
    Deny,
    Struck,
    Restore,
    Quiesce,
    Settle,
}

fn generate(seed: u64, limits: &Limits) -> Schedule {
    let mut rng = Rng::new(seed);
    let layout = if limits.slaves().get() == 1 {
        Layout::One
    } else if rng.below(2) == 0 {
        Layout::Two {
            second: SecondView::Whole,
        }
    } else {
        Layout::Two {
            second: SecondView::Nested,
        }
    };
    let actors = actors(&layout);
    let n = limits.steps() as usize;
    let mut steps = Vec::with_capacity(n);
    let mut frozen: Vec<Actor> = Vec::new();
    let mut settles = 0u32;
    let mut disk = false;
    let mut deny = false;
    while steps.len() < n {
        let remaining = n - steps.len();
        if !frozen.is_empty() && remaining <= frozen.len() {
            let actor = frozen.pop().expect("frozen");
            steps.push(Step::Thaw(actor));
            continue;
        }
        let mut menu: Vec<(u32, Gen)> = Vec::new();
        if remaining > frozen.len() + 1 && frozen.len() < actors.len() {
            menu.push((3, Gen::Freeze));
        }
        if !frozen.is_empty() {
            menu.push((3, Gen::Thaw));
        }
        if !frozen.is_empty() && remaining > frozen.len() {
            if deny {
                // The epoch already has a deny. No further disk op.
            } else if disk {
                menu.extend([
                    (5, Gen::Put),
                    (2, Gen::Retouch),
                    (2, Gen::Chmod),
                    (2, Gen::Symlink),
                    (2, Gen::Unlink),
                    (2, Gen::Rename),
                    (2, Gen::Mkdir),
                    (1, Gen::Special),
                    (1, Gen::NonUtf8),
                    (1, Gen::HardLink),
                    (1, Gen::Escape),
                ]);
            } else {
                menu.extend([
                    (5, Gen::Put),
                    (2, Gen::Retouch),
                    (2, Gen::Chmod),
                    (2, Gen::Symlink),
                    (2, Gen::Unlink),
                    (2, Gen::Rename),
                    (2, Gen::Mkdir),
                    (1, Gen::Special),
                    (1, Gen::NonUtf8),
                    (1, Gen::HardLink),
                    (1, Gen::Escape),
                    (1, Gen::Deny),
                ]);
            }
        }
        let master_frozen = frozen.contains(&Actor::Master);
        if !master_frozen {
            menu.push((1, Gen::Struck));
        }
        if frozen.is_empty() && settles > 0 {
            menu.push((1, Gen::Restore));
        }
        menu.push((3, Gen::Quiesce));
        if frozen.is_empty() {
            menu.push((4, Gen::Settle));
        }
        let choice = rng.pick(&menu);
        match choice {
            Gen::Freeze => {
                let open: Vec<Actor> = actors.iter().copied().filter(|a| !frozen.contains(a)).collect();
                let actor = open[rng.below(open.len())];
                frozen.push(actor);
                steps.push(Step::Freeze(actor));
            }
            Gen::Thaw => {
                let idx = rng.below(frozen.len());
                let actor = frozen.remove(idx);
                steps.push(Step::Thaw(actor));
            }
            Gen::Put => steps.push(Step::Disk(put(&mut rng, &frozen))),
            Gen::Retouch => steps.push(Step::Disk(retouch(&mut rng, &frozen))),
            Gen::Chmod => steps.push(Step::Disk(chmod(&mut rng, &frozen))),
            Gen::Symlink => steps.push(Step::Disk(symlink(&mut rng, &frozen))),
            Gen::Unlink => steps.push(Step::Disk(unlink(&mut rng, &frozen))),
            Gen::Rename => steps.push(Step::Disk(rename(&mut rng, &frozen))),
            Gen::Mkdir => steps.push(Step::Disk(mkdir(&mut rng, &frozen))),
            Gen::Special => steps.push(Step::Disk(special(&mut rng, &frozen))),
            Gen::NonUtf8 => steps.push(Step::Disk(non_utf8(&mut rng, &frozen))),
            Gen::HardLink => steps.push(Step::Disk(hard_link(&mut rng, &frozen))),
            Gen::Escape => steps.push(Step::Disk(escape(&mut rng, &frozen))),
            Gen::Deny => {
                deny = true;
                disk = true;
                steps.push(Step::Disk(deny_op(&mut rng, &frozen)));
            }
            Gen::Struck => steps.push(Step::World(WorldOp::StruckReload)),
            Gen::Restore => {
                let epoch = rng.below(settles as usize) as u32;
                steps.push(Step::World(WorldOp::Restore { epoch }));
            }
            Gen::Quiesce => steps.push(Step::Quiesce),
            Gen::Settle => {
                settles += 1;
                disk = false;
                deny = false;
                steps.push(Step::Settle);
            }
        }
        if matches!(
            choice,
            Gen::Put
                | Gen::Retouch
                | Gen::Chmod
                | Gen::Symlink
                | Gen::Unlink
                | Gen::Rename
                | Gen::Mkdir
                | Gen::Special
                | Gen::NonUtf8
                | Gen::HardLink
                | Gen::Escape
        ) {
            disk = true;
        }
    }
    Schedule {
        seed,
        layout,
        steps,
    }
}

fn actors(layout: &Layout) -> Vec<Actor> {
    let mut out = vec![Actor::Master];
    for index in 0..layout.slave_count().get() {
        out.push(Actor::Slave(SlaveIx(index)));
    }
    out
}

fn frozen_actor(rng: &mut Rng, frozen: &[Actor]) -> Actor {
    frozen[rng.below(frozen.len())]
}

fn rel(parts: &[&str]) -> RelPath {
    RelPath::new(parts.iter().map(|p| (*p).to_string()).collect()).expect("static path")
}

fn path(rng: &mut Rng) -> RelPath {
    const PATHS: &[&[&str]] = &[
        &["a"],
        &["b"],
        &["c"],
        &["dir", "x"],
        &["nested", "y"],
        &["nested", "z"],
    ];
    rel(PATHS[rng.below(PATHS.len())])
}

fn other_path(rng: &mut Rng, first: &RelPath) -> RelPath {
    for _ in 0..8 {
        let next = path(rng);
        if next != *first {
            return next;
        }
    }
    rel(&["b"])
}

fn mode(rng: &mut Rng) -> UnixMode {
    let bits = [0o644, 0o600, 0o755][rng.below(3)];
    UnixMode::new(bits).expect("mode")
}

fn mtime(rng: &mut Rng) -> MtimeNs {
    MtimeNs::new(1_700_000_000_000_000_000 + (rng.below(1000) as i64) * 1000)
}

fn bytes(rng: &mut Rng) -> Vec<u8> {
    (0..4).map(|_| (rng.next_u64() & 0xff) as u8).collect()
}

fn put(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    DiskOp::Put {
        actor: frozen_actor(rng, frozen),
        path: path(rng),
        bytes: bytes(rng),
        mode: mode(rng),
        mtime: mtime(rng),
    }
}

fn retouch(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    DiskOp::RetouchSameStamp {
        actor: frozen_actor(rng, frozen),
        path: path(rng),
        bytes: bytes(rng),
    }
}

fn chmod(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    DiskOp::Chmod {
        actor: frozen_actor(rng, frozen),
        path: path(rng),
        mode: mode(rng),
    }
}

fn symlink(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    DiskOp::Symlink {
        actor: frozen_actor(rng, frozen),
        path: path(rng),
        target: bytes(rng),
    }
}

fn unlink(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    DiskOp::Unlink {
        actor: frozen_actor(rng, frozen),
        path: path(rng),
    }
}

fn rename(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    let from = path(rng);
    let to = other_path(rng, &from);
    DiskOp::Rename {
        actor: frozen_actor(rng, frozen),
        from,
        to,
    }
}

fn mkdir(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    DiskOp::Mkdir {
        actor: frozen_actor(rng, frozen),
        path: path(rng),
        mode: mode(rng),
    }
}

fn special(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    let kind = [SpecialFile::Fifo, SpecialFile::Socket, SpecialFile::Device][rng.below(3)];
    DiskOp::Special {
        actor: frozen_actor(rng, frozen),
        path: path(rng),
        kind,
    }
}

fn non_utf8(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    DiskOp::NonUtf8 {
        actor: frozen_actor(rng, frozen),
        parent: rel(&["dir"]),
        name: vec![0xff, (rng.next_u64() & 0xff) as u8],
    }
}

fn hard_link(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    let from = path(rng);
    let to = other_path(rng, &from);
    DiskOp::HardLink {
        actor: frozen_actor(rng, frozen),
        from,
        to,
    }
}

fn escape(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    DiskOp::EscapeLink {
        actor: frozen_actor(rng, frozen),
        path: path(rng),
    }
}

fn deny_op(rng: &mut Rng, frozen: &[Actor]) -> DiskOp {
    DiskOp::Deny {
        actor: frozen_actor(rng, frozen),
        path: path(rng),
    }
}

fn step_token(step: &Step) -> String {
    match step {
        Step::Freeze(actor) => format!("freeze {}", actor_token(*actor)),
        Step::Thaw(actor) => format!("thaw {}", actor_token(*actor)),
        Step::Quiesce => "quiesce".into(),
        Step::Settle => "settle".into(),
        Step::World(op) => format!("world {}", world_token(op)),
        Step::Disk(op) => format!("disk {}", disk_token(op)),
    }
}

fn actor_token(actor: Actor) -> String {
    match actor {
        Actor::Master => "master".into(),
        Actor::Slave(ix) => format!("slave:{}", ix.get()),
    }
}

fn world_token(op: &WorldOp) -> String {
    match op {
        WorldOp::StruckReload => "struck".into(),
        WorldOp::Restore { epoch } => format!("restore:{epoch}"),
        WorldOp::BadBulk { path } => format!("badbulk:{}", path.join()),
        WorldOp::Grammar(fault) => format!("grammar:{}", grammar_token(*fault)),
    }
}

fn grammar_token(fault: GrammarFault) -> &'static str {
    match fault {
        GrammarFault::TruncatedLength => "truncated",
        GrammarFault::ControlOver1MiB => "control",
        GrammarFault::TrailingBytes => "trailing",
        GrammarFault::BadVersion => "version",
        GrammarFault::UnknownVariant => "variant",
        GrammarFault::BulkChunkOver16MiB => "bulk",
        GrammarFault::BodySizeMismatch => "bodysize",
        GrammarFault::UnknownBulkEncoding => "encoding",
        GrammarFault::BadAsp1 => "asp1",
    }
}

fn disk_token(op: &DiskOp) -> String {
    match op {
        DiskOp::Put {
            actor,
            path,
            bytes,
            mode,
            mtime,
        } => format!(
            "put {} {} {:o} {} {}",
            actor_token(*actor),
            path.join(),
            mode.bits(),
            mtime.get(),
            hex_encode(bytes)
        ),
        DiskOp::RetouchSameStamp { actor, path, bytes } => format!(
            "retouch {} {} {}",
            actor_token(*actor),
            path.join(),
            hex_encode(bytes)
        ),
        DiskOp::Chmod { actor, path, mode } => {
            format!("chmod {} {} {:o}", actor_token(*actor), path.join(), mode.bits())
        }
        DiskOp::Symlink { actor, path, target } => format!(
            "symlink {} {} {}",
            actor_token(*actor),
            path.join(),
            hex_encode(target)
        ),
        DiskOp::Unlink { actor, path } => {
            format!("unlink {} {}", actor_token(*actor), path.join())
        }
        DiskOp::Rename { actor, from, to } => format!(
            "rename {} {} {}",
            actor_token(*actor),
            from.join(),
            to.join()
        ),
        DiskOp::Mkdir { actor, path, mode } => {
            format!("mkdir {} {} {:o}", actor_token(*actor), path.join(), mode.bits())
        }
        DiskOp::Special { actor, path, kind } => format!(
            "special {} {} {}",
            actor_token(*actor),
            path.join(),
            match kind {
                SpecialFile::Fifo => "fifo",
                SpecialFile::Socket => "socket",
                SpecialFile::Device => "device",
            }
        ),
        DiskOp::NonUtf8 { actor, parent, name } => format!(
            "nonutf8 {} {} {}",
            actor_token(*actor),
            parent.join(),
            hex_encode(name)
        ),
        DiskOp::HardLink { actor, from, to } => format!(
            "hardlink {} {} {}",
            actor_token(*actor),
            from.join(),
            to.join()
        ),
        DiskOp::EscapeLink { actor, path } => {
            format!("escape {} {}", actor_token(*actor), path.join())
        }
        DiskOp::Deny { actor, path } => format!("deny {} {}", actor_token(*actor), path.join()),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, HarnessError> {
    if text.len() % 2 != 0 {
        return Err(HarnessError::Artifact("odd hex".into()));
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_val(bytes[i])?;
        let lo = hex_val(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_val(byte: u8) -> Result<u8, HarnessError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(HarnessError::Artifact("bad hex".into())),
    }
}

fn parse_artifact(text: &str) -> Result<Schedule, HarnessError> {
    let seed = scan_u64(text, "\"seed\":")?;
    let layout_name = scan_str(text, "\"layout\":\"")?;
    let layout = match layout_name {
        "one" => Layout::One,
        "two-whole" => Layout::Two {
            second: SecondView::Whole,
        },
        "two-nested" => Layout::Two {
            second: SecondView::Nested,
        },
        other => {
            return Err(HarnessError::Artifact(format!("unknown layout {other}")));
        }
    };
    let steps_body = scan_array(text, "\"steps\":[")?;
    let mut steps = Vec::new();
    if !steps_body.is_empty() {
        for raw in split_json_strings(steps_body)? {
            steps.push(parse_step(raw, layout.slave_count())?);
        }
    }
    Schedule::try_from_parts(seed, layout, steps)
}

fn scan_u64(text: &str, key: &str) -> Result<u64, HarnessError> {
    let rest = text
        .split_once(key)
        .ok_or_else(|| HarnessError::Artifact(format!("missing {key}")))?
        .1;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits
        .parse()
        .map_err(|_| HarnessError::Artifact(format!("bad number after {key}")))
}

fn scan_str<'a>(text: &'a str, key: &str) -> Result<&'a str, HarnessError> {
    let rest = text
        .split_once(key)
        .ok_or_else(|| HarnessError::Artifact(format!("missing {key}")))?
        .1;
    let end = rest
        .find('"')
        .ok_or_else(|| HarnessError::Artifact(format!("unterminated {key}")))?;
    Ok(&rest[..end])
}

fn scan_array<'a>(text: &'a str, key: &str) -> Result<&'a str, HarnessError> {
    let rest = text
        .split_once(key)
        .ok_or_else(|| HarnessError::Artifact(format!("missing {key}")))?
        .1;
    let end = rest
        .find(']')
        .ok_or_else(|| HarnessError::Artifact("unterminated steps".into()))?;
    Ok(&rest[..end])
}

fn split_json_strings(body: &str) -> Result<Vec<&str>, HarnessError> {
    let mut out = Vec::new();
    let mut rest = body.trim();
    while !rest.is_empty() {
        rest = rest.trim_start_matches(|c: char| c == ',' || c.is_whitespace());
        if rest.is_empty() {
            break;
        }
        if !rest.starts_with('"') {
            return Err(HarnessError::Artifact("step is not a string".into()));
        }
        let end = rest[1..]
            .find('"')
            .ok_or_else(|| HarnessError::Artifact("unterminated step".into()))?;
        out.push(&rest[1..1 + end]);
        rest = &rest[1 + end + 1..];
    }
    Ok(out)
}

fn parse_step(token: &str, slaves: SlaveCount) -> Result<Step, HarnessError> {
    let mut parts = token.split_whitespace();
    let kind = parts
        .next()
        .ok_or_else(|| HarnessError::Artifact("empty step".into()))?;
    match kind {
        "freeze" => Ok(Step::Freeze(parse_actor(parts.next(), slaves)?)),
        "thaw" => Ok(Step::Thaw(parse_actor(parts.next(), slaves)?)),
        "quiesce" => Ok(Step::Quiesce),
        "settle" => Ok(Step::Settle),
        "world" => Ok(Step::World(parse_world(parts.next())?)),
        "disk" => {
            let op = parts.next().ok_or_else(|| HarnessError::Artifact("disk op".into()))?;
            let rest: Vec<&str> = parts.collect();
            Ok(Step::Disk(parse_disk(op, &rest, slaves)?))
        }
        other => Err(HarnessError::Artifact(format!("unknown step {other}"))),
    }
}

fn parse_actor(token: Option<&str>, slaves: SlaveCount) -> Result<Actor, HarnessError> {
    match token {
        Some("master") => Ok(Actor::Master),
        Some(other) => {
            let index = other
                .strip_prefix("slave:")
                .ok_or_else(|| HarnessError::Artifact(format!("bad actor {other}")))?;
            let index: u8 = index
                .parse()
                .map_err(|_| HarnessError::Artifact(format!("bad actor {other}")))?;
            Ok(Actor::Slave(SlaveIx::new(index, slaves)?))
        }
        None => Err(HarnessError::Artifact("missing actor".into())),
    }
}

fn parse_world(token: Option<&str>) -> Result<WorldOp, HarnessError> {
    let token = token.ok_or_else(|| HarnessError::Artifact("missing world op".into()))?;
    if token == "struck" {
        return Ok(WorldOp::StruckReload);
    }
    if let Some(epoch) = token.strip_prefix("restore:") {
        let epoch = epoch
            .parse()
            .map_err(|_| HarnessError::Artifact("bad restore epoch".into()))?;
        return Ok(WorldOp::Restore { epoch });
    }
    if let Some(path) = token.strip_prefix("badbulk:") {
        return Ok(WorldOp::BadBulk {
            path: parse_path(path)?,
        });
    }
    if let Some(name) = token.strip_prefix("grammar:") {
        let fault = match name {
            "truncated" => GrammarFault::TruncatedLength,
            "control" => GrammarFault::ControlOver1MiB,
            "trailing" => GrammarFault::TrailingBytes,
            "version" => GrammarFault::BadVersion,
            "variant" => GrammarFault::UnknownVariant,
            "bulk" => GrammarFault::BulkChunkOver16MiB,
            "bodysize" => GrammarFault::BodySizeMismatch,
            "encoding" => GrammarFault::UnknownBulkEncoding,
            "asp1" => GrammarFault::BadAsp1,
            _ => return Err(HarnessError::Artifact(format!("bad grammar {name}"))),
        };
        return Ok(WorldOp::Grammar(fault));
    }
    Err(HarnessError::Artifact(format!("bad world op {token}")))
}

fn parse_path(text: &str) -> Result<RelPath, HarnessError> {
    RelPath::new(text.split('/').map(|part| part.to_string()).collect())
}

fn parse_mode(text: &str) -> Result<UnixMode, HarnessError> {
    let bits = u32::from_str_radix(text, 8)
        .map_err(|_| HarnessError::Artifact(format!("bad mode {text}")))?;
    UnixMode::new(bits)
}

fn parse_disk(op: &str, rest: &[&str], slaves: SlaveCount) -> Result<DiskOp, HarnessError> {
    let need = |n: usize| -> Result<(), HarnessError> {
        if rest.len() != n {
            Err(HarnessError::Artifact(format!("{op} has wrong arity")))
        } else {
            Ok(())
        }
    };
    match op {
        "put" => {
            need(5)?;
            Ok(DiskOp::Put {
                actor: parse_actor(Some(rest[0]), slaves)?,
                path: parse_path(rest[1])?,
                mode: parse_mode(rest[2])?,
                mtime: MtimeNs::new(
                    rest[3]
                        .parse()
                        .map_err(|_| HarnessError::Artifact("bad mtime".into()))?,
                ),
                bytes: hex_decode(rest[4])?,
            })
        }
        "retouch" => {
            need(3)?;
            Ok(DiskOp::RetouchSameStamp {
                actor: parse_actor(Some(rest[0]), slaves)?,
                path: parse_path(rest[1])?,
                bytes: hex_decode(rest[2])?,
            })
        }
        "chmod" => {
            need(3)?;
            Ok(DiskOp::Chmod {
                actor: parse_actor(Some(rest[0]), slaves)?,
                path: parse_path(rest[1])?,
                mode: parse_mode(rest[2])?,
            })
        }
        "symlink" => {
            need(3)?;
            Ok(DiskOp::Symlink {
                actor: parse_actor(Some(rest[0]), slaves)?,
                path: parse_path(rest[1])?,
                target: hex_decode(rest[2])?,
            })
        }
        "unlink" => {
            need(2)?;
            Ok(DiskOp::Unlink {
                actor: parse_actor(Some(rest[0]), slaves)?,
                path: parse_path(rest[1])?,
            })
        }
        "rename" => {
            need(3)?;
            Ok(DiskOp::Rename {
                actor: parse_actor(Some(rest[0]), slaves)?,
                from: parse_path(rest[1])?,
                to: parse_path(rest[2])?,
            })
        }
        "mkdir" => {
            need(3)?;
            Ok(DiskOp::Mkdir {
                actor: parse_actor(Some(rest[0]), slaves)?,
                path: parse_path(rest[1])?,
                mode: parse_mode(rest[2])?,
            })
        }
        "special" => {
            need(3)?;
            let kind = match rest[2] {
                "fifo" => SpecialFile::Fifo,
                "socket" => SpecialFile::Socket,
                "device" => SpecialFile::Device,
                other => {
                    return Err(HarnessError::Artifact(format!("bad special {other}")));
                }
            };
            Ok(DiskOp::Special {
                actor: parse_actor(Some(rest[0]), slaves)?,
                path: parse_path(rest[1])?,
                kind,
            })
        }
        "nonutf8" => {
            need(3)?;
            Ok(DiskOp::NonUtf8 {
                actor: parse_actor(Some(rest[0]), slaves)?,
                parent: parse_path(rest[1])?,
                name: hex_decode(rest[2])?,
            })
        }
        "hardlink" => {
            need(3)?;
            Ok(DiskOp::HardLink {
                actor: parse_actor(Some(rest[0]), slaves)?,
                from: parse_path(rest[1])?,
                to: parse_path(rest[2])?,
            })
        }
        "escape" => {
            need(2)?;
            Ok(DiskOp::EscapeLink {
                actor: parse_actor(Some(rest[0]), slaves)?,
                path: parse_path(rest[1])?,
            })
        }
        "deny" => {
            need(2)?;
            Ok(DiskOp::Deny {
                actor: parse_actor(Some(rest[0]), slaves)?,
                path: parse_path(rest[1])?,
            })
        }
        other => Err(HarnessError::Artifact(format!("unknown disk op {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_seed_is_valid_and_omits_uninjected_ops() {
        for seed in [0, 1, 7, 99] {
            for slaves in [1u8, 2] {
                let schedule = Schedule::from_seed(seed, Limits::new(8, slaves).unwrap());
                schedule.validate().unwrap();
                assert_eq!(schedule.steps().len(), 8);
                for step in schedule.steps() {
                    match step {
                        Step::World(WorldOp::Grammar(_)) | Step::World(WorldOp::BadBulk { .. }) => {
                            panic!("seed {seed} slaves {slaves} emitted {step:?}");
                        }
                        _ => {}
                    }
                }
                let again = Schedule::from_bytes(&schedule.to_bytes()).unwrap();
                assert_eq!(again, schedule);
            }
        }
    }

    #[test]
    fn limits_reject_zero_steps_and_three_slaves() {
        assert!(Limits::new(0, 1).is_err());
        assert!(Limits::new(4, 0).is_err());
        assert!(Limits::new(4, 3).is_err());
    }
}

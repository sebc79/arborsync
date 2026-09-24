use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::keys::parse_hex_key;
use crate::path::{CanonicalPath, PathError, local_paths_overlap};
use crate::storage::CheckoutId;
use crate::tune::{Tune, TuneRole, TuneSpec, project_tune};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlaveAcl {
    pub id: String,
    pub public_keys: Vec<String>,
    pub allowed_prefixes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MasterConfig {
    pub central_root: String,
    pub listen_addr: String,
    pub master_key_path: String,
    pub db_path: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_debounce_ms")]
    pub watcher_debounce_ms: u64,
    #[serde(default = "default_rescan_seconds")]
    pub rescan_interval_seconds: u64,
    #[serde(default = "default_status_seconds")]
    pub status_interval_seconds: u64,
    #[serde(default = "default_max_checkouts")]
    pub max_checkouts_per_slave: u32,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_max_attempts")]
    pub max_connection_attempts_per_minute: u32,
    #[serde(default)]
    pub slaves: Vec<SlaveAcl>,
    #[serde(default)]
    pub tune: TuneSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckoutConfig {
    pub id: String,
    pub central: String,
    pub local: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlaveConfig {
    pub slave_id: String,
    pub master_addr: String,
    pub slave_key_path: String,
    pub master_public_keys: Vec<String>,
    pub db_path: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_max_checkouts")]
    pub max_checkouts_per_slave: u32,
    #[serde(default = "default_debounce_ms")]
    pub watcher_debounce_ms: u64,
    #[serde(default = "default_rescan_seconds")]
    pub rescan_interval_seconds: u64,
    #[serde(default = "default_status_seconds")]
    pub status_interval_seconds: u64,
    #[serde(default)]
    pub checkouts: Vec<CheckoutConfig>,
    #[serde(default)]
    pub tune: TuneSpec,
}

fn default_log_level() -> String {
    "info".into()
}

fn default_debounce_ms() -> u64 {
    200
}

fn default_rescan_seconds() -> u64 {
    60
}

fn default_status_seconds() -> u64 {
    5
}

fn default_max_checkouts() -> u32 {
    100
}

fn default_max_connections() -> u32 {
    100
}

fn default_max_attempts() -> u32 {
    60
}

impl MasterConfig {
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }

    pub fn from_toml(s: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(s)
    }
}

impl SlaveConfig {
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }

    pub fn from_toml(s: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(s)
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("parse {path}: {source}")]
    Toml {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("watcher_debounce_ms {value} not in 200..=500")]
    DebounceOutOfRange { value: u64 },
    #[error("unknown log_level {value}")]
    BadLogLevel { value: String },
    #[error("status_interval_seconds {value} exceeds 3600")]
    StatusIntervalOutOfRange { value: u64 },
    #[error("rescan_interval_seconds must be at least 1")]
    RescanIntervalZero,
    #[error("invalid pin at {field}")]
    BadPin { field: String },
    #[error("invalid prefix at {field}: {source}")]
    BadPrefix {
        field: String,
        #[source]
        source: PathError,
    },
    #[error("prefix at {field} is not canonical: {value} normalizes to {normalized}")]
    NonNormalizedPrefix {
        field: String,
        value: String,
        normalized: String,
    },
    #[error("invalid id at {field}: {value}")]
    BadId { field: String, value: String },
    #[error("duplicate id {value} at {field}")]
    DuplicateId { field: String, value: String },
    #[error("duplicate public key at {field}")]
    DuplicatePin { field: String },
    #[error("empty key list at {field}")]
    EmptyPins { field: String },
    #[error("local paths overlap: {a} and {b}")]
    LocalOverlap { a: String, b: String },
    #[error("{count} checkouts exceed max_checkouts_per_slave {max}")]
    TooManyCheckouts { count: usize, max: u32 },
    #[error("invalid listen_addr {value}")]
    BadListenAddr { value: String },
    #[error("invalid master_addr {value}")]
    BadMasterAddr { value: String },
    #[error("invalid host path at {field}: {value}")]
    BadHostPath { field: String, value: String },
    #[error("insecure mode {mode:o} on {path}")]
    InsecureMode { path: PathBuf, mode: u32 },
    #[error("tune.hashing.workers {value} not in 1..=256")]
    WorkersOutOfRange { value: u16 },
    #[error("tune.fulfill_parked.inflight {value} not in 1..=64")]
    InflightOutOfRange { value: u16 },
    #[error("tune.hashing.workers {value} is not \"nproc\" or an integer")]
    BadWorkers { value: String },
    #[error("{field} is not valid on {role}")]
    TuneNotOnRole { field: String, role: &'static str },
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ReloadError {
    #[error("restart required to change {fields:?}")]
    RestartRequired { fields: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterReload {
    pub log_level: String,
    pub watcher_debounce_ms: u64,
    pub rescan_interval_seconds: u64,
    pub status_interval_seconds: u64,
    pub max_checkouts_per_slave: u32,
    pub max_connections: u32,
    pub max_connection_attempts_per_minute: u32,
    pub drop_slave_ids: Vec<String>,
    pub drop_peers: Vec<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlaveReload {
    pub log_level: String,
    pub watcher_debounce_ms: u64,
    pub rescan_interval_seconds: u64,
    pub status_interval_seconds: u64,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub resubscribe: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedMaster {
    slaves: Vec<LoadedAcl>,
    by_key: HashMap<[u8; 32], usize>,
    listen_addr: SocketAddr,
    central_root: PathBuf,
    master_key_path: PathBuf,
    db_path: PathBuf,
    log_level: String,
    watcher_debounce_ms: u64,
    rescan_interval_seconds: u64,
    status_interval_seconds: u64,
    max_checkouts_per_slave: u32,
    max_connections: u32,
    max_connection_attempts_per_minute: u32,
    tune: Tune,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedAcl {
    id: String,
    public_keys: Vec<[u8; 32]>,
    allowed_prefixes: Vec<CanonicalPath>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedSlave {
    slave_id: String,
    master_addr: String,
    master_public_keys: Vec<[u8; 32]>,
    checkouts: Vec<LoadedCheckout>,
    slave_key_path: PathBuf,
    db_path: PathBuf,
    log_level: String,
    watcher_debounce_ms: u64,
    rescan_interval_seconds: u64,
    status_interval_seconds: u64,
    max_checkouts_per_slave: u32,
    tune: Tune,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedCheckout {
    id: CheckoutId,
    central: CanonicalPath,
    local: PathBuf,
}

impl LoadedMaster {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let config = MasterConfig::from_toml(s).map_err(|source| ConfigError::Toml {
            path: PathBuf::from("<input>"),
            source,
        })?;
        project_master(config)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        require_mode_600(path)?;
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        match Self::parse(&text) {
            Ok(loaded) => Ok(loaded),
            Err(ConfigError::Toml { source, .. }) => Err(ConfigError::Toml {
                path: path.to_path_buf(),
                source,
            }),
            Err(err) => Err(err),
        }
    }

    pub fn slaves(&self) -> &[LoadedAcl] {
        &self.slaves
    }

    pub fn acl_for_public_key(&self, pin: &[u8; 32]) -> Option<&LoadedAcl> {
        self.by_key.get(pin).map(|&i| &self.slaves[i])
    }

    pub fn acl_for_id(&self, id: &str) -> Option<&LoadedAcl> {
        self.slaves.iter().find(|acl| acl.id == id)
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub fn central_root(&self) -> &Path {
        &self.central_root
    }

    pub fn master_key_path(&self) -> &Path {
        &self.master_key_path
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    pub fn watcher_debounce_ms(&self) -> u64 {
        self.watcher_debounce_ms
    }

    pub fn rescan_interval_seconds(&self) -> u64 {
        self.rescan_interval_seconds
    }

    pub fn status_interval_seconds(&self) -> u64 {
        self.status_interval_seconds
    }

    pub fn max_checkouts_per_slave(&self) -> u32 {
        self.max_checkouts_per_slave
    }

    pub fn max_connections(&self) -> u32 {
        self.max_connections
    }

    pub fn max_connection_attempts_per_minute(&self) -> u32 {
        self.max_connection_attempts_per_minute
    }

    pub fn tune(&self) -> &Tune {
        &self.tune
    }

    pub fn plan_reload(&self, next: &Self) -> Result<MasterReload, ReloadError> {
        let mut fields = Vec::new();
        if self.listen_addr != next.listen_addr {
            fields.push("listen_addr".into());
        }
        if self.db_path != next.db_path {
            fields.push("db_path".into());
        }
        if self.central_root != next.central_root {
            fields.push("central_root".into());
        }
        if self.master_key_path != next.master_key_path {
            fields.push("master_key_path".into());
        }
        fields.sort();
        if !fields.is_empty() {
            return Err(ReloadError::RestartRequired { fields });
        }

        let next_by_id: HashMap<&str, &LoadedAcl> = next
            .slaves
            .iter()
            .map(|acl| (acl.id.as_str(), acl))
            .collect();
        let mut drop_slave_ids: Vec<String> = self
            .slaves
            .iter()
            .filter(|acl| !next_by_id.contains_key(acl.id.as_str()))
            .map(|acl| acl.id.clone())
            .collect();
        drop_slave_ids.sort();

        Ok(MasterReload {
            log_level: next.log_level.clone(),
            watcher_debounce_ms: next.watcher_debounce_ms,
            rescan_interval_seconds: next.rescan_interval_seconds,
            status_interval_seconds: next.status_interval_seconds,
            max_checkouts_per_slave: next.max_checkouts_per_slave,
            max_connections: next.max_connections,
            max_connection_attempts_per_minute: next.max_connection_attempts_per_minute,
            drop_slave_ids,
            drop_peers: Vec::new(),
        })
    }
}

impl LoadedAcl {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn public_keys(&self) -> &[[u8; 32]] {
        &self.public_keys
    }

    pub fn allowed_prefixes(&self) -> &[CanonicalPath] {
        &self.allowed_prefixes
    }

    pub fn allows_central(&self, central: &CanonicalPath) -> bool {
        self.allowed_prefixes
            .iter()
            .any(|prefix| prefix.covers(central))
    }
}

impl LoadedSlave {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let config = SlaveConfig::from_toml(s).map_err(|source| ConfigError::Toml {
            path: PathBuf::from("<input>"),
            source,
        })?;
        project_slave(config)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        require_mode_600(path)?;
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        match Self::parse(&text) {
            Ok(loaded) => Ok(loaded),
            Err(ConfigError::Toml { source, .. }) => Err(ConfigError::Toml {
                path: path.to_path_buf(),
                source,
            }),
            Err(err) => Err(err),
        }
    }

    pub fn slave_id(&self) -> &str {
        &self.slave_id
    }

    pub fn master_addr(&self) -> &str {
        &self.master_addr
    }

    pub fn pins_master(&self) -> &[[u8; 32]] {
        &self.master_public_keys
    }

    pub fn checkouts(&self) -> &[LoadedCheckout] {
        &self.checkouts
    }

    pub fn checkout(&self, id: &str) -> Option<&LoadedCheckout> {
        self.checkouts.iter().find(|c| c.id.as_str() == id)
    }

    pub fn owning_local(&self, host_path: &Path) -> Option<&LoadedCheckout> {
        self.checkouts.iter().find(|c| {
            let local: Vec<_> = c.local.components().collect();
            let host: Vec<_> = host_path.components().collect();
            host.starts_with(&local)
        })
    }

    pub fn slave_key_path(&self) -> &Path {
        &self.slave_key_path
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    pub fn watcher_debounce_ms(&self) -> u64 {
        self.watcher_debounce_ms
    }

    pub fn rescan_interval_seconds(&self) -> u64 {
        self.rescan_interval_seconds
    }

    pub fn status_interval_seconds(&self) -> u64 {
        self.status_interval_seconds
    }

    pub fn max_checkouts_per_slave(&self) -> u32 {
        self.max_checkouts_per_slave
    }

    pub fn tune(&self) -> &Tune {
        &self.tune
    }

    pub fn plan_reload(&self, next: &Self) -> Result<SlaveReload, ReloadError> {
        let mut fields = Vec::new();
        if self.master_addr != next.master_addr {
            fields.push("master_addr".into());
        }
        if self.db_path != next.db_path {
            fields.push("db_path".into());
        }
        if self.slave_key_path != next.slave_key_path {
            fields.push("slave_key_path".into());
        }
        if self.slave_id != next.slave_id {
            fields.push("slave_id".into());
        }
        fields.sort();
        if !fields.is_empty() {
            return Err(ReloadError::RestartRequired { fields });
        }

        let same_identity = |a: &LoadedCheckout, b: &LoadedCheckout| {
            a.id == b.id && a.central == b.central && a.local == b.local
        };
        let removed: Vec<String> = self
            .checkouts
            .iter()
            .filter(|current| {
                !next
                    .checkouts
                    .iter()
                    .any(|upcoming| same_identity(current, upcoming))
            })
            .map(|current| current.id.as_str().to_string())
            .collect();
        let added: Vec<String> = next
            .checkouts
            .iter()
            .filter(|upcoming| {
                !self
                    .checkouts
                    .iter()
                    .any(|current| same_identity(current, upcoming))
            })
            .map(|upcoming| upcoming.id.as_str().to_string())
            .collect();
        let mut removed = removed;
        let mut added = added;
        removed.sort();
        added.sort();
        let resubscribe = !added.is_empty() || !removed.is_empty();
        Ok(SlaveReload {
            log_level: next.log_level.clone(),
            watcher_debounce_ms: next.watcher_debounce_ms,
            rescan_interval_seconds: next.rescan_interval_seconds,
            status_interval_seconds: next.status_interval_seconds,
            added,
            removed,
            resubscribe,
        })
    }
}

impl LoadedCheckout {
    pub fn id(&self) -> &CheckoutId {
        &self.id
    }

    pub fn central(&self) -> &CanonicalPath {
        &self.central
    }

    pub fn local(&self) -> &Path {
        &self.local
    }
}

fn project_master(config: MasterConfig) -> Result<LoadedMaster, ConfigError> {
    check_debounce(config.watcher_debounce_ms)?;
    check_rescan_interval(config.rescan_interval_seconds)?;
    check_status_interval(config.status_interval_seconds)?;
    check_log_level(&config.log_level)?;
    let listen_addr =
        config
            .listen_addr
            .parse::<SocketAddr>()
            .map_err(|_| ConfigError::BadListenAddr {
                value: config.listen_addr.clone(),
            })?;
    let central_root = expand_host_path("central_root", &config.central_root)?;
    let master_key_path = expand_host_path("master_key_path", &config.master_key_path)?;
    let db_path = expand_host_path("db_path", &config.db_path)?;
    let tune = project_tune(config.tune, TuneRole::Master)?;

    let mut slaves = Vec::with_capacity(config.slaves.len());
    let mut by_key = HashMap::new();
    let mut seen_ids = HashSet::new();
    for (i, acl) in config.slaves.into_iter().enumerate() {
        let field_id = format!("slaves[{i}].id");
        check_id(&field_id, &acl.id)?;
        if !seen_ids.insert(acl.id.clone()) {
            return Err(ConfigError::DuplicateId {
                field: field_id,
                value: acl.id,
            });
        }
        if acl.public_keys.is_empty() {
            return Err(ConfigError::EmptyPins {
                field: format!("slaves[{i}].public_keys"),
            });
        }
        let mut public_keys = Vec::with_capacity(acl.public_keys.len());
        for (j, pin) in acl.public_keys.iter().enumerate() {
            let field = format!("slaves[{i}].public_keys[{j}]");
            let bytes = parse_pin(&field, pin)?;
            if by_key.insert(bytes, slaves.len()).is_some() {
                return Err(ConfigError::DuplicatePin { field });
            }
            public_keys.push(bytes);
        }
        let mut allowed_prefixes = Vec::with_capacity(acl.allowed_prefixes.len());
        for (j, prefix) in acl.allowed_prefixes.iter().enumerate() {
            allowed_prefixes.push(check_prefix(
                &format!("slaves[{i}].allowed_prefixes[{j}]"),
                prefix,
            )?);
        }
        slaves.push(LoadedAcl {
            id: acl.id,
            public_keys,
            allowed_prefixes,
        });
    }

    Ok(LoadedMaster {
        slaves,
        by_key,
        listen_addr,
        central_root,
        master_key_path,
        db_path,
        log_level: config.log_level,
        watcher_debounce_ms: config.watcher_debounce_ms,
        rescan_interval_seconds: config.rescan_interval_seconds,
        status_interval_seconds: config.status_interval_seconds,
        max_checkouts_per_slave: config.max_checkouts_per_slave,
        max_connections: config.max_connections,
        max_connection_attempts_per_minute: config.max_connection_attempts_per_minute,
        tune,
    })
}

fn project_slave(config: SlaveConfig) -> Result<LoadedSlave, ConfigError> {
    check_id("slave_id", &config.slave_id)?;
    check_master_addr(&config.master_addr)?;
    let slave_key_path = expand_host_path("slave_key_path", &config.slave_key_path)?;
    if config.master_public_keys.is_empty() {
        return Err(ConfigError::EmptyPins {
            field: "master_public_keys".into(),
        });
    }
    let mut seen_pins = HashSet::new();
    let mut master_public_keys = Vec::with_capacity(config.master_public_keys.len());
    for (j, pin) in config.master_public_keys.iter().enumerate() {
        let field = format!("master_public_keys[{j}]");
        let bytes = parse_pin(&field, pin)?;
        if !seen_pins.insert(bytes) {
            return Err(ConfigError::DuplicatePin { field });
        }
        master_public_keys.push(bytes);
    }
    let db_path = expand_host_path("db_path", &config.db_path)?;
    check_log_level(&config.log_level)?;
    check_debounce(config.watcher_debounce_ms)?;
    check_rescan_interval(config.rescan_interval_seconds)?;
    check_status_interval(config.status_interval_seconds)?;
    let tune = project_tune(config.tune, TuneRole::Slave)?;

    if config.checkouts.len() > config.max_checkouts_per_slave as usize {
        return Err(ConfigError::TooManyCheckouts {
            count: config.checkouts.len(),
            max: config.max_checkouts_per_slave,
        });
    }

    let mut checkouts = Vec::with_capacity(config.checkouts.len());
    let mut seen_ids = HashSet::new();
    for (i, ck) in config.checkouts.into_iter().enumerate() {
        let field_id = format!("checkouts[{i}].id");
        check_id(&field_id, &ck.id)?;
        if !seen_ids.insert(ck.id.clone()) {
            return Err(ConfigError::DuplicateId {
                field: field_id,
                value: ck.id,
            });
        }
        let central = check_prefix(&format!("checkouts[{i}].central"), &ck.central)?;
        let local = resolve_existing_local(expand_host_path(
            &format!("checkouts[{i}].local"),
            &ck.local,
        )?)?;
        checkouts.push(LoadedCheckout {
            id: CheckoutId::new(ck.id),
            central,
            local,
        });
    }

    for (i, a) in checkouts.iter().enumerate() {
        for b in checkouts.iter().skip(i + 1) {
            if local_paths_overlap(&a.local, &b.local) {
                return Err(ConfigError::LocalOverlap {
                    a: a.local.display().to_string(),
                    b: b.local.display().to_string(),
                });
            }
        }
    }

    Ok(LoadedSlave {
        slave_id: config.slave_id,
        master_addr: config.master_addr,
        master_public_keys,
        checkouts,
        slave_key_path,
        db_path,
        log_level: config.log_level,
        watcher_debounce_ms: config.watcher_debounce_ms,
        rescan_interval_seconds: config.rescan_interval_seconds,
        status_interval_seconds: config.status_interval_seconds,
        max_checkouts_per_slave: config.max_checkouts_per_slave,
        tune,
    })
}

fn require_mode_600(path: &Path) -> Result<(), ConfigError> {
    let metadata = fs::metadata(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mode = metadata.permissions().mode() & 0o777;
    if mode != 0o600 {
        return Err(ConfigError::InsecureMode {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

fn resolve_existing_local(path: PathBuf) -> Result<PathBuf, ConfigError> {
    if !path.exists() {
        return Ok(path);
    }
    path.canonicalize()
        .map_err(|source| ConfigError::Io { path, source })
}

fn expand_host_path(field: &str, raw: &str) -> Result<PathBuf, ConfigError> {
    let path = if raw == "~" {
        home_dir(field, raw)?
    } else if let Some(rest) = raw.strip_prefix("~/") {
        home_dir(field, raw)?.join(rest)
    } else {
        PathBuf::from(raw)
    };
    if !path.is_absolute() {
        return Err(ConfigError::BadHostPath {
            field: field.into(),
            value: raw.into(),
        });
    }
    Ok(path)
}

fn home_dir(field: &str, raw: &str) -> Result<PathBuf, ConfigError> {
    match std::env::var_os("HOME") {
        Some(home) if !home.is_empty() => Ok(PathBuf::from(home)),
        _ => Err(ConfigError::BadHostPath {
            field: field.into(),
            value: raw.into(),
        }),
    }
}

fn check_id(field: &str, value: &str) -> Result<(), ConfigError> {
    let ok = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'));
    if ok {
        Ok(())
    } else {
        Err(ConfigError::BadId {
            field: field.into(),
            value: value.into(),
        })
    }
}

pub fn log_level_filter(level: &str) -> Option<log::LevelFilter> {
    match level {
        "error" => Some(log::LevelFilter::Error),
        "warn" => Some(log::LevelFilter::Warn),
        "info" => Some(log::LevelFilter::Info),
        "debug" => Some(log::LevelFilter::Debug),
        "trace" => Some(log::LevelFilter::Trace),
        _ => None,
    }
}

fn check_log_level(value: &str) -> Result<(), ConfigError> {
    match log_level_filter(value) {
        Some(_) => Ok(()),
        None => Err(ConfigError::BadLogLevel {
            value: value.into(),
        }),
    }
}

fn check_debounce(ms: u64) -> Result<(), ConfigError> {
    if (200..=500).contains(&ms) {
        Ok(())
    } else {
        Err(ConfigError::DebounceOutOfRange { value: ms })
    }
}

fn check_rescan_interval(seconds: u64) -> Result<(), ConfigError> {
    if seconds == 0 {
        Err(ConfigError::RescanIntervalZero)
    } else {
        Ok(())
    }
}

fn check_status_interval(seconds: u64) -> Result<(), ConfigError> {
    if seconds <= 3600 {
        Ok(())
    } else {
        Err(ConfigError::StatusIntervalOutOfRange { value: seconds })
    }
}

fn check_master_addr(value: &str) -> Result<(), ConfigError> {
    let Some((host, port)) = value.rsplit_once(':') else {
        return Err(ConfigError::BadMasterAddr {
            value: value.into(),
        });
    };
    if host.is_empty() {
        return Err(ConfigError::BadMasterAddr {
            value: value.into(),
        });
    }
    match port.parse::<u16>() {
        Ok(port) if port > 0 => Ok(()),
        _ => Err(ConfigError::BadMasterAddr {
            value: value.into(),
        }),
    }
}

fn parse_pin(field: &str, value: &str) -> Result<[u8; 32], ConfigError> {
    parse_hex_key(value).map_err(|_| ConfigError::BadPin {
        field: field.into(),
    })
}

fn check_prefix(field: &str, value: &str) -> Result<CanonicalPath, ConfigError> {
    match CanonicalPath::parse(value) {
        Ok(prefix) => Ok(prefix),
        Err(PathError::NonCanonical { value, normalized }) => {
            Err(ConfigError::NonNormalizedPrefix {
                field: field.into(),
                value,
                normalized,
            })
        }
        Err(source) => Err(ConfigError::BadPrefix {
            field: field.into(),
            source,
        }),
    }
}

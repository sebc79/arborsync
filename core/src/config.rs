//! TOML configuration types (`spec.md` §14). Validation is a later TDD slice.

use serde::{Deserialize, Serialize};

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
    #[serde(default = "default_max_checkouts")]
    pub max_checkouts_per_slave: u32,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_max_attempts")]
    pub max_connection_attempts_per_minute: u32,
    #[serde(default)]
    pub slaves: Vec<SlaveAcl>,
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
    #[serde(default)]
    pub checkouts: Vec<CheckoutConfig>,
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

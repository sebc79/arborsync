use std::path::PathBuf;

use anyhow::Context;
use arborsync_core::master::recompute_index;
use arborsync_core::storage::Storage;
use arborsync_core::{LoadedMaster, RedbStorage};

const DEFAULT_CONFIG: &str = "/etc/arborsync/master.toml";

pub fn run(config: Option<PathBuf>) -> anyhow::Result<()> {
    let config_path = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG));
    let cfg = LoadedMaster::load(&config_path)
        .with_context(|| format!("load {}", config_path.display()))?;
    if let Some(parent) = cfg.db_path().parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let store = RedbStorage::open(cfg.db_path())
        .with_context(|| format!("open index {}", cfg.db_path().display()))?;
    if recompute_index(&store).context("recompute")? {
        println!("recomputed directory hashes");
    } else {
        println!("directory hashes already match");
    }
    Ok(())
}

use std::path::PathBuf;

use anyhow::Context;
use arborsync_core::path::CanonicalPath;
use arborsync_core::restore::{pretend_lines, restore_prefix};
use arborsync_core::storage::Storage;
use arborsync_core::{LoadedMaster, RedbStorage};

const DEFAULT_CONFIG: &str = "/etc/arborsync/master.toml";

pub fn run(
    config: Option<PathBuf>,
    source: PathBuf,
    prefix: CanonicalPath,
    pretend: bool,
) -> anyhow::Result<()> {
    let config_path = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG));
    let cfg = LoadedMaster::load(&config_path)
        .with_context(|| format!("load {}", config_path.display()))?;
    let source = std::fs::canonicalize(&source)
        .with_context(|| format!("source directory {}", source.display()))?;
    if pretend {
        let lines = pretend_lines(cfg.central_root(), &source, &prefix).context("pretend")?;
        for line in lines {
            println!("{line}");
        }
        return Ok(());
    }
    if let Some(parent) = cfg.db_path().parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let store = match RedbStorage::open(cfg.db_path()) {
        Ok(store) => store,
        Err(err) if err.is_index_locked() => {
            anyhow::bail!(
                "stop the master before restore. The index at {} is locked.",
                cfg.db_path().display()
            )
        }
        Err(err) => {
            return Err(err).with_context(|| format!("open index {}", cfg.db_path().display()));
        }
    };
    let outcome =
        restore_prefix(&store, cfg.central_root(), &source, &prefix).context("restore")?;
    println!("{}", outcome.summary());
    Ok(())
}

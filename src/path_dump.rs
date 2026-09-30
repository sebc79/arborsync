use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::Context;
use arborsync_core::keys::format_hex_key;
use arborsync_core::merkle::file_node;
use arborsync_core::meta::{self, EntryKind, FileMetadata};
use arborsync_core::path::{
    CanonicalPath, canonical_to_host, host_to_canonical, local_to_canonical, strip_central,
};
use arborsync_core::storage::{CheckoutId, RedbStorage, Storage};
use arborsync_core::{LoadedMaster, LoadedSlave};

const DEFAULT_MASTER_CONFIG: &str = "/etc/arborsync/master.toml";

pub fn run(config: Option<PathBuf>, path: PathBuf) -> anyhow::Result<()> {
    let config_path = config.unwrap_or_else(|| PathBuf::from(DEFAULT_MASTER_CONFIG));
    let text = fs::read_to_string(&config_path)
        .with_context(|| format!("read {}", config_path.display()))?;
    if text.contains("central_root") {
        dump_master(&config_path, &path)
    } else if text.contains("slave_id") {
        dump_slave(&config_path, &path)
    } else {
        anyhow::bail!(
            "{} is neither a master nor a slave config",
            config_path.display()
        )
    }
}

fn dump_master(config_path: &Path, arg: &Path) -> anyhow::Result<()> {
    let cfg = LoadedMaster::load(config_path)
        .with_context(|| format!("load {}", config_path.display()))?;
    let root = existing_root(cfg.central_root());
    let (central, host) = master_path(&root, arg)?;
    let snap = snapshot(cfg.db_path())?;
    println!("role=master");
    println!("central_root={}", root.display());
    println!("db={}", cfg.db_path().display());
    println!("snapshot={}", snap.label);
    println!("checkout=-");
    print_record(&snap.store, &CheckoutId::master(), &central, &host)
}

fn dump_slave(config_path: &Path, arg: &Path) -> anyhow::Result<()> {
    let cfg = LoadedSlave::load(config_path)
        .with_context(|| format!("load {}", config_path.display()))?;
    let (checkout, central, host, local) = slave_path(&cfg, arg)?;
    let snap = snapshot(cfg.db_path())?;
    println!("role=slave");
    println!("local={}", local.display());
    println!("db={}", cfg.db_path().display());
    println!("snapshot={}", snap.label);
    println!("checkout={}", checkout.as_str());
    print_record(&snap.store, &checkout, &central, &host)
}

struct Snapshot {
    store: RedbStorage,
    label: &'static str,
    copy: Option<PathBuf>,
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        if let Some(path) = self.copy.take() {
            let _ = fs::remove_file(path);
        }
    }
}

fn snapshot(db: &Path) -> anyhow::Result<Snapshot> {
    match RedbStorage::open_existing(db) {
        Ok(store) => Ok(Snapshot {
            store,
            label: "live",
            copy: None,
        }),
        Err(err) if database_locked(&err) => {
            let copy =
                std::env::temp_dir().join(format!("arborsync-path-{}.redb", std::process::id()));
            fs::copy(db, &copy).with_context(|| format!("copy locked index {}", db.display()))?;
            let store = RedbStorage::open_existing(&copy)
                .with_context(|| format!("open copy of {}", db.display()))?;
            Ok(Snapshot {
                store,
                label: "copy",
                copy: Some(copy),
            })
        }
        Err(err) => Err(err).with_context(|| format!("open index {}", db.display())),
    }
}

fn database_locked(err: &impl std::fmt::Display) -> bool {
    err.to_string().contains("already open")
}

fn print_record(
    store: &RedbStorage,
    checkout: &CheckoutId,
    central: &CanonicalPath,
    host: &Path,
) -> anyhow::Result<()> {
    let indexed = store
        .get_meta(checkout, central)
        .with_context(|| format!("read meta {}", central.as_str()))?;
    let dir_node = store
        .get_dir_node(checkout, central)
        .with_context(|| format!("read dir node {}", central.as_str()))?;
    let last_synced = store
        .get_last_synced(checkout, central)
        .with_context(|| format!("read last_synced {}", central.as_str()))?;
    let last_content = store
        .get_last_synced_content(checkout, central)
        .with_context(|| format!("read last_synced content {}", central.as_str()))?;
    let disk = read_disk(host)?;

    println!("central={}", central.as_str());
    println!("host={}", host.display());
    match &disk {
        Disk::Absent => println!("disk=absent"),
        Disk::Skipped { reason } => println!("disk=skipped reason={reason}"),
        Disk::Error { message } => println!("disk=error {message}"),
        Disk::Here(meta) => println!("disk={}", meta_fields(meta)),
    }
    match &indexed {
        None => println!("index=absent"),
        Some(meta) => println!("index={}", meta_fields(meta)),
    }
    match dir_node {
        None => println!("dir_node=absent"),
        Some(node) => println!("dir_node={}", format_hex_key(node.as_bytes())),
    }
    match last_synced {
        None => println!("last_synced=absent"),
        Some(node) => println!("last_synced={}", format_hex_key(node.as_bytes())),
    }
    match last_content {
        None => println!("last_synced_content=absent"),
        Some(hash) => println!("last_synced_content={}", format_hex_key(hash.as_bytes())),
    }
    println!(
        "disk_index={}",
        match (&disk, &indexed) {
            (Disk::Here(live), Some(stored)) if live == stored => "match",
            (Disk::Here(_), Some(_)) => "differ",
            (Disk::Here(_), None) => "disk_only",
            (Disk::Absent | Disk::Skipped { .. } | Disk::Error { .. }, Some(_)) => "index_only",
            (Disk::Absent | Disk::Skipped { .. } | Disk::Error { .. }, None) => "both_absent",
        }
    );
    println!(
        "index_last_synced={}",
        match (&indexed, last_synced) {
            (None, _) => "no_index",
            (Some(_), None) => "unset",
            (Some(meta), Some(synced)) if file_node(meta) == synced => "match",
            (Some(_), Some(_)) => "differ",
        }
    );
    Ok(())
}

enum Disk {
    Absent,
    Skipped { reason: &'static str },
    Error { message: String },
    Here(FileMetadata),
}

fn read_disk(host: &Path) -> anyhow::Result<Disk> {
    match fs::symlink_metadata(host) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Disk::Absent),
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => Ok(Disk::Error {
            message: err.to_string(),
        }),
        Err(err) => Ok(Disk::Error {
            message: err.to_string(),
        }),
        Ok(md) => {
            let kind = md.file_type();
            if !kind.is_file() && !kind.is_dir() && !kind.is_symlink() {
                return Ok(Disk::Skipped {
                    reason: "not a file, directory, or symlink",
                });
            }
            match meta::collect_from_path(host)
                .with_context(|| format!("stat {}", host.display()))?
            {
                Some(found) => Ok(Disk::Here(found)),
                None => Ok(Disk::Skipped {
                    reason: "unreadable",
                }),
            }
        }
    }
}

fn meta_fields(meta: &FileMetadata) -> String {
    format!(
        "{} size={} mode={:06o} mtime_ns={} content_hash={} file_node={}",
        kind_name(meta.kind),
        meta.size,
        meta.mode,
        meta.mtime_ns,
        format_hex_key(meta.content_hash.as_bytes()),
        format_hex_key(file_node(meta).as_bytes()),
    )
}

fn kind_name(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::File => "file",
        EntryKind::Dir => "dir",
        EntryKind::Symlink => "symlink",
    }
}

fn master_path(root: &Path, arg: &Path) -> anyhow::Result<(CanonicalPath, PathBuf)> {
    let host = normalize_host(arg)?;
    if host.starts_with(root) {
        let central = host_to_canonical(root, &host)
            .with_context(|| format!("{} is under {}", host.display(), root.display()))?;
        return Ok((central, host));
    }
    let central = canonical_arg(arg)?;
    Ok((central.clone(), canonical_to_host(root, &central)))
}

fn slave_path(
    cfg: &LoadedSlave,
    arg: &Path,
) -> anyhow::Result<(CheckoutId, CanonicalPath, PathBuf, PathBuf)> {
    let host = normalize_host(arg)?;
    let mut best: Option<(usize, &arborsync_core::config::LoadedCheckout)> = None;
    for checkout in cfg.checkouts() {
        let local = existing_root(checkout.local());
        if host.starts_with(&local) {
            let len = local.components().count();
            if best.map(|(best_len, _)| len > best_len).unwrap_or(true) {
                best = Some((len, checkout));
            }
        }
    }
    if let Some((_, checkout)) = best {
        let local = existing_root(checkout.local());
        let central = local_to_canonical(&local, checkout.central(), &host).with_context(|| {
            format!(
                "{} is under checkout {}",
                host.display(),
                checkout.id().as_str()
            )
        })?;
        return Ok((checkout.id().clone(), central, host, local));
    }
    let central = canonical_arg(arg)?;
    let checkout = cfg
        .checkouts()
        .iter()
        .filter(|checkout| checkout.central().covers(&central))
        .max_by_key(|checkout| checkout.central().as_str().len())
        .with_context(|| format!("{} is not in a checkout", central.as_str()))?;
    let local = existing_root(checkout.local());
    let relative = strip_central(checkout.central(), &central)?;
    let host = canonical_to_host(&local, &relative);
    Ok((checkout.id().clone(), central, host, local))
}

fn canonical_arg(arg: &Path) -> anyhow::Result<CanonicalPath> {
    let text = arg.to_str().context("path is not valid UTF-8")?;
    CanonicalPath::parse(text).with_context(|| format!("{text} is not a canonical path"))
}

fn existing_root(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn normalize_host(path: &Path) -> anyhow::Result<PathBuf> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("current directory")?
            .join(path)
    };
    if abs.exists() {
        return abs
            .canonicalize()
            .with_context(|| format!("canonicalize {}", abs.display()));
    }
    let mut out = PathBuf::new();
    for component in abs.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    Ok(out)
}

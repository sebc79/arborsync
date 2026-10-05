use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use filetime::FileTime;
use thiserror::Error;

use crate::apply;
use crate::hash::DirNode;
use crate::index::{self, LastSynced, LeafChange};
use crate::merkle::{DirChild, dir_node, empty_dir_node, file_node};
use crate::meta::{self, EntryKind, FileMetadata};
use crate::path::{
    CanonicalPath, EntryName, PathError, RESERVED_TMP, canonical_to_host, confine_host,
    is_reserved_root_entry,
};
use crate::storage::{CheckoutId, RestoreMark, Storage};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreAction {
    Copy,
    Replace,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreChange {
    pub path: CanonicalPath,
    pub action: RestoreAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreOutcome {
    pub prefix: CanonicalPath,
    pub generation: u64,
    pub copied: u64,
    pub replaced: u64,
    pub deleted: u64,
    pub already_matched: bool,
}

impl RestoreOutcome {
    pub fn summary(&self) -> String {
        if self.already_matched {
            format!(
                "{} already matches at generation {}, copied {}, replaced {}, deleted {}",
                self.prefix.as_str(),
                self.generation,
                self.copied,
                self.replaced,
                self.deleted
            )
        } else {
            format!(
                "restored {} at generation {}, copied {}, replaced {}, deleted {}",
                self.prefix.as_str(),
                self.generation,
                self.copied,
                self.replaced,
                self.deleted
            )
        }
    }
}

#[derive(Debug, Error)]
pub enum RestoreError {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    Apply(#[from] apply::ApplyError),
    #[error("index: {0}")]
    Index(String),
    #[error("source {} is not a directory", .0.display())]
    SourceNotDirectory(PathBuf),
}

fn io_at(path: &Path, source: io::Error) -> RestoreError {
    RestoreError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn index_err(err: impl std::fmt::Display) -> RestoreError {
    RestoreError::Index(err.to_string())
}

/// Compare `source` with the central tree under `prefix`. `source` is that prefix.
pub fn plan_prefix(
    central_root: &Path,
    source: &Path,
    prefix: &CanonicalPath,
) -> Result<Vec<RestoreChange>, RestoreError> {
    Ok(diff_trees(central_root, source, prefix)?.0)
}

/// What `restore` would change that a slave holding `source` would otherwise pull.
///
/// The slave's live tree is `source`, and `last_synced` matches those files.
/// A path only in `source` is `AnnounceCreate` (a push), so it is not listed.
/// A path only on central is a pull (`delete`). A file or symlink whose
/// `FileNode` differs is a pull (`replace`). Two directories are not a pull.
/// Reconcile recurses on their child hash. No pulls is an empty vec, including
/// when the only differences are pushes.
pub fn pretend_lines(
    central_root: &Path,
    source: &Path,
    prefix: &CanonicalPath,
) -> Result<Vec<String>, RestoreError> {
    require_source_dir(source)?;
    let from_source = collect_tree(source, prefix)?;
    let central_host = canonical_to_host(central_root, prefix);
    let on_central = collect_tree(&central_host, prefix)?;
    let mut paths: BTreeSet<CanonicalPath> = from_source.keys().cloned().collect();
    paths.extend(on_central.keys().cloned());
    let mut lines = Vec::new();
    for path in paths {
        if let Some(line) = pull_line(&path, on_central.get(&path), from_source.get(&path)) {
            lines.push(line);
        }
    }
    Ok(lines)
}

fn pull_line(
    path: &CanonicalPath,
    central: Option<&FileMetadata>,
    source: Option<&FileMetadata>,
) -> Option<String> {
    match (source, central) {
        (None, Some(_)) => Some(format!("delete {}", path.as_str())),
        (Some(src), Some(live)) if src.kind == EntryKind::Dir && live.kind == EntryKind::Dir => {
            None
        }
        (Some(src), Some(live)) if file_node(src) != file_node(live) => {
            Some(format!("replace {}", path.as_str()))
        }
        _ => None,
    }
}

fn diff_trees(
    central_root: &Path,
    source: &Path,
    prefix: &CanonicalPath,
) -> Result<(Vec<RestoreChange>, BTreeMap<CanonicalPath, FileMetadata>), RestoreError> {
    require_source_dir(source)?;
    let from_source = collect_tree(source, prefix)?;
    let central_host = canonical_to_host(central_root, prefix);
    let on_central = collect_tree(&central_host, prefix)?;
    let mut changes = Vec::new();
    for (path, meta) in &from_source {
        match on_central.get(path) {
            None => changes.push(RestoreChange {
                path: path.clone(),
                action: RestoreAction::Copy,
            }),
            Some(live) if live != meta => changes.push(RestoreChange {
                path: path.clone(),
                action: RestoreAction::Replace,
            }),
            Some(_) => {}
        }
    }
    for path in on_central.keys() {
        if !from_source.contains_key(path) {
            changes.push(RestoreChange {
                path: path.clone(),
                action: RestoreAction::Delete,
            });
        }
    }
    changes.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((changes, from_source))
}

/// Copy `source` onto `prefix`, commit the index, then seal the epoch.
pub fn restore_prefix<S: Storage>(
    store: &S,
    central_root: &Path,
    source: &Path,
    prefix: &CanonicalPath,
) -> Result<RestoreOutcome, RestoreError> {
    let (changes, manifest) = diff_trees(central_root, source, prefix)?;
    let copied = count(&changes, RestoreAction::Copy);
    let replaced = count(&changes, RestoreAction::Replace);
    let deleted = count(&changes, RestoreAction::Delete);
    let walk_node = manifest_dir_node(prefix, &manifest)?;
    let mark = store.restore_mark(prefix).map_err(index_err)?;

    if let Some(RestoreMark::Sealed {
        generation,
        dir_node,
    }) = mark
    {
        if dir_node == walk_node {
            if !changes.is_empty() {
                apply_disk(central_root, source, prefix, &changes, &manifest)?;
                commit_manifest(store, prefix, &manifest)?;
            }
            return Ok(RestoreOutcome {
                prefix: prefix.clone(),
                generation,
                copied,
                replaced,
                deleted,
                already_matched: changes.is_empty(),
            });
        }
    }

    let generation = match mark {
        Some(RestoreMark::Replacing { generation }) => generation,
        Some(RestoreMark::Sealed { generation, .. }) => generation.saturating_add(1),
        None => 1,
    };
    if !changes.is_empty() {
        store
            .put_restore_mark(prefix, RestoreMark::Replacing { generation })
            .map_err(index_err)?;
    }
    apply_disk(central_root, source, prefix, &changes, &manifest)?;
    commit_manifest(store, prefix, &manifest)?;
    let dir_node = store
        .get_dir_node(&CheckoutId::master(), prefix)
        .map_err(index_err)?
        .unwrap_or(walk_node);
    store
        .put_restore_mark(
            prefix,
            RestoreMark::Sealed {
                generation,
                dir_node,
            },
        )
        .map_err(index_err)?;
    Ok(RestoreOutcome {
        prefix: prefix.clone(),
        generation,
        copied,
        replaced,
        deleted,
        already_matched: false,
    })
}

fn manifest_dir_node(
    prefix: &CanonicalPath,
    manifest: &BTreeMap<CanonicalPath, FileMetadata>,
) -> Result<DirNode, RestoreError> {
    let mut dirs: Vec<&CanonicalPath> = manifest
        .iter()
        .filter(|(_, meta)| meta.kind == EntryKind::Dir)
        .map(|(path, _)| path)
        .collect();
    if !dirs.iter().any(|dir| *dir == prefix) {
        dirs.push(prefix);
    }
    dirs.sort_by_key(|path| std::cmp::Reverse(depth(path)));
    let mut computed: HashMap<CanonicalPath, DirNode> = HashMap::new();
    for dir in dirs {
        let mut entries = Vec::new();
        for (path, meta) in manifest {
            if path.parent().as_ref() != Some(dir) {
                continue;
            }
            let name = EntryName::parse(path.name())?;
            entries.push(match meta.kind {
                EntryKind::File => DirChild::File {
                    name,
                    node: file_node(meta),
                },
                EntryKind::Symlink => DirChild::Symlink {
                    name,
                    node: file_node(meta),
                },
                EntryKind::Dir => DirChild::Directory {
                    name,
                    node: computed.get(path).copied().unwrap_or_else(empty_dir_node),
                },
            });
        }
        computed.insert(dir.clone(), dir_node(&entries));
    }
    Ok(computed.get(prefix).copied().unwrap_or_else(empty_dir_node))
}

fn count(changes: &[RestoreChange], action: RestoreAction) -> u64 {
    changes
        .iter()
        .filter(|change| change.action == action)
        .count() as u64
}

fn require_source_dir(source: &Path) -> Result<(), RestoreError> {
    let meta = fs::symlink_metadata(source).map_err(|err| io_at(source, err))?;
    if !meta.is_dir() {
        return Err(RestoreError::SourceNotDirectory(source.to_path_buf()));
    }
    Ok(())
}

fn collect_tree(
    host_root: &Path,
    prefix: &CanonicalPath,
) -> Result<BTreeMap<CanonicalPath, FileMetadata>, RestoreError> {
    match fs::symlink_metadata(host_root) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(err) => return Err(io_at(host_root, err)),
        Ok(_) => {}
    }
    let mut out = BTreeMap::new();
    walk(host_root, prefix, prefix, &mut out)?;
    Ok(out)
}

fn walk(
    host: &Path,
    path: &CanonicalPath,
    prefix: &CanonicalPath,
    out: &mut BTreeMap<CanonicalPath, FileMetadata>,
) -> Result<(), RestoreError> {
    let Some(meta) = meta::collect_from_path(host).map_err(|err| io_at(host, err))? else {
        return Ok(());
    };
    out.insert(path.clone(), meta.clone());
    if meta.kind != EntryKind::Dir {
        return Ok(());
    }
    let entries = fs::read_dir(host).map_err(|err| io_at(host, err))?;
    for entry in entries {
        let entry = entry.map_err(|err| io_at(host, err))?;
        let raw = entry.file_name();
        let Some(name) = raw.to_str() else {
            log::warn!("skipping non-UTF-8 name under {}", host.display());
            continue;
        };
        if prefix.as_str() == "/" && path.as_str() == "/" && is_reserved_root_entry(name) {
            continue;
        }
        let child = crate::path::join_central(path, name)?;
        walk(&entry.path(), &child, prefix, out)?;
    }
    Ok(())
}

fn apply_disk(
    central_root: &Path,
    source: &Path,
    prefix: &CanonicalPath,
    changes: &[RestoreChange],
    manifest: &BTreeMap<CanonicalPath, FileMetadata>,
) -> Result<(), RestoreError> {
    let mut deletes: Vec<&RestoreChange> = changes
        .iter()
        .filter(|change| change.action == RestoreAction::Delete)
        .collect();
    deletes.sort_by_key(|change| std::cmp::Reverse(depth(&change.path)));
    for change in deletes {
        apply::remove_live(central_root, &change.path)?;
    }

    let mut writes: Vec<&RestoreChange> = changes
        .iter()
        .filter(|change| change.action != RestoreAction::Delete)
        .collect();
    writes.sort_by_key(|change| depth(&change.path));
    for change in writes {
        let Some(meta) = manifest.get(&change.path) else {
            return Err(RestoreError::Index(format!(
                "missing manifest row {}",
                change.path.as_str()
            )));
        };
        install(central_root, source, prefix, &change.path, meta)?;
    }
    for (path, meta) in manifest {
        if meta.kind == EntryKind::Dir {
            apply::mkdir_live(central_root, path, meta)?;
        }
    }
    Ok(())
}

fn depth(path: &CanonicalPath) -> usize {
    path.as_str().bytes().filter(|byte| *byte == b'/').count()
}

fn source_host(source: &Path, prefix: &CanonicalPath, path: &CanonicalPath) -> PathBuf {
    if path == prefix {
        return source.to_path_buf();
    }
    let relative = if prefix.as_str() == "/" {
        path.as_str().trim_start_matches('/')
    } else {
        path.as_str()
            .strip_prefix(prefix.as_str())
            .unwrap_or("")
            .trim_start_matches('/')
    };
    source.join(relative)
}

fn install(
    central_root: &Path,
    source: &Path,
    prefix: &CanonicalPath,
    path: &CanonicalPath,
    meta: &FileMetadata,
) -> Result<(), RestoreError> {
    let host = confine_host(central_root, path)?;
    if let Some(live) = meta::collect_from_path(&host).map_err(|err| io_at(&host, err))? {
        if live.kind != meta.kind {
            apply::remove_live(central_root, path)?;
        }
    }
    match meta.kind {
        EntryKind::Dir => apply::mkdir_live(central_root, path, meta)?,
        EntryKind::File => stage_file(
            central_root,
            &host,
            &source_host(source, prefix, path),
            meta,
        )?,
        EntryKind::Symlink => stage_symlink(
            central_root,
            &host,
            &source_host(source, prefix, path),
            meta,
        )?,
    }
    Ok(())
}

fn stage_file(
    central_root: &Path,
    target: &Path,
    source_host: &Path,
    meta: &FileMetadata,
) -> Result<(), RestoreError> {
    let tmp = TmpPath::create(central_root)?;
    {
        let mut src = File::open(source_host).map_err(|err| io_at(source_host, err))?;
        let mut dst = File::create(&tmp.0).map_err(|err| io_at(&tmp.0, err))?;
        io::copy(&mut src, &mut dst).map_err(|err| io_at(&tmp.0, err))?;
        dst.sync_all().map_err(|err| io_at(&tmp.0, err))?;
    }
    fs::set_permissions(&tmp.0, fs::Permissions::from_mode(meta.mode & 0o7777))
        .map_err(|err| io_at(&tmp.0, err))?;
    filetime::set_file_mtime(&tmp.0, file_time(meta.mtime_ns)).map_err(|err| io_at(&tmp.0, err))?;
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|err| io_at(parent, err))?;
    }
    fs::rename(&tmp.0, target).map_err(|err| io_at(target, err))?;
    Ok(())
}

fn stage_symlink(
    central_root: &Path,
    target: &Path,
    source_host: &Path,
    meta: &FileMetadata,
) -> Result<(), RestoreError> {
    let link_target = fs::read_link(source_host).map_err(|err| io_at(source_host, err))?;
    let tmp = TmpPath::create(central_root)?;
    std::os::unix::fs::symlink(&link_target, &tmp.0).map_err(|err| io_at(&tmp.0, err))?;
    let mtime = file_time(meta.mtime_ns);
    filetime::set_symlink_file_times(&tmp.0, mtime, mtime).map_err(|err| io_at(&tmp.0, err))?;
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|err| io_at(parent, err))?;
    }
    match fs::symlink_metadata(target) {
        Ok(meta) if !meta.is_dir() => fs::remove_file(target).map_err(|err| io_at(target, err))?,
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(io_at(target, err)),
    }
    fs::rename(&tmp.0, target).map_err(|err| io_at(target, err))?;
    Ok(())
}

fn file_time(mtime_ns: i64) -> FileTime {
    FileTime::from_unix_time(
        mtime_ns.div_euclid(1_000_000_000),
        mtime_ns.rem_euclid(1_000_000_000) as u32,
    )
}

struct TmpPath(PathBuf);

impl TmpPath {
    fn create(central_root: &Path) -> Result<Self, RestoreError> {
        let dir = central_root.join(RESERVED_TMP);
        fs::create_dir_all(&dir).map_err(|err| io_at(&dir, err))?;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = dir.join(format!(
            "restore-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Ok(Self(path))
    }
}

impl Drop for TmpPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn reserved_kept(prefix: &CanonicalPath, path: &CanonicalPath) -> bool {
    prefix.as_str() == "/" && path.has_reserved_root_name()
}

fn commit_manifest<S: Storage>(
    store: &S,
    prefix: &CanonicalPath,
    manifest: &BTreeMap<CanonicalPath, FileMetadata>,
) -> Result<(), RestoreError> {
    let ck = CheckoutId::master();
    let indexed = store.range_meta(&ck, prefix).map_err(index_err)?;
    let mut rows: Vec<(CanonicalPath, Option<FileMetadata>)> = Vec::new();
    for (path, _) in indexed {
        if manifest.contains_key(&path) || reserved_kept(prefix, &path) {
            continue;
        }
        rows.push((path, None));
    }
    for (path, meta) in manifest {
        rows.push((path.clone(), Some(meta.clone())));
    }
    let changes: Vec<LeafChange<'_>> = rows
        .iter()
        .map(|(path, meta)| LeafChange {
            path,
            meta: meta.as_ref(),
            last_synced: LastSynced::AdoptLeaf,
        })
        .collect();
    if !changes.is_empty() {
        index::commit_leaves(store, &ck, changes).map_err(index_err)?;
    }
    index::repair_dir_nodes(store, &ck, &CanonicalPath::root()).map_err(index_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::merkle::DirChild;
    use crate::reconcile::{WalkAction, decide_child};
    use crate::storage::RedbStorage;
    use crate::test_support::name;

    fn prefix(path: &str) -> CanonicalPath {
        CanonicalPath::parse(path).unwrap()
    }

    fn set_mtime(path: &Path, mtime_ns: i64) {
        filetime::set_file_mtime(path, file_time(mtime_ns)).unwrap();
    }

    #[test]
    fn prefix_plan_does_not_touch_a_sibling() {
        let tmp = tempfile::tempdir().unwrap();
        let central = tmp.path().join("central");
        let source = tmp.path().join("source");
        fs::create_dir_all(central.join("src")).unwrap();
        fs::create_dir_all(central.join("other")).unwrap();
        fs::write(central.join("src/keep.txt"), b"old").unwrap();
        fs::write(central.join("other/stay.txt"), b"stay").unwrap();
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("keep.txt"), b"new").unwrap();

        let plan = plan_prefix(&central, &source, &prefix("/src")).unwrap();
        assert!(
            plan.iter()
                .all(|change| change.path.as_str().starts_with("/src"))
        );
        assert!(plan.iter().all(|change| change.path.as_str() != "/other"));
        assert!(
            plan.iter()
                .all(|change| change.path.as_str() != "/other/stay.txt")
        );
        assert_eq!(fs::read(central.join("other/stay.txt")).unwrap(), b"stay");
    }

    #[test]
    fn same_size_same_mtime_byte_change_is_replace() {
        let tmp = tempfile::tempdir().unwrap();
        let central = tmp.path().join("central");
        let source = tmp.path().join("source");
        fs::create_dir_all(central.join("src")).unwrap();
        fs::create_dir_all(&source).unwrap();
        let central_file = central.join("src/file.txt");
        let source_file = source.join("file.txt");
        fs::write(&central_file, b"aaaa").unwrap();
        fs::write(&source_file, b"bbbb").unwrap();
        let mtime = 1_700_000_000_000;
        set_mtime(&central_file, mtime);
        set_mtime(&source_file, mtime);
        fs::set_permissions(&central_file, fs::Permissions::from_mode(0o644)).unwrap();
        fs::set_permissions(&source_file, fs::Permissions::from_mode(0o644)).unwrap();

        let plan = plan_prefix(&central, &source, &prefix("/src")).unwrap();
        let file = plan
            .iter()
            .find(|change| change.path.as_str() == "/src/file.txt")
            .expect("file is in the plan");
        assert_eq!(file.action, RestoreAction::Replace);
    }

    #[test]
    fn second_plan_against_the_restored_tree_is_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let central = tmp.path().join("central");
        let source = tmp.path().join("source");
        fs::create_dir_all(central.join("src")).unwrap();
        fs::create_dir_all(central.join("other")).unwrap();
        fs::write(central.join("src/keep.txt"), b"old").unwrap();
        fs::write(central.join("other/stay.txt"), b"stay").unwrap();
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("keep.txt"), b"new-bytes").unwrap();
        fs::write(source.join("added.txt"), b"added").unwrap();

        let db = tmp.path().join("index.redb");
        let store = RedbStorage::open(&db).unwrap();
        let first = restore_prefix(&store, &central, &source, &prefix("/src")).unwrap();
        assert_eq!(first.generation, 1);
        assert!(first.copied >= 1);
        assert_eq!(
            fs::read(central.join("src/keep.txt")).unwrap(),
            b"new-bytes"
        );
        assert_eq!(fs::read(central.join("src/added.txt")).unwrap(), b"added");
        assert_eq!(fs::read(central.join("other/stay.txt")).unwrap(), b"stay");

        let again = plan_prefix(&central, &source, &prefix("/src")).unwrap();
        assert_eq!(again, Vec::new());

        let second = restore_prefix(&store, &central, &source, &prefix("/src")).unwrap();
        assert!(second.already_matched);
        assert_eq!(second.generation, 1);
        assert_eq!(second.copied, 0);
        assert_eq!(second.replaced, 0);
        assert_eq!(second.deleted, 0);
        assert_eq!(
            second.summary(),
            "/src already matches at generation 1, copied 0, replaced 0, deleted 0"
        );
        assert_eq!(
            store
                .restore_epochs()
                .unwrap()
                .into_iter()
                .find(|(path, _)| path.as_str() == "/src")
                .map(|(_, generation)| generation),
            Some(1)
        );
    }

    #[test]
    fn pretend_prints_nothing_when_the_slave_would_only_push() {
        let tmp = tempfile::tempdir().unwrap();
        let central = tmp.path().join("central");
        let source = tmp.path().join("source");
        fs::create_dir_all(central.join("src")).unwrap();
        fs::create_dir_all(&source).unwrap();
        let central_file = central.join("src/keep.txt");
        let source_file = source.join("keep.txt");
        fs::write(&central_file, b"same").unwrap();
        fs::write(&source_file, b"same").unwrap();
        fs::write(source.join("added.txt"), b"added").unwrap();
        set_mtime(&central_file, 1_700_000_000_000);
        set_mtime(&source_file, 1_700_000_000_000);
        fs::set_permissions(&central_file, fs::Permissions::from_mode(0o644)).unwrap();
        fs::set_permissions(&source_file, fs::Permissions::from_mode(0o644)).unwrap();

        let lines = pretend_lines(&central, &source, &prefix("/src")).unwrap();
        assert_eq!(lines, Vec::<String>::new());

        let added = meta::collect_from_path(&source.join("added.txt"))
            .unwrap()
            .unwrap();
        let node = file_node(&added);
        let slave = DirChild::File {
            name: name("added.txt"),
            node,
        };
        assert_eq!(
            decide_child(Some(&slave), None, Some(node), Some(node)),
            WalkAction::AnnounceCreate
        );
        assert_eq!(fs::read(&central_file).unwrap(), b"same");
        assert!(!central.join("src/added.txt").exists());
    }

    #[test]
    fn pretend_lists_the_replace_and_delete_a_slave_would_pull() {
        let tmp = tempfile::tempdir().unwrap();
        let central = tmp.path().join("central");
        let source = tmp.path().join("source");
        fs::create_dir_all(central.join("src")).unwrap();
        fs::create_dir_all(&source).unwrap();
        let central_keep = central.join("src/keep.txt");
        let source_keep = source.join("keep.txt");
        fs::write(&central_keep, b"old").unwrap();
        fs::write(central.join("src/extra.txt"), b"extra").unwrap();
        fs::write(&source_keep, b"new").unwrap();
        fs::write(source.join("added.txt"), b"added").unwrap();

        let lines = pretend_lines(&central, &source, &prefix("/src")).unwrap();
        assert_eq!(
            lines,
            vec![
                "delete /src/extra.txt".to_string(),
                "replace /src/keep.txt".to_string(),
            ]
        );

        let old = meta::collect_from_path(&central_keep).unwrap().unwrap();
        let new = meta::collect_from_path(&source_keep).unwrap().unwrap();
        let old_node = file_node(&old);
        let new_node = file_node(&new);
        let local = DirChild::File {
            name: name("keep.txt"),
            node: new_node,
        };
        let master = DirChild::File {
            name: name("keep.txt"),
            node: old_node,
        };
        assert_eq!(
            decide_child(Some(&local), Some(&master), Some(new_node), Some(new_node)),
            WalkAction::Pull
        );
        let extra = meta::collect_from_path(&central.join("src/extra.txt"))
            .unwrap()
            .unwrap();
        let extra_node = file_node(&extra);
        let extra_child = DirChild::File {
            name: name("extra.txt"),
            node: extra_node,
        };
        assert_eq!(
            decide_child(None, Some(&extra_child), None, None),
            WalkAction::Pull
        );
        assert_eq!(fs::read(&central_keep).unwrap(), b"old");
        assert!(central.join("src/extra.txt").exists());
        assert!(!central.join("src/added.txt").exists());
    }

    #[test]
    fn an_already_correct_tree_seals_generation_one() {
        let tmp = tempfile::tempdir().unwrap();
        let central = tmp.path().join("central");
        let source = tmp.path().join("source");
        fs::create_dir_all(central.join("src")).unwrap();
        fs::create_dir_all(&source).unwrap();
        fs::write(central.join("src/keep.txt"), b"same").unwrap();
        fs::write(source.join("keep.txt"), b"same").unwrap();
        let mtime = 1_700_000_000_000;
        for path in [
            central.join("src"),
            central.join("src/keep.txt"),
            source.clone(),
            source.join("keep.txt"),
        ] {
            set_mtime(&path, mtime);
        }
        let store = RedbStorage::open(&tmp.path().join("index.redb")).unwrap();
        let first = restore_prefix(&store, &central, &source, &prefix("/src")).unwrap();
        assert!(!first.already_matched);
        assert_eq!(first.generation, 1);
        assert_eq!(first.copied, 0);
        assert_eq!(first.replaced, 0);
        assert_eq!(first.deleted, 0);
        assert!(matches!(
            store.restore_mark(&prefix("/src")).unwrap(),
            Some(RestoreMark::Sealed { generation: 1, .. })
        ));

        let second = restore_prefix(&store, &central, &source, &prefix("/src")).unwrap();
        assert!(second.already_matched);
        assert_eq!(second.generation, 1);
        assert_eq!(
            second.summary(),
            "/src already matches at generation 1, copied 0, replaced 0, deleted 0"
        );
    }

    #[test]
    fn a_retry_keeps_the_replacing_generation() {
        let tmp = tempfile::tempdir().unwrap();
        let central = tmp.path().join("central");
        let source = tmp.path().join("source");
        fs::create_dir_all(central.join("src")).unwrap();
        fs::create_dir_all(&source).unwrap();
        fs::write(central.join("src/keep.txt"), b"old").unwrap();
        fs::write(source.join("keep.txt"), b"new").unwrap();
        let store = RedbStorage::open(&tmp.path().join("index.redb")).unwrap();
        let first = restore_prefix(&store, &central, &source, &prefix("/src")).unwrap();
        assert_eq!(first.generation, 1);
        store
            .put_restore_mark(&prefix("/src"), RestoreMark::Replacing { generation: 2 })
            .unwrap();
        fs::write(source.join("keep.txt"), b"newer").unwrap();

        let second = restore_prefix(&store, &central, &source, &prefix("/src")).unwrap();
        assert_eq!(second.generation, 2);
        assert!(!second.already_matched);
        assert_eq!(fs::read(central.join("src/keep.txt")).unwrap(), b"newer");
        match store.restore_mark(&prefix("/src")).unwrap() {
            Some(RestoreMark::Sealed { generation, .. }) => assert_eq!(generation, 2),
            other => panic!("expected a sealed epoch, got {other:?}"),
        }
    }
}

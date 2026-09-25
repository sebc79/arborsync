use std::collections::{BTreeMap, HashMap, HashSet};

use crate::hash::{DirNode, SubtreeRoot};
use crate::merkle::{
    dir_entry_hash_off, dir_node, dir_node_from_concat, empty_dir_node, encode_dir_entry,
    file_node, DirChild,
};
use crate::meta::{EntryKind, FileMetadata};
use crate::path::{CanonicalPath, EntryName};
use crate::storage::{CheckoutId, Storage, WriteBatch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LastSynced {
    AdoptLeaf,
    Keep,
}

pub struct LeafChange<'a> {
    pub path: &'a CanonicalPath,
    pub meta: Option<&'a FileMetadata>,
    pub last_synced: LastSynced,
}

/// Direct children of each loaded directory. Lives across `commit_leaf_with`
/// calls so later ancestor hashes do not `range_meta` the whole tree.
#[derive(Clone, Debug, Default)]
pub struct DirChildren {
    by_parent: HashMap<CanonicalPath, DirKids>,
}

#[derive(Clone, Debug, Default)]
struct DirKids {
    by_name: BTreeMap<EntryName, ChildSlot>,
    concat: Vec<u8>,
    concat_stale: bool,
}

#[derive(Clone, Debug)]
struct ChildSlot {
    path: CanonicalPath,
    kind: EntryKind,
    node: [u8; 32],
    encode_off: usize,
}

impl DirKids {
    fn rebuild_concat(&mut self) {
        self.concat.clear();
        let mut offsets = Vec::with_capacity(self.by_name.len());
        for (name, slot) in &self.by_name {
            offsets.push(self.concat.len());
            encode_dir_entry(&mut self.concat, slot.kind, name, &slot.node);
        }
        for (slot, off) in self.by_name.values_mut().zip(offsets) {
            slot.encode_off = off;
        }
        self.concat_stale = false;
    }

    fn set_hash(&mut self, name: &EntryName, node: &[u8; 32]) {
        let Some(slot) = self.by_name.get_mut(name) else {
            return;
        };
        slot.node = *node;
        if self.concat_stale {
            return;
        }
        let hash_off = slot.encode_off + dir_entry_hash_off(name);
        self.concat[hash_off..hash_off + 32].copy_from_slice(node);
    }

    fn upsert(&mut self, path: CanonicalPath, kind: EntryKind, node: &[u8; 32], mark_stale: bool) {
        let Ok(name) = EntryName::parse(path.name()) else {
            return;
        };
        if self
            .by_name
            .get(&name)
            .is_some_and(|slot| slot.kind == kind)
        {
            self.set_hash(&name, node);
            return;
        }
        if mark_stale || self.concat_stale || self.by_name.contains_key(&name) {
            self.by_name.insert(
                name,
                ChildSlot {
                    path,
                    kind,
                    node: *node,
                    encode_off: 0,
                },
            );
            self.concat_stale = true;
            return;
        }
        self.splice_new(name, path, kind, *node);
    }

    fn splice_new(
        &mut self,
        name: EntryName,
        path: CanonicalPath,
        kind: EntryKind,
        node: [u8; 32],
    ) {
        let mut encoded = Vec::new();
        encode_dir_entry(&mut encoded, kind, &name, &node);
        let added = encoded.len();
        let off = self
            .by_name
            .range(&name..)
            .next()
            .map(|(_, slot)| slot.encode_off)
            .unwrap_or(self.concat.len());
        self.concat.splice(off..off, encoded);
        for slot in self.by_name.values_mut() {
            if slot.encode_off >= off {
                slot.encode_off += added;
            }
        }
        self.by_name.insert(
            name,
            ChildSlot {
                path,
                kind,
                node,
                encode_off: off,
            },
        );
    }

    fn remove_path(&mut self, path: &CanonicalPath) {
        let Ok(name) = EntryName::parse(path.name()) else {
            return;
        };
        if self.by_name.remove(&name).is_some() {
            self.concat_stale = true;
        }
    }

    fn kind_of(&self, path: &CanonicalPath) -> Option<EntryKind> {
        let name = EntryName::parse(path.name()).ok()?;
        self.by_name.get(&name).map(|slot| slot.kind)
    }
}

impl DirChildren {
    fn ensure_loaded<S: Storage>(
        &mut self,
        store: &S,
        ck: &CheckoutId,
        dir: &CanonicalPath,
    ) -> Result<(), S::Error> {
        if self.by_parent.contains_key(dir) {
            return Ok(());
        }
        let mut kids = DirKids::default();
        for (path, meta) in store.range_meta(ck, dir)? {
            if path.parent().as_ref() != Some(dir) {
                continue;
            }
            let Ok(name) = EntryName::parse(path.name()) else {
                continue;
            };
            let node = match meta.kind {
                EntryKind::File | EntryKind::Symlink => *file_node(&meta).as_bytes(),
                EntryKind::Dir => *store
                    .get_dir_node(ck, &path)?
                    .unwrap_or_else(empty_dir_node)
                    .as_bytes(),
            };
            kids.by_name.insert(
                name,
                ChildSlot {
                    path,
                    kind: meta.kind,
                    node,
                    encode_off: 0,
                },
            );
        }
        kids.rebuild_concat();
        self.by_parent.insert(dir.clone(), kids);
        Ok(())
    }

    fn apply(&mut self, path: &CanonicalPath, meta: Option<&FileMetadata>, mark_stale: bool) {
        match meta {
            Some(meta) if meta.kind == EntryKind::Dir => {
                if let Some(parent) = path.parent() {
                    if let Some(kids) = self.by_parent.get_mut(&parent) {
                        if kids.kind_of(path) != Some(EntryKind::Dir) {
                            kids.upsert(
                                path.clone(),
                                EntryKind::Dir,
                                empty_dir_node().as_bytes(),
                                mark_stale,
                            );
                        }
                    }
                }
                self.by_parent.entry(path.clone()).or_default();
            }
            Some(meta) => {
                if self.by_parent.contains_key(path) {
                    self.drop_tree(path);
                }
                if let Some(parent) = path.parent() {
                    if let Some(kids) = self.by_parent.get_mut(&parent) {
                        kids.upsert(
                            path.clone(),
                            meta.kind,
                            file_node(meta).as_bytes(),
                            mark_stale,
                        );
                    }
                }
            }
            None => self.drop_tree(path),
        }
    }

    fn drop_tree(&mut self, prefix: &CanonicalPath) {
        self.by_parent.retain(|dir, kids| {
            if prefix.covers(dir) {
                return false;
            }
            let before = kids.by_name.len();
            kids.by_name.retain(|_, slot| !prefix.covers(&slot.path));
            if kids.by_name.len() != before {
                kids.concat_stale = true;
            }
            true
        });
        if let Some(parent) = prefix.parent() {
            if let Some(kids) = self.by_parent.get_mut(&parent) {
                kids.remove_path(prefix);
            }
        }
    }

    fn hash_dir(&mut self, dir: &CanonicalPath) -> DirNode {
        let Some(kids) = self.by_parent.get_mut(dir) else {
            return empty_dir_node();
        };
        if kids.concat_stale {
            kids.rebuild_concat();
        }
        dir_node_from_concat(&kids.concat)
    }

    fn set_child_node(&mut self, parent: &CanonicalPath, child: &CanonicalPath, node: DirNode) {
        let Some(kids) = self.by_parent.get_mut(parent) else {
            return;
        };
        let Ok(name) = EntryName::parse(child.name()) else {
            return;
        };
        kids.set_hash(&name, node.as_bytes());
    }
}

/// Commit one leaf and every directory hash it changes in a single batch.
/// `leaf` of `None` removes the path and, for a directory, everything under it.
pub fn commit_leaf_with<S: Storage>(
    store: &S,
    ck: &CheckoutId,
    path: &CanonicalPath,
    leaf: Option<&FileMetadata>,
    last_synced: LastSynced,
    cache: &mut DirChildren,
) -> Result<(), S::Error> {
    commit_leaves_with(
        store,
        ck,
        [LeafChange {
            path,
            meta: leaf,
            last_synced,
        }],
        cache,
    )
}

/// Commit many leaves and recompute each dirty directory once.
pub fn commit_leaves<'a, S: Storage>(
    store: &S,
    ck: &CheckoutId,
    changes: impl IntoIterator<Item = LeafChange<'a>>,
) -> Result<(), S::Error> {
    commit_leaves_with(store, ck, changes, &mut DirChildren::default())
}

pub fn commit_leaves_with<'a, S: Storage>(
    store: &S,
    ck: &CheckoutId,
    changes: impl IntoIterator<Item = LeafChange<'a>>,
    cache: &mut DirChildren,
) -> Result<(), S::Error> {
    let changes: Vec<LeafChange<'a>> = changes.into_iter().collect();
    if changes.is_empty() {
        return Ok(());
    }

    let mut dirty = HashSet::new();
    for change in &changes {
        if matches!(change.meta, Some(meta) if meta.kind == EntryKind::Dir) {
            dirty.insert(change.path.clone());
        }
        dirty.extend(change.path.ancestors());
    }
    for dir in &dirty {
        cache.ensure_loaded(store, ck, dir)?;
    }

    let mut batch = store.begin_write()?;
    let mark_stale = changes.len() > 1;

    for change in &changes {
        apply_leaf(store, &mut batch, ck, change)?;
        cache.apply(change.path, change.meta, mark_stale);
    }

    let mut dirty: Vec<CanonicalPath> = dirty.into_iter().collect();
    dirty.sort_by_key(|path| std::cmp::Reverse(depth(path)));

    for dir in dirty {
        let node = cache.hash_dir(&dir);
        if let Some(parent) = dir.parent() {
            cache.set_child_node(&parent, &dir, node);
        }
        batch.put_dir_node(ck, &dir, node)?;
    }
    batch.commit()
}

/// `DirNode(central)`, or the `FileNode` when a checkout maps a single file,
/// or the empty directory when the master has never seen the path.
pub fn subtree_root<S: Storage>(
    store: &S,
    ck: &CheckoutId,
    central: &CanonicalPath,
) -> Result<SubtreeRoot, S::Error> {
    if let Some(node) = store.get_dir_node(ck, central)? {
        return Ok(node.into());
    }
    match store.get_meta(ck, central)? {
        Some(meta) if meta.kind != EntryKind::Dir => Ok(file_node(&meta).into()),
        _ => Ok(empty_dir_node().into()),
    }
}

pub fn root_is_dirty<S: Storage>(store: &S, ck: &CheckoutId) -> Result<bool, S::Error> {
    let root = CanonicalPath::root();
    let recomputed = recompute(store, ck, &root, &HashMap::new(), &HashMap::new())?;
    Ok(store.get_dir_node(ck, &root)? != Some(recomputed))
}

fn apply_leaf<S: Storage>(
    store: &S,
    batch: &mut S::WriteBatch<'_>,
    ck: &CheckoutId,
    change: &LeafChange<'_>,
) -> Result<(), S::Error> {
    match change.meta {
        Some(meta) if meta.kind == EntryKind::Dir => {
            batch.put_meta(ck, change.path, meta)?;
            if change.last_synced == LastSynced::AdoptLeaf {
                batch.put_last_synced(ck, change.path, file_node(meta), Some(meta.content_hash))?;
            }
        }
        Some(meta) => {
            let kept = match change.last_synced {
                LastSynced::Keep => {
                    let node = store.get_last_synced(ck, change.path)?;
                    let content = store.get_last_synced_content(ck, change.path)?;
                    node.map(|node| (node, content))
                }
                LastSynced::AdoptLeaf => None,
            };
            vacate_replaced_dir(store, batch, ck, change.path)?;
            batch.put_meta(ck, change.path, meta)?;
            match change.last_synced {
                LastSynced::AdoptLeaf => {
                    batch.put_last_synced(
                        ck,
                        change.path,
                        file_node(meta),
                        Some(meta.content_hash),
                    )?;
                }
                LastSynced::Keep => {
                    if let Some((node, content)) = kept {
                        batch.put_last_synced(ck, change.path, node, content)?;
                    }
                }
            }
        }
        None if change.last_synced == LastSynced::Keep => {
            batch.del_meta_prefix(ck, change.path)?;
            batch.del_dir_prefix(ck, change.path)?;
        }
        None => batch.purge_prefix(ck, change.path)?,
    }
    Ok(())
}

fn vacate_replaced_dir<S: Storage>(
    store: &S,
    batch: &mut S::WriteBatch<'_>,
    ck: &CheckoutId,
    path: &CanonicalPath,
) -> Result<(), S::Error> {
    if matches!(store.get_meta(ck, path)?, Some(old) if old.kind == EntryKind::Dir) {
        batch.purge_prefix(ck, path)?;
    }
    Ok(())
}

fn depth(path: &CanonicalPath) -> usize {
    if path.as_str() == "/" {
        0
    } else {
        path.as_str().bytes().filter(|&byte| byte == b'/').count()
    }
}

/// Rewrite directory hashes that do not match their indexed children.
///
/// A rescan that finds every file already in `meta` commits nothing, so a hash
/// left behind by an earlier partial commit stays in place and the root keeps
/// matching a tree that does not contain those files. Files in such a directory
/// lose `last_synced` so reconcile announces them instead of treating the gap
/// as a delete.
pub fn repair_dir_nodes<S: Storage>(
    store: &S,
    ck: &CheckoutId,
    root: &CanonicalPath,
) -> Result<bool, S::Error> {
    let rows = store.range_meta(ck, root)?;
    let mut by_parent: HashMap<CanonicalPath, Vec<(CanonicalPath, FileMetadata)>> = HashMap::new();
    for (path, meta) in &rows {
        if let Some(parent) = path.parent() {
            by_parent
                .entry(parent)
                .or_default()
                .push((path.clone(), meta.clone()));
        }
    }
    let mut dirs: Vec<CanonicalPath> = rows
        .iter()
        .filter(|(_, meta)| meta.kind == EntryKind::Dir)
        .map(|(path, _)| path.clone())
        .collect();
    if !dirs.iter().any(|dir| dir == root) {
        dirs.push(root.clone());
    }
    dirs.sort_by_key(|path| std::cmp::Reverse(depth(path)));

    let mut computed: HashMap<CanonicalPath, DirNode> = HashMap::new();
    let mut stale_files: Vec<CanonicalPath> = Vec::new();
    let mut writes: Vec<(CanonicalPath, DirNode)> = Vec::new();
    for dir in &dirs {
        let fresh = hash_children(by_parent.get(dir), &computed, store, ck)?;
        let stored = store.get_dir_node(ck, dir)?;
        if stored != Some(fresh) {
            let against_stored = hash_children(by_parent.get(dir), &HashMap::new(), store, ck)?;
            if stored != Some(against_stored) {
                if let Some(kids) = by_parent.get(dir) {
                    for (path, meta) in kids {
                        if matches!(meta.kind, EntryKind::File | EntryKind::Symlink) {
                            stale_files.push(path.clone());
                        }
                    }
                }
            }
            writes.push((dir.clone(), fresh));
        }
        computed.insert(dir.clone(), fresh);
    }
    if writes.is_empty() {
        return Ok(false);
    }
    let mut batch = store.begin_write()?;
    for path in &stale_files {
        batch.del_last_synced(ck, path)?;
    }
    for (dir, node) in &writes {
        batch.put_dir_node(ck, dir, *node)?;
    }
    batch.commit()?;
    Ok(true)
}

fn hash_children<S: Storage>(
    kids: Option<&Vec<(CanonicalPath, FileMetadata)>>,
    computed: &HashMap<CanonicalPath, DirNode>,
    store: &S,
    ck: &CheckoutId,
) -> Result<DirNode, S::Error> {
    let Some(kids) = kids else {
        return Ok(empty_dir_node());
    };
    let mut entries = Vec::with_capacity(kids.len());
    for (path, meta) in kids {
        let Ok(name) = EntryName::parse(path.name()) else {
            continue;
        };
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
                node: match computed.get(path) {
                    Some(node) => *node,
                    None => store.get_dir_node(ck, path)?.unwrap_or_else(empty_dir_node),
                },
            },
        });
    }
    Ok(dir_node(&entries))
}

fn recompute<S: Storage>(
    store: &S,
    ck: &CheckoutId,
    dir: &CanonicalPath,
    overlay: &HashMap<CanonicalPath, Option<&FileMetadata>>,
    computed: &HashMap<CanonicalPath, DirNode>,
) -> Result<DirNode, S::Error> {
    Ok(dir_node(&children_of(store, ck, dir, overlay, computed)?))
}

pub(crate) fn list_children<S: Storage>(
    store: &S,
    ck: &CheckoutId,
    dir: &CanonicalPath,
) -> Result<Vec<DirChild>, S::Error> {
    children_of(store, ck, dir, &HashMap::new(), &HashMap::new())
}

fn children_of<S: Storage>(
    store: &S,
    ck: &CheckoutId,
    dir: &CanonicalPath,
    overlay: &HashMap<CanonicalPath, Option<&FileMetadata>>,
    computed: &HashMap<CanonicalPath, DirNode>,
) -> Result<Vec<DirChild>, S::Error> {
    let mut children: Vec<(CanonicalPath, FileMetadata)> = store
        .range_meta(ck, dir)?
        .into_iter()
        .filter(|(path, _)| path.parent().as_ref() == Some(dir) && !overlay.contains_key(path))
        .collect();
    for (path, meta) in overlay {
        if let (Some(meta), Some(parent)) = (meta, path.parent()) {
            if &parent == dir {
                children.push((path.clone(), (*meta).clone()));
            }
        }
    }

    let mut entries = Vec::with_capacity(children.len());
    for (path, meta) in children {
        let Ok(name) = EntryName::parse(path.name()) else {
            continue;
        };
        entries.push(match meta.kind {
            EntryKind::File => DirChild::File {
                name,
                node: file_node(&meta),
            },
            EntryKind::Symlink => DirChild::Symlink {
                name,
                node: file_node(&meta),
            },
            EntryKind::Dir => DirChild::Directory {
                name,
                node: match computed.get(&path) {
                    Some(node) => *node,
                    None => store
                        .get_dir_node(ck, &path)?
                        .unwrap_or_else(empty_dir_node),
                },
            },
        });
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::ContentHash;
    use crate::test_support::{expected_dir_node, p, MemoryStorage};

    fn file(byte: u8) -> FileMetadata {
        FileMetadata::file(1, 0, 0o100644, ContentHash::from_bytes([byte; 32]))
    }

    fn commit_cold<S: Storage>(
        store: &S,
        ck: &CheckoutId,
        path: &CanonicalPath,
        leaf: Option<&FileMetadata>,
        last_synced: LastSynced,
    ) -> Result<(), S::Error> {
        commit_leaf_with(
            store,
            ck,
            path,
            leaf,
            last_synced,
            &mut DirChildren::default(),
        )
    }

    #[test]
    fn keep_leaves_last_synced_untouched_when_local_meta_changes() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let path = p("/src/foo.rs");
        let first = file(1);
        commit_cold(&store, &ck, &path, Some(&first), LastSynced::AdoptLeaf).unwrap();
        let committed = store.get_last_synced(&ck, &path).unwrap().unwrap();

        let edited = file(2);
        commit_cold(&store, &ck, &path, Some(&edited), LastSynced::Keep).unwrap();
        assert_eq!(store.get_meta(&ck, &path).unwrap().unwrap(), edited);
        assert_eq!(store.get_last_synced(&ck, &path).unwrap(), Some(committed));
        assert_eq!(
            store.get_last_synced_content(&ck, &path).unwrap(),
            Some(first.content_hash)
        );
        assert_eq!(store.get_meta(&CheckoutId::master(), &path).unwrap(), None);
    }

    #[test]
    fn adopt_leaf_stores_file_node_for_a_directory() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let path = p("/src/nested");
        let dir = FileMetadata::directory(0, 0o040755);
        commit_cold(&store, &ck, &path, Some(&dir), LastSynced::AdoptLeaf).unwrap();
        assert_eq!(
            store.get_last_synced(&ck, &path).unwrap(),
            Some(file_node(&dir))
        );

        let edited = FileMetadata::directory(1, 0o040700);
        commit_cold(&store, &ck, &path, Some(&edited), LastSynced::Keep).unwrap();
        assert_eq!(store.get_meta(&ck, &path).unwrap().unwrap(), edited);
        assert_eq!(
            store.get_last_synced(&ck, &path).unwrap(),
            Some(file_node(&dir))
        );
    }

    #[test]
    fn commit_leaf_file_over_a_dir_drops_the_child() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let dir_path = p("/src/nested");
        let child = p("/src/nested/child.txt");
        let dir = FileMetadata::directory(0, 0o040755);
        commit_cold(&store, &ck, &dir_path, Some(&dir), LastSynced::AdoptLeaf).unwrap();
        commit_cold(&store, &ck, &child, Some(&file(1)), LastSynced::AdoptLeaf).unwrap();
        assert_eq!(store.get_meta(&ck, &child).unwrap().unwrap(), file(1));

        let leaf = file(2);
        commit_cold(&store, &ck, &dir_path, Some(&leaf), LastSynced::AdoptLeaf).unwrap();
        assert_eq!(store.get_meta(&ck, &child).unwrap(), None);
        assert_eq!(store.get_meta(&ck, &dir_path).unwrap().unwrap(), leaf);
    }

    #[test]
    fn repair_dir_nodes_rebuilds_a_stale_hash_and_drops_last_synced() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::master();
        let dir = p("/src");
        let file_path = p("/src/a");
        let leaf = file(1);
        commit_cold(
            &store,
            &ck,
            &dir,
            Some(&FileMetadata::directory(0, 0o040755)),
            LastSynced::AdoptLeaf,
        )
        .unwrap();
        commit_cold(&store, &ck, &file_path, Some(&leaf), LastSynced::AdoptLeaf).unwrap();
        let good = store.get_dir_node(&ck, &dir).unwrap().unwrap();
        assert!(store.get_last_synced(&ck, &file_path).unwrap().is_some());

        let mut batch = store.begin_write().unwrap();
        batch.put_dir_node(&ck, &dir, empty_dir_node()).unwrap();
        batch.commit().unwrap();

        assert!(repair_dir_nodes(&store, &ck, &dir).unwrap());
        assert_eq!(store.get_dir_node(&ck, &dir).unwrap(), Some(good));
        assert!(store.get_last_synced(&ck, &file_path).unwrap().is_none());
        assert!(!repair_dir_nodes(&store, &ck, &dir).unwrap());
    }

    #[test]
    fn repair_dir_nodes_keeps_last_synced_when_the_hash_matches() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::master();
        let dir = p("/src");
        let file_path = p("/src/a");
        commit_cold(
            &store,
            &ck,
            &dir,
            Some(&FileMetadata::directory(0, 0o040755)),
            LastSynced::AdoptLeaf,
        )
        .unwrap();
        commit_cold(
            &store,
            &ck,
            &file_path,
            Some(&file(1)),
            LastSynced::AdoptLeaf,
        )
        .unwrap();
        let synced = store.get_last_synced(&ck, &file_path).unwrap();
        assert!(!repair_dir_nodes(&store, &ck, &dir).unwrap());
        assert_eq!(store.get_last_synced(&ck, &file_path).unwrap(), synced);
    }

    #[test]
    fn repair_dir_nodes_drops_last_synced_on_a_symlink() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::master();
        let dir = p("/src");
        let link_path = p("/src/link");
        let link = FileMetadata::symlink(4, 0, 0o120777, ContentHash::from_bytes([9; 32]));
        commit_cold(
            &store,
            &ck,
            &dir,
            Some(&FileMetadata::directory(0, 0o040755)),
            LastSynced::AdoptLeaf,
        )
        .unwrap();
        commit_cold(
            &store,
            &ck,
            &link_path,
            Some(&link),
            LastSynced::AdoptLeaf,
        )
        .unwrap();
        let mut batch = store.begin_write().unwrap();
        batch.put_dir_node(&ck, &dir, empty_dir_node()).unwrap();
        batch.commit().unwrap();

        assert!(repair_dir_nodes(&store, &ck, &dir).unwrap());
        assert!(store.get_last_synced(&ck, &link_path).unwrap().is_none());
    }

    #[test]
    fn commit_leaves_matches_repeated_cold_commits() {
        let sequential = MemoryStorage::new();
        let batched = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let dir = p("/src");
        let dir_meta = FileMetadata::directory(0, 0o040755);
        let files: Vec<(CanonicalPath, FileMetadata)> = (0..8)
            .map(|i| (p(&format!("/src/f{i:02}")), file(i as u8 + 1)))
            .collect();

        commit_cold(&sequential, &ck, &dir, Some(&dir_meta), LastSynced::Keep).unwrap();
        for (path, meta) in &files {
            commit_cold(&sequential, &ck, path, Some(meta), LastSynced::Keep).unwrap();
        }

        let mut changes = vec![LeafChange {
            path: &dir,
            meta: Some(&dir_meta),
            last_synced: LastSynced::Keep,
        }];
        changes.extend(files.iter().map(|(path, meta)| LeafChange {
            path,
            meta: Some(meta),
            last_synced: LastSynced::Keep,
        }));
        commit_leaves(&batched, &ck, changes).unwrap();

        assert_eq!(
            sequential.range_meta(&ck, &p("/")).unwrap(),
            batched.range_meta(&ck, &p("/")).unwrap()
        );
        assert_eq!(
            sequential.get_dir_node(&ck, &dir).unwrap(),
            batched.get_dir_node(&ck, &dir).unwrap()
        );
        assert_eq!(
            sequential.get_dir_node(&ck, &p("/")).unwrap(),
            batched.get_dir_node(&ck, &p("/")).unwrap()
        );
        assert_ne!(
            sequential.get_dir_node(&ck, &dir).unwrap(),
            Some(empty_dir_node())
        );
    }

    #[test]
    fn commit_leaves_of_a_wide_directory_finishes() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let dir = p("/src");
        let dir_meta = FileMetadata::directory(0, 0o040755);
        let files: Vec<(CanonicalPath, FileMetadata)> = (0..8_000)
            .map(|i| (p(&format!("/src/f{i:05}")), file((i % 200) as u8 + 1)))
            .collect();
        let mut changes = vec![LeafChange {
            path: &dir,
            meta: Some(&dir_meta),
            last_synced: LastSynced::Keep,
        }];
        changes.extend(files.iter().map(|(path, meta)| LeafChange {
            path,
            meta: Some(meta),
            last_synced: LastSynced::Keep,
        }));
        let started = std::time::Instant::now();
        commit_leaves(&store, &ck, changes).unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "wide directory commit took {elapsed:?}"
        );
        assert_ne!(
            store.get_dir_node(&ck, &dir).unwrap(),
            Some(empty_dir_node())
        );
        assert_eq!(store.range_meta(&ck, &dir).unwrap().len(), 8_001);
    }

    #[test]
    fn cached_commits_match_cold_commits() {
        let cold = MemoryStorage::new();
        let hot = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let dir = p("/src");
        let dir_meta = FileMetadata::directory(0, 0o040755);
        let files: Vec<(CanonicalPath, FileMetadata)> = (0..8)
            .map(|i| (p(&format!("/src/f{i:02}")), file(i as u8 + 1)))
            .collect();

        commit_cold(&cold, &ck, &dir, Some(&dir_meta), LastSynced::Keep).unwrap();
        for (path, meta) in &files {
            commit_cold(&cold, &ck, path, Some(meta), LastSynced::Keep).unwrap();
        }

        let mut cache = DirChildren::default();
        commit_leaf_with(
            &hot,
            &ck,
            &dir,
            Some(&dir_meta),
            LastSynced::Keep,
            &mut cache,
        )
        .unwrap();
        for (path, meta) in &files {
            commit_leaf_with(&hot, &ck, path, Some(meta), LastSynced::Keep, &mut cache).unwrap();
        }

        assert_eq!(
            cold.range_meta(&ck, &p("/")).unwrap(),
            hot.range_meta(&ck, &p("/")).unwrap()
        );
        assert_eq!(
            cold.get_dir_node(&ck, &dir).unwrap(),
            hot.get_dir_node(&ck, &dir).unwrap()
        );
        assert_eq!(
            cold.get_dir_node(&ck, &p("/")).unwrap(),
            hot.get_dir_node(&ck, &p("/")).unwrap()
        );
    }

    #[test]
    fn cached_file_over_a_dir_drops_the_child() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let dir_path = p("/src/nested");
        let child = p("/src/nested/child.txt");
        let dir = FileMetadata::directory(0, 0o040755);
        let mut cache = DirChildren::default();
        commit_leaf_with(
            &store,
            &ck,
            &dir_path,
            Some(&dir),
            LastSynced::AdoptLeaf,
            &mut cache,
        )
        .unwrap();
        commit_leaf_with(
            &store,
            &ck,
            &child,
            Some(&file(1)),
            LastSynced::AdoptLeaf,
            &mut cache,
        )
        .unwrap();
        commit_leaf_with(
            &store,
            &ck,
            &dir_path,
            Some(&file(2)),
            LastSynced::AdoptLeaf,
            &mut cache,
        )
        .unwrap();
        assert_eq!(store.get_meta(&ck, &child).unwrap(), None);
        assert_eq!(store.get_meta(&ck, &dir_path).unwrap().unwrap(), file(2));
        commit_leaf_with(
            &store,
            &ck,
            &p("/src/other.txt"),
            Some(&file(3)),
            LastSynced::AdoptLeaf,
            &mut cache,
        )
        .unwrap();
        assert_eq!(store.get_meta(&ck, &dir_path).unwrap().unwrap(), file(2));
        assert_eq!(
            store.get_meta(&ck, &p("/src/other.txt")).unwrap().unwrap(),
            file(3)
        );
    }

    #[test]
    fn cached_sibling_update_keeps_utf8_order() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let dir = p("/src");
        let dir_meta = FileMetadata::directory(0, 0o040755);
        let zeta = p("/src/ζed");
        let alpha = p("/src/alpha");
        let mut cache = DirChildren::default();
        commit_leaf_with(
            &store,
            &ck,
            &dir,
            Some(&dir_meta),
            LastSynced::Keep,
            &mut cache,
        )
        .unwrap();
        commit_leaf_with(
            &store,
            &ck,
            &zeta,
            Some(&file(2)),
            LastSynced::Keep,
            &mut cache,
        )
        .unwrap();
        commit_leaf_with(
            &store,
            &ck,
            &alpha,
            Some(&file(1)),
            LastSynced::Keep,
            &mut cache,
        )
        .unwrap();
        commit_leaf_with(
            &store,
            &ck,
            &alpha,
            Some(&file(9)),
            LastSynced::Keep,
            &mut cache,
        )
        .unwrap();

        assert_eq!(
            store.get_dir_node(&ck, &dir).unwrap(),
            Some(expected_dir_node(vec![
                (
                    EntryKind::File as u8,
                    "ζed".into(),
                    *file_node(&file(2)).as_bytes(),
                ),
                (
                    EntryKind::File as u8,
                    "alpha".into(),
                    *file_node(&file(9)).as_bytes(),
                ),
            ]))
        );
    }

    #[test]
    fn wide_directory_out_of_order_inserts_match_one_batch() {
        let ck = CheckoutId::new("src");
        let dir = p("/src");
        let dir_meta = FileMetadata::directory(0, 0o040755);
        let metas: Vec<(CanonicalPath, FileMetadata)> = (0..64)
            .map(|i| {
                let name = format!("n{:04}", (i * 17) % 64);
                (
                    p(&format!("/src/{name}")),
                    FileMetadata::file(
                        i as u64 + 1,
                        i as i64,
                        0o100644,
                        ContentHash::from_bytes([i as u8; 32]),
                    ),
                )
            })
            .collect();

        let mut hot_changes = vec![LeafChange {
            path: &dir,
            meta: Some(&dir_meta),
            last_synced: LastSynced::Keep,
        }];
        hot_changes.extend(metas.iter().map(|(path, meta)| LeafChange {
            path,
            meta: Some(meta),
            last_synced: LastSynced::Keep,
        }));
        let batched = MemoryStorage::new();
        commit_leaves(&batched, &ck, hot_changes).unwrap();

        let hot = MemoryStorage::new();
        let mut cache = DirChildren::default();
        commit_leaf_with(
            &hot,
            &ck,
            &dir,
            Some(&dir_meta),
            LastSynced::Keep,
            &mut cache,
        )
        .unwrap();
        for (path, meta) in &metas {
            commit_leaf_with(&hot, &ck, path, Some(meta), LastSynced::Keep, &mut cache).unwrap();
        }

        assert_eq!(
            hot.get_dir_node(&ck, &dir).unwrap(),
            batched.get_dir_node(&ck, &dir).unwrap()
        );
        assert_eq!(
            hot.get_dir_node(&ck, &p("/")).unwrap(),
            batched.get_dir_node(&ck, &p("/")).unwrap()
        );
    }

    #[test]
    fn cached_reverse_insert_then_delete_matches_dir_node() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let dir = p("/src");
        let dir_meta = FileMetadata::directory(0, 0o040755);
        let names = ["z", "m", "a"];
        let mut cache = DirChildren::default();
        commit_leaf_with(
            &store,
            &ck,
            &dir,
            Some(&dir_meta),
            LastSynced::Keep,
            &mut cache,
        )
        .unwrap();
        for (i, name) in names.iter().enumerate() {
            commit_leaf_with(
                &store,
                &ck,
                &p(&format!("/src/{name}")),
                Some(&file(i as u8 + 1)),
                LastSynced::Keep,
                &mut cache,
            )
            .unwrap();
        }
        commit_leaf_with(
            &store,
            &ck,
            &p("/src/m"),
            None,
            LastSynced::Keep,
            &mut cache,
        )
        .unwrap();

        assert_eq!(
            store.get_dir_node(&ck, &dir).unwrap(),
            Some(expected_dir_node(vec![
                (
                    EntryKind::File as u8,
                    "a".into(),
                    *file_node(&file(3)).as_bytes(),
                ),
                (
                    EntryKind::File as u8,
                    "z".into(),
                    *file_node(&file(1)).as_bytes(),
                ),
            ]))
        );
    }

    #[test]
    fn cached_batch_reverse_inserts_match_dir_node() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let dir = p("/src");
        let dir_meta = FileMetadata::directory(0, 0o040755);
        let z = p("/src/z");
        let m = p("/src/m");
        let a = p("/src/a");
        let z_meta = file(1);
        let m_meta = file(2);
        let a_meta = file(3);
        let mut cache = DirChildren::default();
        commit_leaf_with(
            &store,
            &ck,
            &dir,
            Some(&dir_meta),
            LastSynced::Keep,
            &mut cache,
        )
        .unwrap();
        commit_leaves_with(
            &store,
            &ck,
            [
                LeafChange {
                    path: &z,
                    meta: Some(&z_meta),
                    last_synced: LastSynced::Keep,
                },
                LeafChange {
                    path: &m,
                    meta: Some(&m_meta),
                    last_synced: LastSynced::Keep,
                },
                LeafChange {
                    path: &a,
                    meta: Some(&a_meta),
                    last_synced: LastSynced::Keep,
                },
            ],
            &mut cache,
        )
        .unwrap();
        assert_eq!(
            store.get_dir_node(&ck, &dir).unwrap(),
            Some(expected_dir_node(vec![
                (
                    EntryKind::File as u8,
                    "a".into(),
                    *file_node(&a_meta).as_bytes(),
                ),
                (
                    EntryKind::File as u8,
                    "m".into(),
                    *file_node(&m_meta).as_bytes(),
                ),
                (
                    EntryKind::File as u8,
                    "z".into(),
                    *file_node(&z_meta).as_bytes(),
                ),
            ]))
        );
    }
}

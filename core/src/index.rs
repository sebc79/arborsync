use std::collections::{HashMap, HashSet};

use crate::hash::{DirNode, SubtreeRoot};
use crate::merkle::{DirChild, dir_node, empty_dir_node, file_node};
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
    by_parent: HashMap<CanonicalPath, HashMap<CanonicalPath, FileMetadata>>,
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
        let kids = store
            .range_meta(ck, dir)?
            .into_iter()
            .filter(|(path, _)| path.parent().as_ref() == Some(dir))
            .collect();
        self.by_parent.insert(dir.clone(), kids);
        Ok(())
    }

    fn apply(&mut self, path: &CanonicalPath, meta: Option<&FileMetadata>) {
        match meta {
            Some(meta) if meta.kind == EntryKind::Dir => {
                if let Some(parent) = path.parent() {
                    if let Some(kids) = self.by_parent.get_mut(&parent) {
                        kids.insert(path.clone(), meta.clone());
                    }
                }
                self.by_parent.entry(path.clone()).or_default();
            }
            Some(meta) => {
                self.drop_tree(path);
                if let Some(parent) = path.parent() {
                    if let Some(kids) = self.by_parent.get_mut(&parent) {
                        kids.insert(path.clone(), meta.clone());
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
            kids.retain(|path, _| !prefix.covers(path));
            true
        });
        if let Some(parent) = prefix.parent() {
            if let Some(kids) = self.by_parent.get_mut(&parent) {
                kids.remove(prefix);
            }
        }
    }

    fn children<S: Storage>(
        &self,
        store: &S,
        ck: &CheckoutId,
        dir: &CanonicalPath,
        computed: &HashMap<CanonicalPath, DirNode>,
    ) -> Result<Vec<DirChild>, S::Error> {
        let Some(kids) = self.by_parent.get(dir) else {
            return Ok(Vec::new());
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
        Ok(entries)
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

    let mut computed: HashMap<CanonicalPath, DirNode> = HashMap::new();
    let mut batch = store.begin_write()?;

    for change in &changes {
        apply_leaf(store, &mut batch, ck, change)?;
        cache.apply(change.path, change.meta);
    }

    let mut dirty: Vec<CanonicalPath> = dirty.into_iter().collect();
    dirty.sort_by_key(|path| std::cmp::Reverse(depth(path)));

    for dir in dirty {
        let node = dir_node(&cache.children(store, ck, &dir, &computed)?);
        computed.insert(dir.clone(), node);
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
    use crate::test_support::{MemoryStorage, p};

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
}

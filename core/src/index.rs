use std::collections::HashMap;

use crate::hash::{DirNode, SubtreeRoot};
use crate::merkle::{dir_node, empty_dir_node, file_node, DirChild};
use crate::meta::{EntryKind, FileMetadata};
use crate::path::{CanonicalPath, EntryName};
use crate::storage::{CheckoutId, Storage, WriteBatch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LastSynced {
    AdoptLeaf,
    Keep,
}

struct Pending<'a> {
    path: &'a CanonicalPath,
    meta: Option<&'a FileMetadata>,
}

/// Commit one leaf and every directory hash it changes in a single batch.
/// `leaf` of `None` removes the path and, for a directory, everything under it.
/// A file or symlink leaf drops the path prefix first so a type change cannot
/// leave descendants.
pub fn commit_leaf<S: Storage>(
    store: &S,
    ck: &CheckoutId,
    path: &CanonicalPath,
    leaf: Option<&FileMetadata>,
    last_synced: LastSynced,
) -> Result<(), S::Error> {
    let pending = Pending { path, meta: leaf };
    let mut computed: HashMap<CanonicalPath, DirNode> = HashMap::new();
    let mut batch = store.begin_write()?;

    match leaf {
        Some(meta) if meta.kind == EntryKind::Dir => {
            let node = recompute(store, ck, path, &pending, &computed)?;
            computed.insert(path.clone(), node);
            batch.put_meta(ck, path, meta)?;
            batch.put_dir_node(ck, path, node)?;
            if last_synced == LastSynced::AdoptLeaf {
                batch.put_last_synced(ck, path, file_node(meta))?;
            }
        }
        Some(meta) => {
            let kept = match last_synced {
                LastSynced::Keep => store.get_last_synced(ck, path)?,
                LastSynced::AdoptLeaf => None,
            };
            batch.purge_prefix(ck, path)?;
            batch.put_meta(ck, path, meta)?;
            match last_synced {
                LastSynced::AdoptLeaf => {
                    batch.put_last_synced(ck, path, file_node(meta))?;
                }
                LastSynced::Keep => {
                    if let Some(node) = kept {
                        batch.put_last_synced(ck, path, node)?;
                    }
                }
            }
        }
        None if last_synced == LastSynced::Keep => {
            batch.del_meta_prefix(ck, path)?;
            batch.del_dir_prefix(ck, path)?;
        }
        None => batch.purge_prefix(ck, path)?,
    }

    for ancestor in path.ancestors() {
        let node = recompute(store, ck, &ancestor, &pending, &computed)?;
        computed.insert(ancestor.clone(), node);
        batch.put_dir_node(ck, &ancestor, node)?;
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
    let nothing_pending = Pending {
        path: &root,
        meta: None,
    };
    let recomputed = recompute(store, ck, &root, &nothing_pending, &HashMap::new())?;
    Ok(store.get_dir_node(ck, &root)? != Some(recomputed))
}

fn recompute<S: Storage>(
    store: &S,
    ck: &CheckoutId,
    dir: &CanonicalPath,
    pending: &Pending<'_>,
    computed: &HashMap<CanonicalPath, DirNode>,
) -> Result<DirNode, S::Error> {
    Ok(dir_node(&children_of(store, ck, dir, pending, computed)?))
}

pub(crate) fn list_children<S: Storage>(
    store: &S,
    ck: &CheckoutId,
    dir: &CanonicalPath,
) -> Result<Vec<DirChild>, S::Error> {
    let unused = Pending {
        path: dir,
        meta: None,
    };
    children_of(store, ck, dir, &unused, &HashMap::new())
}

fn children_of<S: Storage>(
    store: &S,
    ck: &CheckoutId,
    dir: &CanonicalPath,
    pending: &Pending<'_>,
    computed: &HashMap<CanonicalPath, DirNode>,
) -> Result<Vec<DirChild>, S::Error> {
    let mut children: Vec<(CanonicalPath, FileMetadata)> = store
        .range_meta(ck, dir)?
        .into_iter()
        .filter(|(path, _)| path.parent().as_ref() == Some(dir) && path != pending.path)
        .collect();
    if let (Some(meta), Some(parent)) = (pending.meta, pending.path.parent()) {
        if &parent == dir {
            children.push((pending.path.clone(), meta.clone()));
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
    use crate::test_support::{p, MemoryStorage};

    fn file(byte: u8) -> FileMetadata {
        FileMetadata::file(1, 0, 0o100644, ContentHash::from_bytes([byte; 32]))
    }

    #[test]
    fn keep_leaves_last_synced_untouched_when_local_meta_changes() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let path = p("/src/foo.rs");
        let first = file(1);
        commit_leaf(&store, &ck, &path, Some(&first), LastSynced::AdoptLeaf).unwrap();
        let committed = store.get_last_synced(&ck, &path).unwrap().unwrap();

        let edited = file(2);
        commit_leaf(&store, &ck, &path, Some(&edited), LastSynced::Keep).unwrap();
        assert_eq!(store.get_meta(&ck, &path).unwrap().unwrap(), edited);
        assert_eq!(store.get_last_synced(&ck, &path).unwrap(), Some(committed));
        assert_eq!(store.get_meta(&CheckoutId::master(), &path).unwrap(), None);
    }

    #[test]
    fn adopt_leaf_stores_file_node_for_a_directory() {
        let store = MemoryStorage::new();
        let ck = CheckoutId::new("src");
        let path = p("/src/nested");
        let dir = FileMetadata::directory(0, 0o040755);
        commit_leaf(&store, &ck, &path, Some(&dir), LastSynced::AdoptLeaf).unwrap();
        assert_eq!(
            store.get_last_synced(&ck, &path).unwrap(),
            Some(file_node(&dir))
        );

        let edited = FileMetadata::directory(1, 0o040700);
        commit_leaf(&store, &ck, &path, Some(&edited), LastSynced::Keep).unwrap();
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
        commit_leaf(&store, &ck, &dir_path, Some(&dir), LastSynced::AdoptLeaf).unwrap();
        commit_leaf(&store, &ck, &child, Some(&file(1)), LastSynced::AdoptLeaf).unwrap();
        assert_eq!(store.get_meta(&ck, &child).unwrap().unwrap(), file(1));

        let leaf = file(2);
        commit_leaf(&store, &ck, &dir_path, Some(&leaf), LastSynced::AdoptLeaf).unwrap();
        assert_eq!(store.get_meta(&ck, &child).unwrap(), None);
        assert_eq!(store.get_meta(&ck, &dir_path).unwrap().unwrap(), leaf);
    }
}

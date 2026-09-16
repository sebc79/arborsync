use std::collections::HashMap;

use crate::hash::{DirNode, SubtreeRoot};
use crate::merkle::{DirChild, dir_node, empty_dir_node, file_node};
use crate::meta::{EntryKind, FileMetadata};
use crate::path::{CanonicalPath, EntryName};
use crate::storage::{CheckoutId, Storage, WriteBatch};

fn master() -> CheckoutId {
    CheckoutId::master()
}

struct Pending<'a> {
    path: &'a CanonicalPath,
    meta: Option<&'a FileMetadata>,
}

/// Commit one leaf and every directory hash it changes in a single batch.
/// `leaf` of `None` removes the path and, for a directory, everything under it.
pub fn commit_leaf<S: Storage>(
    store: &S,
    path: &CanonicalPath,
    leaf: Option<&FileMetadata>,
) -> Result<(), S::Error> {
    let ck = master();
    let pending = Pending { path, meta: leaf };
    let mut computed: HashMap<CanonicalPath, DirNode> = HashMap::new();
    let mut batch = store.begin_write()?;

    match leaf {
        Some(meta) if meta.kind == EntryKind::Dir => {
            let node = recompute(store, path, &pending, &computed)?;
            computed.insert(path.clone(), node);
            batch.put_meta(&ck, path, meta)?;
            batch.put_dir_node(&ck, path, node)?;
            batch.del_last_synced(&ck, path)?;
        }
        Some(meta) => {
            batch.put_meta(&ck, path, meta)?;
            batch.put_last_synced(&ck, path, file_node(meta))?;
            batch.del_dir_node(&ck, path)?;
        }
        None => batch.purge_prefix(&ck, path)?,
    }

    for ancestor in path.ancestors() {
        let node = recompute(store, &ancestor, &pending, &computed)?;
        computed.insert(ancestor.clone(), node);
        batch.put_dir_node(&ck, &ancestor, node)?;
    }
    batch.commit()
}

/// `DirNode(central)`, or the `FileNode` when a checkout maps a single file,
/// or the empty directory when the master has never seen the path.
pub fn subtree_root<S: Storage>(
    store: &S,
    central: &CanonicalPath,
) -> Result<SubtreeRoot, S::Error> {
    if let Some(node) = store.get_dir_node(&master(), central)? {
        return Ok(node.into());
    }
    match store.get_meta(&master(), central)? {
        Some(meta) if meta.kind != EntryKind::Dir => Ok(file_node(&meta).into()),
        _ => Ok(empty_dir_node().into()),
    }
}

pub fn root_is_dirty<S: Storage>(store: &S) -> Result<bool, S::Error> {
    let root = CanonicalPath::root();
    let nothing_pending = Pending {
        path: &root,
        meta: None,
    };
    let recomputed = recompute(store, &root, &nothing_pending, &HashMap::new())?;
    Ok(store.get_dir_node(&master(), &root)? != Some(recomputed))
}

fn recompute<S: Storage>(
    store: &S,
    dir: &CanonicalPath,
    pending: &Pending<'_>,
    computed: &HashMap<CanonicalPath, DirNode>,
) -> Result<DirNode, S::Error> {
    let mut children: Vec<(CanonicalPath, FileMetadata)> = store
        .range_meta(&master(), dir)?
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
        let Some(name) = leaf_name(&path) else {
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
                        .get_dir_node(&master(), &path)?
                        .unwrap_or_else(empty_dir_node),
                },
            },
        });
    }
    Ok(dir_node(&entries))
}

fn leaf_name(path: &CanonicalPath) -> Option<EntryName> {
    EntryName::parse(path.as_str().rsplit('/').next()?).ok()
}

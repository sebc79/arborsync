# Building and Updating Path Merkle Trees

Normative source: `spec.md` §5 and §10. This document is the procedure for those sections.

## Role

ArborSync compares **directory hashes**, not inclusion proofs over a flat leaf list. Each index (the master’s global tree, and one tree per slave checkout) is a path-addressed tree: a directory’s hash is BLAKE3 of its sorted children; a file or symlink’s hash is a `FileNode` over content and metadata. The path is the walk from `/`, not a field inside the leaf.

Do not use `rs_merkle`. That crate’s proofs cannot enumerate a set difference, and its leaf indices move on insert or delete.

## Node encodings

These byte layouts are part of the on-disk and on-the-wire contract. Change them only with an `Envelope.version` bump and a rebuild of every index.

**Kinds:** `File = 1`, `Dir = 2`, `Symlink = 3`.

```
FileNode = BLAKE3(
    u8 kind                         # 1 or 3
    || content_hash                 # 32 bytes
    || u64be size
    || i64be mtime_ns
    || u32be mode
)

# children sorted by name as raw UTF-8
entry    = u8 kind || u32be name_len || name || [u8; 32] child_node
DirNode  = BLAKE3(concat(entries))
```

- File `content_hash` = BLAKE3(file bytes), streamed.
- Symlink `content_hash` = BLAKE3(target bytes from the OS).
- Empty directory: `DirNode = BLAKE3("")` (no entries). First-class; do not omit empty dirs from the parent.

`FileNode` is the CAS object for a non-directory. `DirNode` is never CAS’d as content; it is recomputed after child mutations.

## What is stored

| Key | Value |
|---|---|
| `(checkout_id, file path)` in `meta` | `FileMetadata` |
| `(checkout_id, dir path)` in `dir_nodes` | `DirNode` |
| `(checkout_id, path)` in `last_synced` | last agreed `FileNode` (files/symlinks only) |

Master uses `checkout_id = ""`. Cache **every** directory node, not a handful of “important” subtrees. The subtree root of a checkout is `dir_nodes[central]` (or the file’s `FileNode` if `central` names a file).

## Initial build

1. Walk the tree (`central_root` or checkout `local`). Skip reserved names (`.arborsync-tmp`, `.arborsync-conflicts`).
2. Collect `FileMetadata` for files, symlinks, and directories (see `collecting-metadata.md`).
3. Bottom-up: compute `FileNode` for each non-dir; compute `DirNode` from sorted children; write `meta` + `dir_nodes` in one batch per directory if memory requires chunking, otherwise one batch for the tree.
4. `last_synced` stays empty until the first successful reconcile/apply for that path.

## Incremental update

On a changed path `P` (file, symlink, or directory metadata):

1. Re-read metadata; recompute `FileNode` or, for a directory, recompute `DirNode` from current children.
2. Write `meta` (and new `DirNode` if `P` is a dir).
3. Walk parents from `dirname(P)` to the tree root (checkout `central` on a slave; `/` on the master). For each parent, reload children from the index (not a full FS walk), recompute `DirNode`, write it.
4. Commit one write batch covering the leaf change and every ancestor `DirNode`.

Insert: new `meta` row, then ancestor recompute.  
Delete: remove `meta` (and `dir_nodes` if it was a dir, plus all descendants), then ancestor recompute.  
Rename (same checkout, one debounce window): specified as delete `from` plus insert `to` in the same batch. As built: two `Changed` events and two `commit_leaf` calls. `ProtocolMessage::Rename` is unanswered.

No global leaf array, no “rebuild on insert.”

## Comparing two trees

Given roots `A` and `B` for the same canonical directory path:

- Equal → that subtree is in sync.
- Differ → exchange `DirList` (`name`, `kind`, `node_hash`) for that directory, then:
  - name only in A or only in B → that child is a create or delete (see `spec.md` §10 for which side announces);
  - both present, `kind` differs → specified as type change (delete + create) in one master transaction. As built: `decide_cas` rejects the kind change. Reconcile Pull on the slave can remove then mkdir.
  - both present, hashes differ, both dirs → recurse;
  - both present, hashes differ, both files/symlinks → 3-way using `last_synced` (`spec.md` §10).

This is the entire “difference narrowing” protocol. There is no Merkle inclusion proof and no `proof: Vec<u8>` field.

Complexity is O(directory entries touched + size of the symmetric difference), not O(log n) in the total file count. That is the correct bound for a path tree.

## Slave tree vs master tree

A slave checkout of `/src` stores only nodes under `/src` (paths still stored as canonical `/src/...`, not rewritten to be relative to `/src`). Its root is `DirNode("/src")`. That value is **equal** to the master’s `dir_nodes["/src"]` when the checkout is in sync, because the encoding does not include siblings of `/src`.

A flat global binary tree cannot make that equality hold. This encoding can.

## Integrity

- After every committed batch, `DirNode(P)` must equal a recomputation from children in `meta` / `dir_nodes`. A debug assertion on small trees; a periodic full recompute on master if an operator requests it.
- A mismatched root is not “corrupt”; it is a reconcile trigger. Rebuild from the filesystem only when the index is unreadable (redb recovery failure). Then reconcile.

## Non-goals (struck from earlier drafts)

- Cached “subtree roots” that are internal nodes of a binary hash tree.
- `tree.update_leaf(index, hash)` / leaf-index maps.
- O(log n) set reconciliation via proofs.
- Hashing the canonical path into the leaf.
- Configurable tree depth limits, “tree pruning for unused subtrees,” or streaming a flat leaf vector.

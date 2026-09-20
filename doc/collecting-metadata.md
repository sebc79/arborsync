# Collecting Metadata

Normative source: `spec.md` §6.

## Structure

```rust
pub struct FileMetadata {
    pub kind: EntryKind,        // File | Dir | Symlink
    pub size: u64,
    pub mtime_ns: i64,          // Unix nanoseconds
    pub mode: u32,              // st_mode, including type bits
    pub content_hash: [u8; 32], // file bytes, symlink target, or unused for dirs
}

pub enum EntryKind { File = 1, Dir = 2, Symlink = 3 }
```

Directories still have a `FileMetadata` row (kind `Dir`, `size` = 0, `content_hash` = 32 zero bytes). The directory’s identity in the Merkle tree is `DirNode`, stored separately. Do not put `DirNode` into `content_hash`.

## Paths

Index keys are **canonical** (`spec.md` §1): `/src/foo.rs`, never `/central/src/foo.rs` and never the slave’s local path. Conversion:

- Master: `canonical = "/" + relative(central_root, os_path)` with `/` for the root itself.
- Slave: `canonical = checkout.central` joined with `relative(checkout.local, os_path)`.

UTF-8 only. Reject non-UTF-8 names (log, skip). `central_root` and each `local` are `canonicalize`d at `Master::open` / `Slave::open` (symlinks resolved there only). Parse also `canonicalize`s a local that already exists before the overlap check. `Slave::open` rejects resolved overlap.

## Scan

**Initial and rescan:** recursive walk. Skip `.arborsync-tmp` and `.arborsync-conflicts` at the tree root being walked. For each entry:

1. `symlink_metadata` (do not follow).
2. Classify: file, dir, symlink, or other. Other (devices, sockets, FIFOs): log a warn with the host path and return `None`.
3. Fill `size`, `mtime_ns` (`modified()` → duration since epoch; if unavailable, skip and log), `mode` (`PermissionsExt::mode()` on Unix).
4. **Hash decision:**
   - File (`collect_for_rescan`): hash if no index row, or stored size, mtime, or kind differ. Mode is not a miss. The returned row carries the fresh mode. Rescan, Create, Write, Metadata, and Rename use this.
   - `collect_from_path` hashes a file unconditionally. The miss path falls through to it.
   - Symlink: always read the target and hash it (cheap) on a miss. Same-size same-mtime reuse applies.
   - Dir: no content hash.
5. Streaming BLAKE3 for files.

Rescan is a **full `stat` walk** of the checkout or `central_root`. It is not limited to “changed subtrees.”

**Watcher:** after debounce, re-read each affected path with `collect_for_rescan`. ENOENT during that read is treated as delete.

## Unix mode

**Kept:** file type bits, `rwx` ugo, setuid, setgid, sticky.  
**Ignored:** UID, GID, xattrs, ACLs, BSD flags.

Apply: `set_permissions` with `mode & 0o7777` on files and directories after the rename lands. Symlink permissions are platform-specific; do not fail the apply if setting them is unsupported. mtime via `filetime::set_symlink_file_times` (or `set_file_mtime` for non-links) using the announced `mtime_ns`.

## Symlinks

In-tree: first-class. `content_hash = BLAKE3(target)`, `size = target.len()`. Apply with `std::os::unix::fs::symlink` after removing the previous entry if needed. Broken targets are valid.

Do not resolve in-tree links during scan, index, or apply. Resolving duplicates the target as a second leaf and can walk out of the checkout.

## Errors

- `EACCES` / `EPERM` (`PermissionDenied`): log a warn with the host path, return `None`, continue the walk.
- Transient I/O: specified as retry once, then skip. As built: the error propagates.
- Non-UTF-8 names: log and skip (matches spec).
- Partial trees are allowed only when a name is skipped as `None` (special files).

## Batching

Specified: rescans commit per directory, and one debounce window is one batch. Slave `rescan` writes every changed leaf and recomputes each dirty `DirNode` once in one `commit_leaves` batch. Watcher events still call `commit_leaf` per path. Master apply and slave announce use the same per-path commit, with a `DirChildren` cache so ancestor hashes do not re-list the tree. `CasAccept` on the origin slave writes `last_synced` in a second batch.

## Hardlinks

Two names, two rows, two `FileNode`s. Applying one name does not try to recreate a hardlink. Content may be identical; that is fine.

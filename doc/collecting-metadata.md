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

UTF-8 only. Reject non-UTF-8 names (log, skip). After config load, `central_root` and each `local` are `canonicalize`d (symlinks resolved **there only**).

## Scan

**Initial and rescan:** recursive walk. Skip `.arborsync-tmp` and `.arborsync-conflicts` at the tree root being walked. For each entry:

1. `symlink_metadata` (do not follow).
2. Classify: file / dir / symlink / other. Other (devices, sockets, FIFOs): log, skip.
3. Fill `size`, `mtime_ns` (`modified()` → duration since epoch; if unavailable, skip and log), `mode` (`PermissionsExt::mode()` on Unix).
4. **Hash decision:**
   - File: hash if no index row, or stored size/mtime/kind differ, or the caller is a Write/Create watcher event.
   - Symlink: always read the target and hash it (cheap).
   - Dir: no content hash.
5. Streaming BLAKE3 for files.

Rescan is a **full `stat` walk** of the checkout or `central_root`. It is not limited to “changed subtrees.” Hashing stays lazy via size+mtime.

**Watcher:** after debounce, re-read each affected path with the same rules. `Remove` → drop the row (and descendants if a dir). ENOENT during a read that was not a Remove: treat as delete.

## Unix mode

**Kept:** file type bits, `rwx` ugo, setuid, setgid, sticky.  
**Ignored:** UID, GID, xattrs, ACLs, BSD flags.

Apply: `set_permissions` with `mode & 0o7777` on files and directories after the rename lands. Symlink permissions are platform-specific; do not fail the apply if setting them is unsupported. mtime via `filetime::set_symlink_file_times` (or `set_file_mtime` for non-links) using the announced `mtime_ns`.

## Symlinks

In-tree: first-class. `content_hash = BLAKE3(target)`, `size = target.len()`. Apply with `std::os::unix::fs::symlink` after removing the previous entry if needed. Broken targets are valid.

Do not resolve in-tree links during scan, index, or apply. Resolving duplicates the target as a second leaf and can walk out of the checkout.

## Errors

- `EACCES`: log, skip that name, continue the walk. The index simply lacks that path; a later successful stat is a create.
- Transient I/O: retry once; then skip and leave the previous index row (rescan will try again).
- Partial trees are allowed. Do not abort a scan because one name failed.

## Batching

Rescans buffer metadata and commit per directory (leaf rows + that `DirNode` + ancestors in the same batch as `spec.md` §13). Watcher events for one debounce window are one batch.

## Hardlinks

Two names, two rows, two `FileNode`s. Applying one name does not try to recreate a hardlink. Content may be identical; that is fine.

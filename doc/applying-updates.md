# Applying Updates

Normative source: `spec.md` §8 and §9.

## Incoming messages

On the control stream: `FileAnnounce`, `Delete`, `Rename`, `CasAccept`, `CasReject`.  
On a bulk stream: `BulkHeader` plus raw whole-file bytes, symlink target bytes, or a `copia` delta.

`MerkleUpdate` with a proof is not a message. Path translation uses the message’s `checkout_id` + canonical `path` → `local.join(relative(central, path))`. Reject if the resolved path is outside that checkout’s `local`. Directories: apply mkdir / rmdir only, no bulk. Symlinks: bulk body is the target.

## CAS table (slave receiving from master)

Master is the replica of record. Live path converges to the committed version. `basis` is the pre-change `FileNode` (or `None` for create). `new` is the incoming `FileMetadata`.

| Local | Action |
|---|---|
| absent and create (`basis` is `None`) | apply `new` |
| `FileNode(local) == basis` | apply `new` |
| `FileNode(local) == FileNode(new)` | no-op; set `last_synced` |
| `content_hash` differs from `new` | sidecar local bytes; apply `new` |
| `content_hash` matches, meta differs | apply meta (mode, mtime); no sidecar |
| absent and `basis` is `Some` (we never had it, or already deleted) | apply `new` (create) |

After a successful apply: write `meta`, recompute ancestor `DirNode`s, set `last_synced = FileNode(new)`, register `inflight` so the watcher ignores this write (`spec.md` §7).

## CAS table (master receiving from slave)

Accept iff:

- create (`basis` is `None`) and the path is absent, or
- `FileNode(current) == basis`.

Then atomic-write `central_root`, update the global index, reply `CasAccept`, and fan out (`pushing-updates.md`) to every interested checkout except the announcing `(slave_id, checkout_id)`.

Otherwise `CasReject { path, current }`. The announcing slave sidecars its local bytes if `content_hash` differs, then pulls `current` (or deletes if `current` is `None`).

Meta-only mismatch (same `content_hash`, different `FileNode`): no sidecar; the loser adopts winner metadata.

## Sidecar

```
{local}/.arborsync-conflicts/{canonical-relative}--{first 16 hex chars of local content_hash}
```

Create parent directories as needed. Overwrite that exact sidecar name if it already exists (same losing content). Do **not** write `file.conflict-DATE.ext` next to the live file. The reserved directory is not watched, not indexed, not synced. Conflict copies therefore cannot fan out.

No `conflict_resolution` knob, no `max_conflict_files_per_dir`, no `max_update_age_seconds`.

## Atomic write

1. Ensure parent dirs exist (mode from announce or `0o755`).
2. Write `{local}/.arborsync-tmp/{unique}` (same filesystem as `local`). For a delta: `copia` `patch` from the live file into that tmp file. For whole-file: stream bytes into it. For a symlink: tmp is unused; `symlink` after removing the previous name.
3. `fsync` the tmp file.
4. Verify BLAKE3 of the tmp file (or symlink target) equals `new.content_hash`. Mismatch: drop tmp, send `SignatureRequest` with empty signature (whole-file retry) once; still wrong → log, keep the previous live file, leave `last_synced` unchanged.
5. Set tmp mode (`mode & 0o7777`) and mtime (`filetime`, announced `mtime_ns`).
6. `rename` over the live path.
7. Index batch + `inflight`.

Never patch in place. Crash between rename and index: rescan sees the new bytes; reconcile sets `last_synced`.

Directory create: `create_dir_all` + mode + mtime, then index.  
Directory delete: children first (index prefix delete + FS remove), then the directory.  
File delete: `remove_file` after the CAS check.

Type change: delete the old kind, create the new kind, one master transaction; slaves apply the same pair in order.

## Content transfer

Recipient-driven (`spec.md` §9). After the slave (or master) **decides it will apply** `want_hash`:

1. If no local basis, basis size < 4 KiB, or kind is symlink: `SignatureRequest` with empty `signature` → sender opens a bulk stream `encoding = Whole`.
2. Else: `copia` signature of the live file (or of the tmp copy of the basis) → bulk `encoding = Delta` → `patch` into tmp.
3. Sender that does not have `want_hash` replies `Error` and the recipient waits for the next reconcile.

Do not compute a delta against a remembered remote snapshot. Do not put bodies in bincode.

## Delete and rename

**Delete** applies only if local `FileNode == basis` (or `== last_synced` when the announce’s basis is that). If local content differs: sidecar, then delete live (master won). If local is already absent: no-op, clear `last_synced`.

**Rename** (`from` and `to` in one debounce window, same checkout): treat as atomic delete `from` + create `to` with `from_basis` and `to_new`. If `from` content no longer matches `from_basis`, sidecar `from` and still place `to` from a pull of `to_new`. If the implementation cannot `rename(2)` across the two local paths, copy+delete using the atomic write path.

## Echo

`inflight[(checkout_id, canonical)] = content_hash` from just before rename until the next watcher event for that path is consumed or 2× debounce elapses. Matching hash → drop the event, do not announce. A later real edit has a different hash and announces as usual.

Set announced mtime on the file **before** clearing `inflight`.

## Same-slave overlap

Fan-out is per `checkout_id`. The originating checkout does not re-apply. A second checkout on the same slave whose `central` covers the path receives a normal `FileAnnounce` and runs this document independently (its own `last_synced`, sidecar, `inflight`).

## Failures

- Permission denied / disk full / file busy: log, skip that path, do not advance `last_synced`. Next reconcile retries.
- Path length / invalid name on this OS: log, skip.
- Partial success across a batch of files is allowed; each path is independent except rename’s two paths.

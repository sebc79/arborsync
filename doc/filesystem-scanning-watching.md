# Filesystem Scanning and Watching

Normative source: `spec.md` §7.

## Watcher

- `notify` 8 + `notify-debouncer-mini` 0.5, recursive.
- Debounce default 200 ms (config `watcher_debounce_ms`, allowed 200–500).
- Master: one watch on `central_root`.
- Slave: one watch per checkout `local`.
- Ignore events under `.arborsync-tmp` and `.arborsync-conflicts`.

## Event mapping

| Event | Action |
|---|---|
| Create (file/symlink) | re-read, hash, index, announce create (`basis = None`) if `FileNode != last_synced` |
| Create (dir) | index empty dir, recompute ancestors; no bulk transfer |
| Write | re-read, hash if size/mtime changed, announce update with `basis = last_synced` |
| Metadata (mode/mtime) | re-read, announce if `FileNode` changed (meta-only CAS; no sidecar on conflict) |
| Remove | if `last_synced` is `Some`, announce `Delete { basis = last_synced }`; drop index rows |
| Rename | if both sides in this debounce window and the same checkout: `Rename`; else Remove + Create |

`last_synced` absent and the file exists: treat as create (leftover local file, or first run).

## Echo

Before applying a remote write, set `inflight[(checkout_id, path)] = incoming content_hash`. On a watcher event, `stat`+hash (or target hash for a symlink). If it equals `inflight`, clear `inflight` and stop. Timeout: 2× debounce, then clear anyway.

Master uses the same map with `checkout_id = ""` when applying a slave CAS onto `central_root`, so the master watcher does not re-announce the write it just made.

Never announce when `FileNode(local) == last_synced`.

## Rescan

Every `rescan_interval_seconds` (default 60):

1. Full `stat` walk of `central_root` or the checkout `local` (`collecting-metadata.md`). Not “changed subtrees only.”
2. Compare to the index: missing on disk → local delete; missing in index → local create; size/mtime/kind differ → rehash and treat as write.
3. Announce any `FileNode != last_synced` (slave) or apply to the global index (master).
4. Slave then runs reconcile (`spec.md` §10) so missed *remote* changes are pulled even if the local walk was clean.

Rescan exists because inotify/kqueue/NFS drop events. It cannot know dirty subtrees without walking.

## Recovery vs watching

Watching is the fast path. Correctness is reconcile:

- Slave `RootReport` ↔ master `RootAck`.
- Walk `DirList*` on mismatch.
- 3-way with `last_synced`.

Triggers: after each rescan, after `SubscribeAck`, after reconnect. Not “after every batch of FS events” (that is the announce fast path).

## Watcher death

If the watch handle dies (unmount, overflow): log, drop the handle, rescan immediately, re-arm the watch. The daemon does not exit. Overflow is why rescan exists.

## Integration

- Index writes: `indexing.md` batches.
- Ancestor `DirNode` updates: `building-updating-merkle-trees.md`.
- Announce / apply: `pushing-updates.md`, `applying-updates.md`.
- Conflicts: never based on mtime.

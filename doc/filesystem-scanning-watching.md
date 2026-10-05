# Filesystem Scanning and Watching

Normative source: `spec.md` §7.

## Watcher

- File watches use `notify` 8 + `notify-debouncer-full` 0.5, recursive.
- Config reload uses `notify-debouncer-mini` 0.5 on the config parent directory.
- Debounce default 200 ms (config `watcher_debounce_ms`, allowed 200–500).
- Master: one watch on `central_root`.
- Slave: one watch per checkout `local`.
- Events under `.arborsync-tmp` and `.arborsync-conflicts` still fire. Core drops them when the path's first component after the watch root is a reserved name.

`notify-debouncer-full` keeps event kinds and pairs `RenameMode::From` with `RenameMode::To` in one debounce window when both paths are present. The binary maps `notify::Event` to `WatchEvent` at the process edge. Core `to_local_events` turns those into `LocalEvent`. Core does not import notify.

A `need_rescan()` event runs the existing rescan path. It is not mapped as a file event.

## Event mapping

| `WatchKind` | `LocalEvent` |
|---|---|
| Create or Write | `Changed` if the path is inside the watch |
| Metadata | `Metadata` if the path is inside the watch |
| Remove | `Removed` if the path is inside the watch |
| Rename with both paths canonical | `Renamed { from, to }` |
| Rename with only `from` | `Removed` |
| Rename with only `to` | `Changed(to)` |
| Path outside the watch | dropped |

| What is on disk | Action |
|---|---|
| File or symlink present after Create or Write | `collect_for_rescan` (hash only on kind, size, or mtime miss), index, announce if `FileNode != last_synced` (slave) or meta changed (master) |
| Metadata (chmod or mtime) | `collect_for_rescan` (hash only on kind, size, or mtime miss), then the same announce rule |
| Directory present | index, recompute ancestors, no bulk |
| Remove, path still present | same as a write. `git checkout` unlinks and recreates inside one debounce window. Deleting from the event alone accepts on the master, `CasReject`s the new bytes with no current file, and the slave sidecars then removes the live file |
| Path gone after stat | `note_removed`. Announce `Delete` if an index row exists |
| Same-window same-checkout rename | one `ProtocolMessage::Rename`. Apply is `fs::rename` plus index update |
| Unpaired or cross-checkout rename | `Removed` plus `Changed`, which is Delete plus Create |

`last_synced` absent and the file exists: treat as create (leftover local file, or first run).

## Echo

After a successful file or symlink apply, set `inflight[(checkout_id, path)] = incoming content_hash`. On a watcher event, `stat` plus hash (or target hash for a symlink). If it equals `inflight`, clear `inflight` and stop. Timeout: 2× debounce, then clear anyway. Arm before the live `rename`, `mkdir`, or replace; disarm if that apply fails so a failed write is not treated as an echo.

Dirs and meta-only apply arm `inflight` (`ContentHash::ZERO` for dirs). Applied dirs keep `last_synced` as that `FileNode`. Incoming rename arms `inflight` on `to`.

Master uses the same map with `checkout_id = ""` when applying a slave CAS onto `central_root`, so the master watcher does not re-announce the write it just made.

Never announce when `FileNode(local) == last_synced`.

## Rescan

The interval is `recv_timeout` on the notify channel (default 60 s). A busy tree postpones rescan.

1. Full `stat` walk of `central_root` or the checkout `local` (`collecting-metadata.md`). Not "changed subtrees only." The master watch thread stats and reads the index without the session mutex, then locks only to commit.
2. Compare to the index: missing on disk → local delete; missing in index → local create; size/mtime/kind differ → treat as write. A master path is deleted only when a stat taken under the lock still says it is gone. A surveyed row is written only when that later stat still matches.
3. Slave hashes again only when size, mtime, or kind disagree with the stored row (`collect_for_rescan`). Mode is not a miss. Master `walk_central` also uses `collect_for_rescan`.
4. Rescan `lstat` runs off the session task, 64 names at a time, so `status`, `read_control`, and the hash pump stay live while those stats are in flight. A partial directory is not a delete. A path missing from the walk is deleted only when a later stat returns not found. A dir-list `Delete` waits until that rescan finishes. A name that is new or changed in the index is `FileAnnounce`d when its stat returns. The slave holds hashed announces until 64 are ready or hashing workers are idle, then one `commit_leaves` writes them. `SubscribeAck` rewrites directory hashes that do not match indexed children, then sends the current `RootReport` before the walk finishes, so leftover `DirList*` can start on rows already in the index. A stale parent hash used to match the master and hide those rows until the content walk finished. Known index rows still reconcile via that `RootReport`. Leftover pages take the next turn when both a rescan and a page are queued. When the walk ends, one `commit_leaves` batch writes deletes and leftover diffs, then the live directory cache for that checkout is dropped so the next write reloads children from the index. A `RootReport` (`spec.md` §10) pulls missed remote changes. `Slave::rescan` still drains the walk for tests. The watch thread only queues a rescan. It does not drain it. An announce after the index row already matches disk does not call `commit_leaf` again. A `DirListRequest` records the checkout's `CasAccept` count. `apply_dir_child` suppresses `AnnounceDelete` on that page when the count has moved since the request. A later request records its own count and does not clear the count for a page already in hand. A nested listing used to zero the shared counter, so a page fetched before those accepts deleted files the master had just stored.

Rescan exists because inotify/kqueue/NFS drop events. It cannot know dirty subtrees without walking.

## Recovery vs watching

Watching is the fast path. Correctness is reconcile:

- Slave `RootReport` ↔ master `RootAck`.
- Walk `DirList*` on mismatch. Continue with `DirListRequest.after` while `more` is true.
- 3-way with `last_synced`. A name only on the slave is announced, including when local still equals `last_synced`.

Triggers: after each rescan, after `SubscribeAck`, after reconnect. Not “after every batch of FS events” (that is the announce fast path).

## Watcher death

If the watch handle dies (unmount, overflow): log, drop the handle, rescan immediately, re-arm the watch. The daemon does not exit. Overflow is why rescan exists. If re-arm fails again immediately, sleep one second before the next attempt so a broken watch cannot spin the CPU.

## Integration

- Index writes: `indexing.md` batches.
- Ancestor `DirNode` updates: `building-updating-merkle-trees.md`.
- Announce / apply: `pushing-updates.md`, `applying-updates.md`.
- Conflicts: never based on mtime.

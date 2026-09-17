# Filesystem Scanning and Watching

Normative source: `spec.md` §7.

## Watcher

- `notify` 8 + `notify-debouncer-mini` 0.5, recursive.
- Debounce default 200 ms (config `watcher_debounce_ms`, allowed 200–500).
- Master: one watch on `central_root`.
- Slave: one watch per checkout `local`.
- Events under `.arborsync-tmp` and `.arborsync-conflicts` still fire. Core `is_reserved` drops them.

`notify-debouncer-mini` delivers `{ path, kind: Any }`. It does not emit Create, Write, Remove, Rename, or `Modify(Metadata)`. A notify rename with `[from, to]` becomes two unpaired path events.

## Event mapping

The binaries map every path to `LocalEvent::Changed`. Core then `stat`s.

| What is on disk | Action |
|---|---|
| File or symlink present | re-read, always hash, index, announce if `FileNode != last_synced` (slave) or meta changed (master) |
| Directory present | index, recompute ancestors, no bulk |
| Path gone | `note_removed`: announce `Delete` if an index row exists |
| Same-window rename | two `Changed` events: delete the old name, create the new one. `ProtocolMessage::Rename` is defined and unanswered |

`last_synced` absent and the file exists: treat as create (leftover local file, or first run).

Specified §7 kinds (Write vs metadata, paired `Rename`) need a watcher that keeps event types. Mini cannot do that.

## Echo

After a successful file or symlink apply, set `inflight[(checkout_id, path)] = incoming content_hash`. On a watcher event, `stat` plus hash (or target hash for a symlink). If it equals `inflight`, clear `inflight` and stop. Timeout: 2× debounce, then clear anyway.

Dirs and meta-only apply arm `inflight` (`ContentHash::ZERO` for dirs). Applied dirs keep `last_synced` as that `FileNode`.

Master uses the same map with `checkout_id = ""` when applying a slave CAS onto `central_root`, so the master watcher does not re-announce the write it just made.

Never announce when `FileNode(local) == last_synced`.

## Rescan

The interval is `recv_timeout` on the notify channel (default 60 s). A busy tree postpones rescan.

1. Full `stat` walk of `central_root` or the checkout `local` (`collecting-metadata.md`). Not “changed subtrees only.”
2. Compare to the index: missing on disk → local delete; missing in index → local create; size/mtime/kind differ → treat as write.
3. Slave hashes again only when size, mtime, kind, or mode disagree with the stored row (`collect_for_rescan`). Master walk always calls `collect_from_path` and rehashes every file.
4. Slave sends `RootReport` after the walk (`spec.md` §10) so missed remote changes are pulled even if the local walk was clean. It does not announce during rescan. Mismatch on the root starts the `DirList*` walk, which announces or pulls.

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

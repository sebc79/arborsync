# Pushing Updates from the Master

Normative source: `spec.md` §8–§10.

The master pushes **decisions**, not guessed deltas. Disconnected slaves are not queued; they catch up with reconcile.

## Fast path (someone just committed)

A commit is either:

- the master watcher/rescan applied a local FS change to the global index, or
- the master accepted a slave `FileAnnounce` / `Delete` / `Rename` (CAS succeeded).

Then:

1. Recompute ancestor `DirNode`s (`building-updating-merkle-trees.md`).
2. Look up interested **connected** checkouts: `central` is a prefix of the changed path (`spec.md` §2).
3. Skip the `(slave_id, checkout_id)` pair that just committed, when the commit came from a slave.
4. For each remaining `(slave_id, checkout_id)`, send `FileAnnounce` / `Delete` / `Rename` with that target `checkout_id`. `basis` is the pre-commit `FileNode`.
5. The **recipient** asks for bytes (`SignatureRequest` + bulk stream) if it decides to apply (`applying-updates.md`).

Do not send `MerkleUpdate` proofs. Do not generate a `copia` delta until a recipient has sent a signature (or asked for whole-file).

## Interest examples

Changed `/src/project1/file.txt`:

| Checkout `central` | Notified? |
|---|---|
| `/` | yes |
| `/src` | yes |
| `/src/project1` | yes |
| `/src/project1/file.txt` | yes (single-file map) |
| `/src/project1/subdir` | no |
| `/src/project2` | no |
| `/src2` | no |

## Slave-originated commit

1. Slave watcher or rescan sees a real local change (`FileNode != last_synced`, not `inflight`).
2. Slave sends `FileAnnounce` with `checkout_id` = that checkout and `basis = last_synced` (`None` if never synced).
3. Master CAS (`applying-updates.md`). On success: write `central_root`, index, `CasAccept { checkout_id, path, file_node: Some(new FileNode) }`, fan-out. On failure: `CasReject`; slave sidecars if needed and pulls.
4. Origin slave sets `last_synced` from `CasAccept` (`None` file_node → clear `last_synced` after a delete). Other slaves set `last_synced` after they apply.

## Slow path (reconcile)

Missed watchers, offline periods, apply failures:

1. Slave `RootReport` for each checkout central.
2. Master `RootAck` (compare to `dir_nodes[central]`).
3. On mismatch, slave walks `DirList*` and 3-way (`spec.md` §10). Walks are bidirectional: the slave both pulls and announces. Initial sync is the same walk with empty `last_synced`.

Master never walks a slave unsolicited. Master never stores a per-slave update queue, “max_queued_updates,” or retry buffer of file contents.

Reconnect = new connection + `Subscribe` + this slow path. “Resume interrupted transfer” = the next walk notices the path still differs and transfers again. There is no byte-range resume.

## What is not a push trigger

- Application heartbeats (QUIC keep-alive only).
- Wall-clock age of a file.
- File-type priority (config vs docs).
- Administrative “force push this blob to offline nodes.”

A manual resync is: trigger the slave’s reconcile walk (SIGHUP on the slave or a future `arborsync slave reconcile` subcommand).

## Backpressure

If a slave’s control stream is blocked, stop sending it more announces; the next `RootReport` after it catches up repairs anything missed. Do not grow an unbounded in-memory queue. `max_concurrent` bulk streams per connection is `quic_max_concurrent_streams` (config). Slow apply on the slave is the slave’s problem; the master does not snapshot file contents for it.

## Offline

Disconnect drops interest. The 1000-update queue in earlier drafts is gone. Catch-up cost is the Merkle walk, not a replay log.

## Observability

Log at `info`: connect/disconnect, Subscribe accept/reject, CAS accept/reject counts, reconcile starts, apply failures. No metrics port in v1.

## Config knobs that remain

```toml
watcher_debounce_ms = 200
rescan_interval_seconds = 60
quic_max_concurrent_streams = 256
```

Removed: `push_debounce_ms` as a second debounce (the watcher debounce is enough), `max_queued_updates_per_slave`, `max_push_attempts`, `enable_delta_caching`, `max_delta_cache_size_mb`, `push_batch_size` as a correctness parameter. A sender may coalesce multiple control messages in one syscall; that is not a specified batch protocol.

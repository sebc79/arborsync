# Resume a leftover checkout drop

A tree already on the slave checkout at connect is announced and copied to `central_root`. Interval `status` lines keep appearing. `pending` on the master drains. Bulk counters move.

## Sub-features

- `leftover-nested` copies a directory of files that existed before the slave connected.
- `leftover-status` keeps `status ` lines on both daemons while the copy runs.

## How to get to it (user POV)

- Stop the slave.
- Copy a tree into the checkout `local` path.
- Start the slave again and wait for the same relative paths under `central_root`.

## Driving it with control-arborsync

Preconditions:

- ArborSync is healthy from `control-arborsync doctor`.
- `LOCAL` and `CENTRAL_ROOT` come from `control-arborsync paths`.
- The helper pair is the one this run launched.

- **Stop the slave.** Kill only `SLAVE_PID` from `control-arborsync paths`. Leave the master running.
- **Drop a leftover tree.** Create `$LOCAL/drop/d00` through `$LOCAL/drop/d15`, each with `f00.txt` through `f15.txt`.
- **Start the slave.** Run `"$BIN" slave --config "$SLAVE_CONFIG"` with `ARBORSYNC_LOG_LEVEL=info`. Wait for `connected to ` in `SLAVE_LOG`.
- **Wait for the last file.** Run `control-arborsync wait-file "$CENTRAL_ROOT/src/drop/d15/f15.txt" "d15-f15"` with a long timeout.
- **Status still ticks.** `grep 'status ' "$SLAVE_LOG"` and `grep 'status ' "$MASTER_LOG"` each match a line after connect. A later master line has `apply_ok` or `bulk_in` greater than 0.
- **Proof.** Run `control-arborsync capture leftover-drop`. The replica file is present. Both captured logs contain `status ` lines.

## Gotchas

- Do not treat a master `health=stuck` line during a large file as a hang if the next window shows `bulk_in` or a lower `pending`.
- A slave that stops emitting `status ` while master `pending` stays high and `bulk_in` stays 0 is the old write-all stall.
- Prefill the checkout before the slave process starts. A live watcher write is a different path.

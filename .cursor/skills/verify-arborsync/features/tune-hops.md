# Raise a named hashing or fulfill hop

When a `status ` line names `bottleneck=hashing` or `bottleneck=fulfill_parked`, edit the matching `[tune.<hop>]` table and send SIGHUP. Both knobs apply live. Watch the same `bottleneck=` field. Master lines also print `fanout_dropped=`.

## Sub-features

- `tune-default` starts with omitted tune tables and logs `tune hashing.workers=` with `from=nproc`.
- `tune-fulfill-live` applies a slave `[tune.fulfill_parked]` change on SIGHUP without a restart.
- `tune-master-line` keeps `fanout_dropped=` on each master `status ` line.

## How to get to it (user POV)

- Start the daemons with the default TOML (no `[tune]` tables).
- Read `master.log` and `slave.log` for `tune hashing.workers=` and `status `.
- Add `[tune.fulfill_parked]` to the slave file and send SIGHUP to that process.
- Write a checkout file and wait for it under `central_root`.

## Driving it with control-arborsync

Preconditions:

- ArborSync is healthy from `control-arborsync doctor`.
- `LOCAL`, `CENTRAL_ROOT`, `SLAVE_CONFIG`, `SLAVE_PID`, `MASTER_LOG`, and `SLAVE_LOG` come from `control-arborsync paths`. Source that file with `set -a`.
- The helper TOML omits `[tune]` tables.

- **Default workers.** `grep 'tune hashing.workers=' "$MASTER_LOG"` and the same grep on `$SLAVE_LOG` each match a line that contains `from=nproc`.
- **Idle master line.** Wait 6 s. `grep 'status ' "$MASTER_LOG"` matches a line that contains `fanout_dropped=`.
- **Live inflight.** Append this block to `$SLAVE_CONFIG`, then `chmod 600 "$SLAVE_CONFIG"`, then `kill -HUP "$SLAVE_PID"`:

```toml
[tune.fulfill_parked]
inflight = 8
```

- **Still running.** `control-arborsync doctor` still exits 0. The slave log has no `reload requires restart`.
- **Write still crosses.** Run `printf 'tuned' > "$LOCAL/tune.txt"`. Then `control-arborsync wait-file "$CENTRAL_ROOT/src/tune.txt" "tuned"`.
- **Proof.** Run `control-arborsync capture tune-hops`. Both captured logs contain `tune hashing.workers=`. The master log contains `fanout_dropped=`. The replica file is `tuned`.

## Gotchas

- `workers = 0` and `inflight = 0` fail parse. The reload stays on the old file and logs `reload <path>:`.
- Master TOML that contains `[tune.fulfill_parked]` is rejected. Do not add that table to `$MASTER_CONFIG`.
- `[tune.origin_bytes]` fails parse even though extra top-level keys still vanish.
- The status line does not grow a `workers=` field. Proof that the hop moved is `bottleneck=` leaving `hashing` or `fulfill_parked`, or `depth` falling.
- `chmod 600` after you edit the helper file. Any other mode is `InsecureMode` and the reload is ignored.

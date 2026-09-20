# Read interval status lines

Master and slave log one summary line each status interval (default 5 s). The line names health (`idle`, `busy`, `stuck`, `failed`), `bottleneck=` (the hop that is limiting progress), interval counters, queue depths, and the last error. The master also logs one line per slave.

## Sub-features

- `status-idle` appears after connect with no file writes.
- `status-busy` appears after a slave write that CAS-commits.
- `status-fields` keeps `health=`, `bottleneck=`, `last_error=`, and queue depths on the same line.

## How to get to it (user POV)

- Start the daemons with the default `status_interval_seconds` (or omit the key).
- Read `master.log` and `slave.log` for lines that start with `status `.

## Driving it with control-arborsync

Preconditions:

- ArborSync is healthy from `control-arborsync doctor`.
- `LOCAL` and `CENTRAL_ROOT` come from `control-arborsync paths`.
- The helper TOML omits `status_interval_seconds`, so the default is 5 s.

- **Idle pair.** Wait 6 s after doctor. `grep 'status ' "$MASTER_LOG"` and `grep 'status ' "$SLAVE_LOG"` each match at least one line that contains `health=idle` or `health=busy` from subscribe, and `bottleneck=`. Neither line lists one path per file.
- **Slave write.** Run `printf 'from-slave' > "$LOCAL/status.txt"`. Then `control-arborsync wait-file "$CENTRAL_ROOT/src/status.txt" "from-slave"`.
- **Busy window.** Wait 6 s. A later `status ` line on the master contains `slave=dev-alice` and a non-zero `cas_ok` or `in`. A slave `status ` line contains a non-zero `out` or `cas_ok`.
- **Proof.** Run `control-arborsync capture interval-status`. Both captured logs contain `status ` lines. The replica file is `from-slave`.

## Gotchas

- Do not treat per-event `info` lines as the interval summary. The summary starts with `status `.
- A 40 s idle wait in `slave-connect` will also emit these lines. That is expected.
- `status_interval_seconds = 0` disables the lines. The helper does not set that.

# Name the limiting hop on status lines

Each `status ` line names `bottleneck=` after `health=`. Idle is `bottleneck=none`. A wait that is open also prints `age=` and `depth=`. The master's per-slave line adds `hint=fresh`, `hint=stale`, or `hint=absent` so you can tell a slave verdict that arrived as a datagram from a silent peer.

## Sub-features

- `bottleneck-idle` prints `bottleneck=none` on the slave after connect.
- `bottleneck-hint` prints `hint=fresh` on a master `status slave=dev-alice` line once gauges are flowing.
- `bottleneck-busy` keeps `bottleneck=` on the same line as `health=` after a slave write.

## How to get to it (user POV)

- Start the daemons with the default `status_interval_seconds` (or omit the key).
- Read `master.log` and `slave.log` for lines that start with `status `.

## Driving it with control-arborsync

Preconditions:

- ArborSync is healthy from `control-arborsync doctor`.
- `LOCAL` and `CENTRAL_ROOT` come from `control-arborsync paths`.
- The helper TOML omits `status_interval_seconds`, so the default is 5 s.

- **Idle hop.** Wait 6 s after doctor. `grep 'status ' "$SLAVE_LOG"` matches a line that contains `bottleneck=none`. `grep 'status ' "$MASTER_LOG"` matches a line that contains `bottleneck=`.
- **Gauge hint.** A later master line that contains `slave=dev-alice` also contains `hint=fresh` or `hint=absent`. After two status periods a live pair should show `hint=fresh`.
- **Slave write.** Run `printf 'from-slave' > "$LOCAL/bottleneck.txt"`. Then `control-arborsync wait-file "$CENTRAL_ROOT/src/bottleneck.txt" "from-slave"`.
- **Busy hop.** Wait 6 s. A later `status ` line on each daemon still contains `bottleneck=`.
- **Proof.** Run `control-arborsync capture bottleneck-status`. Both captured logs contain `bottleneck=`. The replica file is `from-slave`.

## Gotchas

- Do not treat per-event `info` lines as the interval summary. The summary starts with `status `.
- `hint=absent` right after connect is expected until the first slave status tick lands a datagram.
- A connected slave with no local wait and no usable gauge prints `bottleneck=unobserved` on the master's per-slave line. The aggregate line stays `bottleneck=none`.
- `status_interval_seconds = 0` disables both the lines and the datagram. The helper does not set that.

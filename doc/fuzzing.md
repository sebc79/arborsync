# Run a private sync campaign

`arborsync-fuzz` starts an `arborsync master` and one or two `arborsync slave` processes. Every config, key, and tree lives under a directory the fuzzer creates in the temp dir. The run does not open `/etc/arborsync/master.toml`, `~/.config/arborsync/slave.toml`, or `nix/test-master.toml`.

## Build the two binaries

From the repo root, build the daemon and the fuzzer.

```bash
cargo build -p arborsync -p arborsync-fuzz
```

The binaries are `target/debug/arborsync` and `target/debug/arborsync-fuzz`.

## Run one seed

Change to an empty directory first. A finding is written to `./findings` in the current directory.

```bash
cd "$(mktemp -d)"
/path/to/arborsync-fuzz run \
  --bin /path/to/arborsync \
  --seed 1 \
  --steps 16 \
  --slaves 1
```

`--steps` defaults to 32. `--slaves` defaults to 2 and accepts only 1 or 2. Omit `--seed` and the process prints the seed it drew.

Exit 0 means the campaign finished with no finding. The process prints `seed <n>` and writes nothing else.

Exit 1 means one finding. The process prints the kind and the artifact path. The artifact is `findings/<kind>-<seed>.json`.

Exit 2 means the fuzzer could not start. The temp dir was not writable, or `--bin` was missing.

## Replay a finding

```bash
/path/to/arborsync-fuzz replay --bin /path/to/arborsync findings/mismatch-1.json
```

Replay runs that artifact with shrink turned off.

## Read a clean result

A clean exit does not mean every filesystem case ran. The seed draws freeze, thaw, disk, and settle steps. It does not draw grammar faults or a bad bulk body.

These commands exited 0 after the status check below was in place.

- `run --seed 3 --steps 12 --slaves 1`
- `run --seed 11 --steps 12 --slaves 1`
- `run --seed 13 --steps 12 --slaves 1`
- `run --seed 1 --steps 16 --slaves 2`
- `run --seed 5 --steps 16 --slaves 2`

`cargo test -p arborsync-fuzz` covers a live `hello.txt` copy and a same-length, same-mtime rewrite. The slave kept the previous bytes on that rewrite.

## Judge the master status line

With `rescan_interval_seconds` at 2 and `status_interval_seconds` at 1, a connected master's `status 1s` line stayed `health=busy` on every sampled second. The busy windows alternated between `rescan=1` and `root=2`. `pending=0` on those lines. The slave's own `status 1s` line did reach `health=idle` with every counter at 0.

The fuzzer treats a master line as drained when `pending=0` after a rescan bump. It still requires the slave line to be `health=idle` with `pending=0`. A 30 second wait with neither of those is a hang finding.

## Directory create with children already on disk

A `Create` can name only the new directory when the file was written while the daemon was stopped, or before the recursive watch was armed. `note_changed` walks children already on disk into the same `HashPlan`.

These commands exit 0 after that walk.

```bash
arborsync-fuzz run --bin /path/to/arborsync --seed 8 --steps 16 --slaves 1
arborsync-fuzz replay --bin /path/to/arborsync findings/mismatch-6.json
```

The second command is the shrunk seed 6 artifact (`nested/z` on a slave). A fresh `run --seed 6 --steps 16 --slaves 2` can still exit 1 on a struck-key restore parse (`duplicate key quic_idle_timeout_ms`). That is a harness stop, not a tree mismatch.

Seed 7 (`--steps 16`, either `--slaves 1` or `--slaves 2`) exits 1 with `path=dir unexpected` after a non-UTF-8 name under `dir`. The directory is on the master. The non-UTF-8 name stays on the slave. That matches the skip for those names.

Some seeds stop inside the fuzzer and do not judge the trees. Seed 1 with `--steps 32 --slaves 2` exits 2 with `File exists` on `mkdir` of a file. Seed 2 with `--steps 16 --slaves 2` exits 2 with `Is a directory` on a put. Seed 10 with `--steps 16 --slaves 2` does not return within 400s. The slave being mutated stops logging while that disk op is still running.

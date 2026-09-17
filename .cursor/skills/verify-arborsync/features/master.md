# Run the master daemon

Master starts from a mode-`600` TOML file, watches `central_root`, and logs the UDP address it actually bound.

## Sub-features

- `master-listen` stays up and logs `listening on 127.0.0.1:<port>`.
- `master-config-flag` reads `--config PATH`.
- `master-config-env` reads `ARBORSYNC_CONFIG`.
- `master-insecure-mode` refuses a world-readable config.

## How to get to it (user POV)

- Run `arborsync master --config PATH`.
- Run `arborsync master` with `ARBORSYNC_CONFIG` set.
- Run `arborsync master --help`.

## Driving it with control-arborsync

Preconditions:

- A disposable pair is healthy, or this feature's isolated launch created one.
- `control-arborsync doctor` reports `master_pid` alive and `master_addr` set.

- **Listen.** Launch the pair. Run `control-arborsync launch` then `control-arborsync doctor`. Stdout includes `master_addr=127.0.0.1:<port>`. The master log contains `listening on` that same address and `master watching` the printed `central_root`.
- **Config flag.** Confirm the flag path. Run `control-arborsync paths` and check `MASTER_CONFIG`. The process was started with `master --config` that file.
- **Config env.** After `cleanup`, `launch` a new `--run-id`, `kill` only that run's `MASTER_PID` from `paths`, then start `ARBORSYNC_CONFIG="$MASTER_CONFIG" "$BIN" master` (no `--config`). The new master log contains `listening on `.
- **Insecure mode.** Copy a valid `master.toml` to a temp path, `chmod 644`, and run `target/debug/arborsync master --config` that copy. The process exits non-zero. Stderr mentions the path and insecure mode.
- **Proof.** Run `control-arborsync capture master`. `master.log` in the artifact dir contains `listening on` and the process was still running at capture time.

## Gotchas

- `listen_addr = "127.0.0.1:0"` is required for isolation. The ready address is the log line, not the TOML value.
- Config files must be mode `600`. `LoadedMaster` rejects anything else.
- `central_root`, `listen_addr`, `db_path`, and `master_key_path` do not reload on SIGHUP. Changing them needs a new process.
- Do not bind `0.0.0.0:8443` in verification. That collides with a real install.

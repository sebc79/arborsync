# Restore a central prefix onto the slave

`arborsync restore --prefix /src` replaces that prefix from a directory while the master is stopped. After the master starts again, the checkout shows the restored bytes and drops a file the source did not contain.

## Sub-features

- `restore-prefix` writes the source bytes under `central_root/src` and leaves `central_root/other` alone.
- `restore-reaches-slave` shows those bytes in the checkout and removes the extra file.

## How to get to it (user POV)

- Stop the master this run started.
- Run `arborsync restore` with that run's master config, a source directory, and `--prefix /src`.
- Start `arborsync master` with the same config.
- Read the checkout file.

## Driving it with control-arborsync

Preconditions:

- ArborSync is healthy from `control-arborsync doctor`.
- `eval "$(control-arborsync paths)"` loads `BIN`, `ROOT`, `MASTER_PID`, `MASTER_CONFIG`, `MASTER_LOG`, `CENTRAL_ROOT`, and `LOCAL` from `state.env`.
- No file named `restored.txt` or `extra.txt` exists under `LOCAL` or `CENTRAL_ROOT/src`.
- No file named `keep.txt` exists under `CENTRAL_ROOT/other`.

- **Plant the wrong tree.** Write the files the restore will correct. Run `mkdir -p "$CENTRAL_ROOT/src" "$CENTRAL_ROOT/other"` then `printf 'wrong' > "$CENTRAL_ROOT/src/restored.txt"`, `printf 'extra' > "$CENTRAL_ROOT/src/extra.txt"`, and `printf 'keep' > "$CENTRAL_ROOT/other/keep.txt"`.
- **Wait for the slave.** Watch the bad replica. Run `control-arborsync wait-file "$LOCAL/restored.txt" "wrong"` and `control-arborsync wait-file "$LOCAL/extra.txt" "extra"`. Both exit `0`.
- **Write the source.** The source directory is the prefix, not a parent named `src`. Run `mkdir -p "$ROOT/pristine"` and `printf 'restored' > "$ROOT/pristine/restored.txt"`. The source file reads `restored`.
- **Stop the master.** Kill the pid from `state.env`. Run `kill "$MASTER_PID"` and wait until `kill -0 "$MASTER_PID"` fails. The index lock is gone.
- **Restore `/src`.** Run `"$BIN" restore --config "$MASTER_CONFIG" --source "$ROOT/pristine" --prefix /src`. Exit code `0`. Stdout names `/src` and `generation`. `cat "$CENTRAL_ROOT/src/restored.txt"` is `restored`. `test ! -e "$CENTRAL_ROOT/src/extra.txt"`. `cat "$CENTRAL_ROOT/other/keep.txt"` is still `keep`.
- **Start the master.** Use the same config. Run `ARBORSYNC_LOG_LEVEL=info nohup "$BIN" master --config "$MASTER_CONFIG" >>"$ROOT/master.stdout" 2>>"$MASTER_LOG" &`. The log gains a `listening on` line. Do not pass `/etc/arborsync/master.toml`.
- **Wait for the slave.** Watch the corrected replica. Run `control-arborsync wait-file "$LOCAL/restored.txt" "restored"`. Exit code `0` and the checkout bytes are `restored`. `test ! -e "$LOCAL/extra.txt"`.
- **Proof.** Run `control-arborsync capture restore`. `trees.txt` lists `./src/restored.txt` and `./other/keep.txt` under central, and `./restored.txt` under the checkout.

## Gotchas

- Do not start `/etc/arborsync/master.toml`. The config is `MASTER_CONFIG` from `state.env`.
- The source root is the prefix. Nesting `src/` inside the source directory writes `/src/src/restored.txt`.
- Restore refuses a locked index and tells you to stop the master. `kill` without waiting leaves the lock held.
- If restore stops halfway, run it again before starting the master. The master refuses to start while the prefix is still replacing.
- `--pretend` prints `replace` and `delete` lines and does not write. No stdout means a slave whose checkout is the source directory would only upload.
- A slave binary that does not know `RestoreEpochs` logs a bad frame and disconnects. This run's binary is this version, so it stays up and converges.

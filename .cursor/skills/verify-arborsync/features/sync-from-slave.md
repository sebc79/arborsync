# Sync a slave write to the master

A file created in the slave checkout becomes the same bytes at `central_root/src` on the master after the watcher and CAS commit.

## Sub-features

- `slave-write-file` creates a new file in the `src` checkout.
- `slave-write-appears` shows those bytes on the master under `/src`.
- `slave-write-survives-reread` still matches after a second read.

## How to get to it (user POV)

- Create or edit a file under the checkout `local` path mapped to central `/src`.
- Confirm the same relative path under the master's `central_root`.

## Driving it with control-arborsync

Preconditions:

- ArborSync is healthy from `control-arborsync doctor`.
- `LOCAL` and `CENTRAL_ROOT` come from `control-arborsync paths`.
- No file named `hello.txt` exists under `LOCAL` or `CENTRAL_ROOT/src`.

- **Write on the slave.** Create the file. Run `printf 'from-slave' > "$LOCAL/hello.txt"`. The checkout file reads `from-slave`.
- **Wait for the master.** Watch the replica. Run `control-arborsync wait-file "$CENTRAL_ROOT/src/hello.txt" "from-slave"`. Exit code `0` and the master file bytes are `from-slave`.
- **Reread.** Read both sides again. `cat "$LOCAL/hello.txt"` and `cat "$CENTRAL_ROOT/src/hello.txt"` are identical.
- **Proof.** Run `control-arborsync capture sync-from-slave`. `trees.txt` lists `./hello.txt` under the checkout and `./src/hello.txt` under central. Both logs are from the same run. The captured trees were taken after `wait-file` succeeded.

## Gotchas

- Debounce is 200 ms. Do not assert immediately; use `wait-file`.
- `.arborsync-tmp` and `.arborsync-conflicts` are reserved and do not sync. Ignore them in listings unless proving a CAS loss.
- Writing outside the checkout `local` path does not announce.
- A log line without matching replica bytes is not proof.

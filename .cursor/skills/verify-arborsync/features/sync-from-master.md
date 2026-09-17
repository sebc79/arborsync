# Sync a master write to the slave

A file created under the master's `central_root/src` becomes the same bytes in the slave checkout after fan-out.

## Sub-features

- `master-write-file` creates a new file under central `/src`.
- `master-write-appears` shows those bytes in the checkout.
- `master-write-survives-reread` still matches after a second read.

## How to get to it (user POV)

- Create or edit a file under the master's `central_root` inside a prefix the slave subscribed (`/src`).
- Confirm the same relative path in the checkout `local` directory.

## Driving it with control-arborsync

Preconditions:

- ArborSync is healthy from `control-arborsync doctor`.
- `LOCAL` and `CENTRAL_ROOT` come from `control-arborsync paths`.
- No file named `from-master.txt` exists under `LOCAL` or `CENTRAL_ROOT/src`.

- **Write on the master.** Create the file. Run `mkdir -p "$CENTRAL_ROOT/src"` and `printf 'from-master' > "$CENTRAL_ROOT/src/from-master.txt"`. The central file reads `from-master`.
- **Wait for the slave.** Watch the replica. Run `control-arborsync wait-file "$LOCAL/from-master.txt" "from-master"`. Exit code `0` and the checkout file bytes are `from-master`.
- **Reread.** Read both sides again. `cat "$CENTRAL_ROOT/src/from-master.txt"` and `cat "$LOCAL/from-master.txt"` are identical.
- **Proof.** Run `control-arborsync capture sync-from-master`. `trees.txt` lists `./src/from-master.txt` under central and `./from-master.txt` under the checkout.

## Gotchas

- The slave ACL is `/src`. A write at `central_root/other/file` does not appear in this checkout.
- Debounce is 200 ms. Use `wait-file`, not a single `sleep`.
- Creating `central_root/src` may already have happened from a prior slave write. That is fine.
- Capture after `wait-file` succeeds. Capturing early records an empty checkout and fails the feature.

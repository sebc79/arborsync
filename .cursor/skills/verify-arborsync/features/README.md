# ArborSync verification map

This directory is the maintained source for verifying the user-facing behavior of ArborSync. Read the index before driving the daemons, then use the matching feature file as the recipe.

## Baseline preconditions

- Build `target/debug/arborsync` with `control-arborsync build`.
- Launch an isolated pair with `control-arborsync launch` unless the feature is keygen-only.
- Run `control-arborsync doctor` and require the printed `master_addr`, `central_root`, `local`, `slave_id=dev-alice`, and `checkout_id=src`.
- Configs and keys live under `/tmp/arborsync-verify-<run-id>/` and are mode `600`.
- Never drive `/etc/arborsync`, `~/.config/arborsync`, or an instance this run did not start.
- On macOS, logs may show `/private/tmp/...` while helper paths stay `/tmp/...`. Same directory.

## Driving conventions

- Start every recipe from the baseline state unless its preconditions say otherwise.
- Treat every command as literal. Keep quoted names and flags unchanged.
- Run process and file actions through `control-arborsync`.
- User mutation is an ordinary `printf`/`cp` into `CENTRAL_ROOT` or `LOCAL`, then `wait-file` on the replica.
- Restore the trees after a mutation if a later sub-feature needs an empty checkout. Do not remove proof artifacts during cleanup.

## Proof and skip reporting

- Capture the user action and the resulting state, not only the final file.
- Daemon proof includes the command, both log files, and the process still running.
- Keygen proof includes stdout, the 32-byte secret file, and exit code `0`.
- Mutation proof includes a read of the replica bytes after `wait-file`.
- Record the feature ID and entry point used with every artifact.
- Report an unreachable path with the attempted command and the unmet precondition.
- Do not report a skipped entry point as verified through a different path.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible behavior. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line for each behavior.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with control-arborsync` starts with `Preconditions:` and uses labeled bullets that pair each user action with an exact command and observable result.
4. `Gotchas` lists traps that can waste or invalidate a verification run.

Keep implementation details out of the map. Name only user paths, stable handles, required state, commands, and observable proof.

## Features

- [Generate a static key](./keygen.md) covers `keygen --out`, the printed pin, and the required `--out` flag.
- [Run the master daemon](./master.md) covers listen, config path flag vs `ARBORSYNC_CONFIG`, insecure-mode reject, and the startup public pin.
- [Connect a slave](./slave-connect.md) covers connect after listen, missing key path, idle retry while the master is down, and the startup public pin.
- [Sync a slave write to the master](./sync-from-slave.md) covers a checkout file appearing under `central_root/src`.
- [Sync a master write to the slave](./sync-from-master.md) covers a central file appearing in the checkout.
- [Read interval status lines](./interval-status.md) covers the 5 s master and slave summaries.
- [Name the limiting hop on status lines](./bottleneck-status.md) covers `bottleneck=` and the slave-to-master gauge.
- [Raise a named hashing or fulfill hop](./tune-hops.md) covers `[tune.hashing]`, slave `[tune.fulfill_parked]`, live SIGHUP, and `fanout_dropped=`.
- [Resume a leftover checkout drop](./leftover-drop.md) covers a tree already on the slave at connect.

---
name: verify-arborsync
description: Drive the ArborSync master/slave CLI daemons the way a user does — isolated keys, TOML, file writes, and log proof. Use when proving ArborSync behavior, verifying a sync change, or checking keygen/master/slave locally.
---

# Verify ArborSync

Primary surface: the `arborsync` CLI (`master`, `slave`, `keygen`) plus the two trees those daemons watch. There is no web UI. `arborsync-core` is a library; do not treat `core/tests` or in-memory `MemoryTransport` as user proof.

Read [features/README.md](features/README.md) before driving. Use the matching feature file. One convenient path is incomplete when the map lists others.

Helper: `.cursor/skills/verify-arborsync/scripts/control-arborsync` (executable). All commands below are literal.

## Launch

Never start `/etc/arborsync/master.toml` or `~/.config/arborsync/slave.toml`. Those are live installs. Always use the helper's disposable tree.

The helper sets `CARGO_TARGET_DIR` to this repo's `target/` so a redirected cargo cache cannot serve a stale binary.

```bash
.cursor/skills/verify-arborsync/scripts/control-arborsync launch
```

Ready when stdout prints `launched run <id> master=127.0.0.1:<port>` and `doctor` exits 0. Master ready line is `master watching <central_root> and listening on <addr>`. Slave ready line is `connected to <addr>`.

A second run can sit beside the first: pass `--run-id` (or `ARBORSYNC_VERIFY_RUN_ID`). Each run binds `127.0.0.1:0`, its own keys, redb files, and trees under `/tmp/arborsync-verify-<id>`. Do not drive a pair you did not launch.

Teardown is Cleanup, not Ctrl-C by binary name.

For `keygen` only, skip `launch`:

```bash
.cursor/skills/verify-arborsync/scripts/control-arborsync build
.cursor/skills/verify-arborsync/scripts/control-arborsync keygen --out /tmp/arborsync-verify-keygen/k
```

## Doctor

Run first whenever anything looks off:

```bash
.cursor/skills/verify-arborsync/scripts/control-arborsync doctor
```

Pass means: both helper PIDs alive, binary is `target/debug/arborsync`, master log owns `listening on <addr>`, slave log has `connected to `, checkout and `central_root` exist, both TOML files are mode `600`. Fail means stop driving; read the printed logs; `cleanup` the stranded run before another `launch`.

## Drive

User actions are CLI invocations and ordinary filesystem writes. Stable handles:

| Handle | Value |
|---|---|
| Commands | `arborsync master`, `arborsync slave`, `arborsync keygen --out PATH` |
| Config flags | `--config PATH`, env `ARBORSYNC_CONFIG` |
| Log override | `ARBORSYNC_LOG_LEVEL` |
| Slave id | `dev-alice` |
| Checkout | id `src`, central `/src` |
| Master ready | log substring `listening on ` |
| Slave ready | log substring `connected to ` |
| Key stdout | `hex:` + 64 hex digits |

Print paths for the current run:

```bash
.cursor/skills/verify-arborsync/scripts/control-arborsync paths
```

Write through the OS, not an internal setter. After a mutation, wait for the other tree:

```bash
.cursor/skills/verify-arborsync/scripts/control-arborsync wait-file "$LOCAL/hello.txt" "from-slave"
```

Configs the helper did not write must be `chmod 600` or `LoadedMaster` / `LoadedSlave` reject `InsecureMode`. Key material is files only; there is no `ARBORSYNC_*` for secrets.

Existing `tests/cli.rs` and `tests/sync.rs` are regression tests. They may inform expected strings. They are not a substitute for driving the launched pair.

## Evidence

Proof lives under `/tmp/arborsync-verify-artifacts/<run-id>/`. Cleanup must not delete that directory.

```bash
.cursor/skills/verify-arborsync/scripts/control-arborsync capture <feature-id>
```

Standards:

- Exercise the real user path: `keygen`, TOML, daemon, then a file write. Do not call `Master::handle` or `note_local`.
- Capture the action and the resulting state: the write command, both trees, and both daemon logs. A final file alone is not enough.
- Side effects: the replica file bytes, plus `.arborsync-conflicts` when asserting a CAS loss.
- Record the feature id and entry point (`keygen --out`, slave write, master write) with every artifact.
- Mocks only at a production boundary. This skill does not mock QUIC.

`capture` copies `master.log`, `slave.log`, and `find` listings of both trees.

## Cleanup

```bash
.cursor/skills/verify-arborsync/scripts/control-arborsync cleanup
```

Kills only `MASTER_PID` and `SLAVE_PID` from that run's `state.env`, then removes `/tmp/arborsync-verify-<id>`. Artifacts stay at `/tmp/arborsync-verify-artifacts/<id>/`. After a failed iteration, run this before launching again so ports and trees are not stranded.

## Helpers

`.cursor/skills/verify-arborsync/scripts/control-arborsync`

```bash
.cursor/skills/verify-arborsync/scripts/control-arborsync build
.cursor/skills/verify-arborsync/scripts/control-arborsync keygen --out PATH
.cursor/skills/verify-arborsync/scripts/control-arborsync launch [--run-id ID]
.cursor/skills/verify-arborsync/scripts/control-arborsync doctor [--run-id ID]
.cursor/skills/verify-arborsync/scripts/control-arborsync paths [--run-id ID]
.cursor/skills/verify-arborsync/scripts/control-arborsync wait-file PATH EXPECTED [--timeout SECS]
.cursor/skills/verify-arborsync/scripts/control-arborsync capture FEATURE_ID [--run-id ID]
.cursor/skills/verify-arborsync/scripts/control-arborsync cleanup [--run-id ID]
```

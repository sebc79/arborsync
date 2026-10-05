# Configuration

Normative source: `spec.md` §14. TOML only. No YAML, no templating, no include files.

## Files

| Role | Default path | Override |
|---|---|---|
| Master | `/etc/arborsync/master.toml` | `--config`, `ARBORSYNC_CONFIG` |
| Slave | `~/.config/arborsync/slave.toml` | `--config`, `ARBORSYNC_CONFIG` |
| Master static key | `master_key_path` | — |
| Slave static key | `slave_key_path` | — |

```
arborsync master [--config PATH]
arborsync slave  [--config PATH]
arborsync keygen [--out PATH]
arborsync recompute [--config PATH]
arborsync path [--config PATH] PATH
```

`keygen` writes a 32-byte X25519 secret (0600) and prints the public key as `hex:` + 64 hex chars to stdout for pasting into the peer’s config.
`recompute` loads a master config and rewrites directory hashes that do not match indexed children. It prints `recomputed directory hashes` when it writes, and `directory hashes already match` when it does not.
`path` prints what that config’s index and the live disk know about one path. The argument is a host path under `central_root` or a checkout `local`, or a canonical path such as `/src/hello.txt`. A running daemon holds the index lock, so the command copies the file and prints `snapshot=copy`. The copy can miss a write that has not reached disk.
`master` and `slave` log that same public pin at startup after they read the secret file.

## Master

```toml
central_root = "/central"
listen_addr = "0.0.0.0:8443"
master_key_path = "/etc/arborsync/master.key"
db_path = "/var/lib/arborsync/index.redb"
log_level = "info"                    # error | warn | info | debug | trace
watcher_debounce_ms = 200             # 200–500
rescan_interval_seconds = 60          # at least 1
status_interval_seconds = 5           # 0 disables, max 3600
max_checkouts_per_slave = 100
max_connections = 100
max_connection_attempts_per_minute = 60

# [tune.hashing]
# workers = "nproc"                 # or 1–256. Omit the table for this default.

[[slaves]]
id = "dev-alice"
public_keys = ["hex:0123…"]          # 32-byte X25519 public, one or more
allowed_prefixes = ["/src", "/docs"]

[[slaves]]
id = "backup-1"
public_keys = ["hex:89ab…"]
allowed_prefixes = ["/"]
```

Required: `central_root`, `listen_addr`, `master_key_path`, `db_path`, at least one `[[slaves]]` row to accept anyone (an empty list means “accept nobody,” which is valid for a dry start).

`central_root` is created if missing (mode `0o755`). It is canonicalized at startup.

## Slave

```toml
slave_id = "dev-alice"
master_addr = "master.example.com:8443"
slave_key_path = "~/.config/arborsync/slave.key"
master_public_keys = ["hex:cdef…"]    # current master pin, plus previous during rotation
db_path = "/var/cache/arborsync/cache.redb"
log_level = "info"
max_checkouts_per_slave = 100         # same name and default as the master
watcher_debounce_ms = 200
rescan_interval_seconds = 60          # at least 1
status_interval_seconds = 5           # 0 disables, max 3600
# peer_socket = "/var/cache/arborsync/peers.sock"
# peer_socket_mode = "660"              # octal. Omitted means 660. Not a metrics port.

# [tune.hashing]
# workers = "nproc"                   # or 1–256. Omit the table for this default.
# [tune.fulfill_parked]
# inflight = 4                        # 1–64. Slave only.

checkouts = [
    { id = "src",  central = "/src",  local = "/opt/projects/src" },
    { id = "docs", central = "/docs", local = "/opt/projects/docs" },
]
```

A `/` checkout on `dev-alice` is rejected (her ACL is `/src` and `/docs`). Full-replica and same-slave overlap checkouts go on `backup-1`.

Required: `slave_id`, `master_addr`, `slave_key_path`, `master_public_keys`, `checkouts` (may be empty: the process idles until the config watch adds some).

`peer_socket` is optional. Omitted, the slave binds `peers.sock` beside `db_path`. `peer_socket_mode` is an octal string. Omitted, the mode is `0660`, so the slave's group can connect. `600` is owner-only. A client needs write permission on the socket. A leftover socket at that path is removed before bind. Bind failure is logged and sync continues. The socket is not a metrics port. Changing the path or the mode requires a restart.

`local` paths are created if missing (`0o755`) and canonicalized at `Slave::open`. Parse checks tilde-expanded paths and `canonicalize`s a local that already exists. `Slave::open` and checkout-add reload reject `LocalOverlap` on the resolved paths. `id` unique. `central` absolute canonical (see `subscriptions.md`).

Reconnect backoff is hardcoded at 1 s, doubling, cap 60 s. `reconnect_initial_ms`, `reconnect_max_ms`, and the `quic_*` keys from older drafts are not parsed.

`max_checkouts_per_slave` must be the same idea on both sides. A slave config longer than the master’s limit is `SubscribeReject`ed; validate locally against the slave’s own copy of the setting as a first check.

## Tune

`[tune.hashing]` and `[tune.fulfill_parked]` are the only hop tables. A `[tune]` key that does not name one of those hops is a parse error. Extra top-level TOML keys still vanish. `0` is never auto.

`[tune.hashing].workers` is `"nproc"` or an integer from 1 through 256. The default is `"nproc"`. Omit the table when that is what you want. `"nproc"` uses `available_parallelism` and clamps to 256. If the OS returns no width, the process uses 1 and logs that. Both roles accept this table.

`[tune.fulfill_parked].inflight` is an integer from 1 through 64. The default is 4. Slave only. Master TOML that contains this table is `ConfigError::TuneNotOnRole`.

Both knobs apply on SIGHUP. Hash admission and leftover `kick` read `Loaded*` after the cfg swap. There is no dedicated hash pool. There is no NixOS option for either key.

`STEP_BUDGET`, `OUTBOX_BACKPRESSURE`, and the 16 MiB large-lane cutoff stay hardcoded.

## Validation

- TOML types and ranges (`watcher_debounce_ms` in 200–500, `status_interval_seconds` in 0–3600, `log_level` enum, ports, `tune.hashing.workers` in 1–256 or `"nproc"`, `tune.fulfill_parked.inflight` in 1–64).
- Keys: each `hex:` value decodes to exactly 32 bytes. Secret key files are 32 raw bytes or the same `hex:` form.
- Prefixes: absolute, normalized, no `..`.
- Slave `slave_id` is a non-empty UTF-8 string matching `[A-Za-z0-9._:-]+`.
- Do not require network reachability at config parse time. The slave retries connect.

## Reload (SIGHUP)

**Applied live:** `log_level`, `max_connection_attempts_per_minute`, `max_connections` (affects new accepts), `[[slaves]]` (add/remove rows, change `allowed_prefixes`, add rotation keys), slave `master_public_keys`, slave `checkouts` (add/remove per `spec.md` §3), debounce, rescan, and status intervals (the next window uses the new value), `[tune.hashing].workers`, and slave `[tune.fulfill_parked].inflight`. Watcher restart is only for debounce or rescan. A status interval change is read on the next tick. A worker or inflight change is read on the next hash admission or leftover `kick`.

**Requires restart:** `listen_addr`, `db_path`, `central_root`, `master_addr`, `*_key_path`, slave `slave_id`, slave `peer_socket`, and slave `peer_socket_mode`.

SIGHUP reloads this file. It does not start reconcile.

The process watches the config file's parent directory for changes and applies the same reload path as SIGHUP. If watch setup fails (for example the parent is missing), it logs and retries with backoff until the watch is running. A later successful setup delivers config-change events. SIGHUP still reloads while the watch is down.

Removing an ACL row disconnects that `slave_id` if connected. Tightening `allowed_prefixes` drops those checkouts from interest and leaves the QUIC session up. The slave is not sent `SubscribeReject`. Later `RootReport`s for those ids return `not_subscribed`.

## Environment

```
ARBORSYNC_CONFIG=/path/to.toml
ARBORSYNC_LOG_LEVEL=debug
```

No `ARBORSYNC_*` for keys or key paths.

## Permissions

Config and key files are `0600`, owner = the daemon user. `write_static_key` sets `0600`. `LoadedMaster::load` / `LoadedSlave::load` reject any other config mode (`ConfigError::InsecureMode`). `parse` does not check mode. The process does not need root if it can read the tree, bind the UDP port, and write `db_path`.

## Enable the master on NixOS

Import `nixosModules.default` from this flake. Set `services.arborsync.master.enable` and `services.arborsync.master.configFile` to your TOML.

```nix
{
  services.arborsync.master.enable = true;
  services.arborsync.master.configFile = ./master.toml;
}
```

The unit is `arborsync-master.service`. It copies `configFile` to `/run/arborsync/master.toml` with mode `0600`, then runs `arborsync master --config /run/arborsync/master.toml`. A store path is `0444`, and `LoadedMaster::load` rejects that mode, including when `/etc` is a symlink to the store. `systemctl reload arborsync-master` copies the file again and sends SIGHUP.

Write the static key on the host, not in the Nix store:

```
arborsync keygen --out /var/lib/arborsync/master.key
```

Point `master_key_path` and `db_path` at paths the `arborsync` user can write. `StateDirectory=arborsync` creates `/var/lib/arborsync`. The unit does not create `/central` and does not run `keygen`. Open the UDP port in `listen_addr` yourself.

## Examples

Minimal master (accept a full-replica slave):

```toml
central_root = "/central"
listen_addr = "0.0.0.0:8443"
master_key_path = "/etc/arborsync/master.key"
db_path = "/var/lib/arborsync/index.redb"

[[slaves]]
id = "backup-1"
public_keys = ["hex:…"]
allowed_prefixes = ["/"]
```

Development slave (`dev-alice`; ACL `/src` + `/docs`):

```toml
slave_id = "dev-alice"
master_addr = "localhost:8443"
slave_key_path = "/home/dev/.config/arborsync/slave.key"
master_public_keys = ["hex:…"]
db_path = "/home/dev/.cache/arborsync/cache.redb"
checkouts = [
    { id = "src",  central = "/src",  local = "/home/dev/project/src" },
    { id = "docs", central = "/docs", local = "/home/dev/project/docs" },
]
log_level = "debug"
```

Same ids and centrals as the main `dev-alice` snippet; locals differ because this host is a laptop. A checkout of `/` is rejected.

Backup / overlap slave (`backup-1`; ACL `/`):

```toml
slave_id = "backup-1"
master_addr = "master.example.com:8443"
slave_key_path = "/etc/arborsync/backup.key"
master_public_keys = ["hex:…"]
db_path = "/var/lib/arborsync/cache.redb"
checkouts = [
    { id = "src", central = "/src", local = "/opt/src" },
    { id = "bak", central = "/",    local = "/backup/central" },
]
```

## Struck from earlier drafts

- `psk`, `transport`, `db_backend`, `sled`.
- `allow_overlapping_mappings` (local overlap is always illegal; central overlap is always legal).
- `conflict_resolution`, `conflict_file_template`, `max_conflict_files_per_dir`, `max_update_age_seconds`.
- `metrics_enabled` / `metrics_bind_addr`.
- Config templating, include files, automatic VCS backup of toml.
- Separate `max_subscriptions_per_connection` (use `max_checkouts_per_slave`).
- `ARBORSYNC_MASTER_ADDR` as a silent override (use the file or `--config`).
- `quic_max_concurrent_streams`, `quic_idle_timeout_ms`, `quic_initial_mtu`, `reconnect_initial_ms`, `reconnect_max_ms`.

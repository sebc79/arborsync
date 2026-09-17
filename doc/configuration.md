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
```

`keygen` writes a 32-byte X25519 secret (0600) and prints the public key as `hex:` + 64 hex chars to stdout for pasting into the peer’s config.

## Master

```toml
central_root = "/central"
listen_addr = "0.0.0.0:8443"
master_key_path = "/etc/arborsync/master.key"
db_path = "/var/lib/arborsync/index.redb"
log_level = "info"                    # error | warn | info | debug | trace
watcher_debounce_ms = 200             # 200–500
rescan_interval_seconds = 60
max_checkouts_per_slave = 100
max_connections = 100
max_connection_attempts_per_minute = 60

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
rescan_interval_seconds = 60

checkouts = [
    { id = "src",  central = "/src",  local = "/opt/projects/src" },
    { id = "docs", central = "/docs", local = "/opt/projects/docs" },
]
```

A `/` checkout on `dev-alice` is rejected (her ACL is `/src` and `/docs`). Full-replica and same-slave overlap checkouts go on `backup-1`.

Required: `slave_id`, `master_addr`, `slave_key_path`, `master_public_keys`, `checkouts` (may be empty: the process idles until the config watch adds some).

`local` paths are created if missing (`0o755`) and canonicalized at `Slave::open`. Parse checks tilde-expanded paths and `canonicalize`s a local that already exists. `Slave::open` and checkout-add reload reject `LocalOverlap` on the resolved paths. `id` unique. `central` absolute canonical (see `subscriptions.md`).

Reconnect backoff is hardcoded at 1 s, doubling, cap 60 s. `reconnect_initial_ms`, `reconnect_max_ms`, and the `quic_*` keys from older drafts are not parsed.

`max_checkouts_per_slave` must be the same idea on both sides. A slave config longer than the master’s limit is `SubscribeReject`ed; validate locally against the slave’s own copy of the setting as a first check.

## Validation

- TOML types and ranges (`watcher_debounce_ms` in 200–500, `log_level` enum, ports).
- Keys: each `hex:` value decodes to exactly 32 bytes. Secret key files are 32 raw bytes or the same `hex:` form.
- Prefixes: absolute, normalized, no `..`.
- Slave `slave_id` is a non-empty UTF-8 string matching `[A-Za-z0-9._:-]+`.
- Do not require network reachability at config parse time. The slave retries connect.

## Reload (SIGHUP)

**Applied live:** `log_level`, `max_connection_attempts_per_minute`, `max_connections` (affects new accepts), `[[slaves]]` (add/remove rows, change `allowed_prefixes`, add rotation keys), slave `master_public_keys`, slave `checkouts` (add/remove per `spec.md` §3), debounce/rescan intervals (next window uses the new value).

**Requires restart:** `listen_addr`, `db_path`, `central_root`, `master_addr`, `*_key_path`, and slave `slave_id`.

SIGHUP reloads this file. It does not start reconcile.

Removing an ACL row disconnects that `slave_id` if connected. Tightening `allowed_prefixes` drops those checkouts from interest and leaves the QUIC session up. The slave is not sent `SubscribeReject`. Later `RootReport`s for those ids return `not_subscribed`.

## Environment

```
ARBORSYNC_CONFIG=/path/to.toml
ARBORSYNC_LOG_LEVEL=debug
```

No `ARBORSYNC_*` for keys or key paths.

## Permissions

Config and key files are `0600`, owner = the daemon user. `write_static_key` sets `0600`. `LoadedMaster::load` / `LoadedSlave::load` reject any other config mode (`ConfigError::InsecureMode`). `parse` does not check mode. The process does not need root if it can read the tree, bind the UDP port, and write `db_path`.

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

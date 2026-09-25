# Subscriptions and Mappings

Normative source: `spec.md` §2–§4 and §11.

## Mapping

Restricted slave (`dev-alice`, ACL `/src` + `/docs`):

```toml
checkouts = [
    { id = "src",  central = "/src",  local = "/opt/projects/src" },
    { id = "docs", central = "/docs", local = "/opt/projects/docs" },
]
```

Same-slave central overlap (`backup-1`, ACL `/`):

```toml
checkouts = [
    { id = "src", central = "/src", local = "/opt/src" },
    { id = "bak", central = "/",    local = "/backup/central" },
]
```

| Field | Meaning |
|---|---|
| `id` | Stable string. Unique on that slave. Index prefix. Not reused for a different `(central, local)` without a remove + add. |
| `central` | Canonical prefix (`spec.md` §1). `/` is the full tree. |
| `local` | Absolute host path. `canonicalize` (symlink resolve) runs at `Slave::open`, not at TOML parse. |

The slave does **not** send `local` to the master. `Subscribe` carries `{ id, central }` only.

## Overlap

- **Local:** forbidden. Reject config if two locals are equal or one is a parent of the other after comparing path components. `/opt/a` and `/opt/a/b` overlap. `/opt/a` and `/opt/ab` do not. Spec §3 wants this check after symlink-resolved canonicalize. As built, parse compares the tilde-expanded path before `open` resolves symlinks.
- **Central:** allowed, including on the same slave. `/src` + `/` is the backup-plus-subset pattern. Inner mappings do **not** “win.” Each checkout is an independent replica of its prefix. The same canonical file may exist as two local files; apply is per `checkout_id`.
- **Across slaves:** any number of slaves may map the same `central`.

“Circular mappings” are not a thing in a star topology. Do not check for them.

## Interest

A checkout is interested in canonical path `P` iff `central` is a prefix of `P`: `P == central` or `P` starts with `central + "/"`. `/` matches all. `/src` does not match `/src2`.

This is the opposite of “self or deeper mapping paths.” A parent mapping (`/src`) **does** receive `/src/project1/file.txt`. A child mapping (`/src/project1/subdir`) does **not**.

Master keeps an in-memory trie of `(slave_id, checkout_id, central)` for connected slaves only.

## Subscribe

1. Slave completes Noise XX. Master now has the slave’s static public key and the ACL row (`spec.md` §4).
2. Slave opens the control stream, sends framed `Subscribe { slave_id, checkouts }`.
3. `slave_id` must match the ACL row for that key. Each `central` must sit under at least one `allowed_prefixes` entry (same prefix rule). Count must be ≤ `max_checkouts_per_slave` (same name and default on both sides: 100).
4. Central paths need not exist yet. Pre-subscribe is allowed; creates under the ACL succeed later.
5. Success: `SubscribeAck` with master’s current `DirNode` (or `FileNode`) for each `central`. Failure: `SubscribeReject` with `denied_centrals` and a reason. Specified: the slave logs and does not retry those prefixes until config or ACL changes. As built: `Slave::handle` hangs up and the binary reconnects with the same `Subscribe`.
6. A second `Subscribe` on the same connection **replaces** the set. Removed ids are forgotten on the master; the slave drops those index prefixes. Added ids start reconcile.

`Subscribe` is not authenticated by a shared PSK. The key *is* the identity. There is no separate “path authorization” mechanism beyond `allowed_prefixes`.

## Lifecycle

- Disconnect: drop that slave’s in-memory interest. No payload queue.
- Reconnect: new XX, `Subscribe`, `RootReport` / walk (`spec.md` §10).
- Same `slave_id` already connected: the new session replaces the old.
- Config watch: add/remove checkouts as above. Changing `id` or `central` is remove + add.

## Path safety

- Reject `central` that is not absolute, that contains `.` / `..` components after normalization, or that is not valid UTF-8.
- After normalization, `central` must start with `/`.
- Master never writes outside `central_root`. Slave never writes outside `local`. `confine_host` canonicalizes each existing ancestor and rejects a symlink that leaves the root. `canonical_to_host` remains the lexical join.

## Examples

`dev-alice` (ACL `allowed_prefixes = ["/src", "/docs"]`) may map any path under those prefixes. `{ id = "root", central = "/", local = "/home/dev/all" }` is rejected.

`backup-1` (ACL `/`) may map `/` and any subset at once; see the overlap snippet above. A full-replica-only variant is just:

```toml
checkouts = [
    { id = "bak", central = "/", local = "/backup/central" },
]
```

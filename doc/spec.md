**Full Specification: ArborSync – Selective Subtree File Synchronization Daemon**

**Version:** 2.0 (September 2026)

**Normative.** This file is the source of truth. Start with [`overview.md`](overview.md) for the mental model. Topic documents under `doc/` expand procedures. They must not add requirements that contradict this file.

**Purpose:** A lightweight, bidirectional, central-master file sync daemon:

- one central hierarchy on the master;
- arbitrary subtree checkouts to arbitrary local paths on slaves;
- path-Merkle indexing (mode, mtime, size, content hash);
- rsync-style block deltas via `copia`;
- filesystem watching plus periodic full metadata rescans;
- mutually authenticated QUIC (Noise XX, per-slave static keys);
- prefix ACLs per slave;
- Unix permissions preserved (no UID/GID);
- compare-and-swap on content hash; the loser is kept, never overwritten.

**Name:** ArborSync (crate / binary: `arborsync`)

```
arborsync master [--config /etc/arborsync/master.toml]
arborsync slave  [--config ~/.config/arborsync/slave.toml]
arborsync keygen [--out PATH]
```

Config is TOML only.

---

### 0. Non-goals (v1)

- Multi-master, master-of-masters, sharded registries, or a networked SQL backend.
- A second embedded database (`sled`) or `--db-backend` switch.
- Shared cluster PSK as the authenticator; Noise PSK modifier; QUIC 0-RTT (mutating RPCs must not use 0-RTT even if a crate later grows it).
- TCP fallback, UID/GID, xattrs, ACLs, BSD flags, hardlink preservation.
- Metrics HTTP port, config templating / include files, file-type push priority.
- Rejecting updates by wall-clock age.

---

### 1. Core requirements

- Master holds one filesystem hierarchy bound at `central_root` (example host path: `/central`) and one global index.
- **Canonical paths** are absolute paths in the *logical* hierarchy, never including `central_root`. The hierarchy root is `/`. A file the OS sees as `/central/src/foo.rs` has canonical path `/src/foo.rs`. This string is the only path that appears in the index, the Merkle tree, and the wire protocol.
- A **checkout** is `{ id, central, local }`:
  - `id`: stable UTF-8 string, unique among that slave’s checkouts. Not a recycled integer.
  - `central`: canonical prefix (e.g. `/src`, or `/` for a full replica).
  - `local`: absolute host path on the slave.
- **Local paths on one slave must not overlap** (neither `/a` and `/a/b` nor identical paths). **Central prefixes on one slave may overlap** (e.g. `/src` → `/opt/src` and `/` → `/backup/central` on the same host).
- Each checkout has its own index (metadata, directory hashes, last-synced hashes), updated by that checkout’s watcher and rescan.
- Bidirectional: a change in any checkout CAS-applies on the master, then fans out to every other interested checkout (including another checkout on the originating slave).
- Transport: QUIC + Noise `XX` with persisted X25519 static keys. Master authorizes the peer key against a slave ACL and restricts that slave to configured central prefixes.
- Metadata: Unix mode (type + perms + setuid/setgid/sticky), mtime, size. Ignore UID/GID.
- Conflicts: CAS on content hash. Live path always converges to the CAS winner. The loser’s bytes are written under a reserved, unsynced sidecar. mtime is preserved metadata, never an arbiter.
- Daemonized, Rust-only, no extra processes.

---

### 2. Architecture

**Master** (`arborsync master`)

- Watches `central_root`.
- Maintains the global path-Merkle index (no checkout prefix).
- Listens on a QUIC/UDP endpoint.
- Authenticates slaves by static public key; enforces prefix ACL.
- Accepts `Subscribe` (checkout id + central prefix only; local paths stay on the slave).
- On local change or accepted slave CAS: write the tree, recompute affected directory hashes, push to interested checkouts. Do not queue payloads for disconnected slaves.

**Slave** (`arborsync slave`)

- Reads TOML. Validates no local-path overlap before connecting.
- One QUIC connection to the master. Identifies as `slave_id`, bound to its static key.
- Per-checkout watchers and indexes (keys prefixed by checkout id).
- Translates canonical ↔ local using that checkout’s mapping.
- Drives reconcile after subscribe, every rescan interval, and on reconnect.

**Shared library** (`arborsync-core`): types, framing, path-Merkle, storage trait, delta helpers, transport.

**Interest rule** (single definition): a checkout is interested in a canonical path `P` iff `checkout.central` is a prefix of `P`. Prefix means `P == central` or `P` starts with `central` + `/`. `/` matches everything. Sibling `/src` does not match `/src2`.

**Data-flow example**

1. Slave `dev-alice` (ACL `/src`, `/docs`) has checkout `src` (`/src` → `/opt/projects/src`).
2. Slave `backup-1` (ACL `/`) has checkout `src` (`/src` → `/opt/src`) and checkout `bak` (`/` → `/backup/central`) — same-slave central overlap.
3. `/opt/projects/src/foo.rs` changes on alice/`src`. Alice updates that checkout’s index and sends `FileAnnounce { checkout_id = "src", ... }`.
4. Master CAS-applies onto `central_root` + global index, `CasAccept`s alice/`src`, then announces to every other interested checkout. `backup-1`/`src` and `backup-1`/`bak` both apply. alice/`src` does not.

---

### 3. Checkout identity and overlap

Restricted slave (`dev-alice`, ACL `/src` + `/docs`):

```toml
checkouts = [
    { id = "src",  central = "/src",  local = "/opt/projects/src" },
    { id = "docs", central = "/docs", local = "/opt/projects/docs" },
]
```

Same-slave central overlap requires `allowed_prefixes = ["/"]` (e.g. `backup-1`):

```toml
checkouts = [
    { id = "src", central = "/src", local = "/opt/src" },
    { id = "bak", central = "/",    local = "/backup/central" },
]
```

- Changing `id` or `central` is a remove + add: drop that id’s index rows; leave files on disk.
- Removing a checkout unsubscribes it and drops its index; files on disk are untouched.
- Adding a checkout subscribes and runs initial reconcile (slave root empty or leftover files vs master).
- Config file is watched; those add/remove rules apply at runtime.

**Overlap test (local):** two absolute paths overlap iff they are equal or one is a parent of the other after canonicalization (symlink-resolved at config load only). Reject the config.

**Overlap (central):** allowed. Master emits one announce per interested `(slave_id, checkout_id)`.

---

### 4. Identity and prefix ACL

Every node has a persisted X25519 static keypair (`arborsync keygen`).
`master` and `slave` log the public pin at startup after they read the secret file.
The pin is `hex:` plus 64 hex digits, the same form `keygen` prints to stdout.

- Slave config: `slave_id`, `slave_key_path`, `master_public_keys` (one or more pins, for master-key rotation).
- Master config: `master_key_path` and a list of slaves:

```toml
[[slaves]]
id = "dev-alice"
public_keys = ["hex:…"]          # current, plus previous during rotation
allowed_prefixes = ["/src", "/docs"]
```

Handshake: Noise `XX_25519_ChaChaPoly_BLAKE2s` via `quinn-hyphae`. After XX, each side has the peer’s static public key.

- Slave disconnects if the master’s key is not in `master_public_keys`.
- Master looks up the peer key in `[[slaves]]`. Unknown key: disconnect. `slave_id` in `Subscribe` must match that ACL row.
- Every `Subscribe` checkout `central` must be under at least one `allowed_prefixes` entry (same prefix rule as interest). Otherwise `SubscribeReject`.
- Announces, deletes, and mkdirs from a slave for a path outside its ACL are rejected.
- `allowed_prefixes = ["/"]` is a full-replica grant.
- One live connection per `slave_id`. A new session with a valid key for that id replaces the old connection.

**Rotation**

- Slave key: add the new public key to `public_keys`, reload master, switch the slave key, then drop the old key.
- Master key: add the new public key to every slave’s `master_public_keys` first, rotate the master key, then remove the old pin.

**Reloadable** without restart: log level, rate limits, ACL rows (add/remove slaves, prefixes, extra public keys).  
**Not reloadable:** `listen_addr`, `db_path`, `central_root`, key *paths*, `master_addr`.

---

### 5. Path Merkle

One tree per index (master global; one per slave checkout). This is a **directory hash tree**, not a flat `rs_merkle` leaf list. Do not use `rs_merkle`.

**Kinds:** `File = 1`, `Dir = 2`, `Symlink = 3`.

**File / symlink node** (the CAS object for a non-dir):

```
FileNode = BLAKE3(
    u8 kind
    || content_hash          # 32 bytes
    || u64be size
    || i64be mtime_ns        # Unix nanoseconds; negative allowed
    || u32be mode            # st_mode including type bits
)
```

- File `content_hash` = BLAKE3 of raw file bytes.
- Symlink `content_hash` = BLAKE3 of the target bytes as returned by the OS (no extra NUL). `size` is the target length.

**Directory node:**

```
# each child, names sorted as raw UTF-8 bytes:
entry = u8 kind || u32be name_len || name || 32-byte child_FileNode_or_DirNode
DirNode = BLAKE3(concat(entries))
```

The empty directory hash is `BLAKE3("")` (no entries). Directories are first-class: an empty dir is a node.

**Path is location, not payload.** `/src/foo.rs` is the walk `/` → `src` → `foo.rs`. Do not hash the path into the node.

**Subtree root** of a checkout is `DirNode(central)` if `central` is a directory, or the `FileNode` if someone maps a single file (allowed). Master caches every directory’s `DirNode` in the DB.

**Reconcile is a tree walk, not an inclusion proof.** If two roots differ, exchange that directory’s child list `(name, kind, node_hash)`, recurse where hashes differ, then transfer or delete only those files. Complexity follows the size of the symmetric difference, not a binary-tree proof.

Insert and delete change only ancestor `DirNode`s. No global leaf-index shift.

---

### 6. Metadata

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileMetadata {
    pub kind: EntryKind,       // File | Dir | Symlink
    pub size: u64,
    pub mtime_ns: i64,
    pub mode: u32,
    pub content_hash: [u8; 32],
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum EntryKind { File = 1, Dir = 2, Symlink = 3 }
```

`FileNode` / `DirNode` are computed from this plus, for dirs, the child list.

**Collection:** `std::fs::metadata` + streaming BLAKE3. Hash only when size or mtime differs from the index, or the row is missing (rescan). Watcher events always re-read metadata; hash files when kind is File and content may have changed (Write/Create), and always re-hash on size/mtime mismatch.

**Mode:** preserve type bits, `rwx` for ugo, setuid/setgid/sticky. Never read or write UID/GID. Apply file/dir perms with `std::fs::set_permissions` using `mode & 0o7777`. Set mtime with the `filetime` crate (not `std::os::unix::fs::FileExt` — that API is `read_at`/`write_at`).

**Symlinks:** resolve *only* the checkout `local` and master’s `central_root` at config load. In-tree symlinks are first-class: store the target, do not follow. Following would escape the checkout and duplicate leaves.

**Skipped:** device files, sockets, FIFOs. Log and continue. Hardlinks are two independent paths (same bytes, two index rows).

**Reserved names** at a checkout root (and at `central_root`): `.arborsync-tmp`, `.arborsync-conflicts`. Not indexed, not watched, not synced.

---

### 7. Change detection

- File watches use `notify` 8 + `notify-debouncer-full` 0.5, recursive, debounce default 200 ms (configurable 200–500). Config reload stays on `notify-debouncer-mini`.
- Master: one watch on `central_root`.
- Slave: one watch per checkout `local`.
- Events: Create, Write, Remove, Rename, chmod/mtime (`Modify(Metadata)`).
- **Rename:** if both `from` and `to` land in the same debounce window *and* both are inside the same checkout, announce `Rename`. Otherwise treat as Delete + Create.
- **Rescan every 60 s** (configurable): full `stat` walk of the checkout (or `central_root`). Not “changed subtrees only” — the rescan exists because events were missed. Hash only on size/mtime mismatch or missing row. Then reconcile with master.
- **Echo suppression:** each side keeps `inflight: (checkout_id, canonical_path) → expected content_hash`. A watcher event whose current content hash equals `inflight` is ignored and the entry cleared. After apply, set the announced mtime *before* releasing inflight, so the write does not look like a new local edit.
- Never push a path whose local `FileNode` equals `last_synced`.

---

### 8. CAS, apply, and conflicts

Master is the replica of record. The **winner** is whatever CAS commits on the master. Every live path converges to that. The **loser** is preserved.

**Version identity:** `FileNode` (content + meta).  
**Conflict** (sidecar) only when **content_hash** differs. Meta-only CAS failure: adopt the winner’s metadata, no sidecar.

**Slave → master announce**

```
FileAnnounce { checkout_id, path, new: FileMetadata, basis: Option<FileNode> }
```

`checkout_id` is the announcing checkout (slave → master) or the target checkout (master → slave). Master skips fan-out to the pair that just committed; it does not put origin fields on the wire.

- Create: `basis = None`; master accepts if the path is absent.
- Update: master accepts if current `FileNode == basis`.
- Success: master writes the file under `central_root` (atomic apply), updates index, recomputes ancestor `DirNode`s, replies `CasAccept { checkout_id, path, file_node: Some(...) }`, fans out one `FileAnnounce` per other interested checkout. The origin slave sets `last_synced` from `CasAccept`.
- Failure: `CasReject { checkout_id, path, current }`. Slave writes its local bytes to the sidecar (if content differs), then pulls `current` onto the live path and sets `last_synced`.
- `kind = Dir`: announce only, no bulk stream. `kind = Symlink`: bulk body is the target bytes (`Whole`).

**Delete**

```
Delete { checkout_id, path, basis: FileNode }
```

Master accepts iff current `FileNode == basis`, then removes the path, replies `CasAccept { checkout_id, path, file_node: None }`, fans out `Delete` with that basis.

Replica delete on a slave: delete the live path only if local `FileNode == last_synced` (or `==` announced basis). If local content differs, sidecar the local bytes, then delete the live path (master won).

**Master → slave announce** (watcher or fan-out)

Same `FileAnnounce` / `Delete` with `origin` = master (no checkout) and `basis` = the pre-change `FileNode` (or `None` for create). Slave:

| Local state | Action |
|---|---|
| absent, create | apply |
| `FileNode == basis` | apply |
| `FileNode == new` | no-op, refresh `last_synced` |
| content_hash differs | sidecar local, apply incoming |
| content_hash same, meta differs | apply incoming meta |

**Atomic apply:** write to `{checkout_local}/.arborsync-tmp/<unique>` (same filesystem), `fsync`, `rename` over the target, then update the index. Never patch in place. Directories: `create_dir_all` with mode; deletes are children-first. Corrupt delta → request `WholeFile` once; still fail → leave last good live file, log, wait for next reconcile.

**Sidecar path:** `{checkout_local}/.arborsync-conflicts/{canonical-relative}--{content_hash_hex[0..16]}`. Create parent dirs as needed. Never place conflict files beside the original under a syncable name.

**Type change** (file ↔ dir ↔ symlink): Delete + Create in one master transaction, each CAS-guarded.

**Same-slave central overlap:** apply per checkout independently. Origin checkout is skipped on fan-out; the other local copy is updated via announce, not by copying locally out-of-band.

There is no `latest-wins` / `local-wins` / `manual` policy knob and no `max_update_age_seconds`.

---

### 9. Content transfer (`copia`)

Recipient-driven. Never compute a forward delta against a cached snapshot of the other side.

1. After a successful CAS decision (or a pull the slave already knows it wants), the **recipient** of bytes sends `SignatureRequest { checkout_id, path, want_hash, signature }` where `signature` is `copia`’s signature of the local basis, or empty if there is no basis / size < 4 KiB / first create / symlink / `encode_control` of that request would exceed 1 MiB. An empty signature means the sender must use `encoding = Whole`.
2. Sender replies on a **bulk stream**: raw file bytes (`Whole`), symlink target bytes (`Whole`), or a `copia` delta (`Delta`). Directories have no bulk transfer.
3. Recipient verifies BLAKE3 == `want_hash` before rename (`want_hash` is `content_hash`, not `FileNode`).

`copia` is the delta engine only. Do not use its hub/bisync CLI protocol.

---

### 10. Reconcile (slave-driven)

After `SubscribeAck`, every rescan interval, and on reconnect:

1. Slave sends `RootReport { checkout_id, path: central, root }` for each checkout. The `SubscribeAck` report uses the current index and does not wait for a still-running rescan walk. Names the walk indexes in that session are announced as they are found.
2. Master replies `RootAck { matched, master_root }`.
3. On mismatch (or empty slave), slave walks:
   - `DirListRequest` / `DirListResponse` for the directory (`name`, `kind`, `node_hash`). A listing that would exceed the 1 MiB control frame is split. `DirListResponse.more` means later names remain. The slave must not treat those unsent names as absent. It continues with `DirListRequest.after` set to the last name on the page.
   - Name only on master → pull create (or `Mkdir`).
   - Name only on slave → if `last_synced` absent, `FileAnnounce` create; if `last_synced` present and local == it, `Delete`; if local differs, announce CAS (slave thinks it changed) or, if master deleted, slave will `CasReject` and follow §8. After a create or CAS announce of a directory, the slave also sends `DirListRequest` for that path. The master lists the directory after it applies the announce. That walk covers nested leftover files in the same session.
   - Both present, hashes differ → recurse if dir; if file, 3-way on `last_synced`:
     - local == last_synced, master != last_synced → pull;
     - local != last_synced, master == last_synced → announce CAS;
     - both differ and local != master → announce CAS; expect `CasReject` or win; §8 handles the loser.

Initial populate is this walk with `last_synced` empty.

Master does **not** buffer updates for offline slaves. Reconnect + reconcile is the only catch-up.

---

### 11. Wire protocol

**Framing.** Quinn streams are byte streams. Every control message is:

```
u32be length || bincode(Envelope)
Envelope { version: u16 = 1, msg: ProtocolMessage }
```

One long-lived **control stream** (opened by the slave after handshake). **Bulk streams** are one transfer each: a framed `BulkHeader`, then exactly `size` raw bytes. Do not put file bodies inside bincode.

Handshake preamble / first Noise payload: ASCII `arborsync-v1`. Hyphae has no ALPN; this is the version pin. Mismatch → disconnect.

```rust
pub struct CheckoutRef { pub id: String, pub central: String }
pub struct CheckoutAck { pub id: String, pub central: String, pub master_root: [u8; 32] }

pub enum ProtocolMessage {
    Subscribe { slave_id: String, checkouts: Vec<CheckoutRef> },
    SubscribeAck { checkouts: Vec<CheckoutAck> }, // id, central, master_root
    SubscribeReject { reason: String, denied_centrals: Vec<String> },

    RootReport { checkout_id: String, path: String, root: [u8; 32] },
    RootAck { checkout_id: String, path: String, matched: bool, master_root: [u8; 32] },
    DirListRequest { checkout_id: String, path: String, after: Option<String> },
    DirListResponse {
        checkout_id: String,
        path: String,
        after: Option<String>,
        entries: Vec<DirEntry>,
        more: bool,
    },

    FileAnnounce {
        checkout_id: String,           // origin (slave→master) or target (master→slave)
        path: String,
        new: FileMetadata,
        basis: Option<[u8; 32]>,       // FileNode; None = create
    },
    Delete {
        checkout_id: String,
        path: String,
        basis: [u8; 32],
    },
    Rename {
        checkout_id: String,
        from: String,
        to: String,
        from_basis: [u8; 32],
        to_new: FileMetadata,
    },
    CasAccept { checkout_id: String, path: String, file_node: Option<[u8; 32]> }, // None = delete
    CasReject { checkout_id: String, path: String, current: Option<FileMetadata> },

    SignatureRequest { checkout_id: String, path: String, want_hash: [u8; 32], signature: Vec<u8> },
    Error { code: String, message: String },
    Disconnect { reason: String },
}

pub struct DirEntry { pub name: String, pub kind: EntryKind, pub node_hash: [u8; 32] }

pub struct BulkHeader {
    pub path: String,
    pub checkout_id: String,
    pub want_hash: [u8; 32],
    pub encoding: BulkEncoding, // Whole = 1, Delta = 2
    pub size: u64,
}
```

Paths in every message are canonical. `checkout_id` is required on slave-scoped messages so a slave with central overlap can route to the right local tree. Keep-alive is QUIC’s; no application `Heartbeat`.

---

### 12. Transport

- Library: `quinn` 0.11 + `quinn-hyphae` 0.1 (Noise XX + static keys). The hyphae **PSK modifier and QUIC 0-RTT are not implemented** — do not specify them.
- Pattern: `Noise_XX_25519_ChaChaPoly_BLAKE2s`.
- Listen: UDP, default `0.0.0.0:8443`.
- Slave verifies master static key; master maps slave static key → ACL.
- Unknown keys and ACL misses are disconnects, rate-limited per source IP.
- Reconnect: exponential backoff, then `Subscribe` + reconcile. No 0-RTT, no replay of announces.
- `Transport` is a live session after handshake. It exposes the peer static key, one control stream pair, on-demand bulk transfers, and `close`.
- `impl Transport for quinn::Connection` is the QUIC path.
- `MemoryTransport::pair` is the in-memory test impl.
- The v1 production path is QUIC only. There is no TCP path.

---

### 13. Storage

**redb only.** No backend switch.

```rust
pub struct CheckoutId(pub String); // master: CheckoutId("")

pub trait Storage: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    fn open(path: &std::path::Path) -> Result<Self, Self::Error> where Self: Sized;

    fn get_meta(&self, ck: &CheckoutId, path: &str) -> Result<Option<FileMetadata>, Self::Error>;
    fn get_dir_node(&self, ck: &CheckoutId, path: &str) -> Result<Option<[u8; 32]>, Self::Error>;
    fn get_last_synced(&self, ck: &CheckoutId, path: &str) -> Result<Option<[u8; 32]>, Self::Error>;
    fn range_meta(&self, ck: &CheckoutId, prefix: &str)
        -> Result<Vec<(String, FileMetadata)>, Self::Error>;
    fn range_dir_nodes(&self, ck: &CheckoutId, prefix: &str)
        -> Result<Vec<(String, [u8; 32])>, Self::Error>;

    fn begin_write(&self) -> Result<WriteBatch<'_>, Self::Error>;
    fn delete_checkout(&self, ck: &CheckoutId) -> Result<(), Self::Error>;
}

pub trait WriteBatch {
    fn put_meta(&mut self, ck: &CheckoutId, path: &str, meta: &FileMetadata) -> Result<(), Error>;
    fn del_meta(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Error>;
    fn del_meta_prefix(&mut self, ck: &CheckoutId, prefix: &str) -> Result<(), Error>;
    fn put_dir_node(&mut self, ck: &CheckoutId, path: &str, node: [u8; 32]) -> Result<(), Error>;
    fn del_dir_node(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Error>;
    fn del_dir_prefix(&mut self, ck: &CheckoutId, prefix: &str) -> Result<(), Error>;
    fn put_last_synced(&mut self, ck: &CheckoutId, path: &str, file_node: [u8; 32]) -> Result<(), Error>;
    fn del_last_synced(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Error>;
    fn commit(self) -> Result<(), Error>;
}
```

A metadata change and its ancestor `DirNode` updates and `last_synced` write commit in **one** batch. The earlier sketch with `get`/`put` outside `transaction()` is not the API.

**Tables:** `meta` (ck, path) → `FileMetadata`; `dir_nodes` (ck, path) → `[u8;32]`; `last_synced` (ck, path) → `FileNode`. Master `ck` is the empty string.

---

### 14. Configuration (summary)

**Master** `/etc/arborsync/master.toml`

```toml
central_root = "/central"
listen_addr = "0.0.0.0:8443"
master_key_path = "/etc/arborsync/master.key"
db_path = "/var/lib/arborsync/index.redb"
log_level = "info"
watcher_debounce_ms = 200
rescan_interval_seconds = 60
status_interval_seconds = 5
max_checkouts_per_slave = 100
max_connections = 100
max_connection_attempts_per_minute = 60

[[slaves]]
id = "dev-alice"
public_keys = ["hex:32-byte-x25519-public"]
allowed_prefixes = ["/src", "/docs"]

[[slaves]]
id = "backup-1"
public_keys = ["hex:32-byte-x25519-public"]
allowed_prefixes = ["/"]
```

**Slave** `~/.config/arborsync/slave.toml`

```toml
slave_id = "dev-alice"
master_addr = "master.example.com:8443"
slave_key_path = "~/.config/arborsync/slave.key"
master_public_keys = ["hex:32-byte-master-public"]
db_path = "/var/cache/arborsync/cache.redb"
log_level = "info"
max_checkouts_per_slave = 100
watcher_debounce_ms = 200
rescan_interval_seconds = 60
status_interval_seconds = 5

checkouts = [
    { id = "src",  central = "/src",  local = "/opt/projects/src" },
    { id = "docs", central = "/docs", local = "/opt/projects/docs" },
]
```

A `/` checkout on `dev-alice` is `SubscribeReject`ed. Full-replica / overlap checkouts belong on a slave whose ACL includes `/` (see `backup-1` in §3).

Both sides enforce the same `max_checkouts_per_slave`. Config files are `0600` (they hold key *paths*, and slaves hold master pins). Private key files are `0600`.

CLI overrides: `--config PATH`. Env: `ARBORSYNC_CONFIG`, `ARBORSYNC_LOG_LEVEL`. Do not put key material in the environment.

---

### 15. Workspace and dependencies

```
arborsync/
├── Cargo.toml          # workspace, binary `arborsync`
├── flake.nix           # package + NixOS module
├── flake.lock
├── nix/                # module.nix, eval fixture
├── core/               # arborsync-core
└── src/                # master / slave / keygen subcommands
```

```toml
[dependencies]
arborsync-core = { path = "core" }
tokio = { version = "1", features = ["full"] }
quinn = { version = "0.11", default-features = false, features = ["runtime-tokio"] }
quinn-hyphae = "0.1.0-beta.0"
copia = "0.3"
notify = "8"
notify-debouncer-full = "0.5"
notify-debouncer-mini = "0.5"
redb = "2"
bincode = "2"
serde = { version = "1", features = ["derive"] }
blake3 = "1"
time = { version = "0.3", features = ["serde"] }
filetime = "0.2"
clap = { version = "4", features = ["derive"] }
log = "0.4"
env_logger = "0.11"
thiserror = "2"
anyhow = "1"
```

Quinn is `runtime-tokio` only. Hyphae supplies crypto. See [`quic-transport.md`](quic-transport.md).

---

### 16. Implementation status

Status icons: ✅ built · ⚠️ partial · ❌ open · ➖ struck.

Items 1–7 below are in `arborsync-core` and the `master`, `slave`, and `keygen` binaries.

1. ✅ `core`: `FileMetadata`, path-Merkle encode/hash, `Storage` + redb, frame codec, canonical-path helpers, reserved-name filter, local-overlap check.
2. ✅ `keygen` + config parse/validate (ACL, pins, checkouts).
3. ✅ Master: watch `central_root`, index, QUIC XX accept, ACL, Subscribe, CAS apply to disk, fan-out, log public pin at startup.
4. ✅ Slave: connect, pin check, Subscribe, per-checkout watch, announce, apply, sidecar, log public pin at startup.
5. ✅ Bulk `copia` streams; whole-file fallback. Apply reconstructs in memory, then writes the full buffer through `.arborsync-tmp`.
6. ✅ Reconcile walk + rescan + reconnect.
7. ✅ Config watch for checkout add/remove; SIGHUP ACL/log/status-interval reload.
8. ✅ Tests: `core/tests/scenarios.rs` covers reserved dirs, two checkouts on one slave (`/src` and `/`), CAS conflict, echo suppression, ACL deny, and rescan-as-missed-watcher. Those tests call `handle`, `note_local`, and `rescan` on `MemoryStorage`. `src/watch.rs` starts a real `notify-debouncer-full` thread and asserts a FileAnnounce after a post-arm write. `tests/sync.rs` starts master and slave over QUIC and asserts a post-connect write crosses.

**✅ Framing and storage.** `decode_control` reads `Envelope.version`, then `ProtocolMessage`. An unknown version is `FrameError::UnsupportedVersion`, including a v2 variant index under version 2. On-disk `FileMetadata` is `u16le META_SCHEMA_VERSION || bincode` with its own `meta_bincode_config`. Wire frames use `wire_bincode_config`. Both configs are `bincode::config::standard()` today. The schema prefix is what stops a wire change from silently reinterpreting stored rows.

**Against this spec**

| | Requirement | As built |
|---|---|---|
| ✅ | §3 overlap after symlink-resolved canonicalize | Parse checks tilde-expanded paths and `canonicalize`s a local that already exists. `Slave::open` and checkout-add reload `canonicalize` again and reject `LocalOverlap` on the resolved paths. |
| ✅ | §4 startup public pin | After `read_static_key`, `master` and `slave` log `hex:` plus 64 hex digits. Same form as `keygen` stdout. |
| ✅ | §4 / §14 config files `0600` | `LoadedMaster::load` / `LoadedSlave::load` reject a file whose mode is not `0600` (`ConfigError::InsecureMode`). `parse` does not check mode. The NixOS unit copies `services.arborsync.master.configFile` to `/run/arborsync/master.toml` with mode `0600` and passes that path to `--config`. |
| ✅ | §4 / §12 unknown-key rate limit | `AttemptLimiter::limited` drops the accept before XX. After XX, unknown key records `allow` and closes. `slave_id` mismatch is `Reply::Hangup` (the binary also `allow`s). Prefix deny stays `SubscribeReject`. |
| ✅ | §6 skip device, socket, FIFO | `collect_from_path` / `collect_for_rescan` return `Ok(None)` and log a warn for device, socket, FIFO, and `PermissionDenied`. The walk continues. |
| ✅ | §6 / §7 hash only on size/mtime miss | `collect_for_rescan` reuses `content_hash` when kind, size, and mtime match. Mode is not a miss. The returned row carries the fresh mode. Kind is a reuse guard. Rescan walks, Create, Write, Metadata, and same-window Rename use it. A miss falls through to `collect_from_path`. |
| ✅ | §7 event kinds and same-window `Rename` | `notify-debouncer-full` 0.5 keeps Create, Write, Remove, Rename, and `Modify(Metadata)`. Same-window same-checkout rename is one `ProtocolMessage::Rename`. Unpaired or cross-checkout rename stays Delete plus Create. Apply is `fs::rename` plus index update, not a bulk copy. |
| ✅ | §7 inflight before apply | Armed before the live `rename` / `mkdir` / meta apply. Files, symlinks, dirs (`ContentHash::ZERO`), and meta-only apply all arm. |
| ✅ | §8 directory CAS | Live directories CAS on `FileNode` like files (create, meta update, delete). `last_synced` stores that `FileNode`. Master-local dir edits still `commit` as replica of record, same as files. Mode or mtime `PermissionDenied` on an existing directory is a warn, not a session error. |
| ✅ | §8 type change in one master transaction | One `FileAnnounce`. CAS on the old `FileNode`. Delete then create in the accept. One `commit_leaf`. One `CasAccept`. Fan-out `FileAnnounce` with previous `FileNode` as basis. |
| ✅ | §8 sidecar only for the content-hash loser | Slave announce apply, `CasReject`, and incoming `Delete` use `sidecar_if_content_differs`. Delete compares live `content_hash` with the last-synced content hash. Master `publish` does not sidecar a successful replace. |
| ✅ | §8 children-first directory delete | `remove_live` removes each child, then `remove_dir`. Files and symlinks use `remove_file`. |
| ✅ | §9 patch from the live file into tmp | Specified as built. Spec §8 says never patch in place. `reconstruct` patches in RAM, then `atomic_put` writes the whole buffer. |
| ✅ | §9 / §11 signature fits the control frame | `signature_request` omits the `copia` signature when `encode_control` would exceed 1 MiB. The recipient then asks for `Whole`. An oversized `SignatureRequest` used to fail `encode_control` and drop the session. |
| ✅ | §10 / §11 DirList fits the control frame | `page_dir_list` splits a directory listing so each `DirListResponse` prefers `MAX_DIR_LIST_PAYLOAD` (`MAX_CONTROL_FRAME / 4`). One child that exceeds that still goes if `encode_control` succeeds. `more` keeps the slave from treating unsent names as master-absent. An unpaged 50k-file listing used to fail `encode_control` and drop the session. |
| ✅ | §11 bulk read is not cancelled by `select!` | Master and slave `accept_uni` inside `select!`, then `read_bulk` after that arm wins. Cancelling `accept_bulk` mid-body dropped the `RecvStream` and Quinn sent `STOP_SENDING` 0. |
| ✅ | §11 control read keeps bytes across `select!` | `ControlReader.pending` retains bytes that a cancelled `read_control` already copied. Quinn `read` is cancel-safe. A cancelled `read_exact` used to treat leftover path bytes as the next length prefix. |
| ✅ | §10 `SubscribeReject` | Slave stores `denied_centrals`. `subscribe()` omits those centrals. On `SubscribeReject`, insert, log, and `Reply::Send(vec![subscribe()])` if any checkout remains, else `Reply::Hangup`. Cleared on a checkout or pin reload. Master prefix-deny stays `SubscribeReject`. |
| ✅ | §10 slave-only directory walk | `on_dir_list` follows `AnnounceCreate` / `AnnounceCas` of a directory with `DirListRequest` for that path. Nested leftover files are announced in the same session. A walk that only announced the directory used to stall resume of a tree dropped into the checkout. |
| ✅ | §11 slave control write is not on the session `select!` | The slave binary owns a writer task for the control `SendStream` and a spawned `write_bulk`. The session task keeps `read_control` live. A leftover walk that wrote a whole `Reply::Send` before reading filled the 1.25 MiB stream window with unread `SignatureRequest`s and never opened bulk. One in-flight bulk at a time. Extra inbound `SignatureRequest`s stay parked until that bulk finishes. A second `SignatureRequest` for the same `(checkout_id, path, want_hash)` is dropped. Watcher plus leftover walk used to fulfill the same ask twice. The second bulk hit `unknown_transfer` and held later leftover asks behind a full-file send. `apply_bulk` treats a missing pending row as success when the live bytes already hash to `want_hash`. Inbound `Error` is recorded and not echoed as `unsupported`. Outbound pull `SignatureRequest`s after `CasReject` take a second writer lane that the control task prefers over leftover `FileAnnounce`s. |
| ✅ | §7 / §10 slave crawl yields | `SubscribeAck` rescan and leftover `DirListResponse` pages walk at most 64 names per session turn. A new or changed index row is announced in that turn. `SubscribeAck` sends the current `RootReport` before a long walk finishes. Leftover pages are stepped before a queued rescan. The watch thread queues `request_rescan` and does not drain the walk. `status` and `read_control` stay live. `Slave::rescan` drains the walk for tests. A 300k-file walk that ran inside one `handle` used to mute `status 5s` and stop reading control. Waiting for the whole walk before `RootReport` left leftover files on the slave. |
| ✅ | §11 master control write is not on the session `select!` | `dispatch_master` and `flush_outbox` enqueue onto a writer task. Master `write_bulk` is spawned. The session keeps `read_control` and `accept_uni` live while the control window is full. |
| ✅ | §12 one live connection per `slave_id` | Replacing a session `close`s the previous `Connection` and signals the old task. |
| ✅ | §12 `Transport` trait | `Transport` is a live session. It exposes the peer static key, one control stream pair, on-demand bulk, and `close`. `impl Transport for quinn::Connection` is the QUIC path. The master and slave binaries and `core/tests/transport.rs` call the trait. `MemoryTransport::pair` is the in-memory test impl. Most unit tests still call `handle`. |
| ➖ | §14 `quic_*` / `reconnect_*` | Struck in `doc/configuration.md`. Not struct fields. Reconnect is 1 s, doubling, cap 60 s. |
| ✅ | §11 / §12 keep-alive | Idle timeout stays at the Quinn 30 s default. `listen` and `client_endpoint` set `keep_alive_interval` to 10 s. No application `Heartbeat`. |
| ✅ | Backpressure (`set_writable`) | `flush_outbox` polls the batch and `set_writable(peer, false)` when the batch is longer than 32. The writer task sets `writable` true after that batch is written. `set_writable(false)` still clears the leftover outbox. |
| ✅ | In-flight bulk after disconnect | `Master::disconnect` drops pending rows whose `peer` is the disconnected peer. |
| ✅ | Interval status reports | Master and slave log a `status ` summary each `status_interval_seconds` (default 5, `0` disables, max 3600). Health is `idle`, `busy`, `stuck`, or `failed`. The master adds one line per slave. Reload applies the interval live. No metrics port. |

Topic documents:

| File | Role |
|---|---|
| `overview.md` | Mental model. Not normative. |
| `subscriptions.md` | §3, §4, interest, Subscribe |
| `building-updating-merkle-trees.md` | §5, §10 |
| `collecting-metadata.md` | §6 |
| `indexing.md` | §13 |
| `filesystem-scanning-watching.md` | §7 |
| `applying-updates.md` | §8, §9 |
| `pushing-updates.md` | §8–§10 (master push + fan-out) |
| `quic-transport.md` | §11, §12 |
| `configuration.md` | §14 |

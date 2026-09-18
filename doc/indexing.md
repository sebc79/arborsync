# Indexing and Database

Normative source: `spec.md` §13. redb is the only backend.

## Why redb

Embedded, single-file, crash-safe, MVCC, range queries on path keys. v1 does not ship `sled`, Postgres, or a `--db-backend` flag. There is no zero-downtime backend switch and no export/import story beyond “copy the redb file while the daemon is stopped.”

## Trait

Reads use a consistent snapshot. Writes go through a batch that **must** be able to update metadata, directory nodes, and `last_synced` together. The old sketch with `get`/`put` beside an undefined `transaction()` closure is not the API.

```rust
pub struct CheckoutId(pub String);

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

`CheckoutId("")` is the master’s global tree. Slaves pass the checkout’s stable string id. Do not stringify optional integers and do not use `Option<&str>`.

`range_*` is inclusive of `prefix` itself and every canonical path where `interest(prefix, path)` holds (`spec.md` §2). Implement with a redb range on the concatenated key.

## Key layout

```
key = checkout_id bytes || 0x00 || canonical path UTF-8
```

`0x00` cannot appear in UTF-8, so prefix ranges stay inside one checkout. Tables:

1. `meta` — key → `u16le META_SCHEMA_VERSION || bincode(FileMetadata)` (`META_SCHEMA_VERSION = 1`)
2. `dir_nodes` — key → 32 raw bytes (`DirNode`)
3. `last_synced` — key → 32 raw bytes (`FileNode` of the last CAS-agreed version), or 64 bytes (`FileNode` || `content_hash`) when the agreed content hash is known. A 32-byte row still reads as that `FileNode`. The extra hash is what incoming `Delete` uses to skip a meta-only sidecar.

The schema prefix is why a wire change cannot silently reinterpret stored rows. `wire_bincode_config` and `meta_bincode_config` are separate helpers. Both are `bincode::config::standard()` today.

The Rust trait uses `CanonicalPath`, `FileNode`, and `DirNode`, plus `purge_prefix` and `del_entry`. The listing above is the spec sketch. Call the code for the extra methods.

No `slave_subscriptions` table. Active interest is process memory, rebuilt from `Subscribe` after connect. Persisting it would go stale on crash; the slave always resubscribes.

## Transactions

Specified: one batch per debounce window, per accepted CAS, or per reconcile directory. Slave `rescan` uses one `commit_leaves` batch for every create, update, and local delete the walk found. Watcher events and apply still use one `commit_leaf` per path. The master keeps a `DirChildren` map so each later `commit_leaf` recomputes a directory from loaded children instead of `range_meta` of every descendant. Origin `CasAccept` writes `last_synced` in a following batch. Order inside a `commit_leaf` / `commit_leaves` batch:

1. Apply leaf `meta` / deletes. A directory remove and a non-dir leaf both drop the path prefix so descendants cannot remain.
2. Recompute and `put_dir_node` for each affected ancestor, root-ward.
3. Update `last_synced` for paths whose live content now matches the agreed version.
4. `commit`.

If apply-to-disk fails, do not commit. If commit fails, the live file may already have been renamed; the next rescan + reconcile repairs `last_synced` / `DirNode`.

## Queries

```rust
// Master: every file under /src
storage.range_meta(&CheckoutId("".into()), "/src")?;
// Slave checkout "bak": same canonical prefix, isolated by ck
storage.range_meta(&CheckoutId("bak".into()), "/src")?;
```

Directory child lists for reconcile come from `range_meta` / `range_dir_nodes` restricted to **direct** children (path has exactly one extra component). Do not send the entire descendant range on the wire.

## Checkout removal

`delete_checkout(ck)` drops all three tables’ keys for that id. Files on disk are not touched.

## Crash safety

redb WAL + commit. On open, if a table is missing, create it. If `DirNode("/")` (or the checkout central) does not match a recompute from children, treat as dirty and run a full metadata walk + recompute before accepting network CAS.

There is no “graceful degradation to read-only with automatic reconnect” — this is a local file, not a server. Open errors are fatal to startup (log and exit). Runtime commit errors are logged; the daemon stays up and retries on the next event.

## Config

```toml
db_path = "/var/lib/arborsync/index.redb"   # master
# db_path = "/var/cache/arborsync/cache.redb"  # slave
```

No `db_backend`, no `db_max_readers` requirement. Optional redb cache sizing may be added later as a single integer; it is not part of v1.

## Maintenance

Compaction is redb’s. SIGHUP reloads config. It does not compact. A future admin subcommand may call it. Not a second database product.

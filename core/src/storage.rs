use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition};

use crate::hash::{ContentHash, DirNode, FileNode};
use crate::meta::FileMetadata;
use crate::path::{CanonicalPath, join_central};

/// Layout of a stored `meta` value. Bumped when the row encoding changes.
const META_SCHEMA_VERSION: u16 = 1;

fn meta_bincode_config() -> impl bincode::config::Config {
    bincode::config::standard()
}

/// `u16le META_SCHEMA_VERSION || bincode(FileMetadata)`.
fn encode_meta(meta: &FileMetadata) -> Result<Vec<u8>, RedbStoreError> {
    let mut out = META_SCHEMA_VERSION.to_le_bytes().to_vec();
    out.extend(
        bincode::serde::encode_to_vec(meta, meta_bincode_config())
            .map_err(|e| RedbStoreError::Bincode(e.to_string()))?,
    );
    Ok(out)
}

fn decode_meta(bytes: &[u8]) -> Result<FileMetadata, RedbStoreError> {
    let (prefix, body) = bytes
        .split_at_checked(2)
        .ok_or_else(|| RedbStoreError::Bincode("meta row has no schema prefix".into()))?;
    let version = u16::from_le_bytes([prefix[0], prefix[1]]);
    if version != META_SCHEMA_VERSION {
        return Err(RedbStoreError::UnsupportedMetaSchema(version));
    }
    let (meta, _) = bincode::serde::decode_from_slice(body, meta_bincode_config())
        .map_err(|e| RedbStoreError::Bincode(e.to_string()))?;
    Ok(meta)
}

/// Master uses the empty string. Slaves pass the checkout’s stable id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CheckoutId(pub String);

impl CheckoutId {
    pub fn master() -> Self {
        Self(String::new())
    }

    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreMark {
    Replacing { generation: u64 },
    Sealed { generation: u64, dir_node: DirNode },
}

/// Reads use a consistent snapshot. Writes go through a batch that can
/// update metadata, directory nodes, and `last_synced` together.
pub trait Storage: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type WriteBatch<'a>: WriteBatch<Error = Self::Error>
    where
        Self: 'a;

    fn open(path: &Path) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn get_meta(
        &self,
        ck: &CheckoutId,
        path: &CanonicalPath,
    ) -> Result<Option<FileMetadata>, Self::Error>;
    fn get_dir_node(
        &self,
        ck: &CheckoutId,
        path: &CanonicalPath,
    ) -> Result<Option<DirNode>, Self::Error>;
    fn get_last_synced(
        &self,
        ck: &CheckoutId,
        path: &CanonicalPath,
    ) -> Result<Option<FileNode>, Self::Error>;
    fn get_last_synced_content(
        &self,
        ck: &CheckoutId,
        path: &CanonicalPath,
    ) -> Result<Option<ContentHash>, Self::Error>;
    fn range_meta(
        &self,
        ck: &CheckoutId,
        prefix: &CanonicalPath,
    ) -> Result<Vec<(CanonicalPath, FileMetadata)>, Self::Error>;
    fn range_dir_nodes(
        &self,
        ck: &CheckoutId,
        prefix: &CanonicalPath,
    ) -> Result<Vec<(CanonicalPath, DirNode)>, Self::Error>;

    /// Direct children of `dir` only (one path component past `dir`).
    /// Default filters [`Self::range_meta`]; redb seeks past each child subtree.
    fn range_meta_children(
        &self,
        ck: &CheckoutId,
        dir: &CanonicalPath,
    ) -> Result<Vec<(CanonicalPath, FileMetadata)>, Self::Error> {
        Ok(self
            .range_meta(ck, dir)?
            .into_iter()
            .filter(|(path, _)| path.parent().as_ref() == Some(dir))
            .collect())
    }

    fn begin_write(&self) -> Result<Self::WriteBatch<'_>, Self::Error>;
    fn delete_checkout(&self, ck: &CheckoutId) -> Result<(), Self::Error>;

    /// Master table `restore_epochs`. Sealed generations only.
    /// A prefix still in `Replacing` is omitted. Slaves must not apply it.
    fn restore_epochs(&self) -> Result<Vec<(CanonicalPath, u64)>, Self::Error>;
    fn put_restore_epoch(&self, prefix: &CanonicalPath, generation: u64)
    -> Result<(), Self::Error>;
    fn restore_mark(&self, prefix: &CanonicalPath) -> Result<Option<RestoreMark>, Self::Error>;
    fn put_restore_mark(
        &self,
        prefix: &CanonicalPath,
        mark: RestoreMark,
    ) -> Result<(), Self::Error>;
    fn replacing_prefixes(&self) -> Result<Vec<CanonicalPath>, Self::Error>;
    fn applied_epoch(&self, prefix: &CanonicalPath) -> Result<Option<u64>, Self::Error>;
    fn put_applied_epoch(&self, prefix: &CanonicalPath, generation: u64)
    -> Result<(), Self::Error>;
}

pub trait WriteBatch {
    type Error: std::error::Error + Send + Sync + 'static;

    fn put_meta(
        &mut self,
        ck: &CheckoutId,
        path: &CanonicalPath,
        meta: &FileMetadata,
    ) -> Result<(), Self::Error>;
    fn del_meta(&mut self, ck: &CheckoutId, path: &CanonicalPath) -> Result<(), Self::Error>;
    fn del_meta_prefix(
        &mut self,
        ck: &CheckoutId,
        prefix: &CanonicalPath,
    ) -> Result<(), Self::Error>;
    fn put_dir_node(
        &mut self,
        ck: &CheckoutId,
        path: &CanonicalPath,
        node: DirNode,
    ) -> Result<(), Self::Error>;
    fn del_dir_node(&mut self, ck: &CheckoutId, path: &CanonicalPath) -> Result<(), Self::Error>;
    fn del_dir_prefix(
        &mut self,
        ck: &CheckoutId,
        prefix: &CanonicalPath,
    ) -> Result<(), Self::Error>;
    fn put_last_synced(
        &mut self,
        ck: &CheckoutId,
        path: &CanonicalPath,
        file_node: FileNode,
        content_hash: Option<ContentHash>,
    ) -> Result<(), Self::Error>;
    fn del_last_synced(&mut self, ck: &CheckoutId, path: &CanonicalPath)
    -> Result<(), Self::Error>;
    fn del_last_synced_prefix(
        &mut self,
        ck: &CheckoutId,
        prefix: &CanonicalPath,
    ) -> Result<(), Self::Error>;

    /// Drop everything at or below `prefix` from all three tables.
    fn purge_prefix(&mut self, ck: &CheckoutId, prefix: &CanonicalPath) -> Result<(), Self::Error> {
        self.del_meta_prefix(ck, prefix)?;
        self.del_dir_prefix(ck, prefix)?;
        self.del_last_synced_prefix(ck, prefix)
    }

    /// Drop one path from all three tables.
    fn del_entry(&mut self, ck: &CheckoutId, path: &CanonicalPath) -> Result<(), Self::Error> {
        self.del_meta(ck, path)?;
        self.del_dir_node(ck, path)?;
        self.del_last_synced(ck, path)
    }

    fn commit(self) -> Result<(), Self::Error>;
}

const META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("meta");
const DIR_NODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("dir_nodes");
const LAST_SYNCED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("last_synced");
const RESTORE_EPOCHS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("restore_epochs");
const APPLIED_EPOCHS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("applied_epochs");

#[derive(Debug, thiserror::Error)]
pub enum RedbStoreError {
    #[error(transparent)]
    Redb(Box<redb::Error>),
    #[error("bincode: {0}")]
    Bincode(String),
    #[error("unsupported meta schema version {0}")]
    UnsupportedMetaSchema(u16),
    #[error("stored hash is not 32 bytes")]
    BadHash,
}

impl RedbStoreError {
    pub fn is_index_locked(&self) -> bool {
        matches!(
            self,
            Self::Redb(err) if matches!(err.as_ref(), redb::Error::DatabaseAlreadyOpen)
        )
    }
}

impl From<redb::Error> for RedbStoreError {
    fn from(err: redb::Error) -> Self {
        Self::Redb(Box::new(err))
    }
}

impl From<redb::DatabaseError> for RedbStoreError {
    fn from(err: redb::DatabaseError) -> Self {
        redb::Error::from(err).into()
    }
}

impl From<redb::TransactionError> for RedbStoreError {
    fn from(err: redb::TransactionError) -> Self {
        redb::Error::from(err).into()
    }
}

impl From<redb::TableError> for RedbStoreError {
    fn from(err: redb::TableError) -> Self {
        redb::Error::from(err).into()
    }
}

impl From<redb::StorageError> for RedbStoreError {
    fn from(err: redb::StorageError) -> Self {
        redb::Error::from(err).into()
    }
}

impl From<redb::CommitError> for RedbStoreError {
    fn from(err: redb::CommitError) -> Self {
        redb::Error::from(err).into()
    }
}

/// On-disk [`Storage`] backed by a single redb file (`spec.md` §13).
#[derive(Clone)]
pub struct RedbStorage {
    db: Arc<Database>,
}

impl RedbStorage {
    /// Open a database that already exists. Does not create a file and does not
    /// write table headers, so a dump can read a daemon's index.
    pub fn open_existing(path: &Path) -> Result<Self, RedbStoreError> {
        let db = Database::open(path)?;
        Ok(Self { db: Arc::new(db) })
    }

    fn ensure_tables(db: &Database) -> Result<(), RedbStoreError> {
        let txn = db.begin_write()?;
        txn.open_table(META)?;
        txn.open_table(DIR_NODES)?;
        txn.open_table(LAST_SYNCED)?;
        txn.open_table(RESTORE_EPOCHS)?;
        txn.open_table(APPLIED_EPOCHS)?;
        txn.commit()?;
        Ok(())
    }
}

impl Storage for RedbStorage {
    type Error = RedbStoreError;
    type WriteBatch<'a> = RedbWriteBatch;

    fn open(path: &Path) -> Result<Self, Self::Error> {
        let db = Database::create(path)?;
        Self::ensure_tables(&db)?;
        Ok(Self { db: Arc::new(db) })
    }

    fn get_meta(
        &self,
        ck: &CheckoutId,
        path: &CanonicalPath,
    ) -> Result<Option<FileMetadata>, Self::Error> {
        get_meta_row(&self.db, META, ck, path)
    }

    fn get_dir_node(
        &self,
        ck: &CheckoutId,
        path: &CanonicalPath,
    ) -> Result<Option<DirNode>, Self::Error> {
        get_hash(&self.db, DIR_NODES, ck, path, decode_dir_node)
    }

    fn get_last_synced(
        &self,
        ck: &CheckoutId,
        path: &CanonicalPath,
    ) -> Result<Option<FileNode>, Self::Error> {
        Ok(last_synced_row(&self.db, ck, path)?.map(|row| row.node))
    }

    fn get_last_synced_content(
        &self,
        ck: &CheckoutId,
        path: &CanonicalPath,
    ) -> Result<Option<ContentHash>, Self::Error> {
        Ok(last_synced_row(&self.db, ck, path)?.and_then(|row| row.content_hash))
    }

    fn range_meta(
        &self,
        ck: &CheckoutId,
        prefix: &CanonicalPath,
    ) -> Result<Vec<(CanonicalPath, FileMetadata)>, Self::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(META)?;
        let mut out = Vec::new();
        for_each_in_prefix(&table, ck, prefix, |path, value| {
            let meta = decode_meta(value)?;
            out.push((path, meta));
            Ok(())
        })?;
        Ok(out)
    }

    fn range_meta_children(
        &self,
        ck: &CheckoutId,
        dir: &CanonicalPath,
    ) -> Result<Vec<(CanonicalPath, FileMetadata)>, Self::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(META)?;
        let mut out = Vec::new();
        for_each_direct_child(&table, ck, dir, |path, value| {
            let meta = decode_meta(value)?;
            out.push((path, meta));
            Ok(())
        })?;
        Ok(out)
    }

    fn range_dir_nodes(
        &self,
        ck: &CheckoutId,
        prefix: &CanonicalPath,
    ) -> Result<Vec<(CanonicalPath, DirNode)>, Self::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(DIR_NODES)?;
        let mut out = Vec::new();
        for_each_in_prefix(&table, ck, prefix, |path, value| {
            out.push((path, decode_dir_node(value)?));
            Ok(())
        })?;
        Ok(out)
    }

    fn begin_write(&self) -> Result<Self::WriteBatch<'_>, Self::Error> {
        Ok(RedbWriteBatch {
            txn: self.db.begin_write()?,
        })
    }

    fn delete_checkout(&self, ck: &CheckoutId) -> Result<(), Self::Error> {
        let txn = self.db.begin_write()?;
        delete_checkout_table(&txn, META, ck)?;
        delete_checkout_table(&txn, DIR_NODES, ck)?;
        delete_checkout_table(&txn, LAST_SYNCED, ck)?;
        txn.commit()?;
        Ok(())
    }

    fn restore_epochs(&self) -> Result<Vec<(CanonicalPath, u64)>, Self::Error> {
        Ok(sealed_generations(list_marks(&self.db, RESTORE_EPOCHS)?))
    }

    fn put_restore_epoch(
        &self,
        prefix: &CanonicalPath,
        generation: u64,
    ) -> Result<(), Self::Error> {
        put_mark(
            &self.db,
            RESTORE_EPOCHS,
            prefix,
            &RestoreMark::Sealed {
                generation,
                dir_node: DirNode::ZERO,
            },
        )
    }

    fn restore_mark(&self, prefix: &CanonicalPath) -> Result<Option<RestoreMark>, Self::Error> {
        get_mark(&self.db, RESTORE_EPOCHS, prefix)
    }

    fn put_restore_mark(
        &self,
        prefix: &CanonicalPath,
        mark: RestoreMark,
    ) -> Result<(), Self::Error> {
        put_mark(&self.db, RESTORE_EPOCHS, prefix, &mark)
    }

    fn replacing_prefixes(&self) -> Result<Vec<CanonicalPath>, Self::Error> {
        Ok(list_marks(&self.db, RESTORE_EPOCHS)?
            .into_iter()
            .filter_map(|(path, mark)| match mark {
                RestoreMark::Replacing { .. } => Some(path),
                RestoreMark::Sealed { .. } => None,
            })
            .collect())
    }

    fn applied_epoch(&self, prefix: &CanonicalPath) -> Result<Option<u64>, Self::Error> {
        get_epoch(&self.db, APPLIED_EPOCHS, prefix)
    }

    fn put_applied_epoch(
        &self,
        prefix: &CanonicalPath,
        generation: u64,
    ) -> Result<(), Self::Error> {
        put_epoch(&self.db, APPLIED_EPOCHS, prefix, generation)
    }
}

fn put_epoch(
    db: &Database,
    table: TableDefinition<&[u8], &[u8]>,
    prefix: &CanonicalPath,
    generation: u64,
) -> Result<(), RedbStoreError> {
    let txn = db.begin_write()?;
    {
        let mut rows = txn.open_table(table)?;
        let bytes = generation.to_le_bytes();
        rows.insert(prefix.as_str().as_bytes(), bytes.as_slice())?;
    }
    txn.commit()?;
    Ok(())
}

fn get_epoch(
    db: &Database,
    table: TableDefinition<&[u8], &[u8]>,
    prefix: &CanonicalPath,
) -> Result<Option<u64>, RedbStoreError> {
    let txn = db.begin_read()?;
    let rows = txn.open_table(table)?;
    let Some(value) = rows.get(prefix.as_str().as_bytes())? else {
        return Ok(None);
    };
    Ok(Some(decode_epoch(value.value())?))
}

fn put_mark(
    db: &Database,
    table: TableDefinition<&[u8], &[u8]>,
    prefix: &CanonicalPath,
    mark: &RestoreMark,
) -> Result<(), RedbStoreError> {
    let txn = db.begin_write()?;
    {
        let mut rows = txn.open_table(table)?;
        let bytes = encode_mark(mark);
        rows.insert(prefix.as_str().as_bytes(), bytes.as_slice())?;
    }
    txn.commit()?;
    Ok(())
}

fn get_mark(
    db: &Database,
    table: TableDefinition<&[u8], &[u8]>,
    prefix: &CanonicalPath,
) -> Result<Option<RestoreMark>, RedbStoreError> {
    let txn = db.begin_read()?;
    let rows = txn.open_table(table)?;
    let Some(value) = rows.get(prefix.as_str().as_bytes())? else {
        return Ok(None);
    };
    Ok(Some(decode_mark(value.value())?))
}

fn list_marks(
    db: &Database,
    table: TableDefinition<&[u8], &[u8]>,
) -> Result<Vec<(CanonicalPath, RestoreMark)>, RedbStoreError> {
    let txn = db.begin_read()?;
    let rows = txn.open_table(table)?;
    let mut out = Vec::new();
    for row in rows.iter()? {
        let (key, value) = row?;
        let path = std::str::from_utf8(key.value())
            .map_err(|err| RedbStoreError::Bincode(err.to_string()))?;
        let path =
            CanonicalPath::parse(path).map_err(|err| RedbStoreError::Bincode(err.to_string()))?;
        out.push((path, decode_mark(value.value())?));
    }
    out.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(out)
}

fn sealed_generations(marks: Vec<(CanonicalPath, RestoreMark)>) -> Vec<(CanonicalPath, u64)> {
    marks
        .into_iter()
        .filter_map(|(path, mark)| match mark {
            RestoreMark::Sealed { generation, .. } => Some((path, generation)),
            RestoreMark::Replacing { .. } => None,
        })
        .collect()
}

fn encode_mark(mark: &RestoreMark) -> Vec<u8> {
    match *mark {
        RestoreMark::Replacing { generation } => {
            let mut out = vec![1];
            out.extend_from_slice(&generation.to_le_bytes());
            out
        }
        RestoreMark::Sealed {
            generation,
            dir_node,
        } => {
            let mut out = vec![2];
            out.extend_from_slice(&generation.to_le_bytes());
            out.extend_from_slice(dir_node.as_bytes());
            out
        }
    }
}

fn decode_mark(bytes: &[u8]) -> Result<RestoreMark, RedbStoreError> {
    let generation = |body: &[u8]| -> Result<u64, RedbStoreError> {
        let raw: [u8; 8] = body
            .try_into()
            .map_err(|_| RedbStoreError::Bincode("epoch generation is not 8 bytes".into()))?;
        Ok(u64::from_le_bytes(raw))
    };
    match bytes.split_first() {
        Some((1, rest)) if rest.len() == 8 => Ok(RestoreMark::Replacing {
            generation: generation(rest)?,
        }),
        Some((2, rest)) if rest.len() == 40 => {
            let (generation_bytes, node) = rest.split_at(8);
            let mut raw = [0u8; 32];
            raw.copy_from_slice(node);
            Ok(RestoreMark::Sealed {
                generation: generation(generation_bytes)?,
                dir_node: DirNode::from_bytes(raw),
            })
        }
        _ => Err(RedbStoreError::Bincode(
            "epoch row is not a restore mark".into(),
        )),
    }
}

fn decode_epoch(bytes: &[u8]) -> Result<u64, RedbStoreError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| RedbStoreError::Bincode("epoch row is not 8 bytes".into()))?;
    Ok(u64::from_le_bytes(bytes))
}

pub struct RedbWriteBatch {
    txn: redb::WriteTransaction,
}

impl WriteBatch for RedbWriteBatch {
    type Error = RedbStoreError;

    fn put_meta(
        &mut self,
        ck: &CheckoutId,
        path: &CanonicalPath,
        meta: &FileMetadata,
    ) -> Result<(), Self::Error> {
        let key = storage_key(ck, path);
        let value = encode_meta(meta)?;
        let mut table = self.txn.open_table(META)?;
        table.insert(key.as_slice(), value.as_slice())?;
        Ok(())
    }

    fn del_meta(&mut self, ck: &CheckoutId, path: &CanonicalPath) -> Result<(), Self::Error> {
        remove_key(&self.txn, META, ck, path)
    }

    fn del_meta_prefix(
        &mut self,
        ck: &CheckoutId,
        prefix: &CanonicalPath,
    ) -> Result<(), Self::Error> {
        delete_interest_prefix(&self.txn, META, ck, prefix)
    }

    fn put_dir_node(
        &mut self,
        ck: &CheckoutId,
        path: &CanonicalPath,
        node: DirNode,
    ) -> Result<(), Self::Error> {
        put_hash(&self.txn, DIR_NODES, ck, path, node.as_bytes())
    }

    fn del_dir_node(&mut self, ck: &CheckoutId, path: &CanonicalPath) -> Result<(), Self::Error> {
        remove_key(&self.txn, DIR_NODES, ck, path)
    }

    fn del_dir_prefix(
        &mut self,
        ck: &CheckoutId,
        prefix: &CanonicalPath,
    ) -> Result<(), Self::Error> {
        delete_interest_prefix(&self.txn, DIR_NODES, ck, prefix)
    }

    fn put_last_synced(
        &mut self,
        ck: &CheckoutId,
        path: &CanonicalPath,
        file_node: FileNode,
        content_hash: Option<ContentHash>,
    ) -> Result<(), Self::Error> {
        let key = storage_key(ck, path);
        let value = LastSyncedRow {
            node: file_node,
            content_hash,
        }
        .encode();
        let mut table = self.txn.open_table(LAST_SYNCED)?;
        table.insert(key.as_slice(), value.as_slice())?;
        Ok(())
    }

    fn del_last_synced(
        &mut self,
        ck: &CheckoutId,
        path: &CanonicalPath,
    ) -> Result<(), Self::Error> {
        remove_key(&self.txn, LAST_SYNCED, ck, path)
    }

    fn del_last_synced_prefix(
        &mut self,
        ck: &CheckoutId,
        prefix: &CanonicalPath,
    ) -> Result<(), Self::Error> {
        delete_interest_prefix(&self.txn, LAST_SYNCED, ck, prefix)
    }

    fn commit(self) -> Result<(), Self::Error> {
        self.txn.commit()?;
        Ok(())
    }
}

fn storage_key(ck: &CheckoutId, path: &CanonicalPath) -> Vec<u8> {
    let path = path.as_str();
    let mut key = Vec::with_capacity(ck.0.len() + 1 + path.len());
    key.extend_from_slice(ck.0.as_bytes());
    key.push(0);
    key.extend_from_slice(path.as_bytes());
    key
}

fn checkout_start(ck: &CheckoutId) -> Vec<u8> {
    let mut key = ck.0.as_bytes().to_vec();
    key.push(0);
    key
}

fn checkout_end(ck: &CheckoutId) -> Vec<u8> {
    let mut key = ck.0.as_bytes().to_vec();
    key.push(1);
    key
}

fn path_from_key(ck: &CheckoutId, key: &[u8]) -> Option<CanonicalPath> {
    let prefix_len = ck.0.len() + 1;
    if key.len() < prefix_len || !key.starts_with(ck.0.as_bytes()) || key[ck.0.len()] != 0 {
        return None;
    }
    let path = std::str::from_utf8(&key[prefix_len..]).ok()?;
    Some(CanonicalPath::from_stored(path))
}

fn descendant_range(ck: &CheckoutId, prefix: &CanonicalPath) -> (Vec<u8>, Vec<u8>) {
    if prefix.as_str() == "/" {
        return (storage_key(ck, prefix), checkout_end(ck));
    }
    let mut start = storage_key(ck, prefix);
    start.push(b'/');
    let mut end = start.clone();
    increment_key(&mut end);
    (start, end)
}

fn increment_key(key: &mut Vec<u8>) {
    for i in (0..key.len()).rev() {
        if key[i] != 0xFF {
            key[i] += 1;
            key.truncate(i + 1);
            return;
        }
    }
    key.push(0);
}

fn decode_dir_node(bytes: &[u8]) -> Result<DirNode, RedbStoreError> {
    Ok(DirNode::from_bytes(
        bytes.try_into().map_err(|_| RedbStoreError::BadHash)?,
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LastSyncedRow {
    node: FileNode,
    content_hash: Option<ContentHash>,
}

impl LastSyncedRow {
    fn encode(self) -> Vec<u8> {
        let mut out = self.node.as_bytes().to_vec();
        if let Some(hash) = self.content_hash {
            out.extend_from_slice(hash.as_bytes());
        }
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, RedbStoreError> {
        match bytes.len() {
            32 => Ok(Self {
                node: FileNode::from_bytes(bytes.try_into().map_err(|_| RedbStoreError::BadHash)?),
                content_hash: None,
            }),
            64 => Ok(Self {
                node: FileNode::from_bytes(
                    bytes[..32]
                        .try_into()
                        .map_err(|_| RedbStoreError::BadHash)?,
                ),
                content_hash: Some(ContentHash::from_bytes(
                    bytes[32..]
                        .try_into()
                        .map_err(|_| RedbStoreError::BadHash)?,
                )),
            }),
            _ => Err(RedbStoreError::BadHash),
        }
    }
}

fn last_synced_row(
    db: &Database,
    ck: &CheckoutId,
    path: &CanonicalPath,
) -> Result<Option<LastSyncedRow>, RedbStoreError> {
    let txn = db.begin_read()?;
    let table = txn.open_table(LAST_SYNCED)?;
    let key = storage_key(ck, path);
    match table.get(key.as_slice())? {
        Some(guard) => Ok(Some(LastSyncedRow::decode(guard.value())?)),
        None => Ok(None),
    }
}

fn get_meta_row(
    db: &Database,
    table_def: TableDefinition<&[u8], &[u8]>,
    ck: &CheckoutId,
    path: &CanonicalPath,
) -> Result<Option<FileMetadata>, RedbStoreError> {
    let txn = db.begin_read()?;
    let table = txn.open_table(table_def)?;
    let key = storage_key(ck, path);
    match table.get(key.as_slice())? {
        Some(guard) => Ok(Some(decode_meta(guard.value())?)),
        None => Ok(None),
    }
}

fn get_hash<T>(
    db: &Database,
    table_def: TableDefinition<&[u8], &[u8]>,
    ck: &CheckoutId,
    path: &CanonicalPath,
    decode: fn(&[u8]) -> Result<T, RedbStoreError>,
) -> Result<Option<T>, RedbStoreError> {
    let txn = db.begin_read()?;
    let table = txn.open_table(table_def)?;
    let key = storage_key(ck, path);
    match table.get(key.as_slice())? {
        Some(guard) => Ok(Some(decode(guard.value())?)),
        None => Ok(None),
    }
}

fn put_hash(
    txn: &redb::WriteTransaction,
    table_def: TableDefinition<&[u8], &[u8]>,
    ck: &CheckoutId,
    path: &CanonicalPath,
    hash: &[u8; 32],
) -> Result<(), RedbStoreError> {
    let key = storage_key(ck, path);
    let mut table = txn.open_table(table_def)?;
    table.insert(key.as_slice(), hash.as_slice())?;
    Ok(())
}

fn remove_key(
    txn: &redb::WriteTransaction,
    table_def: TableDefinition<&[u8], &[u8]>,
    ck: &CheckoutId,
    path: &CanonicalPath,
) -> Result<(), RedbStoreError> {
    let key = storage_key(ck, path);
    let mut table = txn.open_table(table_def)?;
    table.remove(key.as_slice())?;
    Ok(())
}

fn for_each_in_prefix<T>(
    table: &T,
    ck: &CheckoutId,
    prefix: &CanonicalPath,
    mut visit: impl FnMut(CanonicalPath, &[u8]) -> Result<(), RedbStoreError>,
) -> Result<(), RedbStoreError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    if prefix.as_str() != "/" {
        let exact = storage_key(ck, prefix);
        if let Some(value) = table.get(exact.as_slice())? {
            visit(prefix.clone(), value.value())?;
        }
    }
    let (start, end) = descendant_range(ck, prefix);
    for item in table.range(start.as_slice()..end.as_slice())? {
        let (key, value) = item?;
        let Some(path) = path_from_key(ck, key.value()) else {
            continue;
        };
        if !prefix.covers(&path) {
            continue;
        }
        visit(path, value.value())?;
    }
    Ok(())
}

/// Walk only direct children of `dir`.
///
/// After a child row, the next key may be a sibling that sorts before that
/// child's descendants (`pages` then `pages-v2`, then `pages/index.md`). Skip
/// a subtree only once the cursor is already inside it.
fn for_each_direct_child<T>(
    table: &T,
    ck: &CheckoutId,
    dir: &CanonicalPath,
    mut visit: impl FnMut(CanonicalPath, &[u8]) -> Result<(), RedbStoreError>,
) -> Result<(), RedbStoreError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let (range_start, range_end) = descendant_range(ck, dir);
    let mut cursor = if dir.as_str() == "/" {
        let mut after_root = storage_key(ck, dir);
        after_root.push(0);
        after_root
    } else {
        range_start
    };

    while cursor.as_slice() < range_end.as_slice() {
        #[cfg(test)]
        DIRECT_CHILD_RANGE_STEPS.with(|steps| steps.set(steps.get() + 1));

        let mut iter = table.range(cursor.as_slice()..range_end.as_slice())?;
        let Some(item) = iter.next() else {
            break;
        };
        let (key, value) = item?;
        let Some(path) = path_from_key(ck, key.value()) else {
            cursor = key.value().to_vec();
            increment_key(&mut cursor);
            continue;
        };
        if !dir.covers(&path) || path.as_str() == dir.as_str() {
            break;
        }
        let Some(child) = first_direct_child(dir, &path) else {
            break;
        };

        if path == child {
            visit(child.clone(), value.value())?;
            // The next sibling can sort before this child's descendants.
            // `pages-v2` is after `pages` and before `pages/…`, and jumping
            // to the descendant bound (`pages0`) skips it.
            cursor = key.value().to_vec();
            cursor.push(0);
        } else {
            let (_, after_child) = descendant_range(ck, &child);
            cursor = after_child;
        }
    }
    Ok(())
}

fn first_direct_child(dir: &CanonicalPath, path: &CanonicalPath) -> Option<CanonicalPath> {
    if !dir.covers(path) || path == dir {
        return None;
    }
    let relative = if dir.as_str() == "/" {
        path.as_str().trim_start_matches('/')
    } else {
        path.as_str()
            .strip_prefix(dir.as_str())?
            .strip_prefix('/')?
    };
    let name = relative.split('/').next()?;
    join_central(dir, name).ok()
}

#[cfg(test)]
thread_local! {
    static DIRECT_CHILD_RANGE_STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn take_direct_child_range_steps() -> usize {
    DIRECT_CHILD_RANGE_STEPS.with(|steps| steps.replace(0))
}

fn delete_interest_prefix(
    txn: &redb::WriteTransaction,
    table_def: TableDefinition<&[u8], &[u8]>,
    ck: &CheckoutId,
    prefix: &CanonicalPath,
) -> Result<(), RedbStoreError> {
    let keys = {
        let table = txn.open_table(table_def)?;
        let mut keys = Vec::new();
        for_each_in_prefix(&table, ck, prefix, |path, _| {
            keys.push(storage_key(ck, &path));
            Ok(())
        })?;
        keys
    };
    let mut table = txn.open_table(table_def)?;
    for key in keys {
        table.remove(key.as_slice())?;
    }
    Ok(())
}

fn delete_checkout_table(
    txn: &redb::WriteTransaction,
    table_def: TableDefinition<&[u8], &[u8]>,
    ck: &CheckoutId,
) -> Result<(), RedbStoreError> {
    let start = checkout_start(ck);
    let end = checkout_end(ck);
    let keys = {
        let table = txn.open_table(table_def)?;
        let mut keys = Vec::new();
        for item in table.range(start.as_slice()..end.as_slice())? {
            let (key, _) = item?;
            keys.push(key.value().to_vec());
        }
        keys
    };
    let mut table = txn.open_table(table_def)?;
    for key in keys {
        table.remove(key.as_slice())?;
    }
    Ok(())
}

#[cfg(test)]
mod prefix_keys {
    use super::*;

    fn p(value: &str) -> CanonicalPath {
        CanonicalPath::parse(value).unwrap()
    }

    fn in_range(ck: &CheckoutId, prefix: &CanonicalPath, path: &CanonicalPath) -> bool {
        let (start, end) = descendant_range(ck, prefix);
        let key = storage_key(ck, path);
        start.as_slice() <= key.as_slice() && key.as_slice() < end.as_slice()
    }

    fn meta(content_byte: u8) -> FileMetadata {
        FileMetadata::file(1, 0, 0o100644, ContentHash::from_bytes([content_byte; 32]))
    }

    fn open_tmp() -> (tempfile::TempDir, RedbStorage) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedbStorage::open(&dir.path().join("index.redb")).expect("open");
        (dir, store)
    }

    #[test]
    fn descendant_key_range_stops_before_a_sorting_sibling() {
        let ck = CheckoutId::master();
        assert!(!in_range(&ck, &p("/src"), &p("/src")));
        assert!(in_range(&ck, &p("/src"), &p("/src/foo.rs")));
        assert!(!in_range(&ck, &p("/src"), &p("/src2")));
        assert!(!in_range(&ck, &p("/src"), &p("/src.foo")));
        assert!(!in_range(&ck, &p("/src"), &p("/zzz")));
    }

    #[test]
    fn root_prefix_covers_the_whole_checkout() {
        let ck = CheckoutId::master();
        assert!(in_range(&ck, &p("/"), &p("/")));
        assert!(in_range(&ck, &p("/"), &p("/zzz/f00000")));
    }

    #[test]
    fn range_meta_children_returns_only_direct_children() {
        let (_dir, store) = open_tmp();
        let ck = CheckoutId::master();
        let mut batch = store.begin_write().unwrap();
        batch
            .put_meta(&ck, &p("/src"), &FileMetadata::directory(0, 0o040755))
            .unwrap();
        batch
            .put_meta(&ck, &p("/src/a"), &FileMetadata::directory(0, 0o040755))
            .unwrap();
        batch
            .put_meta(&ck, &p("/src/a/nested.txt"), &meta(1))
            .unwrap();
        batch
            .put_meta(&ck, &p("/src/a/deeper/file.txt"), &meta(2))
            .unwrap();
        batch.put_meta(&ck, &p("/src/b.txt"), &meta(3)).unwrap();
        batch
            .put_meta(&ck, &p("/other"), &FileMetadata::directory(0, 0o040755))
            .unwrap();
        batch.commit().unwrap();

        let kids = store.range_meta_children(&ck, &p("/src")).unwrap();
        assert_eq!(
            kids.iter()
                .map(|(path, _)| path.as_str())
                .collect::<Vec<_>>(),
            ["/src/a", "/src/b.txt"]
        );
    }

    #[test]
    fn range_meta_children_seeks_past_grandchild_keys() {
        let (_dir, store) = open_tmp();
        let ck = CheckoutId::master();
        let mut batch = store.begin_write().unwrap();
        batch
            .put_meta(&ck, &p("/wide"), &FileMetadata::directory(0, 0o040755))
            .unwrap();
        for i in 0..8 {
            let child = p(&format!("/wide/c{i:02}"));
            batch
                .put_meta(&ck, &child, &FileMetadata::directory(0, 0o040755))
                .unwrap();
            for j in 0..200 {
                batch
                    .put_meta(&ck, &p(&format!("/wide/c{i:02}/f{j:03}")), &meta(1))
                    .unwrap();
            }
        }
        batch.commit().unwrap();

        let _ = take_direct_child_range_steps();
        let kids = store.range_meta_children(&ck, &p("/wide")).unwrap();
        let steps = take_direct_child_range_steps();
        assert_eq!(kids.len(), 8);
        assert!(
            steps <= 24,
            "direct-child load touched {steps} range steps; expected a couple of seeks per child, not all grandchildren"
        );
    }

    #[test]
    fn range_meta_children_keeps_a_sibling_that_extends_a_shorter_name() {
        let (_dir, store) = open_tmp();
        let ck = CheckoutId::master();
        let mut batch = store.begin_write().unwrap();
        for path in [
            "/course",
            "/course/pages",
            "/course/pages/index.md",
            "/course/pages-v2",
            "/course/pages-v2/brief.md",
            "/course/ice",
            "/course/ice-car",
            "/course/readme",
            "/course/readme.md",
        ] {
            let meta = if path.ends_with(".md") {
                meta(1)
            } else {
                FileMetadata::directory(0, 0o040755)
            };
            batch.put_meta(&ck, &p(path), &meta).unwrap();
        }
        batch.commit().unwrap();

        let kids = store.range_meta_children(&ck, &p("/course")).unwrap();
        assert_eq!(
            kids.iter()
                .map(|(path, _)| path.as_str())
                .collect::<Vec<_>>(),
            [
                "/course/ice",
                "/course/ice-car",
                "/course/pages",
                "/course/pages-v2",
                "/course/readme",
                "/course/readme.md",
            ]
        );
    }

    #[test]
    fn a_second_open_of_the_same_index_is_locked() {
        let (dir, _store) = open_tmp();
        let err = match RedbStorage::open(&dir.path().join("index.redb")) {
            Err(err) => err,
            Ok(_) => panic!("second open should fail while the first handle is live"),
        };
        assert!(err.is_index_locked(), "{err}");
    }
}

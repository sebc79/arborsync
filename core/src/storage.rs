use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};

use crate::hash::{ContentHash, DirNode, FileNode};
use crate::meta::FileMetadata;
use crate::path::CanonicalPath;

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

    fn begin_write(&self) -> Result<Self::WriteBatch<'_>, Self::Error>;
    fn delete_checkout(&self, ck: &CheckoutId) -> Result<(), Self::Error>;
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
pub struct RedbStorage {
    db: Database,
}

impl RedbStorage {
    fn ensure_tables(db: &Database) -> Result<(), RedbStoreError> {
        let txn = db.begin_write()?;
        txn.open_table(META)?;
        txn.open_table(DIR_NODES)?;
        txn.open_table(LAST_SYNCED)?;
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
        Ok(Self { db })
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
}

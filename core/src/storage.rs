//! Storage trait (`spec.md` §13). redb is the only on-disk backend.

use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};

use crate::meta::FileMetadata;
use crate::path::is_interested;
use crate::protocol::{decode_bincode, encode_bincode};

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

    fn get_meta(&self, ck: &CheckoutId, path: &str) -> Result<Option<FileMetadata>, Self::Error>;
    fn get_dir_node(&self, ck: &CheckoutId, path: &str) -> Result<Option<[u8; 32]>, Self::Error>;
    fn get_last_synced(&self, ck: &CheckoutId, path: &str)
    -> Result<Option<[u8; 32]>, Self::Error>;
    fn range_meta(
        &self,
        ck: &CheckoutId,
        prefix: &str,
    ) -> Result<Vec<(String, FileMetadata)>, Self::Error>;
    fn range_dir_nodes(
        &self,
        ck: &CheckoutId,
        prefix: &str,
    ) -> Result<Vec<(String, [u8; 32])>, Self::Error>;

    fn begin_write(&self) -> Result<Self::WriteBatch<'_>, Self::Error>;
    fn delete_checkout(&self, ck: &CheckoutId) -> Result<(), Self::Error>;
}

pub trait WriteBatch {
    type Error: std::error::Error + Send + Sync + 'static;

    fn put_meta(
        &mut self,
        ck: &CheckoutId,
        path: &str,
        meta: &FileMetadata,
    ) -> Result<(), Self::Error>;
    fn del_meta(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Self::Error>;
    fn del_meta_prefix(&mut self, ck: &CheckoutId, prefix: &str) -> Result<(), Self::Error>;
    fn put_dir_node(
        &mut self,
        ck: &CheckoutId,
        path: &str,
        node: [u8; 32],
    ) -> Result<(), Self::Error>;
    fn del_dir_node(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Self::Error>;
    fn del_dir_prefix(&mut self, ck: &CheckoutId, prefix: &str) -> Result<(), Self::Error>;
    fn put_last_synced(
        &mut self,
        ck: &CheckoutId,
        path: &str,
        file_node: [u8; 32],
    ) -> Result<(), Self::Error>;
    fn del_last_synced(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Self::Error>;
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

    fn get_meta(&self, ck: &CheckoutId, path: &str) -> Result<Option<FileMetadata>, Self::Error> {
        get_bincode(&self.db, META, ck, path)
    }

    fn get_dir_node(&self, ck: &CheckoutId, path: &str) -> Result<Option<[u8; 32]>, Self::Error> {
        get_hash(&self.db, DIR_NODES, ck, path)
    }

    fn get_last_synced(
        &self,
        ck: &CheckoutId,
        path: &str,
    ) -> Result<Option<[u8; 32]>, Self::Error> {
        get_hash(&self.db, LAST_SYNCED, ck, path)
    }

    fn range_meta(
        &self,
        ck: &CheckoutId,
        prefix: &str,
    ) -> Result<Vec<(String, FileMetadata)>, Self::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(META)?;
        let mut out = Vec::new();
        for_each_in_prefix(&table, ck, prefix, |path, value| {
            let meta = decode_bincode(value).map_err(RedbStoreError::Bincode)?;
            out.push((path, meta));
            Ok(())
        })?;
        Ok(out)
    }

    fn range_dir_nodes(
        &self,
        ck: &CheckoutId,
        prefix: &str,
    ) -> Result<Vec<(String, [u8; 32])>, Self::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(DIR_NODES)?;
        let mut out = Vec::new();
        for_each_in_prefix(&table, ck, prefix, |path, value| {
            out.push((path, hash32(value)?));
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
        path: &str,
        meta: &FileMetadata,
    ) -> Result<(), Self::Error> {
        let key = storage_key(ck, path);
        let value = encode_bincode(meta).map_err(RedbStoreError::Bincode)?;
        let mut table = self.txn.open_table(META)?;
        table.insert(key.as_slice(), value.as_slice())?;
        Ok(())
    }

    fn del_meta(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Self::Error> {
        let key = storage_key(ck, path);
        let mut table = self.txn.open_table(META)?;
        table.remove(key.as_slice())?;
        Ok(())
    }

    fn del_meta_prefix(&mut self, ck: &CheckoutId, prefix: &str) -> Result<(), Self::Error> {
        delete_interest_prefix(&self.txn, META, ck, prefix)
    }

    fn put_dir_node(
        &mut self,
        ck: &CheckoutId,
        path: &str,
        node: [u8; 32],
    ) -> Result<(), Self::Error> {
        put_hash(&self.txn, DIR_NODES, ck, path, node)
    }

    fn del_dir_node(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Self::Error> {
        let key = storage_key(ck, path);
        let mut table = self.txn.open_table(DIR_NODES)?;
        table.remove(key.as_slice())?;
        Ok(())
    }

    fn del_dir_prefix(&mut self, ck: &CheckoutId, prefix: &str) -> Result<(), Self::Error> {
        delete_interest_prefix(&self.txn, DIR_NODES, ck, prefix)
    }

    fn put_last_synced(
        &mut self,
        ck: &CheckoutId,
        path: &str,
        file_node: [u8; 32],
    ) -> Result<(), Self::Error> {
        put_hash(&self.txn, LAST_SYNCED, ck, path, file_node)
    }

    fn del_last_synced(&mut self, ck: &CheckoutId, path: &str) -> Result<(), Self::Error> {
        let key = storage_key(ck, path);
        let mut table = self.txn.open_table(LAST_SYNCED)?;
        table.remove(key.as_slice())?;
        Ok(())
    }

    fn commit(self) -> Result<(), Self::Error> {
        self.txn.commit()?;
        Ok(())
    }
}

fn storage_key(ck: &CheckoutId, path: &str) -> Vec<u8> {
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

fn path_from_key(ck: &CheckoutId, key: &[u8]) -> Option<String> {
    let prefix_len = ck.0.len() + 1;
    if key.len() < prefix_len || !key.starts_with(ck.0.as_bytes()) || key[ck.0.len()] != 0 {
        return None;
    }
    String::from_utf8(key[prefix_len..].to_vec()).ok()
}

fn hash32(bytes: &[u8]) -> Result<[u8; 32], RedbStoreError> {
    bytes.try_into().map_err(|_| RedbStoreError::BadHash)
}

fn get_bincode(
    db: &Database,
    table_def: TableDefinition<&[u8], &[u8]>,
    ck: &CheckoutId,
    path: &str,
) -> Result<Option<FileMetadata>, RedbStoreError> {
    let txn = db.begin_read()?;
    let table = txn.open_table(table_def)?;
    let key = storage_key(ck, path);
    match table.get(key.as_slice())? {
        Some(guard) => Ok(Some(
            decode_bincode(guard.value()).map_err(RedbStoreError::Bincode)?,
        )),
        None => Ok(None),
    }
}

fn get_hash(
    db: &Database,
    table_def: TableDefinition<&[u8], &[u8]>,
    ck: &CheckoutId,
    path: &str,
) -> Result<Option<[u8; 32]>, RedbStoreError> {
    let txn = db.begin_read()?;
    let table = txn.open_table(table_def)?;
    let key = storage_key(ck, path);
    match table.get(key.as_slice())? {
        Some(guard) => Ok(Some(hash32(guard.value())?)),
        None => Ok(None),
    }
}

fn put_hash(
    txn: &redb::WriteTransaction,
    table_def: TableDefinition<&[u8], &[u8]>,
    ck: &CheckoutId,
    path: &str,
    hash: [u8; 32],
) -> Result<(), RedbStoreError> {
    let key = storage_key(ck, path);
    let mut table = txn.open_table(table_def)?;
    table.insert(key.as_slice(), hash.as_slice())?;
    Ok(())
}

fn for_each_in_prefix<T>(
    table: &T,
    ck: &CheckoutId,
    prefix: &str,
    mut visit: impl FnMut(String, &[u8]) -> Result<(), RedbStoreError>,
) -> Result<(), RedbStoreError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let start = storage_key(ck, prefix);
    let end = checkout_end(ck);
    for item in table.range(start.as_slice()..end.as_slice())? {
        let (key, value) = item?;
        let Some(path) = path_from_key(ck, key.value()) else {
            continue;
        };
        if !is_interested(prefix, &path) {
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
    prefix: &str,
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

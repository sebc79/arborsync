//! Storage trait (`spec.md` §13). redb is the only on-disk backend.

use std::path::Path;

use crate::meta::FileMetadata;

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

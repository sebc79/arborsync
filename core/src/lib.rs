//! Normative source: `doc/spec.md`. Topic documents under `doc/` expand
//! procedures and must not contradict that file.

pub mod config;
pub mod hash;
pub mod keys;
pub mod merkle;
pub mod meta;
pub mod path;
pub mod protocol;
pub mod storage;

pub mod test_support;

pub use config::{ConfigError, LoadedAcl, LoadedCheckout, LoadedMaster, LoadedSlave};
pub use hash::{ContentHash, DirNode, FileNode, SubtreeRoot};
pub use keys::{KeyError, format_hex_key, parse_hex_key, read_static_key, write_static_key};
pub use meta::{EntryKind, FileMetadata};
pub use path::{
    CanonicalPath, EntryName, PathError, RESERVED_CONFLICTS, RESERVED_TMP, is_interested,
    is_reserved_root_entry,
};
pub use storage::{CheckoutId, RedbStorage, Storage, WriteBatch};

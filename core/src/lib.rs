//! Shared ArborSync types and algorithms.
//!
//! Normative source: `doc/spec.md`. Topic documents under `doc/` expand
//! procedures and must not contradict that file.

pub mod config;
pub mod merkle;
pub mod meta;
pub mod path;
pub mod protocol;
pub mod storage;

pub mod test_support;

pub use meta::{EntryKind, FileMetadata};
pub use path::{RESERVED_CONFLICTS, RESERVED_TMP, is_interested, is_reserved_root_entry};
pub use storage::{CheckoutId, Storage, WriteBatch};

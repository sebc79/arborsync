//! Normative source: `doc/spec.md`. Topic documents under `doc/` expand
//! procedures and must not contradict that file.

mod apply;
mod index;
mod inflight;

pub mod config;
pub mod hash;
pub mod keys;
pub mod master;
pub mod merkle;
pub mod meta;
pub mod path;
pub mod protocol;
pub mod reconcile;
pub mod slave;
pub mod storage;
pub mod transfer;
pub mod transport;
pub mod watch;

pub mod test_support;

pub use config::{
    ConfigError, LoadedAcl, LoadedCheckout, LoadedMaster, LoadedSlave, MasterReload, ReloadError,
    SlaveReload, log_level_filter,
};
pub use hash::{ContentHash, DirNode, FileNode, SubtreeRoot};
pub use keys::{
    KeyError, format_hex_key, parse_hex_key, public_from_secret, read_static_key, write_static_key,
};
pub use meta::{EntryKind, FileMetadata};
pub use path::{
    CanonicalPath, EntryName, PathError, RESERVED_CONFLICTS, RESERVED_TMP, canonical_to_host,
    is_reserved_root_entry, local_to_canonical, strip_central,
};
pub use storage::{CheckoutId, RedbStorage, Storage, WriteBatch};
pub use transport::{MemoryTransport, Transport};
pub use watch::LocalEvent;

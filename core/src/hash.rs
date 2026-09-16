//! Branded 32-byte hashes (`spec.md` §5, §6).
//!
//! A `FileNode` cannot be stored where a `DirNode` belongs:
//!
//! ```compile_fail
//! use arborsync_core::hash::{DirNode, FileNode};
//!
//! fn put_dir_node(_: DirNode) {}
//! put_dir_node(FileNode::from_bytes([0; 32]));
//! ```
//!
//! A content hash is not a file node:
//!
//! ```compile_fail
//! use arborsync_core::hash::{ContentHash, FileNode};
//!
//! let _: FileNode = ContentHash::from_bytes([0; 32]);
//! ```
//!
//! Raw bytes do not stand in for a brand:
//!
//! ```compile_fail
//! use arborsync_core::hash::DirNode;
//!
//! fn put_dir_node(_: DirNode) {}
//! put_dir_node([0u8; 32]);
//! ```

use serde::{Deserialize, Serialize};

macro_rules! hash_brand {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
        #[serde(transparent)]
        #[repr(transparent)]
        pub struct $name([u8; 32]);

        impl $name {
            pub const ZERO: Self = Self([0; 32]);

            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }

            pub const fn into_bytes(self) -> [u8; 32] {
                self.0
            }
        }
    };
}

hash_brand! {
    /// BLAKE3 of a file's bytes, or of a symlink target. Directories carry
    /// [`ContentHash::ZERO`]; their Merkle identity is a [`DirNode`].
    ContentHash
}

hash_brand! {
    /// Merkle identity of one file or symlink entry (`spec.md` §5).
    FileNode
}

hash_brand! {
    /// Merkle identity of one directory's listing (`spec.md` §5).
    DirNode
}

hash_brand! {
    /// The node a peer reports as the root of a subtree. Either brand can
    /// fill it, because a checkout root can be a single file.
    SubtreeRoot
}

impl From<FileNode> for SubtreeRoot {
    fn from(node: FileNode) -> Self {
        Self(node.0)
    }
}

impl From<DirNode> for SubtreeRoot {
    fn from(node: DirNode) -> Self {
        Self(node.0)
    }
}

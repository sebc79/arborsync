//! Path-Merkle directory hash tree (`spec.md` §5). Not `rs_merkle`.

use crate::meta::FileMetadata;

/// One child of a directory node, in the order supplied to [`dir_node`].
/// The encoder sorts by raw UTF-8 `name` bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirChild {
    pub kind: u8,
    pub name: String,
    pub node_hash: [u8; 32],
}

/// `FileNode = BLAKE3(u8 kind || content_hash || u64be size || i64be mtime_ns || u32be mode)`.
/// Path is location, not payload — do not hash it into the node.
pub fn file_node(meta: &FileMetadata) -> [u8; 32] {
    let _ = meta;
    todo!("spec §5: FileNode encoding")
}

/// `DirNode = BLAKE3(concat(entries))` with each
/// `entry = u8 kind || u32be name_len || name || [u8; 32] child`.
/// Children are sorted by name as raw UTF-8 bytes.
pub fn dir_node(entries: &[DirChild]) -> [u8; 32] {
    let _ = entries;
    todo!("spec §5: DirNode encoding")
}

/// Empty directory hash is `BLAKE3("")`.
pub fn empty_dir_node() -> [u8; 32] {
    todo!("spec §5: empty DirNode = BLAKE3(\"\")")
}

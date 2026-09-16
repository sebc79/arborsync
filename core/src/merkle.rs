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
    let mut buf = Vec::with_capacity(1 + 32 + 8 + 8 + 4);
    buf.push(u8::from(meta.kind));
    buf.extend_from_slice(&meta.content_hash);
    buf.extend_from_slice(&meta.size.to_be_bytes());
    buf.extend_from_slice(&meta.mtime_ns.to_be_bytes());
    buf.extend_from_slice(&meta.mode.to_be_bytes());
    *blake3::hash(&buf).as_bytes()
}

/// `DirNode = BLAKE3(concat(entries))` with each
/// `entry = u8 kind || u32be name_len || name || [u8; 32] child`.
/// Children are sorted by name as raw UTF-8 bytes.
pub fn dir_node(entries: &[DirChild]) -> [u8; 32] {
    let mut sorted: Vec<&DirChild> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
    let mut buf = Vec::new();
    for child in sorted {
        buf.push(child.kind);
        let name_bytes = child.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&child.node_hash);
    }
    *blake3::hash(&buf).as_bytes()
}

/// Empty directory hash is `BLAKE3("")`.
pub fn empty_dir_node() -> [u8; 32] {
    dir_node(&[])
}

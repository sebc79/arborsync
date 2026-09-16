use serde::{Deserialize, Serialize};

use crate::hash::{DirNode, FileNode};
use crate::meta::{EntryKind, FileMetadata};
use crate::path::EntryName;

/// One child of a directory node. The variant fixes which brand of node hash
/// the child carries, so a directory child cannot hold a [`FileNode`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DirChild {
    File { name: EntryName, node: FileNode },
    Directory { name: EntryName, node: DirNode },
    Symlink { name: EntryName, node: FileNode },
}

impl DirChild {
    pub fn name(&self) -> &EntryName {
        self.parts().1
    }

    pub fn kind(&self) -> EntryKind {
        self.parts().0
    }

    fn parts(&self) -> (EntryKind, &EntryName, &[u8; 32]) {
        match self {
            Self::File { name, node } => (EntryKind::File, name, node.as_bytes()),
            Self::Directory { name, node } => (EntryKind::Dir, name, node.as_bytes()),
            Self::Symlink { name, node } => (EntryKind::Symlink, name, node.as_bytes()),
        }
    }

    fn from_wire(kind: EntryKind, name: EntryName, node_hash: [u8; 32]) -> Self {
        match kind {
            EntryKind::File => Self::File {
                name,
                node: FileNode::from_bytes(node_hash),
            },
            EntryKind::Dir => Self::Directory {
                name,
                node: DirNode::from_bytes(node_hash),
            },
            EntryKind::Symlink => Self::Symlink {
                name,
                node: FileNode::from_bytes(node_hash),
            },
        }
    }
}

/// Field order is the wire order of `DirEntry` (`spec.md` §11).
#[derive(Serialize, Deserialize)]
#[serde(rename = "DirEntry")]
struct WireDirChild {
    name: EntryName,
    kind: EntryKind,
    node_hash: [u8; 32],
}

impl Serialize for DirChild {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (kind, name, node_hash) = self.parts();
        WireDirChild {
            name: name.clone(),
            kind,
            node_hash: *node_hash,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DirChild {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = WireDirChild::deserialize(deserializer)?;
        Ok(Self::from_wire(wire.kind, wire.name, wire.node_hash))
    }
}

/// `FileNode = BLAKE3(u8 kind || content_hash || u64be size || i64be mtime_ns || u32be mode)`.
pub fn file_node(meta: &FileMetadata) -> FileNode {
    let mut buf = Vec::with_capacity(1 + 32 + 8 + 8 + 4);
    buf.push(u8::from(meta.kind));
    buf.extend_from_slice(meta.content_hash.as_bytes());
    buf.extend_from_slice(&meta.size.to_be_bytes());
    buf.extend_from_slice(&meta.mtime_ns.to_be_bytes());
    buf.extend_from_slice(&meta.mode.to_be_bytes());
    FileNode::from_bytes(*blake3::hash(&buf).as_bytes())
}

/// `DirNode = BLAKE3(concat(entries))` with each
/// `entry = u8 kind || u32be name_len || name || [u8; 32] child`.
/// Children are sorted by name as raw UTF-8 bytes.
pub fn dir_node(entries: &[DirChild]) -> DirNode {
    let mut sorted: Vec<&DirChild> = entries.iter().collect();
    sorted.sort_by(|a, b| {
        a.name()
            .as_str()
            .as_bytes()
            .cmp(b.name().as_str().as_bytes())
    });
    let mut buf = Vec::new();
    for child in sorted {
        let (kind, name, node_hash) = child.parts();
        buf.push(u8::from(kind));
        let name_bytes = name.as_str().as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(node_hash);
    }
    DirNode::from_bytes(*blake3::hash(&buf).as_bytes())
}

/// Empty directory hash is `BLAKE3("")`.
pub fn empty_dir_node() -> DirNode {
    dir_node(&[])
}

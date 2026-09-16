//! Wire types and control-stream framing (`spec.md` §11).

use serde::{Deserialize, Serialize};

use crate::meta::{EntryKind, FileMetadata};

pub const PROTOCOL_VERSION: u16 = 1;
/// First Noise payload / preamble. Hyphae has no ALPN.
pub const PROTOCOL_PREAMBLE: &[u8] = b"arborsync-v1";
/// Maximum control frame (header + bincode body). Larger → disconnect.
pub const MAX_CONTROL_FRAME: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("control frame exceeds 1 MiB")]
    TooLarge,
    #[error("truncated frame")]
    Truncated,
    #[error("unsupported envelope version {0}")]
    UnsupportedVersion(u16),
    #[error("bincode error: {0}")]
    Bincode(String),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub version: u16,
    pub msg: ProtocolMessage,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CheckoutRef {
    pub id: String,
    pub central: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CheckoutAck {
    pub id: String,
    pub central: String,
    pub master_root: [u8; 32],
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub kind: EntryKind,
    pub node_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum BulkEncoding {
    Whole = 1,
    Delta = 2,
}

impl Serialize for BulkEncoding {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for BulkEncoding {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match u8::deserialize(deserializer)? {
            1 => Ok(Self::Whole),
            2 => Ok(Self::Delta),
            other => Err(serde::de::Error::custom(format!(
                "unknown bulk encoding {other}"
            ))),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BulkHeader {
    pub path: String,
    pub checkout_id: String,
    pub want_hash: [u8; 32],
    pub encoding: BulkEncoding,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ProtocolMessage {
    Subscribe {
        slave_id: String,
        checkouts: Vec<CheckoutRef>,
    },
    SubscribeAck {
        checkouts: Vec<CheckoutAck>,
    },
    SubscribeReject {
        reason: String,
        denied_centrals: Vec<String>,
    },
    RootReport {
        checkout_id: String,
        path: String,
        root: [u8; 32],
    },
    RootAck {
        checkout_id: String,
        path: String,
        matched: bool,
        master_root: [u8; 32],
    },
    DirListRequest {
        checkout_id: String,
        path: String,
    },
    DirListResponse {
        checkout_id: String,
        path: String,
        entries: Vec<DirEntry>,
    },
    FileAnnounce {
        checkout_id: String,
        path: String,
        new: FileMetadata,
        basis: Option<[u8; 32]>,
    },
    Delete {
        checkout_id: String,
        path: String,
        basis: [u8; 32],
    },
    Rename {
        checkout_id: String,
        from: String,
        to: String,
        from_basis: [u8; 32],
        to_new: FileMetadata,
    },
    CasAccept {
        checkout_id: String,
        path: String,
        file_node: Option<[u8; 32]>,
    },
    CasReject {
        checkout_id: String,
        path: String,
        current: Option<FileMetadata>,
    },
    SignatureRequest {
        checkout_id: String,
        path: String,
        want_hash: [u8; 32],
        signature: Vec<u8>,
    },
    Error {
        code: String,
        message: String,
    },
    Disconnect {
        reason: String,
    },
}

/// `u32be length || bincode(Envelope)`.
pub fn encode_control(msg: &ProtocolMessage) -> Result<Vec<u8>, FrameError> {
    let _ = msg;
    todo!("spec §11: u32be length || bincode(Envelope {{ version: 1, msg }})")
}

/// Decode one control frame. Returns the message and the number of bytes
/// consumed from `buf` (4 + length).
pub fn decode_control(buf: &[u8]) -> Result<(ProtocolMessage, usize), FrameError> {
    let _ = buf;
    todo!("spec §11: frame codec")
}

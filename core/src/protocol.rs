use serde::{Deserialize, Serialize};

use crate::hash::{ContentHash, FileNode, SubtreeRoot};
use crate::merkle::DirChild;
use crate::meta::FileMetadata;
use crate::path::CanonicalPath;

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
    pub central: CanonicalPath,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CheckoutAck {
    pub id: String,
    pub central: CanonicalPath,
    pub master_root: SubtreeRoot,
}

/// One entry of a `DirListResponse`. The wire form is
/// `{ name, kind, node_hash }`; the in-memory form is the variant that fixes
/// which brand of node hash the entry carries.
pub type DirEntry = DirChild;

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
    pub path: CanonicalPath,
    pub checkout_id: String,
    pub want_hash: ContentHash,
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
        denied_centrals: Vec<CanonicalPath>,
    },
    RootReport {
        checkout_id: String,
        path: CanonicalPath,
        root: SubtreeRoot,
    },
    RootAck {
        checkout_id: String,
        path: CanonicalPath,
        matched: bool,
        master_root: SubtreeRoot,
    },
    DirListRequest {
        checkout_id: String,
        path: CanonicalPath,
    },
    DirListResponse {
        checkout_id: String,
        path: CanonicalPath,
        entries: Vec<DirEntry>,
    },
    FileAnnounce {
        checkout_id: String,
        path: CanonicalPath,
        new: FileMetadata,
        basis: Option<FileNode>,
    },
    Delete {
        checkout_id: String,
        path: CanonicalPath,
        basis: FileNode,
    },
    Rename {
        checkout_id: String,
        from: CanonicalPath,
        to: CanonicalPath,
        from_basis: FileNode,
        to_new: FileMetadata,
    },
    CasAccept {
        checkout_id: String,
        path: CanonicalPath,
        file_node: Option<FileNode>,
    },
    CasReject {
        checkout_id: String,
        path: CanonicalPath,
        current: Option<FileMetadata>,
    },
    SignatureRequest {
        checkout_id: String,
        path: CanonicalPath,
        want_hash: ContentHash,
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

fn bincode_config() -> impl bincode::config::Config {
    bincode::config::standard()
}

pub(crate) fn encode_bincode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {
    bincode::serde::encode_to_vec(value, bincode_config()).map_err(|e| e.to_string())
}

pub(crate) fn decode_bincode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let (value, _) =
        bincode::serde::decode_from_slice(bytes, bincode_config()).map_err(|e| e.to_string())?;
    Ok(value)
}

/// `u32be length || bincode(Envelope)`.
pub fn encode_control(msg: &ProtocolMessage) -> Result<Vec<u8>, FrameError> {
    let env = Envelope {
        version: PROTOCOL_VERSION,
        msg: msg.clone(),
    };
    let payload = encode_bincode(&env).map_err(FrameError::Bincode)?;
    if payload.len() > MAX_CONTROL_FRAME {
        return Err(FrameError::TooLarge);
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Decode one control frame. Returns the message and the number of bytes
/// consumed from `buf` (4 + length).
pub fn decode_control(buf: &[u8]) -> Result<(ProtocolMessage, usize), FrameError> {
    if buf.len() < 4 {
        return Err(FrameError::Truncated);
    }
    let len = u32::from_be_bytes(buf[0..4].try_into().expect("4 bytes")) as usize;
    if len > MAX_CONTROL_FRAME {
        return Err(FrameError::TooLarge);
    }
    let total = 4 + len;
    if buf.len() < total {
        return Err(FrameError::Truncated);
    }
    let env: Envelope = decode_bincode(&buf[4..total]).map_err(FrameError::Bincode)?;
    if env.version != PROTOCOL_VERSION {
        return Err(FrameError::UnsupportedVersion(env.version));
    }
    Ok((env.msg, total))
}

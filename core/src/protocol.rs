use serde::{Deserialize, Serialize};

use crate::hash::{ContentHash, FileNode, SubtreeRoot};
use crate::merkle::DirChild;
use crate::meta::FileMetadata;
use crate::path::{CanonicalPath, EntryName};

pub const PROTOCOL_VERSION: u16 = 1;
/// First Noise payload / preamble. Hyphae has no ALPN.
pub const PROTOCOL_PREAMBLE: &[u8] = b"arborsync-v1";
/// Maximum control frame (header + bincode body). Larger → disconnect.
pub const MAX_CONTROL_FRAME: usize = 1024 * 1024;
pub const MAX_DIR_LIST_PAYLOAD: usize = MAX_CONTROL_FRAME / 4;
/// One bulk chunk. A larger chunk is refused before the receiver allocates it.
/// The logical body is the concatenation and may exceed this.
pub const MAX_BULK_CHUNK: usize = 16 * 1024 * 1024;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("control frame exceeds 1 MiB")]
    TooLarge,
    #[error("bulk chunk exceeds 16 MiB")]
    BulkTooLarge,
    #[error("truncated frame")]
    Truncated,
    #[error("unsupported envelope version {0}")]
    UnsupportedVersion(u16),
    #[error("trailing bytes inside control frame")]
    TrailingBytes,
    #[error("bincode error: {0}")]
    Bincode(String),
    #[error("bulk body length {got} does not match header size {want}")]
    BodySize { got: u64, want: u64 },
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
        after: Option<EntryName>,
    },
    DirListResponse {
        checkout_id: String,
        path: CanonicalPath,
        after: Option<EntryName>,
        entries: Vec<DirEntry>,
        more: bool,
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

fn wire_bincode_config() -> impl bincode::config::Config {
    bincode::config::standard()
}

fn encode_wire<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {
    bincode::serde::encode_to_vec(value, wire_bincode_config()).map_err(|e| e.to_string())
}

fn decode_wire<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<(T, usize), String> {
    bincode::serde::decode_from_slice(bytes, wire_bincode_config()).map_err(|e| e.to_string())
}

fn dir_entry_name_bytes(entry: &DirEntry) -> &[u8] {
    entry.name().as_str().as_bytes()
}

/// One `DirListResponse` that `encode_control` can send.
pub fn page_dir_list(
    checkout_id: String,
    path: CanonicalPath,
    after: Option<EntryName>,
    mut entries: Vec<DirEntry>,
) -> ProtocolMessage {
    entries.sort_by(|a, b| dir_entry_name_bytes(a).cmp(dir_entry_name_bytes(b)));
    if let Some(cursor) = &after {
        let cursor = cursor.as_str().as_bytes();
        entries.retain(|entry| dir_entry_name_bytes(entry) > cursor);
    }

    let fits =
        |slice: &[DirEntry], more: bool| match encode_control(&ProtocolMessage::DirListResponse {
            checkout_id: checkout_id.clone(),
            path: path.clone(),
            after: after.clone(),
            entries: slice.to_vec(),
            more,
        }) {
            Ok(frame) => frame.len() - 4 <= MAX_DIR_LIST_PAYLOAD,
            Err(_) => false,
        };

    if fits(&entries, false) {
        return ProtocolMessage::DirListResponse {
            checkout_id,
            path,
            after,
            entries,
            more: false,
        };
    }

    let mut lo = 0;
    let mut hi = entries.len();
    while lo + 1 < hi {
        let mid = (lo + hi) / 2;
        if fits(&entries[..mid], true) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    let n = if lo > 0 { lo } else { 1.min(entries.len()) };
    ProtocolMessage::DirListResponse {
        checkout_id,
        path,
        after,
        entries: entries[..n].to_vec(),
        more: true,
    }
}

/// `u32be length || bincode(version) || bincode(msg)`, the byte layout of
/// `bincode(Envelope)`.
pub fn encode_control(msg: &ProtocolMessage) -> Result<Vec<u8>, FrameError> {
    let mut payload = encode_wire(&PROTOCOL_VERSION).map_err(FrameError::Bincode)?;
    payload.extend(encode_wire(msg).map_err(FrameError::Bincode)?);
    if payload.len() > MAX_CONTROL_FRAME {
        return Err(FrameError::TooLarge);
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Decode one control frame. Returns the message and the number of bytes
/// consumed from `buf` (4 + length). The length prefix must cover exactly
/// `bincode(version) || bincode(msg)` with no trailing bytes inside it.
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
    let payload = &buf[4..total];
    let (version, ver_len): (u16, usize) = decode_wire(payload).map_err(FrameError::Bincode)?;
    if version != PROTOCOL_VERSION {
        return Err(FrameError::UnsupportedVersion(version));
    }
    let (msg, msg_len) = decode_wire(&payload[ver_len..]).map_err(FrameError::Bincode)?;
    if ver_len + msg_len != payload.len() {
        return Err(FrameError::TrailingBytes);
    }
    Ok((msg, total))
}

/// `u32be header_len || bincode(BulkHeader) || chunks`.
/// Each chunk is `u32be len || bytes`, `len` at most [`MAX_BULK_CHUNK`].
/// The concatenation of the chunks is `header.size` bytes.
pub fn encode_bulk(header: &BulkHeader, body: &[u8]) -> Result<Vec<u8>, FrameError> {
    encode_bulk_owned(header, body.to_vec())
}

/// Like [`encode_bulk`], but takes ownership of `body` and moves it into the
/// frame so a whole-file send does not keep a second full copy.
pub fn encode_bulk_owned(header: &BulkHeader, body: Vec<u8>) -> Result<Vec<u8>, FrameError> {
    encode_bulk_chunks_owned(header, body, MAX_BULK_CHUNK)
}

pub fn encode_bulk_chunks(
    header: &BulkHeader,
    body: &[u8],
    chunk_max: usize,
) -> Result<Vec<u8>, FrameError> {
    encode_bulk_chunks_owned(header, body.to_vec(), chunk_max)
}

fn encode_bulk_chunks_owned(
    header: &BulkHeader,
    mut body: Vec<u8>,
    chunk_max: usize,
) -> Result<Vec<u8>, FrameError> {
    if chunk_max == 0 || chunk_max > MAX_BULK_CHUNK {
        return Err(FrameError::BulkTooLarge);
    }
    if body.len() as u64 != header.size {
        return Err(FrameError::BodySize {
            got: body.len() as u64,
            want: header.size,
        });
    }
    let mut frame = bulk_header_frame(header)?;
    while !body.is_empty() {
        let n = body.len().min(chunk_max);
        frame.extend_from_slice(&(n as u32).to_be_bytes());
        frame.extend(body.drain(..n));
    }
    Ok(frame)
}

pub(crate) fn bulk_header_frame(header: &BulkHeader) -> Result<Vec<u8>, FrameError> {
    let payload = encode_wire(header).map_err(FrameError::Bincode)?;
    if payload.len() > MAX_CONTROL_FRAME {
        return Err(FrameError::TooLarge);
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub fn decode_bulk(buf: &[u8]) -> Result<(BulkHeader, Vec<u8>, usize), FrameError> {
    let (header, header_end) = decode_bulk_header(buf)?;
    let (body, consumed) = take_chunks(&buf[header_end..], header.size)?;
    Ok((header, body, header_end + consumed))
}

pub(crate) fn decode_bulk_header(buf: &[u8]) -> Result<(BulkHeader, usize), FrameError> {
    if buf.len() < 4 {
        return Err(FrameError::Truncated);
    }
    let len = u32::from_be_bytes(buf[0..4].try_into().expect("4 bytes")) as usize;
    if len > MAX_CONTROL_FRAME {
        return Err(FrameError::TooLarge);
    }
    let header_end = 4 + len;
    if buf.len() < header_end {
        return Err(FrameError::Truncated);
    }
    let (header, _): (BulkHeader, usize) =
        decode_wire(&buf[4..header_end]).map_err(FrameError::Bincode)?;
    Ok((header, header_end))
}

pub(crate) fn chunk_len(raw: u32) -> Result<usize, FrameError> {
    let len = raw as usize;
    if len == 0 || len > MAX_BULK_CHUNK {
        return Err(FrameError::BulkTooLarge);
    }
    Ok(len)
}

fn take_chunks(buf: &[u8], size: u64) -> Result<(Vec<u8>, usize), FrameError> {
    let mut body = Vec::new();
    let mut pos = 0;
    while (body.len() as u64) < size {
        if buf.len().saturating_sub(pos) < 4 {
            return Err(FrameError::Truncated);
        }
        let raw = u32::from_be_bytes(buf[pos..pos + 4].try_into().expect("4 bytes"));
        let n = chunk_len(raw)?;
        let next = pos + 4 + n;
        if buf.len() < next {
            return Err(FrameError::Truncated);
        }
        let end = (body.len() as u64)
            .checked_add(n as u64)
            .ok_or(FrameError::BulkTooLarge)?;
        if end > size {
            return Err(FrameError::BodySize {
                got: end,
                want: size,
            });
        }
        body.extend_from_slice(&buf[pos + 4..next]);
        pos = next;
    }
    Ok((body, pos))
}

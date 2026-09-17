use std::io::Cursor;

use copia::{Sync, SyncBuilder};

use crate::hash::ContentHash;
use crate::meta::{EntryKind, hash_bytes};
use crate::path::CanonicalPath;
use crate::protocol::{BulkEncoding, BulkHeader};

pub const MIN_DELTA_BASIS: u64 = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AskKind {
    Whole,
    Delta,
}

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error("source bytes do not hash to want_hash")]
    HashMismatch,
    #[error("copia: {0}")]
    Copia(String),
    #[error("bincode: {0}")]
    Bincode(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkTransfer {
    pub header: BulkHeader,
    pub body: Vec<u8>,
}

pub fn ask_kind(kind: EntryKind, basis_size: Option<u64>) -> AskKind {
    match kind {
        EntryKind::File => match basis_size {
            Some(n) if n >= MIN_DELTA_BASIS => AskKind::Delta,
            _ => AskKind::Whole,
        },
        EntryKind::Dir | EntryKind::Symlink => AskKind::Whole,
    }
}

pub fn signature_bytes(basis: &[u8]) -> Result<Vec<u8>, TransferError> {
    let signature = engine()
        .signature(Cursor::new(basis))
        .map_err(|err| TransferError::Copia(err.to_string()))?;
    encode_copia(&signature)
}

pub fn delta_bytes(source: &[u8], signature: &[u8]) -> Result<Vec<u8>, TransferError> {
    let signature: copia::Signature = decode_copia(signature)?;
    let delta = engine()
        .delta(Cursor::new(source), &signature)
        .map_err(|err| TransferError::Copia(err.to_string()))?;
    encode_copia(&delta)
}

pub fn patch_bytes(basis: &[u8], delta: &[u8]) -> Result<Vec<u8>, TransferError> {
    let delta: copia::Delta = decode_copia(delta)?;
    let mut out = Vec::new();
    engine()
        .patch(Cursor::new(basis), &delta, &mut out)
        .map_err(|err| TransferError::Copia(err.to_string()))?;
    Ok(out)
}

pub fn fulfill(
    checkout_id: impl Into<String>,
    path: CanonicalPath,
    want_hash: ContentHash,
    source: &[u8],
    signature: &[u8],
) -> Result<BulkTransfer, TransferError> {
    if hash_bytes(source) != want_hash {
        return Err(TransferError::HashMismatch);
    }
    if signature.is_empty() {
        return Ok(whole(checkout_id, path, want_hash, source));
    }
    match delta_bytes(source, signature) {
        Ok(body) => Ok(BulkTransfer {
            header: BulkHeader {
                checkout_id: checkout_id.into(),
                path,
                want_hash,
                encoding: BulkEncoding::Delta,
                size: body.len() as u64,
            },
            body,
        }),
        Err(_) => Ok(whole(checkout_id, path, want_hash, source)),
    }
}

pub fn reconstruct(
    encoding: BulkEncoding,
    body: &[u8],
    basis: Option<&[u8]>,
) -> Result<Vec<u8>, TransferError> {
    match encoding {
        BulkEncoding::Whole => Ok(body.to_vec()),
        BulkEncoding::Delta => patch_bytes(basis.unwrap_or_default(), body),
    }
}

pub fn signature_for(kind: EntryKind, live: Option<&[u8]>) -> Vec<u8> {
    match (ask_kind(kind, live.map(|b| b.len() as u64)), live) {
        (AskKind::Delta, Some(bytes)) => signature_bytes(bytes).unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn whole(
    checkout_id: impl Into<String>,
    path: CanonicalPath,
    want_hash: ContentHash,
    source: &[u8],
) -> BulkTransfer {
    BulkTransfer {
        header: BulkHeader {
            checkout_id: checkout_id.into(),
            path,
            want_hash,
            encoding: BulkEncoding::Whole,
            size: source.len() as u64,
        },
        body: source.to_vec(),
    }
}

fn engine() -> copia::CopiaSync {
    SyncBuilder::new().build()
}

fn copia_bincode() -> impl bincode::config::Config {
    bincode::config::standard()
}

fn encode_copia<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, TransferError> {
    bincode::serde::encode_to_vec(value, copia_bincode())
        .map_err(|err| TransferError::Bincode(err.to_string()))
}

fn decode_copia<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, TransferError> {
    let (value, _) = bincode::serde::decode_from_slice(bytes, copia_bincode())
        .map_err(|err| TransferError::Bincode(err.to_string()))?;
    Ok(value)
}

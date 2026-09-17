use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;
use x25519_dalek::{PublicKey, StaticSecret};

#[derive(Debug, Error)]
pub enum KeyError {
    #[error("key file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("key file already exists: {path}")]
    AlreadyExists { path: PathBuf },
    #[error("key file {path} is not 32 raw bytes or hex: + 64 hex digits")]
    BadSecret { path: PathBuf },
    #[error("expected hex: followed by 64 hex digits")]
    BadPin,
}

pub fn public_from_secret(secret: &[u8; 32]) -> [u8; 32] {
    *PublicKey::from(&StaticSecret::from(*secret)).as_bytes()
}

pub fn format_hex_key(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(4 + 64);
    out.push_str("hex:");
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

pub fn parse_hex_key(s: &str) -> Result<[u8; 32], KeyError> {
    let hex = s.strip_prefix("hex:").ok_or(KeyError::BadPin)?;
    if hex.len() != 64 || !hex.is_ascii() {
        return Err(KeyError::BadPin);
    }
    let raw = hex.as_bytes();
    let mut out = [0u8; 32];
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = nibble(raw[i * 2]).ok_or(KeyError::BadPin)?;
        let lo = nibble(raw[i * 2 + 1]).ok_or(KeyError::BadPin)?;
        *slot = (hi << 4) | lo;
    }
    Ok(out)
}

fn nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

pub fn write_static_key(path: impl AsRef<Path>) -> Result<[u8; 32], KeyError> {
    let path = path.as_ref();
    let secret = StaticSecret::random();
    let public = PublicKey::from(&secret);

    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(path).map_err(|source| {
        if source.kind() == ErrorKind::AlreadyExists {
            KeyError::AlreadyExists {
                path: path.to_path_buf(),
            }
        } else {
            KeyError::Io {
                path: path.to_path_buf(),
                source,
            }
        }
    })?;
    file.write_all(secret.as_bytes())
        .map_err(|source| KeyError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(*public.as_bytes())
}

pub fn read_static_key(path: impl AsRef<Path>) -> Result<[u8; 32], KeyError> {
    let path = path.as_ref();
    let bytes = fs::read(path).map_err(|source| KeyError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if let Ok(raw) = <[u8; 32]>::try_from(bytes.as_slice()) {
        return Ok(raw);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| KeyError::BadSecret {
        path: path.to_path_buf(),
    })?;
    parse_hex_key(text.trim()).map_err(|_| KeyError::BadSecret {
        path: path.to_path_buf(),
    })
}

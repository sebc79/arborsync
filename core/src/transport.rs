use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use quinn::{Connection, Endpoint, RecvStream, SendStream};
use quinn_hyphae::helper::{hyphae_client_endpoint, hyphae_server_endpoint};
use quinn_hyphae::{HandshakeBuilder, HyphaePeerIdentity, RustCryptoBackend};

use crate::protocol::{
    self, BulkHeader, FrameError, MAX_CONTROL_FRAME, PROTOCOL_PREAMBLE, ProtocolMessage,
};
use crate::transfer::BulkTransfer;

pub const NOISE_PATTERN: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("udp bind: {0}")]
    Bind(#[from] io::Error),
    #[error("hyphae: {0}")]
    Hyphae(String),
    #[error("connect: {0}")]
    Connect(String),
    #[error("peer identity missing after XX")]
    MissingIdentity,
    #[error("peer static key is not 32 bytes")]
    BadPeerKey,
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("stream: {0}")]
    Stream(String),
}

pub struct AttemptLimiter {
    window: Duration,
    max: u32,
    hits: HashMap<IpAddr, Vec<Instant>>,
}

impl AttemptLimiter {
    pub fn new(max_per_window: u32) -> Self {
        Self {
            window: Duration::from_secs(60),
            max: max_per_window,
            hits: HashMap::new(),
        }
    }

    pub fn allow(&mut self, ip: IpAddr, now: Instant) -> bool {
        let hits = self.hits.entry(ip).or_default();
        hits.retain(|t| now.duration_since(*t) < self.window);
        if hits.len() as u32 >= self.max {
            return false;
        }
        hits.push(now);
        true
    }
}

pub fn listen(addr: SocketAddr, secret: &[u8; 32]) -> Result<Endpoint, TransportError> {
    let socket = UdpSocket::bind(addr)?;
    let config = HandshakeBuilder::new(NOISE_PATTERN)
        .with_static_key(secret)
        .with_prologue(PROTOCOL_PREAMBLE)
        .build(RustCryptoBackend)
        .map_err(|err| TransportError::Hyphae(err.to_string()))?;
    hyphae_server_endpoint(config, None, socket)
        .map_err(|err| TransportError::Hyphae(err.to_string()))
}

pub fn client_endpoint(secret: &[u8; 32]) -> Result<Endpoint, TransportError> {
    let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0)))?;
    let config = HandshakeBuilder::new(NOISE_PATTERN)
        .with_static_key(secret)
        .with_prologue(PROTOCOL_PREAMBLE)
        .build(RustCryptoBackend)
        .map_err(|err| TransportError::Hyphae(err.to_string()))?;
    hyphae_client_endpoint(config, None, socket)
        .map_err(|err| TransportError::Hyphae(err.to_string()))
}

pub async fn connect(endpoint: &Endpoint, addr: SocketAddr) -> Result<Connection, TransportError> {
    endpoint
        .connect(addr, "")
        .map_err(|err| TransportError::Connect(err.to_string()))?
        .await
        .map_err(|err| TransportError::Connect(err.to_string()))
}

pub fn peer_static_key(conn: &Connection) -> Result<[u8; 32], TransportError> {
    let identity = conn
        .peer_identity()
        .ok_or(TransportError::MissingIdentity)?
        .downcast::<HyphaePeerIdentity>()
        .map_err(|_| TransportError::MissingIdentity)?;
    let raw = identity
        .remote_public
        .ok_or(TransportError::MissingIdentity)?;
    <[u8; 32]>::try_from(raw.as_slice()).map_err(|_| TransportError::BadPeerKey)
}

pub async fn open_control(conn: &Connection) -> Result<(SendStream, RecvStream), TransportError> {
    conn.open_bi()
        .await
        .map_err(|err| TransportError::Stream(err.to_string()))
}

pub async fn accept_control(conn: &Connection) -> Result<(SendStream, RecvStream), TransportError> {
    conn.accept_bi()
        .await
        .map_err(|err| TransportError::Stream(err.to_string()))
}

pub async fn write_control(
    send: &mut SendStream,
    msg: &ProtocolMessage,
) -> Result<(), TransportError> {
    let frame = protocol::encode_control(msg)?;
    send.write_all(&frame)
        .await
        .map_err(|err| TransportError::Stream(err.to_string()))
}

pub async fn read_control(recv: &mut RecvStream) -> Result<ProtocolMessage, TransportError> {
    let mut header = [0u8; 4];
    recv.read_exact(&mut header)
        .await
        .map_err(|err| TransportError::Stream(err.to_string()))?;
    let len = u32::from_be_bytes(header) as usize;
    if len > MAX_CONTROL_FRAME {
        return Err(TransportError::Frame(FrameError::TooLarge));
    }
    let mut payload = vec![0u8; len];
    recv.read_exact(&mut payload)
        .await
        .map_err(|err| TransportError::Stream(err.to_string()))?;
    let mut frame = Vec::with_capacity(4 + len);
    frame.extend_from_slice(&header);
    frame.extend_from_slice(&payload);
    Ok(protocol::decode_control(&frame)?.0)
}

pub async fn write_bulk(conn: &Connection, xfer: &BulkTransfer) -> Result<(), TransportError> {
    let frame = protocol::encode_bulk(&xfer.header, &xfer.body)?;
    let mut send = conn
        .open_uni()
        .await
        .map_err(|err| TransportError::Stream(err.to_string()))?;
    send.write_all(&frame)
        .await
        .map_err(|err| TransportError::Stream(err.to_string()))?;
    send.finish()
        .map_err(|err| TransportError::Stream(err.to_string()))?;
    Ok(())
}

pub async fn read_bulk(recv: &mut RecvStream) -> Result<(BulkHeader, Vec<u8>), TransportError> {
    let mut len_bytes = [0u8; 4];
    recv.read_exact(&mut len_bytes)
        .await
        .map_err(|err| TransportError::Stream(err.to_string()))?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > MAX_CONTROL_FRAME {
        return Err(TransportError::Frame(FrameError::TooLarge));
    }
    let mut payload = vec![0u8; len];
    recv.read_exact(&mut payload)
        .await
        .map_err(|err| TransportError::Stream(err.to_string()))?;
    let (header, _): (BulkHeader, usize) =
        bincode::serde::decode_from_slice(&payload, bincode::config::standard())
            .map_err(|err| TransportError::Frame(FrameError::Bincode(err.to_string())))?;
    let mut body = vec![0u8; header.size as usize];
    if !body.is_empty() {
        recv.read_exact(&mut body)
            .await
            .map_err(|err| TransportError::Stream(err.to_string()))?;
    }
    Ok((header, body))
}

pub async fn accept_bulk(conn: &Connection) -> Result<(BulkHeader, Vec<u8>), TransportError> {
    let mut recv = conn
        .accept_uni()
        .await
        .map_err(|err| TransportError::Stream(err.to_string()))?;
    read_bulk(&mut recv).await
}

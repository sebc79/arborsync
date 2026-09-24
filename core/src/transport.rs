use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quinn::{Connection, Endpoint, RecvStream, SendStream, TransportConfig, VarInt};
use quinn_hyphae::helper::{hyphae_client_endpoint, hyphae_server_endpoint};
use quinn_hyphae::{HandshakeBuilder, HyphaePeerIdentity, RustCryptoBackend};
use tokio::sync::mpsc;

use crate::protocol::{
    self, BulkHeader, FrameError, MAX_CONTROL_FRAME, PROTOCOL_PREAMBLE, ProtocolMessage,
};
use crate::transfer::BulkTransfer;

pub const NOISE_PATTERN: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

const MAX_IDLE_MS: u32 = 30_000;
const KEEP_ALIVE: Duration = Duration::from_secs(10);

fn session_transport_config() -> Arc<TransportConfig> {
    let mut cfg = TransportConfig::default();
    cfg.max_idle_timeout(Some(VarInt::from_u32(MAX_IDLE_MS).into()));
    cfg.keep_alive_interval(Some(KEEP_ALIVE));
    Arc::new(cfg)
}

pub fn stream_err(err: impl std::error::Error) -> TransportError {
    let mut msg = err.to_string();
    let mut cur = err.source();
    while let Some(src) = cur {
        msg.push_str(": ");
        msg.push_str(&src.to_string());
        cur = src.source();
    }
    TransportError::Stream(msg)
}

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
    #[error("session closed")]
    Closed,
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

    pub fn set_max(&mut self, max: u32) {
        self.max = max;
    }

    pub fn allow(&mut self, ip: IpAddr, now: Instant) -> bool {
        self.prune(now);
        if self.max == 0 {
            return false;
        }
        let hits = self.hits.entry(ip).or_default();
        if hits.len() as u32 >= self.max {
            return false;
        }
        hits.push(now);
        true
    }

    fn prune(&mut self, now: Instant) {
        self.hits.retain(|_, hits| {
            hits.retain(|t| now.duration_since(*t) < self.window);
            !hits.is_empty()
        });
    }

    pub fn limited(&mut self, ip: IpAddr, now: Instant) -> bool {
        self.prune(now);
        self.hits
            .get(&ip)
            .is_some_and(|hits| hits.len() as u32 >= self.max)
    }
}

pub fn listen(addr: SocketAddr, secret: &[u8; 32]) -> Result<Endpoint, TransportError> {
    let socket = UdpSocket::bind(addr)?;
    let config = HandshakeBuilder::new(NOISE_PATTERN)
        .with_static_key(secret)
        .with_prologue(PROTOCOL_PREAMBLE)
        .build(RustCryptoBackend)
        .map_err(|err| TransportError::Hyphae(err.to_string()))?;
    hyphae_server_endpoint(config, Some(session_transport_config()), socket)
        .map_err(|err| TransportError::Hyphae(err.to_string()))
}

pub fn client_endpoint(secret: &[u8; 32]) -> Result<Endpoint, TransportError> {
    let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0)))?;
    let config = HandshakeBuilder::new(NOISE_PATTERN)
        .with_static_key(secret)
        .with_prologue(PROTOCOL_PREAMBLE)
        .build(RustCryptoBackend)
        .map_err(|err| TransportError::Hyphae(err.to_string()))?;
    hyphae_client_endpoint(config, Some(session_transport_config()), socket)
        .map_err(|err| TransportError::Hyphae(err.to_string()))
}

pub async fn connect(endpoint: &Endpoint, addr: SocketAddr) -> Result<Connection, TransportError> {
    endpoint
        .connect(addr, "")
        .map_err(|err| TransportError::Connect(err.to_string()))?
        .await
        .map_err(|err| TransportError::Connect(err.to_string()))
}

pub trait Transport: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type ControlSend: Send;
    type ControlRecv: Send;

    fn peer_static_key(&self) -> Result<[u8; 32], Self::Error>;
    fn close(&self);

    fn open_control(
        &self,
    ) -> impl Future<Output = Result<(Self::ControlSend, Self::ControlRecv), Self::Error>> + Send;
    fn accept_control(
        &self,
    ) -> impl Future<Output = Result<(Self::ControlSend, Self::ControlRecv), Self::Error>> + Send;
    fn write_control(
        send: &mut Self::ControlSend,
        msg: &ProtocolMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn read_control(
        recv: &mut Self::ControlRecv,
    ) -> impl Future<Output = Result<ProtocolMessage, Self::Error>> + Send;
    fn write_bulk(
        &self,
        xfer: &BulkTransfer,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn accept_bulk(
        &self,
    ) -> impl Future<Output = Result<(BulkHeader, Vec<u8>), Self::Error>> + Send;

    /// Diagnostics beside the session, never inside it. `false` means the peer
    /// cannot take this frame, which is a missing measurement and not an error.
    fn send_datagram(&self, bytes: &[u8]) -> bool;

    /// Pends forever when the transport has no datagrams, so a `select!` arm
    /// holding this future is inert instead of a hangup.
    fn recv_datagram(&self) -> impl Future<Output = Vec<u8>> + Send;
}

struct Closable<T> {
    tx: Mutex<Option<mpsc::UnboundedSender<T>>>,
}

impl<T> Closable<T> {
    fn new(tx: mpsc::UnboundedSender<T>) -> Arc<Self> {
        Arc::new(Self {
            tx: Mutex::new(Some(tx)),
        })
    }

    fn send(&self, value: T) -> Result<(), TransportError> {
        self.tx
            .lock()
            .map_err(|_| TransportError::Stream("lock poisoned".into()))?
            .as_ref()
            .ok_or(TransportError::Closed)?
            .send(value)
            .map_err(|_| TransportError::Closed)
    }

    fn close(&self) {
        if let Ok(mut slot) = self.tx.lock() {
            *slot = None;
        }
    }
}

pub struct MemoryControlSend {
    out: Arc<Closable<ProtocolMessage>>,
}

pub struct MemoryControlRecv {
    rx: mpsc::UnboundedReceiver<ProtocolMessage>,
    closed: Arc<AtomicBool>,
}

/// One side's end of a channel pair: what it sends on, what it receives from.
type Lane<T> = (mpsc::UnboundedSender<T>, mpsc::UnboundedReceiver<T>);

pub struct MemoryTransport {
    peer_key: [u8; 32],
    opener: bool,
    closed: Arc<AtomicBool>,
    control: Mutex<Option<(MemoryControlSend, MemoryControlRecv)>>,
    control_out: Arc<Closable<ProtocolMessage>>,
    bulk_out: Arc<Closable<BulkTransfer>>,
    bulk_in: tokio::sync::Mutex<mpsc::UnboundedReceiver<BulkTransfer>>,
    datagram_out: Option<Arc<Closable<Vec<u8>>>>,
    datagram_in: Option<tokio::sync::Mutex<mpsc::UnboundedReceiver<Vec<u8>>>>,
}

impl MemoryTransport {
    /// No datagrams, which is the degraded path every other in-memory test runs
    /// on.
    pub fn pair(a_public: [u8; 32], b_public: [u8; 32]) -> (Self, Self) {
        Self::wire(a_public, b_public, false)
    }

    pub fn pair_with_datagrams(a_public: [u8; 32], b_public: [u8; 32]) -> (Self, Self) {
        Self::wire(a_public, b_public, true)
    }

    fn wire(a_public: [u8; 32], b_public: [u8; 32], datagrams: bool) -> (Self, Self) {
        let closed = Arc::new(AtomicBool::new(false));
        let (left_ctrl_tx, right_ctrl_rx) = mpsc::unbounded_channel();
        let (right_ctrl_tx, left_ctrl_rx) = mpsc::unbounded_channel();
        let (left_bulk_tx, right_bulk_rx) = mpsc::unbounded_channel();
        let (right_bulk_tx, left_bulk_rx) = mpsc::unbounded_channel();
        let (left_dgram, right_dgram) = if datagrams {
            let (left_tx, right_rx) = mpsc::unbounded_channel();
            let (right_tx, left_rx) = mpsc::unbounded_channel();
            (Some((left_tx, left_rx)), Some((right_tx, right_rx)))
        } else {
            (None, None)
        };
        let left = Self::side(
            b_public,
            true,
            Arc::clone(&closed),
            (left_ctrl_tx, left_ctrl_rx),
            (left_bulk_tx, left_bulk_rx),
            left_dgram,
        );
        let right = Self::side(
            a_public,
            false,
            closed,
            (right_ctrl_tx, right_ctrl_rx),
            (right_bulk_tx, right_bulk_rx),
            right_dgram,
        );
        (left, right)
    }

    fn side(
        peer_key: [u8; 32],
        opener: bool,
        closed: Arc<AtomicBool>,
        control_lane: Lane<ProtocolMessage>,
        bulk_lane: Lane<BulkTransfer>,
        datagrams: Option<Lane<Vec<u8>>>,
    ) -> Self {
        let (ctrl_tx, ctrl_rx) = control_lane;
        let (bulk_tx, bulk_rx) = bulk_lane;
        let control_out = Closable::new(ctrl_tx);
        let (datagram_out, datagram_in) = match datagrams {
            Some((tx, rx)) => (Some(Closable::new(tx)), Some(tokio::sync::Mutex::new(rx))),
            None => (None, None),
        };
        Self {
            peer_key,
            opener,
            closed: Arc::clone(&closed),
            control: Mutex::new(Some((
                MemoryControlSend {
                    out: Arc::clone(&control_out),
                },
                MemoryControlRecv {
                    rx: ctrl_rx,
                    closed: Arc::clone(&closed),
                },
            ))),
            control_out,
            bulk_out: Closable::new(bulk_tx),
            bulk_in: tokio::sync::Mutex::new(bulk_rx),
            datagram_out,
            datagram_in,
        }
    }

    fn take_control(
        &self,
        as_opener: bool,
    ) -> Result<(MemoryControlSend, MemoryControlRecv), TransportError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Closed);
        }
        if self.opener != as_opener {
            return Err(TransportError::Stream("wrong control role".into()));
        }
        self.control
            .lock()
            .map_err(|_| TransportError::Stream("lock poisoned".into()))?
            .take()
            .ok_or_else(|| TransportError::Stream("control already taken".into()))
    }
}

impl Transport for MemoryTransport {
    type Error = TransportError;
    type ControlSend = MemoryControlSend;
    type ControlRecv = MemoryControlRecv;

    fn peer_static_key(&self) -> Result<[u8; 32], Self::Error> {
        Ok(self.peer_key)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.control_out.close();
        self.bulk_out.close();
        if let Some(out) = &self.datagram_out {
            out.close();
        }
    }

    fn open_control(
        &self,
    ) -> impl Future<Output = Result<(Self::ControlSend, Self::ControlRecv), Self::Error>> + Send
    {
        std::future::ready(self.take_control(true))
    }

    fn accept_control(
        &self,
    ) -> impl Future<Output = Result<(Self::ControlSend, Self::ControlRecv), Self::Error>> + Send
    {
        std::future::ready(self.take_control(false))
    }

    fn write_control(
        send: &mut Self::ControlSend,
        msg: &ProtocolMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(send.out.send(msg.clone()))
    }

    async fn read_control(recv: &mut Self::ControlRecv) -> Result<ProtocolMessage, Self::Error> {
        if recv.closed.load(Ordering::Acquire) {
            return Err(TransportError::Closed);
        }
        recv.rx.recv().await.ok_or(TransportError::Closed)
    }

    fn write_bulk(
        &self,
        xfer: &BulkTransfer,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(if self.closed.load(Ordering::Acquire) {
            Err(TransportError::Closed)
        } else {
            self.bulk_out.send(xfer.clone())
        })
    }

    async fn accept_bulk(&self) -> Result<(BulkHeader, Vec<u8>), Self::Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Closed);
        }
        let xfer = self
            .bulk_in
            .lock()
            .await
            .recv()
            .await
            .ok_or(TransportError::Closed)?;
        Ok((xfer.header, xfer.body))
    }

    fn send_datagram(&self, bytes: &[u8]) -> bool {
        self.datagram_out
            .as_ref()
            .is_some_and(|out| out.send(bytes.to_vec()).is_ok())
    }

    async fn recv_datagram(&self) -> Vec<u8> {
        let Some(incoming) = self.datagram_in.as_ref() else {
            return std::future::pending().await;
        };
        match incoming.lock().await.recv().await {
            Some(frame) => frame,
            None => std::future::pending().await,
        }
    }
}

pub async fn read_bulk(recv: &mut RecvStream) -> Result<(BulkHeader, Vec<u8>), TransportError> {
    let mut len_bytes = [0u8; 4];
    recv.read_exact(&mut len_bytes).await.map_err(stream_err)?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > MAX_CONTROL_FRAME {
        return Err(TransportError::Frame(FrameError::TooLarge));
    }
    let mut payload = vec![0u8; len];
    recv.read_exact(&mut payload).await.map_err(stream_err)?;
    let (header, _): (BulkHeader, usize) =
        bincode::serde::decode_from_slice(&payload, bincode::config::standard())
            .map_err(|err| TransportError::Frame(FrameError::Bincode(err.to_string())))?;
    let mut body = vec![0u8; header.size as usize];
    if !body.is_empty() {
        recv.read_exact(&mut body).await.map_err(stream_err)?;
    }
    Ok((header, body))
}

fn control_summary(msg: &ProtocolMessage) -> String {
    match msg {
        ProtocolMessage::DirListResponse {
            path,
            entries,
            more,
            after,
            ..
        } => format!(
            "DirListResponse path={} entries={} more={more} after={}",
            path.as_str(),
            entries.len(),
            after.as_ref().map(|name| name.as_str()).unwrap_or("-")
        ),
        ProtocolMessage::DirListRequest { path, after, .. } => format!(
            "DirListRequest path={} after={}",
            path.as_str(),
            after.as_ref().map(|name| name.as_str()).unwrap_or("-")
        ),
        ProtocolMessage::FileAnnounce { path, .. } => {
            format!("FileAnnounce path={}", path.as_str())
        }
        other => format!("{other:?}"),
    }
}

pub struct ControlReader {
    stream: RecvStream,
    pending: Vec<u8>,
}

impl ControlReader {
    fn new(stream: RecvStream) -> Self {
        Self {
            stream,
            pending: Vec::new(),
        }
    }

    async fn fill(&mut self, need: usize) -> Result<(), TransportError> {
        while self.pending.len() < need {
            let mut buf = [0u8; 8192];
            match self.stream.read(&mut buf).await.map_err(stream_err)? {
                None | Some(0) => return Err(TransportError::Closed),
                Some(n) => self.pending.extend_from_slice(&buf[..n]),
            }
        }
        Ok(())
    }
}

impl Transport for Connection {
    type Error = TransportError;
    type ControlSend = SendStream;
    type ControlRecv = ControlReader;

    fn peer_static_key(&self) -> Result<[u8; 32], Self::Error> {
        let identity = self
            .peer_identity()
            .ok_or(TransportError::MissingIdentity)?
            .downcast::<HyphaePeerIdentity>()
            .map_err(|_| TransportError::MissingIdentity)?;
        let raw = identity
            .remote_public
            .ok_or(TransportError::MissingIdentity)?;
        <[u8; 32]>::try_from(raw.as_slice()).map_err(|_| TransportError::BadPeerKey)
    }

    fn close(&self) {
        Connection::close(self, 0u32.into(), b"");
    }

    async fn open_control(&self) -> Result<(Self::ControlSend, Self::ControlRecv), Self::Error> {
        let (send, recv) = self.open_bi().await.map_err(stream_err)?;
        Ok((send, ControlReader::new(recv)))
    }

    async fn accept_control(&self) -> Result<(Self::ControlSend, Self::ControlRecv), Self::Error> {
        let (send, recv) = self.accept_bi().await.map_err(stream_err)?;
        Ok((send, ControlReader::new(recv)))
    }

    async fn write_control(
        send: &mut Self::ControlSend,
        msg: &ProtocolMessage,
    ) -> Result<(), Self::Error> {
        let frame = match protocol::encode_control(msg) {
            Ok(frame) => frame,
            Err(err) => {
                log::warn!("encode_control failed ({err}) for {}", control_summary(msg));
                return Err(TransportError::Frame(err));
            }
        };
        send.write_all(&frame).await.map_err(stream_err)
    }

    async fn read_control(recv: &mut Self::ControlRecv) -> Result<ProtocolMessage, Self::Error> {
        recv.fill(4).await?;
        let header: [u8; 4] = recv.pending[..4].try_into().expect("4 bytes");
        let len = u32::from_be_bytes(header) as usize;
        if len > MAX_CONTROL_FRAME {
            log::warn!(
                "incoming control length {len} exceeds 1 MiB (header {:02x} {:02x} {:02x} {:02x})",
                header[0],
                header[1],
                header[2],
                header[3]
            );
            return Err(TransportError::Frame(FrameError::TooLarge));
        }
        recv.fill(4 + len).await?;
        let frame: Vec<u8> = recv.pending.drain(..4 + len).collect();
        Ok(protocol::decode_control(&frame)?.0)
    }

    async fn write_bulk(&self, xfer: &BulkTransfer) -> Result<(), Self::Error> {
        let frame = protocol::encode_bulk(&xfer.header, &xfer.body)?;
        let mut send = self.open_uni().await.map_err(stream_err)?;
        send.write_all(&frame).await.map_err(stream_err)?;
        send.finish().map_err(stream_err)?;
        Ok(())
    }

    async fn accept_bulk(&self) -> Result<(BulkHeader, Vec<u8>), Self::Error> {
        let mut recv = self.accept_uni().await.map_err(stream_err)?;
        read_bulk(&mut recv).await
    }

    fn send_datagram(&self, bytes: &[u8]) -> bool {
        if self.max_datagram_size().is_none_or(|max| max < bytes.len()) {
            return false;
        }
        Connection::send_datagram(self, bytes.to_vec().into()).is_ok()
    }

    async fn recv_datagram(&self) -> Vec<u8> {
        match self.read_datagram().await {
            Ok(frame) => frame.to_vec(),
            Err(_) => std::future::pending().await,
        }
    }
}

#[cfg(test)]
mod attempt_limiter_tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::{Duration, Instant};

    use super::AttemptLimiter;

    #[test]
    fn drops_an_address_once_its_hits_leave_the_window() {
        let mut limiter = AttemptLimiter::new(2);
        let idle = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let t0 = Instant::now();
        assert!(limiter.allow(idle, t0));
        assert_eq!(limiter.hits.len(), 1);
        let later = t0 + Duration::from_secs(60);
        let other = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        assert!(limiter.allow(other, later));
        assert!(!limiter.hits.contains_key(&idle));
        assert_eq!(limiter.hits.len(), 1);
        assert!(!limiter.limited(idle, later));
    }
}

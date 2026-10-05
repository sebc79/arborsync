use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;

use crate::config::is_slave_id;
use crate::protocol::{FrameError, MAX_CONTROL_FRAME, ProtocolMessage, decode_control};
use crate::status::{Flow, Health, Queues, classify};

const REPORT_PERIOD: Duration = Duration::from_secs(5);

pub fn report_period() -> Duration {
    REPORT_PERIOD
}

const PEER_MAGIC: &[u8] = b"asp1";
const VIEW_MAGIC: &[u8] = b"aspv";
const TAG_DIRECTORY: u8 = 1;
const TAG_REPORT: u8 = 2;
const TAG_WAITING: u8 = 0;
const TAG_CURRENT: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerName(String);

impl PeerName {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn parse(raw: &str) -> Option<Self> {
        is_slave_id(raw).then(|| Self(raw.to_string()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presence {
    Connected,
    Disconnected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pace {
    Idle,
    Busy,
    Stuck,
}

impl Pace {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Busy => "busy",
            Self::Stuck => "stuck",
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::Idle => 1,
            Self::Busy => 2,
            Self::Stuck => 3,
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Idle),
            2 => Some(Self::Busy),
            3 => Some(Self::Stuck),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueueDepth(u32);

impl QueueDepth {
    pub fn get(self) -> u32 {
        self.0
    }

    pub(crate) fn from_units(units: usize) -> Self {
        Self(u32::try_from(units).unwrap_or(u32::MAX))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerSync {
    pub name: PeerName,
    pub presence: Presence,
    pub pace: Pace,
    pub depth: QueueDepth,
    pub fresh: bool,
}

/// `Waiting` means this link has no directory yet.
/// `Current` with an empty vec means the directory arrived and named nobody else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerView {
    Waiting,
    Current(Vec<PeerSync>),
}

impl PeerView {
    pub fn get(&self, name: &str) -> Option<&PeerSync> {
        let PeerView::Current(peers) = self else {
            return None;
        };
        peers.iter().find(|peer| peer.name.as_str() == name)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("peer socket io: {0}")]
    Io(#[from] std::io::Error),
    #[error("peer socket closed before a full snapshot")]
    Closed,
    #[error("peer socket snapshot was malformed")]
    BadFrame,
}

pub fn query_peers(socket: &Path) -> Result<PeerView, PeerError> {
    let mut stream = UnixStream::connect(socket)?;
    let mut len_buf = [0u8; 4];
    read_exact(&mut stream, &mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_CONTROL_FRAME || len < VIEW_MAGIC.len() {
        return Err(PeerError::BadFrame);
    }
    let mut payload = vec![0u8; len];
    read_exact(&mut stream, &mut payload)?;
    let Some(body) = payload.strip_prefix(VIEW_MAGIC) else {
        return Err(PeerError::BadFrame);
    };
    decode_view(body).map_err(|_| PeerError::BadFrame)
}

fn read_exact(stream: &mut UnixStream, buf: &mut [u8]) -> Result<(), PeerError> {
    match stream.read_exact(buf) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Err(PeerError::Closed),
        Err(err) => Err(PeerError::Io(err)),
    }
}

pub(crate) fn serve_peers(
    socket: PathBuf,
    rx: tokio::sync::watch::Receiver<PeerView>,
) -> io::Result<tokio::task::JoinHandle<()>> {
    if let Ok(meta) = fs::symlink_metadata(&socket) {
        if meta.file_type().is_socket() {
            fs::remove_file(&socket)?;
        }
    }
    let listener = std::os::unix::net::UnixListener::bind(&socket)?;
    let mut perms = fs::metadata(&socket)?.permissions();
    perms.set_mode(0o600);
    fs::set_permissions(&socket, perms)?;
    listener.set_nonblocking(true)?;
    let listener = UnixListener::from_std(listener)?;
    Ok(tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let view = rx.borrow().clone();
                    tokio::spawn(async move {
                        if let Ok(bytes) = encode_snapshot(&view) {
                            let _ = stream.write_all(&bytes).await;
                        }
                    });
                }
                Err(err) => {
                    log::warn!("peer socket accept: {err}");
                    break;
                }
            }
        }
    }))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SelfReport {
    pub seq: u64,
    pub pace: Pace,
    pub depth: QueueDepth,
}

pub(crate) fn self_report(seq: u64, flow: &Flow, queues: &Queues, error_count: u64) -> SelfReport {
    let pace = match classify(flow, queues, error_count) {
        Health::Idle => Pace::Idle,
        Health::Busy => Pace::Busy,
        Health::Stuck | Health::Failed => Pace::Stuck,
    };
    SelfReport {
        seq,
        pace,
        depth: queue_depth(queues),
    }
}

pub(crate) fn queue_depth(queues: &Queues) -> QueueDepth {
    let units = queues
        .pending
        .saturating_add(queues.pending_pulls)
        .saturating_add(queues.pending_renames)
        .saturating_add(queues.parked)
        .saturating_add(queues.sending)
        .saturating_add(queues.work);
    QueueDepth::from_units(units)
}

struct Card {
    presence: Presence,
    reported: Pace,
    depth: QueueDepth,
    generation: u64,
    last_seq: u64,
    accept_restart: bool,
    heard_at: Option<Instant>,
}

impl Card {
    fn absent() -> Self {
        Self {
            presence: Presence::Disconnected,
            reported: Pace::Idle,
            depth: QueueDepth::from_units(0),
            generation: 0,
            last_seq: 0,
            accept_restart: false,
            heard_at: None,
        }
    }
}

#[derive(Default)]
pub(crate) struct PeerBoard {
    cards: BTreeMap<PeerName, Card>,
}

impl PeerBoard {
    pub(crate) fn sync_acl(&mut self, ids: &[PeerName]) {
        let keep: BTreeMap<PeerName, ()> = ids.iter().map(|id| (id.clone(), ())).collect();
        self.cards.retain(|id, _| keep.contains_key(id));
        for id in ids {
            self.cards.entry(id.clone()).or_insert_with(Card::absent);
        }
    }

    pub(crate) fn note_session(&mut self, id: &PeerName, generation: u64, _now: Instant) {
        let Some(card) = self.cards.get_mut(id) else {
            return;
        };
        card.presence = Presence::Connected;
        card.generation = generation;
        card.accept_restart = true;
    }

    pub(crate) fn note_disconnect(&mut self, id: &PeerName) {
        let Some(card) = self.cards.get_mut(id) else {
            return;
        };
        card.presence = Presence::Disconnected;
        card.generation = 0;
        card.accept_restart = false;
    }

    pub(crate) fn observe_report(
        &mut self,
        id: &PeerName,
        generation: u64,
        report: SelfReport,
        now: Instant,
    ) -> bool {
        let Some(card) = self.cards.get_mut(id) else {
            return false;
        };
        if generation == 0 || card.generation != generation {
            return false;
        }
        if !card.accept_restart {
            if report.seq == card.last_seq || report.seq < card.last_seq {
                return false;
            }
        }
        let before = snapshot(id, card, now);
        card.accept_restart = false;
        card.reported = report.pace;
        card.depth = report.depth;
        card.last_seq = report.seq;
        card.heard_at = Some(now);
        snapshot(id, card, now) != before
    }

    pub(crate) fn views(&self, now: Instant) -> BTreeMap<PeerName, PeerView> {
        self.cards
            .keys()
            .map(|name| {
                let peers = self
                    .cards
                    .iter()
                    .filter(|(id, _)| *id != name)
                    .map(|(id, card)| snapshot(id, card, now))
                    .collect();
                (name.clone(), PeerView::Current(peers))
            })
            .collect()
    }
}

fn snapshot(name: &PeerName, card: &Card, now: Instant) -> PeerSync {
    PeerSync {
        name: name.clone(),
        presence: card.presence,
        pace: card.reported,
        depth: card.depth,
        fresh: card.presence == Presence::Connected
            && card
                .heard_at
                .is_some_and(|heard| now.saturating_duration_since(heard) < REPORT_PERIOD * 3),
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Outbound {
    Directory(PeerView),
    Report(SelfReport),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Inbound {
    File(ProtocolMessage),
    Directory(PeerView),
    Report(SelfReport),
}

pub(crate) fn encode_outbound(frame: &Outbound) -> Result<Vec<u8>, FrameError> {
    match frame {
        Outbound::Directory(view) => peer_frame(TAG_DIRECTORY, &encode_view_body(view)?),
        Outbound::Report(report) => peer_frame(TAG_REPORT, &encode_report(report)),
    }
}

pub(crate) fn decode_inbound(buf: &[u8]) -> Result<Inbound, FrameError> {
    let payload = payload(buf)?;
    if let Some(body) = payload.strip_prefix(PEER_MAGIC) {
        return decode_peer(body).map_err(|_| FrameError::BadPeer);
    }
    decode_control(buf).map(|(msg, _)| Inbound::File(msg))
}

fn encode_snapshot(view: &PeerView) -> Result<Vec<u8>, FrameError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(VIEW_MAGIC);
    payload.extend(encode_view_body(view)?);
    length_prefix(&payload)
}

fn peer_frame(tag: u8, body: &[u8]) -> Result<Vec<u8>, FrameError> {
    let mut payload = Vec::with_capacity(PEER_MAGIC.len() + 1 + body.len());
    payload.extend_from_slice(PEER_MAGIC);
    payload.push(tag);
    payload.extend_from_slice(body);
    length_prefix(&payload)
}

fn length_prefix(payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    if payload.len() > MAX_CONTROL_FRAME {
        return Err(FrameError::TooLarge);
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn payload(buf: &[u8]) -> Result<&[u8], FrameError> {
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
    Ok(&buf[4..total])
}

fn decode_peer(body: &[u8]) -> Result<Inbound, ()> {
    let (tag, rest) = body.split_first().ok_or(())?;
    match *tag {
        TAG_DIRECTORY => decode_view(rest).map(Inbound::Directory),
        TAG_REPORT => decode_report(rest).map(Inbound::Report),
        _ => Err(()),
    }
}

fn encode_view_body(view: &PeerView) -> Result<Vec<u8>, FrameError> {
    let mut body = Vec::new();
    match view {
        PeerView::Waiting => body.push(TAG_WAITING),
        PeerView::Current(peers) => {
            body.push(TAG_CURRENT);
            body.extend_from_slice(&(peers.len() as u32).to_be_bytes());
            for peer in peers {
                let name = peer.name.as_str().as_bytes();
                if name.len() > u16::MAX as usize {
                    return Err(FrameError::TooLarge);
                }
                body.extend_from_slice(&(name.len() as u16).to_be_bytes());
                body.extend_from_slice(name);
                body.push(presence_tag(peer.presence));
                body.push(peer.pace.tag());
                body.extend_from_slice(&peer.depth.get().to_be_bytes());
                body.push(u8::from(peer.fresh));
            }
        }
    }
    Ok(body)
}

fn decode_view(buf: &[u8]) -> Result<PeerView, ()> {
    let (tag, rest) = buf.split_first().ok_or(())?;
    match *tag {
        TAG_WAITING if rest.is_empty() => Ok(PeerView::Waiting),
        TAG_CURRENT => {
            if rest.len() < 4 {
                return Err(());
            }
            let count = u32::from_be_bytes(rest[0..4].try_into().expect("4 bytes")) as usize;
            let mut peers = Vec::with_capacity(count);
            let mut pos = 4;
            for _ in 0..count {
                let (peer, next) = decode_peer_sync(&rest[pos..])?;
                peers.push(peer);
                pos += next;
            }
            if pos != rest.len() {
                return Err(());
            }
            Ok(PeerView::Current(peers))
        }
        _ => Err(()),
    }
}

fn decode_peer_sync(buf: &[u8]) -> Result<(PeerSync, usize), ()> {
    if buf.len() < 2 {
        return Err(());
    }
    let name_len = u16::from_be_bytes(buf[0..2].try_into().expect("2 bytes")) as usize;
    let end = 2 + name_len;
    if buf.len() < end + 1 + 1 + 4 + 1 {
        return Err(());
    }
    let name = std::str::from_utf8(&buf[2..end]).map_err(|_| ())?;
    let name = PeerName::parse(name).ok_or(())?;
    let presence = presence_from(buf[end])?;
    let pace = Pace::from_tag(buf[end + 1]).ok_or(())?;
    let depth = QueueDepth(u32::from_be_bytes(
        buf[end + 2..end + 6].try_into().expect("4 bytes"),
    ));
    let fresh = match buf[end + 6] {
        0 => false,
        1 => true,
        _ => return Err(()),
    };
    Ok((
        PeerSync {
            name,
            presence,
            pace,
            depth,
            fresh,
        },
        end + 7,
    ))
}

fn encode_report(report: &SelfReport) -> Vec<u8> {
    let mut body = Vec::with_capacity(8 + 1 + 4);
    body.extend_from_slice(&report.seq.to_be_bytes());
    body.push(report.pace.tag());
    body.extend_from_slice(&report.depth.get().to_be_bytes());
    body
}

fn decode_report(buf: &[u8]) -> Result<SelfReport, ()> {
    if buf.len() != 8 + 1 + 4 {
        return Err(());
    }
    let seq = u64::from_be_bytes(buf[0..8].try_into().expect("8 bytes"));
    let pace = Pace::from_tag(buf[8]).ok_or(())?;
    let depth = QueueDepth(u32::from_be_bytes(buf[9..13].try_into().expect("4 bytes")));
    Ok(SelfReport { seq, pace, depth })
}

fn presence_tag(presence: Presence) -> u8 {
    match presence {
        Presence::Connected => 1,
        Presence::Disconnected => 2,
    }
}

fn presence_from(tag: u8) -> Result<Presence, ()> {
    match tag {
        1 => Ok(Presence::Connected),
        2 => Ok(Presence::Disconnected),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::protocol::encode_control;

    fn name(raw: &str) -> PeerName {
        PeerName::parse(raw).unwrap()
    }

    fn depth(units: u32) -> QueueDepth {
        QueueDepth::from_units(units as usize)
    }

    fn report(seq: u64, pace: Pace, units: u32) -> SelfReport {
        SelfReport {
            seq,
            pace,
            depth: depth(units),
        }
    }

    #[test]
    fn queue_depth_sums_slave_work_and_ignores_the_master_outbox() {
        let queues = Queues {
            pending: 1,
            pending_pulls: 0,
            pending_renames: 0,
            parked: 2,
            sending: 3,
            work: 4,
            outbox: 9,
            writable: false,
            fanout_dropped: 8,
        };
        assert_eq!(queue_depth(&queues).get(), 10);
    }

    #[test]
    fn failed_health_reports_as_stuck() {
        let report = self_report(1, &Flow::default(), &Queues::default(), 1);
        assert_eq!(report.pace, Pace::Stuck);
        assert_eq!(report.depth.get(), 0);
    }

    #[test]
    fn board_publish_disconnect_replace_and_stale_fresh() {
        let mut board = PeerBoard::default();
        let alice = name("dev-alice");
        let backup = name("backup-1");
        board.sync_acl(&[alice.clone(), backup.clone()]);
        let t0 = Instant::now();

        let alone = board.views(t0).remove(&alice).unwrap();
        let backup_card = alone.get("backup-1").unwrap();
        assert_eq!(backup_card.presence, Presence::Disconnected);
        assert_eq!(backup_card.pace, Pace::Idle);
        assert_eq!(backup_card.depth.get(), 0);
        assert!(!backup_card.fresh);
        assert!(alone.get("dev-alice").is_none());
        assert!(alone.get("carol").is_none());

        board.note_session(&alice, 1, t0);
        assert!(board.observe_report(&alice, 1, report(5, Pace::Busy, 4), t0));
        let published = board.views(t0).remove(&backup).unwrap();
        let alice_card = published.get("dev-alice").unwrap();
        assert_eq!(
            alice_card,
            &PeerSync {
                name: alice.clone(),
                presence: Presence::Connected,
                pace: Pace::Busy,
                depth: depth(4),
                fresh: true,
            }
        );
        assert!(published.get("backup-1").is_none());

        assert!(!board.observe_report(&alice, 1, report(5, Pace::Idle, 0), t0));
        assert_eq!(
            board
                .views(t0)
                .remove(&backup)
                .unwrap()
                .get("dev-alice")
                .unwrap()
                .pace,
            Pace::Busy
        );

        assert!(!board.observe_report(&alice, 1, report(4, Pace::Idle, 1), t0));
        board.note_session(&alice, 2, t0);
        assert!(!board.observe_report(&alice, 1, report(9, Pace::Idle, 1), t0));
        assert!(board.observe_report(&alice, 2, report(1, Pace::Idle, 0), t0));
        let replaced = board.views(t0).remove(&backup).unwrap();
        assert_eq!(replaced.get("dev-alice").unwrap().pace, Pace::Idle);
        assert_eq!(replaced.get("dev-alice").unwrap().depth.get(), 0);

        board.note_session(&alice, 3, t0);
        assert!(board.observe_report(&alice, 3, report(1, Pace::Busy, 3), t0));
        let stale = board
            .views(t0 + Duration::from_secs(15))
            .remove(&backup)
            .unwrap();
        let stale_alice = stale.get("dev-alice").unwrap();
        assert_eq!(stale_alice.pace, Pace::Busy);
        assert_eq!(stale_alice.depth.get(), 3);
        assert!(!stale_alice.fresh);
        let still = board
            .views(t0 + Duration::from_secs(14))
            .remove(&backup)
            .unwrap();
        assert!(still.get("dev-alice").unwrap().fresh);
        assert_eq!(still.get("dev-alice").unwrap().pace, Pace::Busy);

        board.note_disconnect(&alice);
        let gone = board.views(t0).remove(&backup).unwrap();
        let disconnected = gone.get("dev-alice").unwrap();
        assert_eq!(disconnected.presence, Presence::Disconnected);
        assert_eq!(disconnected.pace, Pace::Busy);
        assert_eq!(disconnected.depth.get(), 3);
        assert!(!disconnected.fresh);

        board.sync_acl(&[backup.clone()]);
        assert!(
            board
                .views(t0)
                .remove(&backup)
                .unwrap()
                .get("dev-alice")
                .is_none()
        );
    }

    #[test]
    fn one_slave_directory_is_empty_current() {
        let mut board = PeerBoard::default();
        let alice = name("dev-alice");
        board.sync_acl(&[alice.clone()]);
        assert_eq!(
            board.views(Instant::now()).remove(&alice).unwrap(),
            PeerView::Current(Vec::new())
        );
    }

    #[test]
    fn bad_asp1_is_bad_peer_and_a_bad_file_frame_is_not() {
        let mut payload = b"asp1".to_vec();
        payload.push(9);
        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
        frame.extend(payload);
        assert_eq!(decode_inbound(&frame), Err(FrameError::BadPeer));

        let junk = b"nope";
        let mut file = (junk.len() as u32).to_be_bytes().to_vec();
        file.extend_from_slice(junk);
        assert!(matches!(
            decode_inbound(&file),
            Err(err) if err != FrameError::BadPeer
        ));
    }

    #[test]
    fn directory_and_report_roundtrip_apart_from_file_frames() {
        let view = PeerView::Current(vec![PeerSync {
            name: name("backup-1"),
            presence: Presence::Disconnected,
            pace: Pace::Idle,
            depth: depth(0),
            fresh: false,
        }]);
        let encoded = encode_outbound(&Outbound::Directory(view.clone())).unwrap();
        assert_eq!(decode_inbound(&encoded), Ok(Inbound::Directory(view)));

        let report = SelfReport {
            seq: 4,
            pace: Pace::Busy,
            depth: depth(7),
        };
        let encoded = encode_outbound(&Outbound::Report(report)).unwrap();
        assert_eq!(
            decode_inbound(&encoded),
            Ok(Inbound::Report(SelfReport {
                seq: 4,
                pace: Pace::Busy,
                depth: depth(7),
            }))
        );

        let msg = ProtocolMessage::Disconnect {
            reason: "bye".into(),
        };
        let encoded = encode_control(&msg).unwrap();
        assert_eq!(decode_inbound(&encoded), Ok(Inbound::File(msg)));
    }

    #[tokio::test]
    async fn query_peers_on_a_real_socket_returns_a_literal_view() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("peers.sock");
        let view = PeerView::Current(vec![PeerSync {
            name: name("backup-1"),
            presence: Presence::Disconnected,
            pace: Pace::Idle,
            depth: depth(0),
            fresh: false,
        }]);
        let (tx, rx) = tokio::sync::watch::channel(PeerView::Waiting);
        let serve = serve_peers(sock.clone(), rx).unwrap();
        let mode = fs::metadata(&sock).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let waiting = tokio::task::spawn_blocking({
            let sock = sock.clone();
            move || query_peers(&sock)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(waiting, PeerView::Waiting);
        tx.send(view.clone()).unwrap();
        let current = tokio::task::spawn_blocking(move || query_peers(&sock))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current, view);
        drop(tx);
        serve.abort();
    }
}

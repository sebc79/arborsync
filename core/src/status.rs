use std::collections::HashMap;
use std::fmt::{self, Write as _};

use crate::protocol::ProtocolMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    Idle,
    Busy,
    Stuck,
    Failed,
}

impl Health {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Busy => "busy",
            Self::Stuck => "stuck",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Flow {
    pub in_msgs: u64,
    pub out_msgs: u64,
    pub cas_accept: u64,
    pub cas_reject: u64,
    pub apply_ok: u64,
    pub apply_fail: u64,
    pub bulk_in: u64,
    pub bulk_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub root: u64,
    pub dir_list: u64,
    pub local: u64,
    pub rescan: u64,
    pub flushed: u64,
}

impl Flow {
    pub fn advancing(&self) -> u64 {
        self.cas_accept
            + self.apply_ok
            + self.bulk_in
            + self.bulk_out
            + self.dir_list
            + self.root
            + self.local
            + self.rescan
            + self.flushed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Queues {
    pub outbox: usize,
    pub pending: usize,
    pub pending_pulls: usize,
    pub pending_renames: usize,
    pub writable: bool,
}

impl Default for Queues {
    fn default() -> Self {
        Self {
            outbox: 0,
            pending: 0,
            pending_pulls: 0,
            pending_renames: 0,
            writable: true,
        }
    }
}

impl Queues {
    pub fn stalled(&self, flow: &Flow) -> bool {
        (self.outbox > 0 && flow.flushed == 0)
            || (self.pending > 0 && flow.bulk_in == 0 && flow.apply_ok == 0)
            || (self.pending_pulls > 0 && flow.dir_list == 0 && flow.in_msgs == 0)
            || (self.pending_renames > 0 && flow.cas_accept == 0 && flow.cas_reject == 0)
    }

    fn add_assign(&mut self, other: &Self) {
        self.outbox += other.outbox;
        self.pending += other.pending;
        self.pending_pulls += other.pending_pulls;
        self.pending_renames += other.pending_renames;
        self.writable = self.writable && other.writable;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastError {
    pub reason: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerLive {
    pub slave_id: String,
    pub checkouts: usize,
    pub queues: Queues,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlavePeerStatus {
    pub slave_id: String,
    pub connected: bool,
    pub checkouts: usize,
    pub health: Health,
    pub flow: Flow,
    pub queues: Queues,
    pub last_error: Option<LastError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterStatus {
    pub health: Health,
    pub connected: usize,
    pub flow: Flow,
    pub queues: Queues,
    pub last_error: Option<LastError>,
    pub slaves: Vec<SlavePeerStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlaveStatus {
    pub health: Health,
    pub connected: bool,
    pub flow: Flow,
    pub queues: Queues,
    pub last_error: Option<LastError>,
}

pub fn classify(flow: &Flow, queues: &Queues, error_count: u64) -> Health {
    if queues.stalled(flow) {
        Health::Stuck
    } else if error_count > 0 && flow.advancing() == 0 {
        Health::Failed
    } else if flow.advancing() > 0 {
        Health::Busy
    } else {
        Health::Idle
    }
}

#[derive(Default)]
struct PeerAcc {
    flow: Flow,
    last_error: Option<LastError>,
}

#[derive(Default)]
pub struct StatusLedger {
    aggregate: Flow,
    last_error: Option<LastError>,
    by_slave: HashMap<String, PeerAcc>,
}

impl StatusLedger {
    pub fn inbound(&mut self, slave: Option<&str>, msg: &ProtocolMessage) {
        self.touch(slave, |flow| {
            flow.in_msgs += 1;
            note_kind(flow, msg);
        });
        if let Some(reason) = error_reason(msg) {
            self.error(slave, reason);
        }
    }

    pub fn outbound(&mut self, slave: Option<&str>, msg: &ProtocolMessage) {
        self.touch(slave, |flow| {
            flow.out_msgs += 1;
            note_kind(flow, msg);
        });
        if let Some(reason) = error_reason(msg) {
            self.error(slave, reason);
        }
    }

    pub fn flushed(&mut self, slave: Option<&str>, n: u64) {
        if n == 0 {
            return;
        }
        self.touch(slave, |flow| flow.flushed += n);
    }

    pub fn local(&mut self, slave: Option<&str>) {
        self.touch(slave, |flow| flow.local += 1);
    }

    pub fn rescan(&mut self, slave: Option<&str>) {
        self.touch(slave, |flow| flow.rescan += 1);
    }

    pub fn bulk_in(&mut self, slave: Option<&str>, bytes: u64) {
        self.touch(slave, |flow| {
            flow.bulk_in += 1;
            flow.bytes_in += bytes;
        });
    }

    pub fn bulk_out(&mut self, slave: Option<&str>, bytes: u64) {
        self.touch(slave, |flow| {
            flow.bulk_out += 1;
            flow.bytes_out += bytes;
        });
    }

    pub fn apply_ok(&mut self, slave: Option<&str>) {
        self.touch(slave, |flow| flow.apply_ok += 1);
    }

    pub fn apply_fail(&mut self, slave: Option<&str>) {
        self.touch(slave, |flow| flow.apply_fail += 1);
    }

    pub fn error(&mut self, slave: Option<&str>, reason: impl Into<String>) {
        let reason = sanitize_reason(&reason.into());
        bump_error(&mut self.last_error, &reason);
        if let Some(id) = slave {
            bump_error(
                &mut self.by_slave.entry(id.to_string()).or_default().last_error,
                &reason,
            );
        }
    }

    pub fn take_master(&mut self, live: Vec<PeerLive>) -> MasterStatus {
        let flow = std::mem::take(&mut self.aggregate);
        let last_error = self.last_error.take();
        let mut accs = std::mem::take(&mut self.by_slave);
        let mut slaves = Vec::new();
        let mut queues = Queues::default();

        for peer in live {
            queues.add_assign(&peer.queues);
            let acc = accs.remove(&peer.slave_id).unwrap_or_default();
            let error_count = acc.last_error.as_ref().map(|e| e.count).unwrap_or(0);
            slaves.push(SlavePeerStatus {
                health: classify(&acc.flow, &peer.queues, error_count),
                slave_id: peer.slave_id,
                connected: true,
                checkouts: peer.checkouts,
                flow: acc.flow,
                queues: peer.queues,
                last_error: acc.last_error,
            });
        }

        for (slave_id, acc) in accs {
            let error_count = acc.last_error.as_ref().map(|e| e.count).unwrap_or(0);
            let empty = Queues::default();
            slaves.push(SlavePeerStatus {
                health: classify(&acc.flow, &empty, error_count),
                slave_id,
                connected: false,
                checkouts: 0,
                flow: acc.flow,
                queues: empty,
                last_error: acc.last_error,
            });
        }

        slaves.sort_by(|a, b| a.slave_id.cmp(&b.slave_id));
        let error_count = last_error.as_ref().map(|e| e.count).unwrap_or(0);
        MasterStatus {
            health: classify(&flow, &queues, error_count),
            connected: slaves.iter().filter(|s| s.connected).count(),
            flow,
            queues,
            last_error,
            slaves,
        }
    }

    pub fn take_slave(&mut self, connected: bool, queues: Queues) -> SlaveStatus {
        let flow = std::mem::take(&mut self.aggregate);
        let last_error = self.last_error.take();
        self.by_slave.clear();
        let error_count = last_error.as_ref().map(|e| e.count).unwrap_or(0);
        SlaveStatus {
            health: classify(&flow, &queues, error_count),
            connected,
            flow,
            queues,
            last_error,
        }
    }

    fn touch(&mut self, slave: Option<&str>, f: impl Fn(&mut Flow)) {
        f(&mut self.aggregate);
        if let Some(id) = slave {
            f(&mut self.by_slave.entry(id.to_string()).or_default().flow);
        }
    }
}

impl MasterStatus {
    pub fn lines(&self, period_secs: u64) -> Vec<String> {
        let mut out = vec![format!(
            "status {period_secs}s health={} connected={} {} {} last_error={}",
            self.health.as_str(),
            self.connected,
            flow_fields(&self.flow),
            queue_fields(&self.queues, true),
            display_error(&self.last_error),
        )];
        for slave in &self.slaves {
            out.push(format!(
                "status slave={} health={} connected={} checkouts={} {} {} last_error={}",
                slave.slave_id,
                slave.health.as_str(),
                slave.connected,
                slave.checkouts,
                flow_fields(&slave.flow),
                queue_fields(&slave.queues, true),
                display_error(&slave.last_error),
            ));
        }
        out
    }
}

impl SlaveStatus {
    pub fn line(&self, period_secs: u64) -> String {
        format!(
            "status {period_secs}s health={} connected={} {} {} last_error={}",
            self.health.as_str(),
            self.connected,
            flow_fields(&self.flow),
            queue_fields(&self.queues, false),
            display_error(&self.last_error),
        )
    }
}

impl fmt::Display for MasterStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.lines(0).join("\n"))
    }
}

impl fmt::Display for SlaveStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.line(0))
    }
}

fn note_kind(flow: &mut Flow, msg: &ProtocolMessage) {
    match msg {
        ProtocolMessage::CasAccept { .. } => flow.cas_accept += 1,
        ProtocolMessage::CasReject { .. } => flow.cas_reject += 1,
        ProtocolMessage::RootReport { .. } | ProtocolMessage::RootAck { .. } => flow.root += 1,
        ProtocolMessage::DirListRequest { .. } | ProtocolMessage::DirListResponse { .. } => {
            flow.dir_list += 1
        }
        _ => {}
    }
}

fn error_reason(msg: &ProtocolMessage) -> Option<String> {
    match msg {
        ProtocolMessage::CasReject { path, .. } => Some(format!("cas_reject:{}", path.as_str())),
        ProtocolMessage::SubscribeReject { reason, .. } => {
            Some(format!("subscribe_reject:{reason}"))
        }
        ProtocolMessage::Error { code, message } => Some(format!("error:{code}:{message}")),
        ProtocolMessage::Disconnect { reason } => Some(format!("disconnect:{reason}")),
        _ => None,
    }
}

fn bump_error(slot: &mut Option<LastError>, reason: &str) {
    match slot {
        Some(existing) if existing.reason == reason => existing.count += 1,
        _ => {
            *slot = Some(LastError {
                reason: reason.to_string(),
                count: 1,
            });
        }
    }
}

fn sanitize_reason(raw: &str) -> String {
    raw.split_whitespace()
        .collect::<Vec<_>>()
        .join("_")
        .chars()
        .map(|c| if c.is_ascii_graphic() { c } else { '_' })
        .take(160)
        .collect()
}

fn display_error(error: &Option<LastError>) -> String {
    match error {
        Some(err) if err.count > 1 => format!("{}*{}", err.reason, err.count),
        Some(err) => err.reason.clone(),
        None => "-".into(),
    }
}

fn flow_fields(flow: &Flow) -> String {
    let mut s = String::new();
    let _ = write!(
        s,
        "in={} out={} cas_ok={} cas_rej={} apply_ok={} apply_fail={} bulk_in={} bulk_out={} bytes_in={} bytes_out={} dir_list={} root={} local={} rescan={} flushed={}",
        flow.in_msgs,
        flow.out_msgs,
        flow.cas_accept,
        flow.cas_reject,
        flow.apply_ok,
        flow.apply_fail,
        flow.bulk_in,
        flow.bulk_out,
        flow.bytes_in,
        flow.bytes_out,
        flow.dir_list,
        flow.root,
        flow.local,
        flow.rescan,
        flow.flushed,
    );
    s
}

fn queue_fields(queues: &Queues, master: bool) -> String {
    if master {
        format!(
            "pending={} outbox={} writable={}",
            queues.pending, queues.outbox, queues.writable
        )
    } else {
        format!(
            "pending={} pending_pulls={} pending_renames={}",
            queues.pending, queues.pending_pulls, queues.pending_renames
        )
    }
}

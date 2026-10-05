use std::collections::HashMap;
use std::fmt::{self, Write as _};
use std::time::{Duration, Instant};

use crate::bottleneck::{Bottleneck, Gauge, Hint, Wait, verdict};
use crate::protocol::ProtocolMessage;

/// A gauge is believable for three status periods. A slave that has gone quiet
/// for longer is `unobserved`, not "still parked".
const GAUGE_PERIODS: u32 = 3;
/// Staleness still needs a window when status logging is off, because a gauge
/// arrives on the slave's period, not ours.
const DEFAULT_STATUS_PERIOD: Duration = Duration::from_secs(5);

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
    pub fanout_dropped: u64,
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
    pub parked: usize,
    pub sending: usize,
    pub work: usize,
    pub writable: bool,
    pub fanout_dropped: u64,
}

impl Default for Queues {
    fn default() -> Self {
        Self {
            outbox: 0,
            pending: 0,
            pending_pulls: 0,
            pending_renames: 0,
            parked: 0,
            sending: 0,
            work: 0,
            writable: true,
            fanout_dropped: 0,
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
        self.fanout_dropped += other.fanout_dropped;
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
    pub waits: Vec<Wait>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlavePeerStatus {
    pub slave_id: String,
    pub connected: bool,
    pub checkouts: usize,
    pub health: Health,
    pub bottleneck: Bottleneck,
    pub hint: Hint,
    pub flow: Flow,
    pub queues: Queues,
    pub last_error: Option<LastError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterStatus {
    pub health: Health,
    pub bottleneck: Bottleneck,
    pub connected: usize,
    pub flow: Flow,
    pub queues: Queues,
    pub last_error: Option<LastError>,
    pub slaves: Vec<SlavePeerStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlaveStatus {
    pub health: Health,
    pub bottleneck: Bottleneck,
    pub connected: bool,
    pub flow: Flow,
    pub queues: Queues,
    pub last_error: Option<LastError>,
}

fn peer_line(found: Bottleneck, hint: Hint, slave_id: &str) -> Bottleneck {
    match found {
        Bottleneck::None if hint != Hint::Fresh => Bottleneck::Unobserved {
            peer: slave_id.to_string(),
        },
        found => found,
    }
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

struct Heard {
    gauge: Gauge,
    at: Instant,
}

#[derive(Clone, Default)]
struct SampleMark {
    flow: Flow,
    error: Option<LastError>,
}

#[derive(Default)]
pub struct StatusLedger {
    aggregate: Flow,
    last_error: Option<LastError>,
    by_slave: HashMap<String, PeerAcc>,
    gauges: HashMap<String, Heard>,
    log_mark: SampleMark,
    report_mark: SampleMark,
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

    pub fn fanout_dropped(&mut self, slave: Option<&str>, n: u64) {
        if n == 0 {
            return;
        }
        self.touch(slave, |flow| flow.fanout_dropped += n);
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

    pub fn observe_gauge(&mut self, slave: &str, gauge: Gauge, now: Instant) {
        match self.gauges.get_mut(slave) {
            Some(heard) if gauge.seq <= heard.gauge.seq => {}
            Some(heard) => *heard = Heard { gauge, at: now },
            None => {
                self.gauges
                    .insert(slave.to_string(), Heard { gauge, at: now });
            }
        }
    }

    /// A gauge describes a session. The next one restarts its sequence at zero,
    /// so keeping this one would silence the new session.
    pub fn forget_gauge(&mut self, slave: &str) {
        self.gauges.remove(slave);
    }

    /// A fresh gauge that names no fulfill work. Stale and absent gauges are
    /// not idle: the peer may still be sending.
    pub fn peer_idle_fulfill(
        &self,
        slave: &str,
        now: Instant,
        status_interval_seconds: u64,
    ) -> bool {
        let Some(heard) = self.gauges.get(slave) else {
            return false;
        };
        if now.saturating_duration_since(heard.at) > gauge_window(status_interval_seconds) {
            return false;
        }
        heard.gauge.stage.is_none() && heard.gauge.sending == 0 && heard.gauge.parked == 0
    }

    pub fn take_master(
        &mut self,
        live: Vec<PeerLive>,
        extra_waits: Vec<Wait>,
        status_interval_seconds: u64,
    ) -> MasterStatus {
        let now = Instant::now();
        let stale_after = gauge_window(status_interval_seconds);
        let flow = std::mem::take(&mut self.aggregate);
        let last_error = self.last_error.take();
        self.log_mark = SampleMark::default();
        self.report_mark = SampleMark::default();
        let mut accs = std::mem::take(&mut self.by_slave);
        let mut slaves = Vec::new();
        let mut queues = Queues::default();
        let mut all_waits = extra_waits;

        for peer in live {
            let acc = accs.remove(&peer.slave_id).unwrap_or_default();
            let mut peer_queues = peer.queues;
            peer_queues.fanout_dropped = acc.flow.fanout_dropped;
            queues.add_assign(&peer_queues);
            let error_count = acc.last_error.as_ref().map(|e| e.count).unwrap_or(0);
            let (hint, heard) = self.heard(&peer.slave_id, now, stale_after);
            let mut waits = peer.waits;
            waits.extend(heard);
            let bottleneck = peer_line(verdict(&waits, now), hint, &peer.slave_id);
            all_waits.extend(waits);
            slaves.push(SlavePeerStatus {
                health: classify(&acc.flow, &peer_queues, error_count),
                slave_id: peer.slave_id,
                connected: true,
                checkouts: peer.checkouts,
                bottleneck,
                hint,
                flow: acc.flow,
                queues: peer_queues,
                last_error: acc.last_error,
            });
        }

        for (slave_id, acc) in accs {
            let error_count = acc.last_error.as_ref().map(|e| e.count).unwrap_or(0);
            let mut empty = Queues::default();
            empty.fanout_dropped = acc.flow.fanout_dropped;
            queues.add_assign(&empty);
            slaves.push(SlavePeerStatus {
                health: classify(&acc.flow, &empty, error_count),
                slave_id,
                connected: false,
                checkouts: 0,
                bottleneck: Bottleneck::None,
                hint: Hint::Absent,
                flow: acc.flow,
                queues: empty,
                last_error: acc.last_error,
            });
        }

        slaves.sort_by(|a, b| a.slave_id.cmp(&b.slave_id));
        let error_count = last_error.as_ref().map(|e| e.count).unwrap_or(0);
        MasterStatus {
            health: classify(&flow, &queues, error_count),
            bottleneck: verdict(&all_waits, now),
            connected: slaves.iter().filter(|s| s.connected).count(),
            flow,
            queues,
            last_error,
            slaves,
        }
    }

    pub fn take_slave(&mut self, connected: bool, queues: Queues, waits: &[Wait]) -> SlaveStatus {
        let flow = flow_since(&self.aggregate, &self.log_mark.flow);
        let last_error = error_since(&self.last_error, &self.log_mark.error);
        self.log_mark = SampleMark {
            flow: self.aggregate.clone(),
            error: self.last_error.clone(),
        };
        let error_count = last_error.as_ref().map(|e| e.count).unwrap_or(0);
        SlaveStatus {
            health: classify(&flow, &queues, error_count),
            bottleneck: verdict(waits, Instant::now()),
            connected,
            flow,
            queues,
            last_error,
        }
    }

    pub fn report_delta(&mut self) -> (Flow, u64) {
        let flow = flow_since(&self.aggregate, &self.report_mark.flow);
        let last_error = error_since(&self.last_error, &self.report_mark.error);
        let error_count = last_error.as_ref().map(|e| e.count).unwrap_or(0);
        self.report_mark = SampleMark {
            flow: self.aggregate.clone(),
            error: self.last_error.clone(),
        };
        (flow, error_count)
    }

    fn heard(&self, slave: &str, now: Instant, stale_after: Duration) -> (Hint, Option<Wait>) {
        let Some(heard) = self.gauges.get(slave) else {
            return (Hint::Absent, None);
        };
        if now.saturating_duration_since(heard.at) > stale_after {
            return (Hint::Stale, None);
        }
        (Hint::Fresh, heard.gauge.wait(slave, heard.at, now))
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
            "status {period_secs}s health={} {} connected={} {} {} last_error={}",
            self.health.as_str(),
            self.bottleneck.field(),
            self.connected,
            flow_fields(&self.flow),
            queue_fields(&self.queues, true),
            display_error(&self.last_error),
        )];
        for slave in &self.slaves {
            out.push(format!(
                "status slave={} health={} {} hint={} connected={} checkouts={} {} {} last_error={}",
                slave.slave_id,
                slave.health.as_str(),
                slave.bottleneck.peer_field(),
                slave.hint.as_str(),
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
    pub fn gauge(&self, seq: u32) -> Gauge {
        let (stage, age_ms, depth) = match &self.bottleneck {
            Bottleneck::At {
                stage, age, depth, ..
            } => (
                Some(*stage),
                u32::try_from(age.as_millis()).unwrap_or(u32::MAX),
                u32::try_from(*depth).unwrap_or(u32::MAX),
            ),
            Bottleneck::None | Bottleneck::Unobserved { .. } => (None, 0, 0),
        };
        Gauge {
            seq,
            stage,
            age_ms,
            depth,
            parked: u16::try_from(self.queues.parked).unwrap_or(u16::MAX),
            sending: u16::try_from(self.queues.sending).unwrap_or(u16::MAX),
            pending: u32::try_from(self.queues.pending).unwrap_or(u32::MAX),
        }
    }

    pub fn line(&self, period_secs: u64) -> String {
        format!(
            "status {period_secs}s health={} {} connected={} {} {} last_error={}",
            self.health.as_str(),
            self.bottleneck.peer_field(),
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

fn flow_since(current: &Flow, mark: &Flow) -> Flow {
    Flow {
        in_msgs: current.in_msgs.saturating_sub(mark.in_msgs),
        out_msgs: current.out_msgs.saturating_sub(mark.out_msgs),
        cas_accept: current.cas_accept.saturating_sub(mark.cas_accept),
        cas_reject: current.cas_reject.saturating_sub(mark.cas_reject),
        apply_ok: current.apply_ok.saturating_sub(mark.apply_ok),
        apply_fail: current.apply_fail.saturating_sub(mark.apply_fail),
        bulk_in: current.bulk_in.saturating_sub(mark.bulk_in),
        bulk_out: current.bulk_out.saturating_sub(mark.bulk_out),
        bytes_in: current.bytes_in.saturating_sub(mark.bytes_in),
        bytes_out: current.bytes_out.saturating_sub(mark.bytes_out),
        root: current.root.saturating_sub(mark.root),
        dir_list: current.dir_list.saturating_sub(mark.dir_list),
        local: current.local.saturating_sub(mark.local),
        rescan: current.rescan.saturating_sub(mark.rescan),
        flushed: current.flushed.saturating_sub(mark.flushed),
        fanout_dropped: current.fanout_dropped.saturating_sub(mark.fanout_dropped),
    }
}

fn error_since(current: &Option<LastError>, mark: &Option<LastError>) -> Option<LastError> {
    match (current, mark) {
        (None, _) => None,
        (Some(cur), None) => Some(cur.clone()),
        (Some(cur), Some(prev)) if cur.reason == prev.reason => {
            let count = cur.count.saturating_sub(prev.count);
            (count > 0).then(|| LastError {
                reason: cur.reason.clone(),
                count,
            })
        }
        (Some(cur), Some(_)) => Some(cur.clone()),
    }
}

fn gauge_window(status_interval_seconds: u64) -> Duration {
    let period = match status_interval_seconds {
        0 => DEFAULT_STATUS_PERIOD,
        seconds => Duration::from_secs(seconds),
    };
    period * GAUGE_PERIODS
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
            "pending={} outbox={} writable={} fanout_dropped={}",
            queues.pending, queues.outbox, queues.writable, queues.fanout_dropped
        )
    } else {
        format!(
            "pending={} pending_pulls={} pending_renames={} parked={} sending={} work={}",
            queues.pending,
            queues.pending_pulls,
            queues.pending_renames,
            queues.parked,
            queues.sending,
            queues.work,
        )
    }
}

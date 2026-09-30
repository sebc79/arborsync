//! Which hop is limiting progress, and for how long.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// A named end-to-end hop. The strings are the operator-facing contract and
/// tests match on them, so they do not change once shipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Stat and hash of a local change, and the announce decision behind it.
    Hashing,
    /// A control frame is queued and the writer has not put it on the wire.
    Uplink,
    /// We asked the peer for a body and no bulk has arrived.
    OriginBytes,
    /// Reading and sending a body for the peer.
    FulfillRead,
    /// The peer's ask is deferred behind the inflight cap or a large send.
    FulfillParked,
    /// Reconstruct, atomic_put, fsync, reread, rename, commit.
    ApplyWrite,
    /// Frames queued in a peer's outbox that have not flushed.
    Fanout,
    /// Announces dropped because the peer is not writable.
    FanoutDropped,
    /// A crawl or dir-list round trip is outstanding.
    Reconcile,
    /// A connected peer whose own verdict has not reached us.
    Unobserved,
}

impl Stage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hashing => "hashing",
            Self::Uplink => "uplink",
            Self::OriginBytes => "origin_bytes",
            Self::FulfillRead => "fulfill_read",
            Self::FulfillParked => "fulfill_parked",
            Self::ApplyWrite => "apply_write",
            Self::Fanout => "fanout",
            Self::FanoutDropped => "fanout_dropped",
            Self::Reconcile => "reconcile",
            Self::Unobserved => "unobserved",
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Hashing => 1,
            Self::Uplink => 2,
            Self::OriginBytes => 3,
            Self::FulfillRead => 4,
            Self::FulfillParked => 5,
            Self::ApplyWrite => 6,
            Self::Fanout => 7,
            Self::FanoutDropped => 8,
            Self::Reconcile => 9,
            Self::Unobserved => 10,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Hashing),
            2 => Some(Self::Uplink),
            3 => Some(Self::OriginBytes),
            4 => Some(Self::FulfillRead),
            5 => Some(Self::FulfillParked),
            6 => Some(Self::ApplyWrite),
            7 => Some(Self::Fanout),
            8 => Some(Self::FanoutDropped),
            9 => Some(Self::Reconcile),
            10 => Some(Self::Unobserved),
            _ => None,
        }
    }

    fn refines(self, coarse: Stage) -> bool {
        coarse == Self::OriginBytes
            && matches!(
                self,
                Self::FulfillParked | Self::FulfillRead | Self::Uplink | Self::Hashing
            )
    }

    fn shadowed_by(self, other: Stage) -> bool {
        self == Self::Reconcile && matches!(other, Self::FulfillParked | Self::FulfillRead)
    }
}

/// One open wait, on this actor's clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Wait {
    pub stage: Stage,
    pub since: Instant,
    pub depth: usize,
    pub peer: Option<String>,
    pub source: Source,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Local,
    Gauge { received: Duration },
}

impl Source {
    fn as_field(self) -> String {
        match self {
            Self::Local => "local".into(),
            Self::Gauge { received } => format!("gauge({})", secs(received)),
        }
    }
}

impl Wait {
    pub fn new(stage: Stage, since: Instant, depth: usize) -> Self {
        Self {
            stage,
            since,
            depth,
            peer: None,
            source: Source::Local,
        }
    }

    pub fn about(mut self, peer: &str) -> Self {
        self.peer = Some(peer.to_string());
        self
    }

    pub fn via(mut self, source: Source) -> Self {
        self.source = source;
        self
    }
}

/// A queue whose rows carry the instant they started waiting. One `Waiting` is
/// one [`Stage`].
///
/// Re-inserting an existing key keeps the original stamp. A retry continues the
/// same wait rather than restarting the clock, which is what makes the master's
/// `retried` path report an honest age.
pub struct Waiting<K, V> {
    stage: Stage,
    rows: HashMap<K, Row<V>>,
}

struct Row<V> {
    since: Instant,
    value: V,
}

impl<K: Eq + Hash, V> Waiting<K, V> {
    pub fn new(stage: Stage) -> Self {
        Self {
            stage,
            rows: HashMap::new(),
        }
    }

    /// Returns the displaced value. The stamp of an existing key is kept.
    pub fn insert(&mut self, key: K, value: V, now: Instant) -> Option<V> {
        match self.rows.get_mut(&key) {
            Some(row) => Some(std::mem::replace(&mut row.value, value)),
            None => {
                self.rows.insert(key, Row { since: now, value });
                None
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.rows.remove(key).map(|row| row.value)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.rows.get(key).map(|row| &row.value)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.rows.get_mut(key).map(|row| &mut row.value)
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&K, &mut V) -> bool) {
        self.rows.retain(|key, row| keep(key, &mut row.value));
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.rows.values().map(|row| &row.value)
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.rows.keys()
    }

    pub fn clear(&mut self) {
        self.rows.clear();
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Oldest row, or `None` when empty. `depth` is [`Waiting::len`].
    pub fn oldest(&self) -> Option<Wait> {
        self.oldest_where(|_, _| true)
    }

    /// Oldest matching row. `depth` counts only the matches.
    pub fn oldest_where(&self, mut matches: impl FnMut(&K, &V) -> bool) -> Option<Wait> {
        let mut depth = 0;
        let mut since: Option<Instant> = None;
        for (key, row) in &self.rows {
            if !matches(key, &row.value) {
                continue;
            }
            depth += 1;
            if since.is_none_or(|best| row.since < best) {
                since = Some(row.since);
            }
        }
        Some(Wait::new(self.stage, since?, depth))
    }

    pub fn oldest_key_where(&self, mut matches: impl FnMut(&K, &V) -> bool) -> Option<&K> {
        self.rows
            .iter()
            .filter(|(key, row)| matches(key, &row.value))
            .min_by_key(|(_, row)| row.since)
            .map(|(key, _)| key)
    }
}

impl<K: Eq + Hash + Ord, V> Waiting<K, V> {
    pub fn drain_ordered(&mut self) -> Vec<V> {
        let mut rows: Vec<(K, Row<V>)> = self.rows.drain().collect();
        rows.sort_by(|(a, _), (b, _)| a.cmp(b));
        rows.into_iter().map(|(_, row)| row.value).collect()
    }
}

/// What the operator reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Bottleneck {
    None,
    /// A connected peer that has told us nothing we can attribute.
    Unobserved {
        peer: String,
    },
    At {
        stage: Stage,
        age: Duration,
        depth: usize,
        peer: Option<String>,
        source: Source,
    },
}

impl Bottleneck {
    /// `bottleneck=<stage>[@<peer>] age=<n>s depth=<n> via=<source>`, for a
    /// line that does not otherwise name the peer.
    pub fn field(&self) -> String {
        self.render(true)
    }

    /// The same field for a line that already names the peer, where the source
    /// is always local.
    pub fn peer_field(&self) -> String {
        self.render(false)
    }

    fn render(&self, attribute: bool) -> String {
        let mut out = String::from("bottleneck=");
        match self {
            Self::None => out.push_str("none"),
            Self::Unobserved { peer } => {
                out.push_str(Stage::Unobserved.as_str());
                if attribute {
                    let _ = write!(out, "@{peer}");
                }
            }
            Self::At {
                stage,
                age,
                depth,
                peer,
                source,
            } => {
                out.push_str(stage.as_str());
                if let (true, Some(peer)) = (attribute, peer.as_ref()) {
                    let _ = write!(out, "@{peer}");
                }
                let _ = write!(out, " age={} depth={depth}", secs(*age));
                if attribute {
                    let _ = write!(out, " via={}", source.as_field());
                }
            }
        }
        out
    }
}

/// Oldest wait, then a peer stage that names the far end of the same wait.
pub fn verdict(candidates: &[Wait], now: Instant) -> Bottleneck {
    let shadowed = |wait: &Wait| {
        candidates
            .iter()
            .any(|other| wait.peer == other.peer && wait.stage.shadowed_by(other.stage))
    };

    let Some(winner) = oldest(candidates.iter().filter(|wait| !shadowed(wait))) else {
        return Bottleneck::None;
    };
    let refined = oldest(
        candidates
            .iter()
            .filter(|wait| wait.peer == winner.peer && wait.stage.refines(winner.stage)),
    );
    let chosen = refined.unwrap_or(winner);
    Bottleneck::At {
        stage: chosen.stage,
        age: now.saturating_duration_since(chosen.since),
        depth: chosen.depth,
        peer: chosen.peer,
        source: chosen.source,
    }
}

/// One actor's own verdict, 25 bytes beside the session. Not a [`crate::protocol::ProtocolMessage`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Gauge {
    /// Monotone per session. The receiver ignores anything not newer.
    pub seq: u32,
    /// `None` when the sender has no bottleneck.
    pub stage: Option<Stage>,
    pub age_ms: u32,
    pub depth: u32,
    pub parked: u16,
    pub sending: u16,
    pub pending: u32,
}

/// 4 magic + 4 seq + 1 stage + 4 age + 4 depth + 2 parked + 2 sending + 4 pending.
pub const GAUGE_LEN: usize = 25;
const GAUGE_MAGIC: [u8; 4] = *b"asg1";

impl Gauge {
    pub fn encode(&self) -> [u8; GAUGE_LEN] {
        let mut out = [0u8; GAUGE_LEN];
        out[0..4].copy_from_slice(&GAUGE_MAGIC);
        out[4..8].copy_from_slice(&self.seq.to_le_bytes());
        out[8] = self.stage.map_or(0, Stage::code);
        out[9..13].copy_from_slice(&self.age_ms.to_le_bytes());
        out[13..17].copy_from_slice(&self.depth.to_le_bytes());
        out[17..19].copy_from_slice(&self.parked.to_le_bytes());
        out[19..21].copy_from_slice(&self.sending.to_le_bytes());
        out[21..25].copy_from_slice(&self.pending.to_le_bytes());
        out
    }

    /// `None` for anything that is not a well-formed gauge. A malformed frame is
    /// dropped in silence. It is not a `ProtocolMessage::Error`, it does not
    /// touch `last_error`, and it cannot move `Health`.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let frame: &[u8; GAUGE_LEN] = bytes.try_into().ok()?;
        if frame[0..4] != GAUGE_MAGIC {
            return None;
        }
        let stage = match frame[8] {
            0 => None,
            code => Some(Stage::from_code(code)?),
        };
        Some(Self {
            seq: u32::from_le_bytes(frame[4..8].try_into().expect("4 bytes")),
            stage,
            age_ms: u32::from_le_bytes(frame[9..13].try_into().expect("4 bytes")),
            depth: u32::from_le_bytes(frame[13..17].try_into().expect("4 bytes")),
            parked: u16::from_le_bytes(frame[17..19].try_into().expect("2 bytes")),
            sending: u16::from_le_bytes(frame[19..21].try_into().expect("2 bytes")),
            pending: u32::from_le_bytes(frame[21..25].try_into().expect("4 bytes")),
        })
    }

    /// Rebase this age onto the receiver clock. `None` when the peer reported no bottleneck.
    pub fn wait(&self, peer: &str, received: Instant, now: Instant) -> Option<Wait> {
        let stage = self.stage?;
        let since = received
            .checked_sub(Duration::from_millis(self.age_ms.into()))
            .unwrap_or(received);
        Some(
            Wait::new(stage, since, self.depth as usize)
                .about(peer)
                .via(Source::Gauge {
                    received: now.saturating_duration_since(received),
                }),
        )
    }
}

/// How much of a peer's own verdict reached us, for a line that already names
/// the peer and therefore cannot say `via=`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hint {
    /// A gauge arrived recently enough to believe.
    Fresh,
    /// The last gauge is older than the window. Its age is not rebased.
    Stale,
    /// No gauge from this peer at all.
    Absent,
}

impl Hint {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Absent => "absent",
        }
    }
}

fn oldest<'a>(waits: impl Iterator<Item = &'a Wait>) -> Option<Wait> {
    waits
        .min_by(|a, b| (a.since, &a.peer).cmp(&(b.since, &b.peer)))
        .cloned()
}

fn secs(span: Duration) -> String {
    format!("{:.1}s", span.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ago(now: Instant, seconds: u64) -> Instant {
        now - Duration::from_secs(seconds)
    }

    #[test]
    fn reinsert_keeps_the_original_stamp_and_replaces_the_value() {
        let now = Instant::now();
        let mut waiting: Waiting<&str, u32> = Waiting::new(Stage::OriginBytes);
        waiting.insert("a", 1, ago(now, 40));
        assert_eq!(waiting.insert("a", 2, now), Some(1));

        assert_eq!(waiting.len(), 1);
        assert_eq!(waiting.get(&"a"), Some(&2));
        assert_eq!(
            waiting.oldest(),
            Some(Wait::new(Stage::OriginBytes, ago(now, 40), 1))
        );
    }

    #[test]
    fn oldest_reports_the_earliest_stamp_and_the_full_depth() {
        let now = Instant::now();
        let mut waiting: Waiting<&str, u32> = Waiting::new(Stage::Fanout);
        waiting.insert("young", 1, ago(now, 1));
        waiting.insert("old", 2, ago(now, 90));
        waiting.insert("middle", 3, ago(now, 30));

        assert_eq!(
            waiting.oldest(),
            Some(Wait::new(Stage::Fanout, ago(now, 90), 3))
        );
    }

    #[test]
    fn oldest_where_counts_only_the_matching_rows() {
        let now = Instant::now();
        let mut waiting: Waiting<&str, &str> = Waiting::new(Stage::OriginBytes);
        waiting.insert("a", "alice", ago(now, 90));
        waiting.insert("b", "bob", ago(now, 60));
        waiting.insert("c", "bob", ago(now, 30));

        assert_eq!(
            waiting.oldest_where(|_, peer| *peer == "bob"),
            Some(Wait::new(Stage::OriginBytes, ago(now, 60), 2))
        );
        assert_eq!(waiting.oldest_where(|_, peer| *peer == "nobody"), None);
    }

    #[test]
    fn oldest_key_where_skips_the_rows_the_scheduler_cannot_serve() {
        let now = Instant::now();
        let mut waiting: Waiting<&str, bool> = Waiting::new(Stage::FulfillParked);
        waiting.insert("blocked", false, ago(now, 90));
        waiting.insert("startable", true, ago(now, 60));
        waiting.insert("younger", true, ago(now, 30));

        assert_eq!(
            waiting.oldest_key_where(|_, startable| *startable),
            Some(&"startable")
        );
    }

    #[test]
    fn drain_ordered_empties_the_queue_oldest_key_first() {
        let now = Instant::now();
        let mut waiting: Waiting<u64, &str> = Waiting::new(Stage::Fanout);
        waiting.insert(2, "second", ago(now, 1));
        waiting.insert(0, "first", ago(now, 90));
        waiting.insert(1, "middle", ago(now, 30));

        assert_eq!(waiting.drain_ordered(), vec!["first", "middle", "second"]);
        assert!(waiting.is_empty());
        assert_eq!(waiting.oldest(), None);
    }

    #[test]
    fn clear_drops_every_wait() {
        let now = Instant::now();
        let mut waiting: Waiting<&str, u32> = Waiting::new(Stage::Reconcile);
        waiting.insert("a", 1, ago(now, 90));
        waiting.insert("b", 2, ago(now, 30));

        waiting.clear();
        assert_eq!(waiting.len(), 0);
        assert_eq!(waiting.values().count(), 0);
        assert_eq!(waiting.oldest(), None);
    }

    #[test]
    fn remove_and_retain_drop_the_wait_with_the_row() {
        let now = Instant::now();
        let mut waiting: Waiting<&str, &str> = Waiting::new(Stage::OriginBytes);
        waiting.insert("a", "alice", ago(now, 90));
        waiting.insert("b", "bob", ago(now, 60));

        assert_eq!(waiting.remove(&"a"), Some("alice"));
        waiting.retain(|_, peer| *peer != "bob");
        assert!(waiting.is_empty());
        assert_eq!(waiting.oldest(), None);
    }

    #[test]
    fn an_empty_candidate_set_has_no_bottleneck() {
        assert_eq!(verdict(&[], Instant::now()), Bottleneck::None);
        assert_eq!(Bottleneck::None.field(), "bottleneck=none");
    }

    #[test]
    fn the_oldest_wait_wins() {
        let now = Instant::now();
        let candidates = vec![
            Wait::new(Stage::Fanout, ago(now, 12), 4).about("backup-1"),
            Wait::new(Stage::OriginBytes, ago(now, 43), 118).about("dev-alice"),
        ];

        assert_eq!(
            verdict(&candidates, now),
            Bottleneck::At {
                stage: Stage::OriginBytes,
                age: Duration::from_secs(43),
                depth: 118,
                peer: Some("dev-alice".into()),
                source: Source::Local,
            }
        );
    }

    #[test]
    fn a_peer_fulfill_stage_refines_a_local_origin_bytes_wait() {
        let now = Instant::now();
        let candidates = vec![
            Wait::new(Stage::OriginBytes, ago(now, 43), 118).about("dev-alice"),
            Wait::new(Stage::FulfillParked, ago(now, 41), 3)
                .about("dev-alice")
                .via(Source::Gauge {
                    received: Duration::from_millis(1200),
                }),
        ];

        let bottleneck = verdict(&candidates, now);
        assert_eq!(
            bottleneck,
            Bottleneck::At {
                stage: Stage::FulfillParked,
                age: Duration::from_secs(41),
                depth: 3,
                peer: Some("dev-alice".into()),
                source: Source::Gauge {
                    received: Duration::from_millis(1200)
                },
            }
        );
        assert_eq!(
            bottleneck.field(),
            "bottleneck=fulfill_parked@dev-alice age=41.0s depth=3 via=gauge(1.2s)"
        );
        assert_eq!(
            bottleneck.peer_field(),
            "bottleneck=fulfill_parked age=41.0s depth=3"
        );
    }

    #[test]
    fn a_refinement_about_another_peer_is_ignored() {
        let now = Instant::now();
        let candidates = vec![
            Wait::new(Stage::OriginBytes, ago(now, 43), 118).about("dev-alice"),
            Wait::new(Stage::FulfillParked, ago(now, 41), 3).about("backup-1"),
        ];

        assert_eq!(
            verdict(&candidates, now).field(),
            "bottleneck=origin_bytes@dev-alice age=43.0s depth=118 via=local"
        );
    }

    #[test]
    fn a_fulfill_wait_shadows_an_older_reconcile_wait() {
        let now = Instant::now();
        let candidates = vec![
            Wait::new(Stage::Reconcile, ago(now, 90), 2).about("dev-alice"),
            Wait::new(Stage::FulfillRead, ago(now, 5), 1).about("dev-alice"),
        ];

        assert_eq!(
            verdict(&candidates, now).field(),
            "bottleneck=fulfill_read@dev-alice age=5.0s depth=1 via=local"
        );
    }

    #[test]
    fn a_fulfill_on_another_peer_does_not_shadow_reconcile() {
        let now = Instant::now();
        let candidates = vec![
            Wait::new(Stage::Reconcile, ago(now, 90), 2).about("dev-alice"),
            Wait::new(Stage::FulfillRead, ago(now, 5), 1).about("backup-1"),
        ];

        assert_eq!(
            verdict(&candidates, now).field(),
            "bottleneck=reconcile@dev-alice age=90.0s depth=2 via=local"
        );
    }

    #[test]
    fn reconcile_wins_when_nothing_is_fulfilling() {
        let now = Instant::now();
        let candidates = vec![
            Wait::new(Stage::Reconcile, ago(now, 90), 2).about("dev-alice"),
            Wait::new(Stage::Fanout, ago(now, 5), 1).about("dev-alice"),
        ];

        assert_eq!(
            verdict(&candidates, now).field(),
            "bottleneck=reconcile@dev-alice age=90.0s depth=2 via=local"
        );
    }

    #[test]
    fn an_unobserved_peer_names_itself_only_on_an_attributed_line() {
        let bottleneck = Bottleneck::Unobserved {
            peer: "dev-alice".into(),
        };
        assert_eq!(bottleneck.field(), "bottleneck=unobserved@dev-alice");
        assert_eq!(bottleneck.peer_field(), "bottleneck=unobserved");
    }

    #[test]
    fn a_gauge_frame_is_25_bytes_and_round_trips() {
        let gauge = Gauge {
            seq: 7,
            stage: Some(Stage::FulfillParked),
            age_ms: 41_000,
            depth: 3,
            parked: 3,
            sending: 1,
            pending: 118,
        };
        let frame = gauge.encode();

        assert_eq!(frame.len(), GAUGE_LEN);
        assert_eq!(&frame[0..4], b"asg1");
        assert_eq!(Gauge::decode(&frame), Some(gauge));
    }

    #[test]
    fn a_gauge_without_a_stage_round_trips_as_none() {
        let gauge = Gauge {
            seq: 1,
            stage: None,
            age_ms: 0,
            depth: 0,
            parked: 0,
            sending: 0,
            pending: 0,
        };

        assert_eq!(Gauge::decode(&gauge.encode()), Some(gauge));
    }

    #[test]
    fn garbage_does_not_decode_as_a_gauge() {
        let good = Gauge {
            seq: 7,
            stage: Some(Stage::Reconcile),
            age_ms: 1,
            depth: 1,
            parked: 0,
            sending: 0,
            pending: 1,
        }
        .encode();

        assert_eq!(Gauge::decode(b"garbage"), None, "wrong length");
        assert_eq!(Gauge::decode(&[]), None, "empty");
        assert_eq!(Gauge::decode(&good[..GAUGE_LEN - 1]), None, "truncated");
        assert_eq!(Gauge::decode(&[0u8; GAUGE_LEN]), None, "zero magic");

        let mut past_the_vocabulary = good;
        past_the_vocabulary[8] = 11;
        assert_eq!(Gauge::decode(&past_the_vocabulary), None, "unknown stage");
    }

    #[test]
    fn a_gauge_age_rebases_onto_the_receiver_clock() {
        let now = Instant::now();
        let gauge = Gauge {
            seq: 4,
            stage: Some(Stage::FulfillParked),
            age_ms: 41_000,
            depth: 3,
            parked: 3,
            sending: 1,
            pending: 0,
        };

        let wait = gauge
            .wait("dev-alice", ago(now, 2), now)
            .expect("a staged gauge is a candidate");

        assert_eq!(
            verdict(&[wait], now).field(),
            "bottleneck=fulfill_parked@dev-alice age=43.0s depth=3 via=gauge(2.0s)"
        );
    }

    #[test]
    fn a_gauge_without_a_stage_is_not_a_candidate() {
        let now = Instant::now();
        let gauge = Gauge {
            seq: 4,
            stage: None,
            age_ms: 0,
            depth: 0,
            parked: 0,
            sending: 0,
            pending: 0,
        };

        assert_eq!(gauge.wait("dev-alice", now, now), None);
    }

    #[test]
    fn every_stage_code_round_trips() {
        let stages = [
            Stage::Hashing,
            Stage::Uplink,
            Stage::OriginBytes,
            Stage::FulfillRead,
            Stage::FulfillParked,
            Stage::ApplyWrite,
            Stage::Fanout,
            Stage::FanoutDropped,
            Stage::Reconcile,
            Stage::Unobserved,
        ];
        for stage in stages {
            assert_ne!(stage.code(), 0, "{}", stage.as_str());
            assert_eq!(Stage::from_code(stage.code()), Some(stage));
        }
    }
}

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::hash::ContentHash;
use crate::path::CanonicalPath;

struct InflightEntry {
    hash: ContentHash,
    until: Instant,
    consumed: bool,
}

pub(crate) struct Inflight {
    window: Duration,
    entries: HashMap<CanonicalPath, InflightEntry>,
}

impl Inflight {
    pub(crate) fn new(debounce: Duration) -> Self {
        Self {
            window: debounce * 2,
            entries: HashMap::new(),
        }
    }

    pub(crate) fn set_window(&mut self, debounce: Duration) {
        self.window = debounce * 2;
    }

    pub(crate) fn arm(&mut self, path: CanonicalPath, hash: ContentHash) {
        let until = Instant::now() + self.window;
        self.entries.insert(
            path,
            InflightEntry {
                hash,
                until,
                consumed: false,
            },
        );
    }

    pub(crate) fn disarm(&mut self, path: &CanonicalPath) {
        self.entries.remove(path);
    }

    pub(crate) fn consume_if_echo(&mut self, path: &CanonicalPath, hash: &ContentHash) -> bool {
        let now = Instant::now();
        self.entries.retain(|_, entry| entry.until > now);
        let Some(entry) = self.entries.get_mut(path) else {
            return false;
        };
        if entry.consumed || &entry.hash != hash {
            return false;
        }
        entry.consumed = true;
        true
    }

    pub(crate) fn is_armed(&mut self, path: &CanonicalPath) -> bool {
        let now = Instant::now();
        self.entries.retain(|_, entry| entry.until > now);
        self.entries.contains_key(path)
    }
}

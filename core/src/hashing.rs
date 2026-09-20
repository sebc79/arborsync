use std::io;
use std::path::PathBuf;

use crate::meta::{self, FileMetadata};
use crate::path::CanonicalPath;
use crate::protocol::ProtocolMessage;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HashKey {
    Central(CanonicalPath),
    Checkout { id: String, path: CanonicalPath },
}

#[derive(Clone, Debug)]
pub struct HashNeed {
    pub key: HashKey,
    pub host: PathBuf,
    pub previous: Option<FileMetadata>,
}

#[derive(Debug)]
pub struct HashDone {
    pub key: HashKey,
    pub previous: Option<FileMetadata>,
    pub outcome: HashOutcome,
}

#[derive(Debug)]
pub enum HashOutcome {
    File(FileMetadata),
    Absent,
    Io(io::ErrorKind),
}

#[derive(Default, Debug)]
pub struct HashPlan {
    pub send: Vec<ProtocolMessage>,
    pub hash: Vec<HashNeed>,
}

impl HashPlan {
    pub fn send(send: Vec<ProtocolMessage>) -> Self {
        Self {
            send,
            hash: Vec::new(),
        }
    }

    pub fn append(&mut self, other: Self) {
        self.send.extend(other.send);
        self.hash.extend(other.hash);
    }
}

impl HashNeed {
    pub fn run(self) -> HashDone {
        let outcome = match meta::collect_for_rescan(&self.host, self.previous.as_ref()) {
            Ok(Some(meta)) => HashOutcome::File(meta),
            Ok(None) => HashOutcome::Absent,
            Err(err) => HashOutcome::Io(err.kind()),
        };
        HashDone {
            key: self.key,
            previous: self.previous,
            outcome,
        }
    }
}

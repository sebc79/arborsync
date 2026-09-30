use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::hashing::{HashKey, HashNeed};
use crate::merkle::DirChild;
use crate::meta::{self, EntryKind, FileMetadata, Inspected};
use crate::path::{
    CanonicalPath, EntryName, PathError, canonical_to_host, is_reserved_root_entry, join_central,
};
use crate::protocol::ProtocolMessage;
use crate::storage::CheckoutId;

pub(crate) enum Walked {
    Ready(CanonicalPath, FileMetadata),
    Needs(HashNeed),
}

pub(crate) const STEP_BUDGET: usize = 64;

pub(crate) enum WalkError<E> {
    Io { path: PathBuf, source: io::Error },
    Index(E),
    Path(PathError),
}

#[derive(Default)]
pub(crate) struct Crawl {
    rescans: VecDeque<RescanWalk>,
    pages: VecDeque<DirListPage>,
}

pub(crate) struct RescanWalk {
    pub checkout_id: String,
    pub ck: CheckoutId,
    pub local: PathBuf,
    pub central: CanonicalPath,
    pending: Vec<CanonicalPath>,
    open: Option<OpenDir>,
    inflight_stat: usize,
    pub found: BTreeMap<CanonicalPath, FileMetadata>,
    pub awaiting_hash: HashSet<CanonicalPath>,
}

struct OpenDir {
    rel: CanonicalPath,
    remaining: Vec<(String, PathBuf)>,
}

pub struct RescanStat {
    checkout_id: String,
    child: CanonicalPath,
    rel_child: CanonicalPath,
    host: PathBuf,
    prior: Option<FileMetadata>,
}

pub struct RescanStated {
    stat: RescanStat,
    result: Result<Inspected, io::Error>,
}

impl RescanStat {
    pub fn host(&self) -> &Path {
        &self.host
    }

    pub fn checkout_id(&self) -> &str {
        &self.checkout_id
    }

    pub fn inspect(self) -> RescanStated {
        let result = meta::inspect_for_hash(&self.host, self.prior.as_ref());
        RescanStated { stat: self, result }
    }
}

impl RescanStated {
    pub fn checkout_id(&self) -> &str {
        self.stat.checkout_id()
    }
}

pub(crate) struct DirListPage {
    pub checkout_id: String,
    pub path: CanonicalPath,
    pub after: Option<EntryName>,
    pub more: bool,
    pub page_end: Option<String>,
    pub list_epoch: u64,
    pub remaining: VecDeque<(String, (Option<DirChild>, Option<DirChild>))>,
}

impl Crawl {
    pub(crate) fn pending(&self) -> bool {
        !self.rescans.is_empty() || !self.pages.is_empty()
    }

    pub(crate) fn runnable(&self) -> bool {
        if self.has_pages() {
            return true;
        }
        let Some(walk) = self.rescans.front() else {
            return false;
        };
        !walk.collect_done() || walk.awaiting_hash.is_empty()
    }

    pub(crate) fn has_rescan(&self) -> bool {
        !self.rescans.is_empty()
    }

    pub(crate) fn has_pages(&self) -> bool {
        !self.pages.is_empty()
    }

    pub(crate) fn rescanning(&self, checkout_id: &str) -> bool {
        self.rescans
            .iter()
            .any(|walk| walk.checkout_id == checkout_id)
    }

    pub(crate) fn push_rescan(&mut self, walk: RescanWalk) {
        if self.rescanning(&walk.checkout_id) {
            return;
        }
        self.rescans.push_back(walk);
    }

    pub(crate) fn push_page(&mut self, page: DirListPage) {
        self.pages.push_back(page);
    }

    pub(crate) fn drop_checkout(&mut self, checkout_id: &str) {
        self.rescans.retain(|walk| walk.checkout_id != checkout_id);
        self.pages.retain(|page| page.checkout_id != checkout_id);
    }

    pub(crate) fn front_rescan(&self) -> Option<&RescanWalk> {
        self.rescans.front()
    }

    pub(crate) fn front_rescan_mut(&mut self) -> Option<&mut RescanWalk> {
        self.rescans.front_mut()
    }

    pub(crate) fn take_finished_rescan(&mut self) -> Option<RescanWalk> {
        let done = self
            .rescans
            .front()
            .is_some_and(|walk| walk.collect_done() && walk.awaiting_hash.is_empty());
        if done { self.rescans.pop_front() } else { None }
    }

    pub(crate) fn front_page_mut(&mut self) -> Option<&mut DirListPage> {
        self.pages.front_mut()
    }

    pub(crate) fn pop_page_if_empty(&mut self) {
        if self
            .pages
            .front()
            .is_some_and(|page| page.remaining.is_empty())
        {
            self.pages.pop_front();
        }
    }
}

impl RescanWalk {
    pub(crate) fn begin(
        checkout_id: String,
        ck: CheckoutId,
        local: PathBuf,
        central: CanonicalPath,
        start_rel: CanonicalPath,
        start_meta: Option<FileMetadata>,
    ) -> Result<Self, crate::path::PathError> {
        let mut found = BTreeMap::new();
        let mut pending = Vec::new();
        if let Some(meta) = start_meta {
            let start_path = if start_rel.as_str() == "/" {
                central.clone()
            } else {
                join_central(&central, start_rel.as_str().trim_start_matches('/'))?
            };
            found.insert(start_path, meta);
            pending.push(start_rel);
        }
        Ok(Self {
            checkout_id,
            ck,
            local,
            central,
            pending,
            open: None,
            inflight_stat: 0,
            found,
            awaiting_hash: HashSet::new(),
        })
    }

    pub(crate) fn collect_done(&self) -> bool {
        self.open.is_none() && self.pending.is_empty() && self.inflight_stat == 0
    }

    pub(crate) fn wants_stat(&self) -> bool {
        self.inflight_stat == 0
            && (self
                .open
                .as_ref()
                .is_some_and(|open| !open.remaining.is_empty())
                || !self.pending.is_empty())
    }

    pub(crate) fn take_unstated<E>(
        &mut self,
        budget: usize,
        mut previous: impl FnMut(&CanonicalPath) -> Result<Option<FileMetadata>, E>,
    ) -> Result<Vec<RescanStat>, WalkError<E>> {
        if self.inflight_stat > 0 {
            return Ok(Vec::new());
        }
        let mut used = 0;
        let mut needs = Vec::new();
        while used < budget {
            if let Some(open) = &mut self.open {
                if let Some((name, host_child)) = open.remaining.pop() {
                    let rel_child = match join_central(&open.rel, &name) {
                        Ok(path) => path,
                        Err(err) => {
                            self.inflight_stat = self.inflight_stat.saturating_sub(needs.len());
                            return Err(WalkError::Path(err));
                        }
                    };
                    let child = match join_central(
                        &self.central,
                        rel_child.as_str().trim_start_matches('/'),
                    ) {
                        Ok(path) => path,
                        Err(err) => {
                            self.inflight_stat = self.inflight_stat.saturating_sub(needs.len());
                            return Err(WalkError::Path(err));
                        }
                    };
                    let prior = match previous(&child) {
                        Ok(prior) => prior,
                        Err(err) => {
                            self.inflight_stat = self.inflight_stat.saturating_sub(needs.len());
                            return Err(WalkError::Index(err));
                        }
                    };
                    needs.push(RescanStat {
                        checkout_id: self.checkout_id.clone(),
                        child,
                        rel_child,
                        host: host_child,
                        prior,
                    });
                    self.inflight_stat += 1;
                    used += 1;
                    continue;
                }
                self.open = None;
            }
            let Some(rel_dir) = self.pending.pop() else {
                return Ok(needs);
            };
            let host = canonical_to_host(&self.local, &rel_dir);
            let remaining = match read_dir_names(&host, rel_dir.as_str() == "/") {
                Ok(names) => names,
                Err(err)
                    if err.kind() == io::ErrorKind::NotFound
                        || err.kind() == io::ErrorKind::PermissionDenied =>
                {
                    if err.kind() == io::ErrorKind::PermissionDenied {
                        log::warn!("skipping {}: permission denied", host.display());
                    }
                    continue;
                }
                Err(source) => {
                    self.inflight_stat = self.inflight_stat.saturating_sub(needs.len());
                    return Err(WalkError::Io { path: host, source });
                }
            };
            self.open = Some(OpenDir {
                rel: rel_dir,
                remaining,
            });
        }
        Ok(needs)
    }

    pub(crate) fn apply_stated(
        &mut self,
        stated: RescanStated,
    ) -> Result<Option<Walked>, WalkError<io::Error>> {
        self.inflight_stat = self.inflight_stat.saturating_sub(1);
        let RescanStated { stat, result } = stated;
        let inspected = result.map_err(|source| WalkError::Io {
            path: stat.host.clone(),
            source,
        })?;
        match inspected {
            Inspected::Ready(meta) => {
                if meta.kind == EntryKind::Dir {
                    self.pending.push(stat.rel_child);
                }
                self.found.insert(stat.child.clone(), meta.clone());
                Ok(Some(Walked::Ready(stat.child, meta)))
            }
            Inspected::NeedHash(host) => {
                self.awaiting_hash.insert(stat.child.clone());
                Ok(Some(Walked::Needs(HashNeed {
                    key: HashKey::Checkout {
                        id: stat.checkout_id,
                        path: stat.child,
                    },
                    host,
                    previous: stat.prior,
                })))
            }
            Inspected::Absent => Ok(None),
        }
    }

    pub(crate) fn collect<E>(
        &mut self,
        budget: usize,
        mut previous: impl FnMut(&CanonicalPath) -> Result<Option<FileMetadata>, E>,
    ) -> Result<Vec<Walked>, WalkError<E>> {
        let mut newly = Vec::new();
        let mut used = 0;
        while used < budget {
            let needs = self.take_unstated(budget - used, &mut previous)?;
            if needs.is_empty() {
                break;
            }
            let total = needs.len();
            for (i, need) in needs.into_iter().enumerate() {
                let stated = need.inspect();
                if let Err(source) = stated.result {
                    let remaining = total - i;
                    self.inflight_stat = self.inflight_stat.saturating_sub(remaining);
                    return Err(WalkError::Io {
                        path: stated.stat.host,
                        source,
                    });
                }
                match self.apply_stated(stated) {
                    Ok(Some(row)) => newly.push(row),
                    Ok(None) => {}
                    Err(WalkError::Io { path, source }) => {
                        let remaining = total - i - 1;
                        self.inflight_stat = self.inflight_stat.saturating_sub(remaining);
                        return Err(WalkError::Io { path, source });
                    }
                    Err(WalkError::Index(_)) | Err(WalkError::Path(_)) => unreachable!(),
                }
                used += 1;
            }
        }
        Ok(newly)
    }
}

fn read_dir_names(
    host: &std::path::Path,
    at_root: bool,
) -> Result<Vec<(String, PathBuf)>, io::Error> {
    let mut names = Vec::new();
    for entry in fs::read_dir(host)? {
        let entry = entry?;
        let raw = entry.file_name();
        let Some(name) = raw.to_str() else {
            log::warn!("skipping non-UTF-8 name under {}", host.display());
            continue;
        };
        if at_root && is_reserved_root_entry(name) {
            continue;
        }
        names.push((name.to_string(), entry.path()));
    }
    Ok(names)
}

impl DirListPage {
    pub(crate) fn from_merge(
        checkout_id: String,
        path: CanonicalPath,
        after: Option<EntryName>,
        more: bool,
        page_end: Option<String>,
        list_epoch: u64,
        by_name: BTreeMap<String, (Option<DirChild>, Option<DirChild>)>,
    ) -> Self {
        Self {
            checkout_id,
            path,
            after,
            more,
            page_end,
            list_epoch,
            remaining: by_name.into_iter().collect(),
        }
    }

    pub(crate) fn next_page_request(&self) -> Option<ProtocolMessage> {
        if !self.more {
            return None;
        }
        let last = self.page_end.as_ref()?;
        Some(ProtocolMessage::DirListRequest {
            checkout_id: self.checkout_id.clone(),
            path: self.path.clone(),
            after: EntryName::parse(last).ok(),
        })
    }
}

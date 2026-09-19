use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io;
use std::path::PathBuf;

use crate::merkle::DirChild;
use crate::meta::{self, EntryKind, FileMetadata};
use crate::path::{
    CanonicalPath, EntryName, PathError, canonical_to_host, is_reserved_root_entry, join_central,
};
use crate::protocol::ProtocolMessage;
use crate::storage::CheckoutId;

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
    pub found: BTreeMap<CanonicalPath, FileMetadata>,
}

struct OpenDir {
    rel: CanonicalPath,
    remaining: Vec<(String, PathBuf)>,
}

pub(crate) struct DirListPage {
    pub checkout_id: String,
    pub path: CanonicalPath,
    pub after: Option<EntryName>,
    pub more: bool,
    pub page_end: Option<String>,
    pub remaining: VecDeque<(String, (Option<DirChild>, Option<DirChild>))>,
}

impl Crawl {
    pub(crate) fn pending(&self) -> bool {
        !self.rescans.is_empty() || !self.pages.is_empty()
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

    pub(crate) fn front_rescan_mut(&mut self) -> Option<&mut RescanWalk> {
        self.rescans.front_mut()
    }

    pub(crate) fn take_finished_rescan(&mut self) -> Option<RescanWalk> {
        let done = self.rescans.front().is_some_and(|walk| walk.collect_done());
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
            found,
        })
    }

    pub(crate) fn collect_done(&self) -> bool {
        self.open.is_none() && self.pending.is_empty()
    }

    pub(crate) fn collect<E>(
        &mut self,
        budget: usize,
        mut previous: impl FnMut(&CanonicalPath) -> Result<Option<FileMetadata>, E>,
    ) -> Result<Vec<(CanonicalPath, FileMetadata)>, WalkError<E>> {
        let mut used = 0;
        let mut newly = Vec::new();
        while used < budget {
            if let Some(open) = &mut self.open {
                if let Some((name, host_child)) = open.remaining.pop() {
                    let rel_child = join_central(&open.rel, &name).map_err(WalkError::Path)?;
                    let child =
                        join_central(&self.central, rel_child.as_str().trim_start_matches('/'))
                            .map_err(WalkError::Path)?;
                    let prior = previous(&child).map_err(WalkError::Index)?;
                    match meta::collect_for_rescan(&host_child, prior.as_ref()) {
                        Ok(Some(meta)) => {
                            if meta.kind == EntryKind::Dir {
                                self.pending.push(rel_child);
                            }
                            self.found.insert(child.clone(), meta.clone());
                            newly.push((child, meta));
                        }
                        Ok(None) => {}
                        Err(source) => {
                            return Err(WalkError::Io {
                                path: host_child,
                                source,
                            });
                        }
                    }
                    used += 1;
                    continue;
                }
                self.open = None;
            }
            let Some(rel_dir) = self.pending.pop() else {
                return Ok(newly);
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
                    return Err(WalkError::Io { path: host, source });
                }
            };
            self.open = Some(OpenDir {
                rel: rel_dir,
                remaining,
            });
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
        by_name: BTreeMap<String, (Option<DirChild>, Option<DirChild>)>,
    ) -> Self {
        Self {
            checkout_id,
            path,
            after,
            more,
            page_end,
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

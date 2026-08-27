// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::BTreeSet;

use crate::Error;
use crate::filesystem::{NamespaceNode, NamespaceRecord, NodeKind};
use crate::format::FileExtentMap;
use crate::volume::Namespace;
use crate::work::OrderedMerge;
use crate::work::WorkContext;

pub(crate) enum ReconcilePlan {
    Conflicted(Vec<String>),
    Remote,
    Publish(Namespace<FileExtentMap>),
}

pub(crate) fn changed_paths(
    base: &Namespace<FileExtentMap>,
    side: &Namespace<FileExtentMap>,
) -> Result<BTreeSet<String>, Error> {
    require_same_volume(base, side)?;
    let mut records = OrderedMerge::from_readers(
        vec![base.reader()?, side.reader()?],
        |record: &NamespaceRecord<FileExtentMap>| Ok(record.path.clone()),
    )?;
    let mut changed = BTreeSet::new();
    while let Some((path, [base_record, side_record])) =
        records.reduce_next_group([None, None], |group, source, record| {
            group[source] = Some(record);
            Ok(())
        })?
    {
        if !same_entry(base_record.as_ref(), side_record.as_ref()) {
            changed.insert(path);
        }
    }
    Ok(changed)
}

pub(crate) fn reconcile(
    common: &Namespace<FileExtentMap>,
    local: &Namespace<FileExtentMap>,
    remote: &Namespace<FileExtentMap>,
    resolved: &BTreeSet<String>,
    workspace: &WorkContext,
) -> Result<ReconcilePlan, Error> {
    require_same_volume(common, local)?;
    require_same_volume(common, remote)?;
    if common.cursor.sequence() > remote.cursor.sequence() {
        return Err(Error::corrupt(
            "reconcile replica",
            "reconciliation ancestry is invalid",
        ));
    }

    let directory_conflicts = directory_conflicts(common, local, remote)?;
    let mut target = workspace.writer("reconciled-namespace")?;
    let mut records = OrderedMerge::from_readers(
        vec![common.reader()?, local.reader()?, remote.reader()?],
        |record: &NamespaceRecord<FileExtentMap>| Ok(record.path.clone()),
    )?;
    let mut directory_conflicts = directory_conflicts.into_iter().peekable();
    let mut active_directories = Vec::<(String, bool)>::new();
    let mut unresolved_directories = 0_usize;
    let mut conflicts = Vec::new();
    let mut resolved_conflicts = BTreeSet::new();
    let mut differs_from_remote = false;

    while let Some((path, [common_record, local_record, remote_record])) = records
        .reduce_next_group([None, None, None], |group, source, record| {
            group[source] = Some(record);
            Ok(())
        })?
    {
        while active_directories
            .last()
            .is_some_and(|(directory, _)| path != *directory && !is_descendant(directory, &path))
        {
            if !active_directories.pop().expect("active directory exists").1 {
                unresolved_directories -= 1;
            }
        }
        while directory_conflicts
            .peek()
            .is_some_and(|directory| directory == &path)
        {
            let directory = directory_conflicts
                .next()
                .expect("peeked directory conflict");
            let is_resolved = resolved.contains(&directory);
            if is_resolved {
                resolved_conflicts.insert(directory.clone());
            } else {
                unresolved_directories += 1;
            }
            active_directories.push((directory, is_resolved));
        }

        let remote_comparison = remote_record.clone();
        let blocked = unresolved_directories != 0;
        let forced_local = !blocked && !active_directories.is_empty();

        let selected = if blocked {
            if active_directories
                .iter()
                .any(|(directory, is_resolved)| directory == &path && !is_resolved)
            {
                conflicts.push(path.clone());
            }
            None
        } else if forced_local {
            local_record.and_then(live_record)
        } else {
            let local_changed = !same_entry(common_record.as_ref(), local_record.as_ref());
            let remote_changed = !same_entry(common_record.as_ref(), remote_record.as_ref());
            match (local_changed, remote_changed) {
                (false, false) | (false, true) => remote_record.and_then(live_record),
                (true, false) => local_record.and_then(live_record),
                (true, true) if same_entry(local_record.as_ref(), remote_record.as_ref()) => {
                    remote_record.and_then(live_record)
                }
                (true, true) if resolved.contains(&path) => {
                    resolved_conflicts.insert(path.clone());
                    local_record.and_then(live_record)
                }
                (true, true) => {
                    conflicts.push(path.clone());
                    None
                }
            }
        };

        if !same_entry(selected.as_ref(), remote_comparison.as_ref()) {
            differs_from_remote = true;
        }
        if let Some(record) = selected {
            target.write(&record)?;
        }
    }

    if resolved_conflicts != *resolved {
        let missing = resolved
            .difference(&resolved_conflicts)
            .cloned()
            .collect::<Vec<_>>();
        return Err(Error::invalid(
            "synchronize replica",
            format!("no unresolved conflict exists for {missing:?}"),
        ));
    }

    conflicts.sort();
    conflicts.dedup();
    if !conflicts.is_empty() {
        return Ok(ReconcilePlan::Conflicted(conflicts));
    }
    if !differs_from_remote {
        return Ok(ReconcilePlan::Remote);
    }

    let sequence =
        remote.cursor.sequence().checked_add(1).ok_or_else(|| {
            Error::corrupt("reconcile replica", "Managed change sequence overflows")
        })?;
    Ok(ReconcilePlan::Publish(Namespace {
        volume_id: remote.volume_id,
        cursor: crate::filesystem::ChangeCursor::from_sequence(sequence),
        root: remote.root,
        entries: target.finish()?,
    }))
}

fn directory_conflicts(
    common: &Namespace<FileExtentMap>,
    local: &Namespace<FileExtentMap>,
    remote: &Namespace<FileExtentMap>,
) -> Result<Vec<String>, Error> {
    let mut records = OrderedMerge::from_readers(
        vec![common.reader()?, local.reader()?, remote.reader()?],
        |record: &NamespaceRecord<FileExtentMap>| Ok(record.path.clone()),
    )?;
    let mut pending = Vec::<DirectoryWatch>::new();
    let mut conflicts = Vec::new();
    let mut local_changes = 0_u64;
    let mut remote_changes = 0_u64;

    while let Some((path, [common_record, local_record, remote_record])) = records
        .reduce_next_group([None, None, None], |group, source, record| {
            group[source] = Some(record);
            Ok(())
        })?
    {
        while pending
            .last()
            .is_some_and(|watch| path != watch.path && !is_descendant(&watch.path, &path))
        {
            let watch = pending.pop().expect("pending directory exists");
            if watch.changed(local_changes, remote_changes) {
                conflicts.push(watch.path);
            }
        }

        let local_changed = !same_entry(common_record.as_ref(), local_record.as_ref());
        let remote_changed = !same_entry(common_record.as_ref(), remote_record.as_ref());
        if local_changed {
            local_changes = local_changes.saturating_add(1);
        }
        if remote_changed {
            remote_changes = remote_changes.saturating_add(1);
        }

        if kind(common_record.as_ref()) == Some(NodeKind::Directory) {
            let local_kept = kind(local_record.as_ref()) == Some(NodeKind::Directory);
            let remote_kept = kind(remote_record.as_ref()) == Some(NodeKind::Directory);
            if !local_kept && remote_kept {
                pending.push(DirectoryWatch {
                    path: path.clone(),
                    side: WatchedSide::Remote,
                    baseline: remote_changes,
                });
            } else if local_kept && !remote_kept {
                pending.push(DirectoryWatch {
                    path,
                    side: WatchedSide::Local,
                    baseline: local_changes,
                });
            }
        }
    }
    conflicts.extend(pending.into_iter().filter_map(|watch| {
        watch
            .changed(local_changes, remote_changes)
            .then_some(watch.path)
    }));
    conflicts.sort();
    conflicts.dedup();
    Ok(conflicts)
}

#[derive(Clone, Copy)]
enum WatchedSide {
    Local,
    Remote,
}

struct DirectoryWatch {
    path: String,
    side: WatchedSide,
    baseline: u64,
}

impl DirectoryWatch {
    fn changed(&self, local_changes: u64, remote_changes: u64) -> bool {
        match self.side {
            WatchedSide::Local => local_changes != self.baseline,
            WatchedSide::Remote => remote_changes != self.baseline,
        }
    }
}

fn same_entry<L, R>(left: Option<&NamespaceRecord<L>>, right: Option<&NamespaceRecord<R>>) -> bool {
    match (
        left.and_then(|record| record.value.as_ref()),
        right.and_then(|record| record.value.as_ref()),
    ) {
        (None, None) => true,
        (Some(left), Some(right)) => same_node(left, right),
        _ => false,
    }
}

fn same_node<L, R>(left: &NamespaceNode<L>, right: &NamespaceNode<R>) -> bool {
    left.node_id == right.node_id
        && left.attributes == right.attributes
        && match (left.file(), right.file()) {
            (None, None) => true,
            (Some((left, _, _)), Some((right, _, _))) => left == right,
            _ => false,
        }
}

fn kind<C>(record: Option<&NamespaceRecord<C>>) -> Option<NodeKind> {
    record.and_then(|record| record.value.as_ref().map(NamespaceNode::kind))
}

fn live_record(record: NamespaceRecord<FileExtentMap>) -> Option<NamespaceRecord<FileExtentMap>> {
    record.value.is_some().then_some(record)
}

fn require_same_volume<L, R>(left: &Namespace<L>, right: &Namespace<R>) -> Result<(), Error> {
    if left.volume_id != right.volume_id || left.root != right.root {
        return Err(Error::corrupt(
            "reconcile replica",
            "reconciliation namespaces belong to different volumes",
        ));
    }
    Ok(())
}

fn is_descendant(directory: &str, path: &str) -> bool {
    if directory.is_empty() {
        return !path.is_empty();
    }
    path.strip_prefix(directory)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

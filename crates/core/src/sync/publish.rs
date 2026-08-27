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

//! Durable file publication followed by atomic namespace publication.

use std::path::{Path, PathBuf};

use crate::Error;
use crate::data::{ContentHasher, ContentReuseLookup};
use crate::filesystem::{
    ContentRef, Digest, NamespaceRecord, NamespaceValue, NodeKind, OperationId,
};
use crate::format::{FileExtentMap, NamespaceRevision};
use crate::volume::AccessFamily;
use crate::volume::{ManagedObservation, ManagedVolume, Namespace};
use crate::work::JoinItem;
use crate::work::{Spool, SpoolWriter, WorkContext};
use futures::{TryStreamExt as _, stream};
use serde::{Deserialize, Serialize};

use super::FileChangeSetEntry;
use super::SyncEngine;
use super::replica::scan::{LocalEntry, LocalFile, LocalRecord};
use super::replica::state_store::ReplicaStateFile;
use super::state::ReplicaState;
use super::transfer::{FilePublication, publish_file, publish_file_into};

impl<A: AccessFamily> SyncEngine<A> {
    pub(super) async fn prepare_and_commit(
        &self,
        workspace: &WorkContext,
        state_file: &mut ReplicaStateFile,
        state: &mut ReplicaState,
        observed: &ManagedObservation,
        target: &Namespace<FileExtentMap>,
    ) -> Result<NamespaceRevision, Error> {
        let operation = OperationId::generate();
        let revision = self
            .volume
            .prepare_publication(workspace, observed, target, operation)
            .await?;
        state.begin_publication(
            observed.revision(),
            revision,
            operation,
            observed.gc_epoch(),
        )?;
        state_file.persist(state)?;
        self.volume
            .commit_publication(observed, revision, operation)
            .await?;
        Ok(revision)
    }

    /// Observe local files without writing remote objects.
    ///
    /// Unchanged base files keep their maps. Changed files keep only a content
    /// identity; [`Self::publish_planned_files`] uploads them after planning.
    pub(super) async fn observe_local_files(
        &self,
        workspace: &WorkContext,
        root: &Path,
        base: &Namespace<FileExtentMap>,
        entries: Spool<LocalEntry>,
        mutations: Option<&[FileChangeSetEntry]>,
    ) -> Result<Spool<LocalRecord>, Error> {
        let trusted_mutations = mutations.is_some();
        let base_has_files = namespace_has_files(base)?;
        let mut completed = workspace.writer("observed-local-files")?;
        let mut pending = workspace.writer("observed-local-files-pending")?;
        let mut mutation_records = workspace.writer("trusted-file-mutations")?;
        for mutation in mutations.unwrap_or_default() {
            mutation_records.write(mutation)?;
        }
        let mutation_records = crate::work::sort(
            workspace,
            &mutation_records.finish()?,
            |mutation: &FileChangeSetEntry| mutation.path.clone(),
        )?;
        let mut entries = entries.reader()?;
        let mut base_records = base.reader()?;
        let mut mutation_records = mutation_records.reader()?;
        let mut base_head = base_records.next()?;
        let mut mutation_head = mutation_records.next()?;
        while let Some(entry) = entries.next()? {
            while base_head
                .as_ref()
                .is_some_and(|record| record.path < entry.path)
            {
                base_head = base_records.next()?;
            }
            if mutation_head
                .as_ref()
                .is_some_and(|mutation| mutation.path < entry.path)
            {
                return Err(Error::invalid(
                    "observe local files",
                    "trusted mutation path is absent from the local directory",
                ));
            }
            if entry.kind != NodeKind::RegularFile {
                if mutation_head
                    .as_ref()
                    .is_some_and(|mutation| mutation.path == entry.path)
                {
                    return Err(Error::invalid(
                        "observe local files",
                        "trusted mutation path is not a regular file",
                    ));
                }
                completed.write(&LocalRecord {
                    path: entry.path,
                    kind: entry.kind,
                    executable: entry.executable,
                    file: None,
                })?;
                continue;
            }
            let previous = base_head
                .as_ref()
                .filter(|record| record.path == entry.path)
                .and_then(|record| record.value.as_ref())
                .and_then(|node| node.file())
                .map(|(_, content, data)| LocalFile {
                    content,
                    data: data.clone(),
                });
            let mutation = if mutation_head
                .as_ref()
                .is_some_and(|mutation| mutation.path == entry.path)
            {
                let mutation = mutation_head.take().expect("matching mutation exists");
                mutation_head = mutation_records.next()?;
                let previous = previous.as_ref().ok_or_else(|| {
                    Error::invalid(
                        "observe local files",
                        "trusted mutation has no base regular file",
                    )
                })?;
                if previous.content != mutation.base {
                    return Err(Error::invalid(
                        "observe local files",
                        "trusted mutation does not match the base file content",
                    ));
                }
                Some(mutation)
            } else {
                None
            };
            let length = entry.length.expect("a scanned regular file has a length");
            if trusted_mutations
                && mutation.is_none()
                && let Some(previous) = previous
            {
                if length != previous.content.length() {
                    return Err(Error::invalid(
                        "observe local files",
                        "trusted mutations omit a file whose length changed",
                    ));
                }
                completed.write(&LocalRecord {
                    path: entry.path,
                    kind: entry.kind,
                    executable: entry.executable,
                    file: Some(previous),
                })?;
                continue;
            }
            // ContentRef is a function of the publish stream. Hash locally only
            // when identity is the decision: same-path no-op detection, or a
            // new path that may be a rename of a known base file.
            let identity_decides_publication = mutation.is_none()
                && (previous
                    .as_ref()
                    .is_some_and(|previous| previous.content.length() == length)
                    || (previous.is_none() && (trusted_mutations || base_has_files)));
            if identity_decides_publication {
                pending.write(&PendingObserve {
                    path: entry.path,
                    executable: entry.executable,
                    length,
                    previous,
                })?;
                continue;
            }
            completed.write(&LocalRecord {
                path: entry.path,
                kind: entry.kind,
                executable: entry.executable,
                file: Some(unpublished_file(length)),
            })?;
        }
        if mutation_head.is_some() {
            return Err(Error::invalid(
                "observe local files",
                "trusted mutation path is absent from the local directory",
            ));
        }
        let hashed = hash_pending_files(
            workspace,
            root,
            pending.finish()?,
            self.volume.stream_concurrency(),
        )
        .await?;
        crate::work::merge_sorted(
            workspace,
            vec![completed.finish()?, hashed],
            |record: &LocalRecord| record.path.clone(),
        )
    }

    /// Upload only files selected by the plan that still lack durable maps.
    pub(super) async fn publish_planned_files(
        &self,
        workspace: &WorkContext,
        root: &Path,
        observed: &ManagedObservation,
        target: &Namespace<FileExtentMap>,
        mutations: Option<&[FileChangeSetEntry]>,
    ) -> Result<Namespace<FileExtentMap>, Error> {
        let mut unpublished = workspace.writer("planned-unpublished-files")?;
        let mut records = target.reader()?;
        let mut needed = false;
        while let Some(record) = records.next()? {
            if let Some(node) = &record.value
                && let Some((.., content, data)) = node.file()
                && needs_remote_publication(content, data)
            {
                needed = true;
                unpublished.write(&LocalEntry {
                    path: record.path,
                    kind: NodeKind::RegularFile,
                    executable: node.attributes.executable,
                    length: Some(content.length()),
                })?;
            }
        }
        if !needed {
            return Ok(target.clone());
        }
        let published = self
            .publish_local_files(
                workspace,
                root,
                observed,
                &observed.namespace,
                unpublished.finish()?,
                mutations,
            )
            .await?;
        splice_published_maps(workspace, target, &published)
    }

    pub(super) async fn publish_local_files(
        &self,
        workspace: &WorkContext,
        root: &Path,
        observed: &ManagedObservation,
        base: &Namespace<FileExtentMap>,
        entries: Spool<LocalEntry>,
        mutations: Option<&[FileChangeSetEntry]>,
    ) -> Result<Spool<LocalRecord>, Error> {
        let trusted_mutations = mutations.is_some();
        let mut completed = workspace.writer("published-local-files")?;
        let shared_segment_target = self.volume.data_segment_target_bytes();
        let concurrency = self.volume.stream_concurrency();
        let mut standalone_lanes = Vec::new();
        let mut standalone_lane = 0_usize;
        let mut placement_groups = Vec::new();
        let mut placement_group = None;
        let mut placement_group_bytes = 0_u64;
        let mut placement_group_stored_bound = Some(0_u64);
        let mut mutation_records = workspace.writer("trusted-file-mutations")?;
        for mutation in mutations.unwrap_or_default() {
            mutation_records.write(mutation)?;
        }
        let mutation_records = crate::work::sort(
            workspace,
            &mutation_records.finish()?,
            |mutation: &FileChangeSetEntry| mutation.path.clone(),
        )?;
        let mut entries = entries.reader()?;
        let mut base_records = base.reader()?;
        let mut mutation_records = mutation_records.reader()?;
        let mut base_head = base_records.next()?;
        let mut mutation_head = mutation_records.next()?;
        while let Some(entry) = entries.next()? {
            while base_head
                .as_ref()
                .is_some_and(|record| record.path < entry.path)
            {
                base_head = base_records.next()?;
            }
            if mutation_head
                .as_ref()
                .is_some_and(|mutation| mutation.path < entry.path)
            {
                return Err(Error::invalid(
                    "publish Managed files",
                    "trusted mutation path is absent from the local directory",
                ));
            }
            if entry.kind != NodeKind::RegularFile {
                if mutation_head
                    .as_ref()
                    .is_some_and(|mutation| mutation.path == entry.path)
                {
                    return Err(Error::invalid(
                        "publish Managed files",
                        "trusted mutation path is not a regular file",
                    ));
                }
                completed.write(&LocalRecord {
                    path: entry.path,
                    kind: entry.kind,
                    executable: entry.executable,
                    file: None,
                })?;
                continue;
            }
            let previous = base_head
                .as_ref()
                .filter(|record| record.path == entry.path)
                .and_then(|record| record.value.as_ref())
                .and_then(|node| node.file())
                .map(|(_, content, data)| LocalFile {
                    content,
                    data: data.clone(),
                });
            let mutation = if mutation_head
                .as_ref()
                .is_some_and(|mutation| mutation.path == entry.path)
            {
                let mutation = mutation_head.take().expect("matching mutation exists");
                mutation_head = mutation_records.next()?;
                let previous = previous.as_ref().ok_or_else(|| {
                    Error::invalid(
                        "publish Managed files",
                        "trusted mutation has no base regular file",
                    )
                })?;
                if previous.content != mutation.base {
                    return Err(Error::invalid(
                        "publish Managed files",
                        "trusted mutation does not match the base file content",
                    ));
                }
                Some(mutation)
            } else {
                None
            };
            if trusted_mutations
                && mutation.is_none()
                && let Some(previous) = previous
            {
                if entry.length != Some(previous.content.length()) {
                    return Err(Error::invalid(
                        "publish Managed files",
                        "trusted mutations omit a file whose length changed",
                    ));
                }
                completed.write(&LocalRecord {
                    path: entry.path,
                    kind: entry.kind,
                    executable: entry.executable,
                    file: Some(previous),
                })?;
                continue;
            }
            let length = entry.length.expect("a scanned regular file has a length");
            let pending = PendingFile {
                path: entry.path,
                executable: entry.executable,
                length,
                publication: match (previous, mutation) {
                    (Some(previous), Some(mutation))
                        if supports_patch(&mutation, mutation.base.length(), length) =>
                    {
                        FilePublication::Changed {
                            previous: Box::new(previous),
                            ranges: mutation.ranges,
                        }
                    }
                    _ => FilePublication::Complete,
                },
            };
            if length != 0 && length <= shared_segment_target {
                if placement_group.is_none() {
                    placement_group = Some(workspace.writer("shared-segment-publication-group")?);
                }
                placement_group
                    .as_mut()
                    .expect("shared placement group is open")
                    .write(&pending)?;
                placement_group_stored_bound = placement_group_stored_bound
                    .zip(self.volume.stored_size_bound(length))
                    .and_then(|(group, extent)| group.checked_add(extent));
                placement_group_bytes =
                    placement_group_bytes.checked_add(length).ok_or_else(|| {
                        Error::invalid("publish Managed files", "placement byte count overflows")
                    })?;
                if placement_group_bytes >= shared_segment_target {
                    placement_groups.push((
                        placement_group
                            .take()
                            .expect("shared placement group is open")
                            .finish()?,
                        placement_group_stored_bound,
                    ));
                    placement_group_bytes = 0;
                    placement_group_stored_bound = Some(0);
                }
            } else {
                if standalone_lanes.len() < concurrency {
                    standalone_lanes.push(workspace.writer("standalone-publication-lane")?);
                }
                standalone_lanes[standalone_lane].write(&pending)?;
                standalone_lane = (standalone_lane + 1) % concurrency;
            }
        }
        if mutation_head.is_some() {
            return Err(Error::invalid(
                "publish Managed files",
                "trusted mutation path is absent from the local directory",
            ));
        }
        if standalone_lanes.is_empty() && placement_group.is_none() && placement_groups.is_empty() {
            return completed.finish();
        }
        if let Some(group) = placement_group {
            placement_groups.push((group.finish()?, placement_group_stored_bound));
        }

        let known = self.volume.known_content(workspace, observed, base).await?;
        let standalone_lanes = standalone_lanes
            .into_iter()
            .map(SpoolWriter::finish)
            .collect::<Result<Vec<_>, _>>()?;
        let gc_epoch = observed.gc_epoch();
        let groups = standalone_lanes
            .into_iter()
            .map(|files| PublicationGroup {
                files,
                placement: PlacementPlan::Isolated,
            })
            .chain(
                placement_groups
                    .into_iter()
                    .map(|(files, stored_payload_bound)| PublicationGroup {
                        files,
                        placement: PlacementPlan::Shared(stored_payload_bound),
                    }),
            );
        let publications = stream::iter(groups.map(Ok)).map_ok({
            let volume = self.volume.clone();
            let workspace = workspace.clone();
            let root = root.to_owned();
            let known = known.clone();
            move |group| {
                publish_group_task(
                    volume.clone(),
                    workspace.clone(),
                    root.clone(),
                    group,
                    known.clone(),
                    gc_epoch,
                )
            }
        });
        let publications = publications.try_buffer_unordered(self.volume.stream_concurrency());
        futures::pin_mut!(publications);
        let mut ordered = vec![completed.finish()?];
        while let Some(publication) = publications.try_next().await? {
            ordered.push(publication);
        }
        crate::work::merge_sorted(workspace, ordered, |record: &LocalRecord| {
            record.path.clone()
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PendingFile {
    path: String,
    executable: bool,
    length: u64,
    publication: FilePublication,
}

fn supports_patch(mutation: &FileChangeSetEntry, old_length: u64, new_length: u64) -> bool {
    if new_length < old_length || mutation.ranges.is_empty() {
        return false;
    }
    if mutation
        .ranges
        .iter()
        .any(|range| range.end().ok().is_none_or(|end| end > new_length))
    {
        return false;
    }
    if new_length == old_length {
        return true;
    }
    let mut covered = old_length;
    for range in &mutation.ranges {
        let Ok(end) = range.end() else {
            return false;
        };
        if end <= old_length {
            continue;
        }
        if range.offset > covered {
            return false;
        }
        covered = covered.max(end);
    }
    covered == new_length
}

struct PublicationGroup {
    files: Spool<PendingFile>,
    placement: PlacementPlan,
}

enum PlacementPlan {
    Isolated,
    Shared(Option<u64>),
}

async fn publish_group_task<A: AccessFamily>(
    volume: ManagedVolume<A>,
    workspace: WorkContext,
    root: PathBuf,
    group: PublicationGroup,
    known: ContentReuseLookup,
    gc_epoch: crate::format::GcEpoch,
) -> Result<Spool<LocalRecord>, Error> {
    let mut placement = match group.placement {
        PlacementPlan::Isolated => None,
        PlacementPlan::Shared(stored_bound) => {
            Some(volume.data_placement(gc_epoch, u64::MAX, stored_bound))
        }
    };
    let result = async {
        let mut output = workspace.writer("file-publication-results")?;
        let mut files = group.files.reader()?;
        let mut retains_segment = false;
        while let Some(file) = files.next()? {
            let path = root.join(&file.path);
            let length = file.length;
            let (content, data) = match placement.as_mut() {
                Some(placement) => {
                    let (content, data, reused) = publish_file_into(
                        &volume,
                        placement,
                        &path,
                        &file.publication,
                        length,
                        &known,
                    )
                    .await?;
                    retains_segment |= !reused && content.length() != 0;
                    (content, data)
                }
                None => {
                    publish_file(&volume, &path, &file.publication, length, &known, gc_epoch)
                        .await?
                }
            };
            output.write(&LocalRecord {
                path: file.path,
                kind: NodeKind::RegularFile,
                executable: file.executable,
                file: Some(LocalFile { content, data }),
            })?;
        }
        if let Some(mut placement) = placement.take() {
            if retains_segment {
                placement.finish().await?;
            } else {
                placement.abort().await;
            }
        }
        output.finish()
    }
    .await;
    match result {
        Ok(files) => Ok(files),
        Err(error) => {
            if let Some(mut placement) = placement {
                placement.abort().await;
            }
            Err(error)
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PendingObserve {
    path: String,
    executable: bool,
    length: u64,
    previous: Option<LocalFile>,
}

async fn hash_pending_files(
    workspace: &WorkContext,
    root: &Path,
    pending: Spool<PendingObserve>,
    concurrency: usize,
) -> Result<Spool<LocalRecord>, Error> {
    let concurrency = concurrency.max(1);
    let mut lanes = Vec::new();
    let mut lane = 0_usize;
    let mut records = pending.reader()?;
    while let Some(item) = records.next()? {
        if lanes.len() < concurrency {
            lanes.push(workspace.writer("observe-hash-lane")?);
        }
        lanes[lane].write(&item)?;
        lane = (lane + 1) % lanes.len();
    }
    if lanes.is_empty() {
        return workspace.writer("observe-hash-empty")?.finish();
    }
    let lanes = lanes
        .into_iter()
        .map(SpoolWriter::finish)
        .collect::<Result<Vec<_>, _>>()?;
    let hashed = stream::iter(lanes.into_iter().map(Ok)).map_ok({
        let root = root.to_owned();
        let workspace = workspace.clone();
        move |lane| hash_observe_lane(workspace.clone(), root.clone(), lane)
    });
    let hashed = hashed.try_buffer_unordered(concurrency);
    futures::pin_mut!(hashed);
    let mut finished = Vec::new();
    while let Some(spool) = hashed.try_next().await? {
        finished.push(spool);
    }
    crate::work::merge_sorted(workspace, finished, |record: &LocalRecord| {
        record.path.clone()
    })
}

async fn hash_observe_lane(
    workspace: WorkContext,
    root: PathBuf,
    pending: Spool<PendingObserve>,
) -> Result<Spool<LocalRecord>, Error> {
    let mut output = workspace.writer("observe-hash-results")?;
    let mut records = pending.reader()?;
    while let Some(item) = records.next()? {
        let path = local_path(&root, &item.path);
        let content = hash_local_file(&path).await?;
        if item.length != content.length() {
            return Err(Error::conflict(
                "observe local files",
                "local file changed while it was being observed",
            ));
        }
        let file = match item.previous.filter(|previous| previous.content == content) {
            Some(previous) => previous,
            None => LocalFile {
                content,
                data: FileExtentMap::empty(),
            },
        };
        output.write(&LocalRecord {
            path: item.path,
            kind: NodeKind::RegularFile,
            executable: item.executable,
            file: Some(file),
        })?;
    }
    output.finish()
}

fn needs_remote_publication(content: ContentRef, data: &FileExtentMap) -> bool {
    content.length() > 0 && data.base_run.is_none() && data.patch_levels.is_empty()
}

fn unpublished_file(length: u64) -> LocalFile {
    LocalFile {
        content: unpublished_content(length),
        data: FileExtentMap::empty(),
    }
}

fn unpublished_content(length: u64) -> ContentRef {
    if length == 0 {
        return ContentRef::new(Digest::from_bytes(*blake3::hash(&[]).as_bytes()), 0);
    }
    // Distinct from any hashed payload so rename matching cannot bind an
    // unpublished path to a known base file.
    ContentRef::new(Digest::from_bytes([0; 32]), length)
}

fn namespace_has_files(namespace: &Namespace<FileExtentMap>) -> Result<bool, Error> {
    let mut records = namespace.reader()?;
    while let Some(record) = records.next()? {
        if record
            .value
            .as_ref()
            .is_some_and(|node| node.file().is_some())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn local_path(root: &Path, relative: &str) -> PathBuf {
    if relative.is_empty() {
        return root.to_path_buf();
    }
    root.join(relative.replace('/', std::path::MAIN_SEPARATOR_STR))
}

async fn hash_local_file(path: &Path) -> Result<ContentRef, Error> {
    use tokio::io::AsyncReadExt as _;

    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| Error::from_io("observe local file", Some(path), error))?;
    let mut hasher = ContentHasher::default();
    let mut buffer = vec![0_u8; 256 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|error| Error::from_io("observe local file", Some(path), error))?;
        hasher.observe(&buffer[..read]);
        if read == 0 {
            break;
        }
    }
    hasher
        .complete_content()
        .ok_or_else(|| Error::corrupt("observe local file", "local file hash did not reach EOF"))
}

fn splice_published_maps(
    workspace: &WorkContext,
    target: &Namespace<FileExtentMap>,
    published: &Spool<LocalRecord>,
) -> Result<Namespace<FileExtentMap>, Error> {
    let mut joined = crate::work::OrderedJoin::new(
        target.reader()?,
        published.reader()?,
        |record: &NamespaceRecord<FileExtentMap>| record.path.clone(),
        |record: &LocalRecord| record.path.clone(),
    );
    let mut output = workspace.writer("planned-published-namespace")?;
    while let Some(item) = joined.next()? {
        match item {
            JoinItem::Left(record) => output.write(&record)?,
            JoinItem::Right(local) => {
                return Err(Error::corrupt(
                    "publish planned files",
                    format!(
                        "published path {} is absent from the planned namespace",
                        local.path
                    ),
                ));
            }
            JoinItem::Match(mut record, local) => {
                if let Some(node) = record.value.as_mut()
                    && let Some(file) = local.file
                    && let NamespaceValue::RegularFile { content, data, .. } = &mut node.value
                {
                    *content = file.content;
                    *data = file.data;
                }
                output.write(&record)?;
            }
        }
    }
    Ok(Namespace {
        volume_id: target.volume_id,
        cursor: target.cursor,
        root: target.root,
        entries: output.finish()?,
    })
}

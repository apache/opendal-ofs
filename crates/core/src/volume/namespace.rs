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

//! Encoding, merging, and validation of path-ordered namespace streams.

use std::future::Future;

use futures::StreamExt as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::data::validate_file_map;
use crate::filesystem::{
    ChangeCursor, NamespaceNode, NamespaceRecord, NamespaceValue, NodeId, VolumeId,
    validate_portable_path,
};
use crate::format::{
    FileExtentMap, GcEpoch, NamespaceChangeSegment, NamespaceCommit, ObjectClass,
    RecordStreamSizer, StreamKind, StreamRef,
};
use crate::storage::{RecordStreamReader, RecordStreamWriter};
use crate::work::{
    AsyncOrderedMerge, AsyncOrderedRead, JoinItem, OrderedJoin, OrderedMerge, OrderedRead,
    RunCompactor, Spool, SpoolReader, WorkContext,
};

use super::open::{AccessFamily, ManagedVolume};

/// Ordered namespace projection shared by Managed authority and Sync.
#[derive(Clone)]
pub struct Namespace<C> {
    pub volume_id: VolumeId,
    pub cursor: ChangeCursor,
    pub root: NodeId,
    pub(crate) entries: Spool<NamespaceRecord<C>>,
}

impl<C: DeserializeOwned> Namespace<C> {
    pub fn reader(&self) -> Result<NamespaceReader<C>, Error> {
        Ok(NamespaceReader {
            inner: self.entries.reader()?,
            previous_path: None,
        })
    }
}

pub struct NamespaceReader<C> {
    inner: SpoolReader<NamespaceRecord<C>>,
    previous_path: Option<String>,
}

impl<C: DeserializeOwned> NamespaceReader<C> {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<NamespaceRecord<C>>, Error> {
        let next = self.inner.next()?;
        if let Some(record) = &next {
            validate_portable_path(&record.path)?;
            if self
                .previous_path
                .as_ref()
                .is_some_and(|previous| previous >= &record.path)
            {
                return Err(Error::corrupt(
                    "read filesystem namespace",
                    "namespace paths are not strictly ordered",
                ));
            }
            self.previous_path = Some(record.path.clone());
        }
        Ok(next)
    }
}

impl<C: DeserializeOwned> OrderedRead for NamespaceReader<C> {
    type Item = NamespaceRecord<C>;

    fn next(&mut self) -> Result<Option<Self::Item>, Error> {
        NamespaceReader::next(self)
    }
}

pub(super) struct PlannedDelta {
    records: Spool<NamespaceRecord<FileExtentMap>>,
    pub(super) compaction_weight_bytes: u64,
}

pub(super) async fn write_genesis<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    root_node_id: NodeId,
    gc_epoch: GcEpoch,
) -> Result<StreamRef, Error> {
    let root = NamespaceRecord::<FileExtentMap> {
        path: String::new(),
        value: Some(NamespaceNode {
            node_id: root_node_id,
            generation: 1,
            attributes: Default::default(),
            value: NamespaceValue::Directory { generation: 1 },
        }),
    };
    let mut writer = RecordStreamWriter::open(
        volume.operator(),
        gc_epoch,
        ObjectClass::NamespaceSegment,
        StreamKind::NAMESPACE_SNAPSHOT,
        volume.multipart_part_bytes(),
    )
    .await?;
    writer.write(&root).await?;
    writer.close().await
}

pub(super) async fn write_snapshot<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    namespace: &Namespace<FileExtentMap>,
    gc_epoch: GcEpoch,
) -> Result<StreamRef, Error> {
    let mut source = namespace.reader()?;
    let mut writer = RecordStreamWriter::open(
        volume.operator(),
        gc_epoch,
        ObjectClass::NamespaceSegment,
        StreamKind::NAMESPACE_SNAPSHOT,
        volume.multipart_part_bytes(),
    )
    .await?;
    while let Some(record) = source.next()? {
        writer.write(&record).await?;
    }
    writer.close().await
}

pub(super) fn plan_delta(
    workspace: &WorkContext,
    previous: &Namespace<FileExtentMap>,
    target: &Namespace<FileExtentMap>,
) -> Result<Option<PlannedDelta>, Error> {
    let mut records = OrderedJoin::new(
        previous.reader()?,
        target.reader()?,
        |record: &NamespaceRecord<FileExtentMap>| record.path.clone(),
        |record: &NamespaceRecord<FileExtentMap>| record.path.clone(),
    );
    let mut writer = workspace.writer("namespace-delta")?;
    let mut sizer = RecordStreamSizer::new();
    let mut changed = false;
    while let Some(record) = records.next()? {
        let (path, value) = match record {
            JoinItem::Left(record) => (record.path, None),
            JoinItem::Right(record) => (record.path, record.value),
            JoinItem::Match(old, new) => {
                if old.value == new.value {
                    continue;
                }
                (new.path, new.value)
            }
        };
        let record = NamespaceRecord { path, value };
        let encoded_bytes = writer.write_sized(&record)?;
        sizer.write_encoded(encoded_bytes)?;
        changed = true;
    }
    if !changed {
        return Ok(None);
    }
    Ok(Some(PlannedDelta {
        records: writer.finish()?,
        compaction_weight_bytes: sizer.finish()?,
    }))
}

pub(super) async fn write_delta<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    delta: PlannedDelta,
    older: Vec<NamespaceChangeSegment>,
    compaction_weight_bytes: u64,
    end_cursor: ChangeCursor,
    gc_epoch: GcEpoch,
) -> Result<NamespaceChangeSegment, Error> {
    for segment in &older {
        segment
            .stream
            .require(StreamKind::NAMESPACE_CHANGES, ObjectClass::NamespaceSegment)?;
    }
    let remotes = futures::stream::iter(older.into_iter().enumerate())
        .map(|(priority, segment)| async move {
            Ok::<_, Error>((
                priority,
                ChangeSource::remote(volume, segment.stream).await?,
            ))
        })
        .buffer_unordered(volume.stream_concurrency());
    futures::pin_mut!(remotes);
    let mut remote_sources = Vec::new();
    while let Some(remote) = remotes.next().await {
        remote_sources.push(remote?);
    }
    remote_sources.sort_by_key(|(priority, _)| *priority);
    let mut readers = Vec::with_capacity(remote_sources.len() + 1);
    readers.push(ChangeSource::local(delta.records)?);
    readers.extend(remote_sources.into_iter().map(|(_, source)| source));
    let mut records =
        AsyncOrderedMerge::from_readers(readers, |record: &NamespaceRecord<FileExtentMap>| {
            Ok(record.path.clone())
        })
        .await?;
    let mut writer = RecordStreamWriter::open(
        volume.operator(),
        gc_epoch,
        ObjectClass::NamespaceSegment,
        StreamKind::NAMESPACE_CHANGES,
        volume.multipart_part_bytes(),
    )
    .await?;
    while let Some((_, selected)) = records
        .reduce_next_group(None, |selected, _, record| {
            if selected.is_none() {
                *selected = Some(record);
            }
            Ok(())
        })
        .await?
    {
        writer
            .write(&selected.expect("ordered group contains one record"))
            .await?;
    }
    Ok(NamespaceChangeSegment {
        end_cursor,
        compaction_weight_bytes,
        stream: writer.close().await?,
    })
}

enum ChangeInput {
    Local(crate::work::SpoolReader<NamespaceRecord<FileExtentMap>>),
    Remote(Box<RecordStreamReader<NamespaceRecord<FileExtentMap>>>),
}

struct ChangeSource {
    input: ChangeInput,
    previous: Option<String>,
}

impl ChangeSource {
    fn local(records: Spool<NamespaceRecord<FileExtentMap>>) -> Result<Self, Error> {
        Ok(Self {
            input: ChangeInput::Local(records.reader()?),
            previous: None,
        })
    }

    async fn remote<A: AccessFamily>(
        volume: &ManagedVolume<A>,
        reference: StreamRef,
    ) -> Result<Self, Error> {
        Ok(Self {
            input: ChangeInput::Remote(Box::new(
                RecordStreamReader::open(volume.operator(), reference).await?,
            )),
            previous: None,
        })
    }

    async fn next(&mut self) -> Result<Option<NamespaceRecord<FileExtentMap>>, Error> {
        let record = match &mut self.input {
            ChangeInput::Local(reader) => reader.next()?,
            ChangeInput::Remote(reader) => reader.next().await?,
        };
        if let Some(record) = &record {
            require_increasing_path(&mut self.previous, &record.path)?;
            self.previous = Some(record.path.clone());
        }
        Ok(record)
    }
}

impl AsyncOrderedRead for ChangeSource {
    type Item = NamespaceRecord<FileExtentMap>;

    fn next(&mut self) -> impl Future<Output = Result<Option<Self::Item>, Error>> {
        ChangeSource::next(self)
    }
}

pub(super) async fn read_views<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    workspace: &WorkContext,
    views: &[(&NamespaceCommit, ChangeCursor)],
) -> Result<Vec<Namespace<FileExtentMap>>, Error> {
    let mut groups = Vec::<Vec<usize>>::new();
    for (index, (commit, cursor)) in views.iter().copied().enumerate() {
        validate_view(commit, cursor)?;
        match groups.iter_mut().find(|group| {
            let snapshot = views[group[0]].0.namespace_snapshot;
            snapshot.change_cursor == commit.namespace_snapshot.change_cursor
                && snapshot.stream == commit.namespace_snapshot.stream
        }) {
            Some(group) => group.push(index),
            None => groups.push(vec![index]),
        }
    }

    let mut output = (0..views.len()).map(|_| None).collect::<Vec<_>>();
    for group in groups {
        let snapshot_ref = views[group[0]].0.namespace_snapshot;
        snapshot_ref.stream.require(
            StreamKind::NAMESPACE_SNAPSHOT,
            ObjectClass::NamespaceSegment,
        )?;
        let snapshot = download(
            volume,
            workspace,
            snapshot_ref.stream,
            snapshot_ref.change_cursor,
        )
        .await?;
        let mut segments = Vec::new();
        for index in group.iter().copied() {
            let (commit, cursor) = views[index];
            if cursor == commit.namespace_snapshot.change_cursor {
                continue;
            }
            for segment in commit.namespace_changes.iter().copied() {
                if !segments.iter().any(|current: &NamespaceChangeSegment| {
                    current.end_cursor == segment.end_cursor && current.stream == segment.stream
                }) {
                    segments.push(segment);
                }
            }
        }
        let downloads = futures::stream::iter(segments)
            .map(|segment| async move {
                segment
                    .stream
                    .require(StreamKind::NAMESPACE_CHANGES, ObjectClass::NamespaceSegment)?;
                Ok::<_, Error>(DownloadedChange {
                    end_cursor: segment.end_cursor,
                    stream: segment.stream,
                    records: download(volume, workspace, segment.stream, segment.end_cursor)
                        .await?,
                })
            })
            .buffer_unordered(volume.stream_concurrency());
        futures::pin_mut!(downloads);
        let mut changes = Vec::new();
        while let Some(download) = downloads.next().await {
            changes.push(download?);
        }
        for index in group {
            let (commit, cursor) = views[index];
            output[index] = Some(read_from_downloads(
                volume,
                commit,
                cursor,
                workspace,
                snapshot.clone(),
                &changes,
            )?);
        }
    }
    output
        .into_iter()
        .map(|view| view.ok_or_else(|| Error::corrupt("read Managed namespace", "view is missing")))
        .collect()
}

fn validate_view(commit: &NamespaceCommit, view_cursor: ChangeCursor) -> Result<(), Error> {
    if view_cursor == commit.namespace_snapshot.change_cursor {
        return Ok(());
    }
    if view_cursor != commit.change_cursor {
        return Err(Error::corrupt(
            "read Managed namespace",
            "commit cursor does not match the requested view",
        ));
    }
    if commit.namespace_changes.is_empty() {
        return Err(Error::corrupt(
            "read Managed namespace",
            "namespace commit has no change stream for its cursor",
        ));
    }
    if commit.namespace_snapshot.change_cursor > view_cursor {
        return Err(Error::corrupt(
            "read Managed namespace",
            "snapshot is newer than the requested view",
        ));
    }
    for segment in commit.namespace_changes.iter().copied() {
        if segment.end_cursor > commit.change_cursor || segment.compaction_weight_bytes == 0 {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace change descriptor is invalid",
            ));
        }
    }
    Ok(())
}

fn select_record(
    selected: &mut Option<VersionedRecord>,
    candidate: VersionedRecord,
) -> Result<(), Error> {
    match selected.as_ref() {
        Some(current) if current.change_cursor == candidate.change_cursor => {
            if current.value != candidate.value {
                return Err(Error::corrupt(
                    "read Managed namespace",
                    "one change cursor has conflicting namespace records",
                ));
            }
        }
        Some(current) if current.change_cursor > candidate.change_cursor => {}
        _ => *selected = Some(candidate),
    }
    Ok(())
}

fn read_from_downloads<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    commit: &NamespaceCommit,
    view_cursor: ChangeCursor,
    workspace: &WorkContext,
    snapshot: Spool<VersionedRecord>,
    changes: &[DownloadedChange],
) -> Result<Namespace<FileExtentMap>, Error> {
    if view_cursor == commit.namespace_snapshot.change_cursor {
        return finish_view(volume, commit, view_cursor, workspace, snapshot);
    }
    let mut streams = RunCompactor::new(workspace.fan_in());
    streams.push(snapshot, |group| merge_group(workspace, group, view_cursor))?;
    for segment in &commit.namespace_changes {
        let stream = changes
            .iter()
            .find(|download| {
                download.end_cursor == segment.end_cursor && download.stream == segment.stream
            })
            .ok_or_else(|| {
                Error::corrupt(
                    "read Managed namespace",
                    "namespace change stream was not downloaded",
                )
            })?;
        streams.push(stream.records.clone(), |group| {
            merge_group(workspace, group, view_cursor)
        })?;
    }
    let merged = streams
        .finish(|group| merge_group(workspace, group, view_cursor))?
        .ok_or_else(|| Error::corrupt("read Managed namespace", "namespace has no streams"))?;
    finish_view(volume, commit, view_cursor, workspace, merged)
}

fn finish_view<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    commit: &NamespaceCommit,
    view_cursor: ChangeCursor,
    workspace: &WorkContext,
    merged: Spool<VersionedRecord>,
) -> Result<Namespace<FileExtentMap>, Error> {
    let mut output = workspace.writer("namespace")?;
    let mut records = merged.reader()?;
    let mut previous_output = None::<String>;
    let mut root_seen = false;
    while let Some(record) = records.next()? {
        let Some(node) = record.value else {
            continue;
        };
        let record = NamespaceRecord {
            path: record.path,
            value: Some(node),
        };
        validate_record(volume, &record, &mut previous_output, &mut root_seen)?;
        output.write(&record)?;
    }
    if !root_seen {
        return Err(Error::corrupt(
            "read Managed namespace",
            "namespace root is missing",
        ));
    }
    Ok(Namespace {
        volume_id: commit.volume_id,
        cursor: view_cursor,
        root: volume.format().root_node_id(),
        entries: output.finish()?,
    })
}

async fn download<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    workspace: &WorkContext,
    reference: StreamRef,
    cursor: ChangeCursor,
) -> Result<Spool<VersionedRecord>, Error> {
    let mut remote =
        RecordStreamReader::<NamespaceRecord<FileExtentMap>>::open(volume.operator(), reference)
            .await?;
    let mut local = workspace.writer("namespace-input")?;
    let mut previous = None::<String>;
    while let Some(record) = remote.next().await? {
        require_increasing_path(&mut previous, &record.path)?;
        previous = Some(record.path.clone());
        local.write(&VersionedRecord {
            path: record.path,
            change_cursor: cursor,
            value: record.value,
        })?;
    }
    local.finish()
}

struct DownloadedChange {
    end_cursor: ChangeCursor,
    stream: StreamRef,
    records: Spool<VersionedRecord>,
}

fn merge_group(
    workspace: &WorkContext,
    streams: &[Spool<VersionedRecord>],
    view_cursor: ChangeCursor,
) -> Result<Spool<VersionedRecord>, Error> {
    let readers = streams
        .iter()
        .map(Spool::reader)
        .collect::<Result<Vec<_>, Error>>()?;
    let mut records =
        OrderedMerge::from_readers(readers, |record: &VersionedRecord| Ok(record.path.clone()))?;
    let mut output = workspace.writer("namespace-merge")?;

    while let Some((_, selected)) =
        records.reduce_next_group(None::<VersionedRecord>, |selected, _, record| {
            if record.change_cursor <= view_cursor {
                select_record(selected, record)?;
            }
            Ok(())
        })?
    {
        if let Some(record) = selected {
            output.write(&record)?;
        }
    }
    output.finish()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct VersionedRecord {
    path: String,
    change_cursor: ChangeCursor,
    value: Option<NamespaceNode<FileExtentMap>>,
}

fn validate_record<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    record: &NamespaceRecord<FileExtentMap>,
    previous: &mut Option<String>,
    root_seen: &mut bool,
) -> Result<(), Error> {
    require_increasing_path(previous, &record.path)?;
    let node = record
        .value
        .as_ref()
        .ok_or_else(|| Error::corrupt("read Managed namespace", "snapshot contains a deletion"))?;
    validate_portable_path(&record.path)?;
    if let NamespaceValue::RegularFile { content, data, .. } = &node.value
        && validate_file_map(data, *content, volume.file_decoding_count()).is_err()
    {
        return Err(Error::corrupt(
            "read Managed namespace",
            "file content does not match its namespace record",
        ));
    }
    if record.path.is_empty() {
        if node.node_id != volume.format().root_node_id()
            || !matches!(node.value, NamespaceValue::Directory { .. })
        {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace root is invalid",
            ));
        }
        *root_seen = true;
    }
    *previous = Some(record.path.clone());
    Ok(())
}

fn require_increasing_path(previous: &mut Option<String>, path: &str) -> Result<(), Error> {
    if previous
        .as_ref()
        .is_some_and(|previous| previous.as_str() >= path)
    {
        return Err(Error::corrupt(
            "read Managed namespace",
            "namespace stream is not strictly path ordered",
        ));
    }
    Ok(())
}

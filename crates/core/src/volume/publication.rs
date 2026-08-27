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

//! Immutable namespace commits, operation receipts, and atomic publication.

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::ErrorKind;
use crate::authority::AuthorityHead;
use crate::data::DataAccess;
use crate::filesystem::{ChangeCursor, NamespaceRecord, NamespaceValue, OperationId};
use crate::format::{
    COMMIT_RECORD, FileExtentMap, GcEpoch, NamespaceCommit, NamespaceRevision, NamespaceSnapshot,
    ObjectClass, OperationReceipt, OperationReceiptSegment, RecordStreamSizer,
};
use crate::storage::{ImmutableWriter, RecordStreamReader, RecordStreamWriter};
use crate::work::{WorkContext, sort};

use super::namespace::{self, Namespace};
use super::open::{AccessFamily, ManagedObservation, ManagedVolume};

/// Absorb older compacted segments whose weight is less than twice the newest.
fn take_merge_tail<T>(
    segments: &mut Vec<T>,
    mut compaction_weight_bytes: u64,
    size: impl Fn(&T) -> u64,
) -> Result<(Vec<T>, u64), Error> {
    let mut merged = Vec::new();
    while segments
        .last()
        .is_some_and(|older| size(older) / 2 < compaction_weight_bytes)
    {
        let segment = segments.pop().expect("a merge candidate exists");
        compaction_weight_bytes = compaction_weight_bytes
            .checked_add(size(&segment))
            .ok_or_else(|| {
                Error::corrupt("compact Managed streams", "compaction weight overflows")
            })?;
        merged.push(segment);
    }
    Ok((merged, compaction_weight_bytes))
}

impl<A: AccessFamily> ManagedVolume<A> {
    /// Prepare and atomically publish one successor namespace.
    pub async fn publish_namespace(
        &self,
        observed: &ManagedObservation,
        target: &Namespace<FileExtentMap>,
        operation: OperationId,
    ) -> Result<NamespaceRevision, Error> {
        let workspace = WorkContext::create(self.work_budget())?;
        let revision = self
            .prepare_publication(&workspace, observed, target, operation)
            .await?;
        self.commit_publication(observed, revision, operation)
            .await?;
        Ok(revision)
    }

    pub(crate) async fn prepare_publication(
        &self,
        workspace: &WorkContext,
        observed: &ManagedObservation,
        target: &Namespace<FileExtentMap>,
        operation: OperationId,
    ) -> Result<NamespaceRevision, Error> {
        if target.volume_id != self.id()
            || target.root != self.format().root_node_id()
            || target.cursor.sequence() != observed.namespace.cursor.sequence() + 1
        {
            return Err(Error::invalid(
                "publish Managed namespace",
                "publication ancestry is invalid",
            ));
        }

        let mut commit = observed.commit.clone();
        commit.change_cursor = target.cursor;
        if observed.namespace.cursor == ChangeCursor::GENESIS {
            commit.namespace_snapshot = NamespaceSnapshot {
                change_cursor: target.cursor,
                stream: namespace::write_snapshot(self, target, observed.gc_epoch()).await?,
            };
            commit.namespace_changes.clear();
        } else if let Some(delta) = namespace::plan_delta(workspace, &observed.namespace, target)? {
            let (older, compaction_weight_bytes) = take_merge_tail(
                &mut commit.namespace_changes,
                delta.compaction_weight_bytes,
                |segment| segment.compaction_weight_bytes,
            )?;
            commit.namespace_changes.push(
                namespace::write_delta(
                    self,
                    delta,
                    older,
                    compaction_weight_bytes,
                    target.cursor,
                    observed.gc_epoch(),
                )
                .await?,
            );
        } else {
            return Err(Error::invalid(
                "publish Managed namespace",
                "publication contains no namespace change",
            ));
        }

        let operation_record = OperationReceipt {
            change_cursor: target.cursor,
            operation_id: operation,
        };
        let mut receipt_size = RecordStreamSizer::new();
        receipt_size.write(&operation_record)?;
        let (older, compaction_weight_bytes) = take_merge_tail(
            &mut commit.operation_receipts,
            receipt_size.finish()?,
            |segment| segment.compaction_weight_bytes,
        )?;
        commit.operation_receipts.push(
            write_operation_receipts(
                self,
                operation_record,
                older,
                compaction_weight_bytes,
                observed.gc_epoch(),
            )
            .await?,
        );
        write_commit(self, observed.gc_epoch(), &commit).await
    }

    pub(crate) async fn commit_publication(
        &self,
        observed: &ManagedObservation,
        target: NamespaceRevision,
        operation: OperationId,
    ) -> Result<(), Error> {
        if target.change_cursor.sequence() != observed.namespace.cursor.sequence() + 1 {
            return Err(Error::invalid(
                "publish Managed namespace",
                "prepared publication ancestry is invalid",
            ));
        }
        let current = observed.authority.head;
        let head = AuthorityHead {
            current_commit: target,
            gc_epoch: current.gc_epoch,
            minimum_retained_cursor: current.minimum_retained_cursor,
        };
        if self.replace_head(&observed.authority, head).await? {
            return Ok(());
        }
        let current = self.read_authority().await?;
        let commit = read_commit(self, current.head.current_commit).await?;
        if operation_in_commit(self, operation, target.change_cursor, &commit).await? {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Conflict,
                "publish Managed namespace",
                "observed generation changed",
            ))
        }
    }

    pub async fn operation_committed(
        &self,
        operation: OperationId,
        expected_cursor: ChangeCursor,
        observed: &ManagedObservation,
    ) -> Result<bool, Error> {
        operation_in_commit(self, operation, expected_cursor, &observed.commit).await
    }

    pub(super) async fn compact_for_collection(
        &self,
        workspace: &WorkContext,
        reference: NamespaceRevision,
        gc_epoch: GcEpoch,
        mut visit: impl FnMut(crate::format::ObjectLocator) -> Result<(), Error> + Send,
    ) -> Result<NamespaceRevision, Error> {
        let source = read_commit(self, reference).await?;
        let current = namespace::read_views(self, workspace, &[(&source, source.change_cursor)])
            .await?
            .pop()
            .expect("one requested namespace view is returned");
        let mut files = workspace.writer("gc-files-by-content")?;
        let mut compacted = workspace.writer("gc-compacted-namespace")?;
        let mut records = current.reader()?;
        while let Some(record) = records.next()? {
            match record.value.as_ref().map(|node| &node.value) {
                Some(NamespaceValue::RegularFile { content, .. }) => {
                    files.write(&ContentFile {
                        content: *content,
                        record,
                    })?;
                }
                _ => compacted.write(&record)?,
            }
        }
        let files = sort(workspace, &files.finish()?, |file: &ContentFile| {
            (file.content, file.record.path.clone())
        })?;
        let mut files = files.reader()?;
        let mut current_content = None;
        let mut current_data = None;
        while let Some(mut file) = files.next()? {
            if current_content != Some(file.content) {
                let data = file_data(&file.record)?.clone();
                crate::data::validate_file_map(&data, file.content, self.file_decoding_count())?;
                let data = crate::data::compact_file_extents(
                    &data,
                    self.operator(),
                    gc_epoch,
                    self.multipart_part_bytes(),
                    |mapping| {
                        self.access().data().validate_extent(&mapping.extent)?;
                        visit(mapping.extent.stored_range.segment)
                    },
                )
                .await?;
                mark_file_data_objects(&data, &mut visit)?;
                current_content = Some(file.content);
                current_data = Some(data);
            }
            *file_data_mut(&mut file.record)? = current_data
                .clone()
                .expect("a content group has compacted file data");
            compacted.write(&file.record)?;
        }
        let entries = sort(
            workspace,
            &compacted.finish()?,
            |record: &NamespaceRecord<FileExtentMap>| record.path.clone(),
        )?;
        let current = Namespace {
            volume_id: current.volume_id,
            cursor: current.cursor,
            root: current.root,
            entries,
        };
        let namespace_stream = namespace::write_snapshot(self, &current, gc_epoch).await?;
        visit(namespace_stream.object.locator)?;
        for receipt in &source.operation_receipts {
            visit(receipt.stream.object.locator)?;
        }
        let commit = NamespaceCommit {
            volume_id: source.volume_id,
            change_cursor: source.change_cursor,
            namespace_snapshot: NamespaceSnapshot {
                change_cursor: source.change_cursor,
                stream: namespace_stream,
            },
            namespace_changes: Vec::new(),
            operation_receipts: source.operation_receipts,
        };
        let revision = write_commit(self, gc_epoch, &commit).await?;
        visit(revision.object.locator)?;
        Ok(revision)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct ContentFile {
    content: crate::filesystem::ContentRef,
    record: NamespaceRecord<FileExtentMap>,
}

fn file_data(record: &NamespaceRecord<FileExtentMap>) -> Result<&FileExtentMap, Error> {
    match record.value.as_ref().map(|node| &node.value) {
        Some(NamespaceValue::RegularFile { data, .. }) => Ok(data),
        _ => Err(Error::corrupt(
            "compact Managed namespace",
            "content-ordered record is not a regular file",
        )),
    }
}

fn file_data_mut(record: &mut NamespaceRecord<FileExtentMap>) -> Result<&mut FileExtentMap, Error> {
    match record.value.as_mut().map(|node| &mut node.value) {
        Some(NamespaceValue::RegularFile { data, .. }) => Ok(data),
        _ => Err(Error::corrupt(
            "compact Managed namespace",
            "content-ordered record is not a regular file",
        )),
    }
}

fn mark_file_data_objects(
    data: &FileExtentMap,
    visit: &mut (impl FnMut(crate::format::ObjectLocator) -> Result<(), Error> + Send),
) -> Result<(), Error> {
    for run in data.runs() {
        if let Some(tail) = run.continuation {
            visit(tail.object.locator)?;
        }
    }
    Ok(())
}

async fn write_operation_receipts<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    receipt: OperationReceipt,
    older: Vec<OperationReceiptSegment>,
    compaction_weight_bytes: u64,
    gc_epoch: GcEpoch,
) -> Result<OperationReceiptSegment, Error> {
    let mut writer = RecordStreamWriter::open(
        volume.operator(),
        gc_epoch,
        ObjectClass::OperationReceiptSegment,
        crate::format::StreamKind::OPERATION_RECEIPTS,
        volume.multipart_part_bytes(),
    )
    .await?;
    writer.write(&receipt).await?;
    for reference in older.iter().map(|segment| segment.stream) {
        reference.require(
            crate::format::StreamKind::OPERATION_RECEIPTS,
            ObjectClass::OperationReceiptSegment,
        )?;
        let mut reader =
            RecordStreamReader::<OperationReceipt>::open(volume.operator(), reference).await?;
        while let Some(record) = reader.next().await? {
            writer.write(&record).await?;
        }
    }
    let first_cursor = older
        .last()
        .map_or(receipt.change_cursor, |segment| segment.first_cursor);
    Ok(OperationReceiptSegment {
        first_cursor,
        last_cursor: receipt.change_cursor,
        compaction_weight_bytes,
        stream: writer.close().await?,
    })
}

pub(super) async fn write_commit<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    gc_epoch: GcEpoch,
    commit: &NamespaceCommit,
) -> Result<NamespaceRevision, Error> {
    let mut writer = ImmutableWriter::open(
        volume.operator(),
        gc_epoch,
        ObjectClass::NamespaceCommit,
        volume.multipart_part_bytes(),
    )
    .await?;
    writer.write(COMMIT_RECORD.encode(commit)?).await?;
    let object = writer.close().await?;
    Ok(NamespaceRevision {
        object,
        change_cursor: commit.change_cursor,
    })
}

pub(super) async fn read_commit<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    reference: NamespaceRevision,
) -> Result<NamespaceCommit, Error> {
    if reference.object.locator.class != ObjectClass::NamespaceCommit {
        return Err(Error::corrupt(
            "read Managed namespace",
            "commit reference has the wrong object class",
        ));
    }
    let bytes = crate::storage::read_immutable(
        volume.operator(),
        reference.object,
        COMMIT_RECORD.maximum_encoded_bytes(),
    )
    .await?;
    let commit: NamespaceCommit = COMMIT_RECORD.decode(&bytes)?;
    commit.validate(volume.id(), reference.change_cursor)?;
    Ok(commit)
}

async fn operation_in_commit<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    operation: OperationId,
    expected_cursor: ChangeCursor,
    commit: &NamespaceCommit,
) -> Result<bool, Error> {
    Ok(read_operation_receipt(volume, expected_cursor, commit)
        .await?
        .is_some_and(|receipt| receipt.operation_id == operation))
}

async fn read_operation_receipt<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    expected_cursor: ChangeCursor,
    commit: &NamespaceCommit,
) -> Result<Option<OperationReceipt>, Error> {
    let Some(segment) = commit.operation_receipts.iter().find(|segment| {
        segment.first_cursor <= expected_cursor && expected_cursor <= segment.last_cursor
    }) else {
        return Ok(None);
    };
    let mut reader =
        RecordStreamReader::<OperationReceipt>::open(volume.operator(), segment.stream).await?;
    let mut previous = None;
    while let Some(record) = reader.next().await? {
        if previous.is_some_and(|previous| previous <= record.change_cursor)
            || record.change_cursor < segment.first_cursor
            || record.change_cursor > segment.last_cursor
        {
            return Err(Error::corrupt(
                "read Managed operation receipt",
                "operation receipts are not newest first",
            ));
        }
        previous = Some(record.change_cursor);
        if record.change_cursor == expected_cursor {
            return Ok(Some(record));
        }
    }
    Err(Error::corrupt(
        "read Managed operation receipt",
        "operation receipt segment does not contain its cursor range",
    ))
}

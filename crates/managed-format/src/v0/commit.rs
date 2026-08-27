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

//! Namespace commit and operation receipt wire types.

use crate::Error;
use crate::v0::model::{ChangeCursor, OperationId, VolumeId};

use super::codec::RecordCodec;
use super::namespace::{NamespaceChangeSegment, NamespaceSnapshot, OperationReceiptSegment};
use super::object::{ObjectClass, ObjectRef};
use super::stream::{StreamKind, StreamRef};

/// Envelope for one namespace commit object.
pub const COMMIT_RECORD: RecordCodec = RecordCodec::new(*b"OFSCMT00", 4 * 1024 * 1024);

#[derive(Clone, Debug)]
pub struct NamespaceCommit {
    pub volume_id: VolumeId,
    pub change_cursor: ChangeCursor,
    pub namespace_snapshot: NamespaceSnapshot,
    pub namespace_changes: Vec<NamespaceChangeSegment>,
    pub operation_receipts: Vec<OperationReceiptSegment>,
}

super::codec::tuple_wire!(NamespaceCommit {
    volume_id: VolumeId,
    change_cursor: ChangeCursor,
    namespace_snapshot: NamespaceSnapshot,
    namespace_changes: Vec<NamespaceChangeSegment>,
    operation_receipts: Vec<OperationReceiptSegment>,
});

#[derive(Clone, Copy, Debug)]
pub struct OperationReceipt {
    pub change_cursor: ChangeCursor,
    pub operation_id: OperationId,
}

super::codec::tuple_wire!(OperationReceipt {
    change_cursor: ChangeCursor,
    operation_id: OperationId,
});

/// Locator of one committed namespace revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceRevision {
    pub object: ObjectRef,
    pub change_cursor: ChangeCursor,
}

impl NamespaceRevision {
    pub const fn cursor(self) -> ChangeCursor {
        self.change_cursor
    }
}

super::codec::tuple_wire!(NamespaceRevision {
    object: ObjectRef,
    change_cursor: ChangeCursor,
});

impl NamespaceCommit {
    pub fn genesis(volume_id: VolumeId, namespace_snapshot: StreamRef) -> Self {
        Self {
            volume_id,
            change_cursor: ChangeCursor::GENESIS,
            namespace_snapshot: NamespaceSnapshot {
                change_cursor: ChangeCursor::GENESIS,
                stream: namespace_snapshot,
            },
            namespace_changes: Vec::new(),
            operation_receipts: Vec::new(),
        }
    }

    pub fn validate(
        &self,
        volume_id: VolumeId,
        reference_cursor: ChangeCursor,
    ) -> Result<(), Error> {
        if self.volume_id != volume_id
            || self.change_cursor != reference_cursor
            || self.namespace_snapshot.change_cursor > self.change_cursor
            || self
                .namespace_snapshot
                .stream
                .require(
                    StreamKind::NAMESPACE_SNAPSHOT,
                    ObjectClass::NamespaceSegment,
                )
                .is_err()
            || !self.valid_namespace_changes()
            || !self.valid_operation_receipts()
        {
            return Err(Error::corrupt(
                "read Managed namespace",
                "namespace commit does not match its reference",
            ));
        }
        Ok(())
    }

    fn valid_namespace_changes(&self) -> bool {
        let mut previous = self.namespace_snapshot.change_cursor;
        for segment in &self.namespace_changes {
            if segment.compaction_weight_bytes == 0
                || segment.end_cursor <= previous
                || segment.end_cursor > self.change_cursor
                || segment
                    .stream
                    .require(StreamKind::NAMESPACE_CHANGES, ObjectClass::NamespaceSegment)
                    .is_err()
            {
                return false;
            }
            previous = segment.end_cursor;
        }
        previous == self.change_cursor
    }

    fn valid_operation_receipts(&self) -> bool {
        if self.change_cursor == ChangeCursor::GENESIS {
            return self.operation_receipts.is_empty();
        }
        let mut previous = None::<ChangeCursor>;
        for segment in &self.operation_receipts {
            if segment.compaction_weight_bytes == 0
                || segment.first_cursor > segment.last_cursor
                || segment.last_cursor > self.change_cursor
                || previous.is_some_and(|cursor| {
                    cursor.sequence().checked_add(1) != Some(segment.first_cursor.sequence())
                })
                || segment
                    .stream
                    .require(
                        StreamKind::OPERATION_RECEIPTS,
                        ObjectClass::OperationReceiptSegment,
                    )
                    .is_err()
            {
                return false;
            }
            previous = Some(segment.last_cursor);
        }
        previous == Some(self.change_cursor)
    }
}

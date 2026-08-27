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

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::authority::AuthorityId;
use crate::filesystem::{ChangeCursor, OperationId, VolumeId};
use crate::format::{GcEpoch, NamespaceRevision};

/// A recoverable binding between one local replica and its remote namespace.
///
/// This record deliberately contains no namespace image or per-path identity map.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaState {
    root: PathBuf,
    volume_id: VolumeId,
    authority_id: AuthorityId,
    authority_name: String,
    common: NamespaceRevision,
    observed: NamespaceRevision,
    phase: SyncPhase,
    conflicts: u64,
    base_expired: bool,
}

/// Origin of the namespace being installed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationOrigin {
    Local,
    Remote,
}

/// Persistent replica execution boundary.
pub type ReplicaPhase = SyncPhase;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPhase {
    Clean,
    Publishing {
        target: NamespaceRevision,
        operation_id: OperationId,
        gc_epoch: GcEpoch,
    },
    Installing {
        published: bool,
    },
}

impl ReplicaState {
    pub(crate) fn new(
        root: PathBuf,
        volume_id: VolumeId,
        authority_id: AuthorityId,
        authority_name: String,
        common: NamespaceRevision,
    ) -> Self {
        Self {
            root,
            volume_id,
            authority_id,
            authority_name,
            common,
            observed: common,
            phase: SyncPhase::Clean,
            conflicts: 0,
            base_expired: false,
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.observed.cursor().sequence() < self.common.cursor().sequence() {
            return Err(Error::invalid(
                "synchronize replica",
                "replica remote cursor is behind its common namespace",
            ));
        }
        match self.phase {
            SyncPhase::Clean => Ok(()),
            SyncPhase::Publishing { target, .. }
                if self.observed.cursor().sequence() >= self.common.cursor().sequence()
                    && target.cursor().sequence() == self.observed.cursor().sequence() + 1 =>
            {
                Ok(())
            }
            SyncPhase::Installing { .. }
                if self.observed.cursor().sequence() >= self.common.cursor().sequence() =>
            {
                Ok(())
            }
            _ => Err(Error::corrupt(
                "read replica state",
                "replica recovery references are invalid",
            )),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub const fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub const fn authority_id(&self) -> AuthorityId {
        self.authority_id
    }

    pub fn authority_name(&self) -> &str {
        &self.authority_name
    }

    pub const fn common_revision(&self) -> NamespaceRevision {
        self.common
    }

    pub const fn remote_cursor(&self) -> ChangeCursor {
        self.observed.cursor()
    }

    pub const fn conflict_count(&self) -> u64 {
        self.conflicts
    }

    pub const fn has_pending(&self) -> bool {
        !matches!(self.phase, SyncPhase::Clean)
    }

    pub const fn base_expired(&self) -> bool {
        self.base_expired
    }

    pub(crate) const fn pending_publication(
        &self,
    ) -> Option<(NamespaceRevision, NamespaceRevision, OperationId, GcEpoch)> {
        match self.phase {
            SyncPhase::Publishing {
                target,
                operation_id,
                gc_epoch,
            } => Some((self.observed, target, operation_id, gc_epoch)),
            _ => None,
        }
    }

    pub(crate) const fn installation(&self) -> Option<(NamespaceRevision, bool)> {
        match self.phase {
            SyncPhase::Installing { published } => Some((self.observed, published)),
            _ => None,
        }
    }

    pub(crate) const fn resume_revision(&self) -> NamespaceRevision {
        match self.phase {
            SyncPhase::Clean => self.common,
            SyncPhase::Publishing { .. } => self.observed,
            SyncPhase::Installing { .. } => self.observed,
        }
    }

    pub(crate) fn advance(&mut self, common: NamespaceRevision) {
        self.common = common;
        self.observed = common;
        self.phase = SyncPhase::Clean;
        self.conflicts = 0;
        self.base_expired = false;
    }

    pub(crate) fn rebase_equivalent(&mut self, common: NamespaceRevision) -> Result<(), Error> {
        if !matches!(self.phase, SyncPhase::Clean)
            || common.cursor().sequence() != self.common.cursor().sequence()
        {
            return Err(Error::corrupt(
                "synchronize replica",
                "equivalent namespace rebase changed the logical cursor",
            ));
        }
        self.common = common;
        self.observed = common;
        self.validate()
    }

    pub(crate) fn begin_publication(
        &mut self,
        expected: NamespaceRevision,
        target: NamespaceRevision,
        operation_id: OperationId,
        gc_epoch: GcEpoch,
    ) -> Result<(), Error> {
        self.phase = SyncPhase::Publishing {
            target,
            operation_id,
            gc_epoch,
        };
        self.observed = expected;
        self.conflicts = 0;
        self.base_expired = false;
        self.validate()
    }

    pub(crate) fn begin_install(&mut self, target: NamespaceRevision, published: bool) {
        self.phase = SyncPhase::Installing { published };
        self.observed = target;
        self.conflicts = 0;
        self.base_expired = false;
    }

    pub(crate) fn retain_conflicts(
        &mut self,
        conflicts: usize,
        remote: NamespaceRevision,
        base_expired: bool,
    ) {
        self.phase = SyncPhase::Clean;
        self.conflicts = conflicts.try_into().unwrap_or(u64::MAX);
        self.observed = remote;
        self.base_expired = base_expired;
    }

    pub(crate) fn cancel_pending(&mut self, remote: NamespaceRevision) {
        self.phase = SyncPhase::Clean;
        if remote.cursor().sequence() == self.common.cursor().sequence() {
            self.common = remote;
        }
        self.observed = remote;
        self.base_expired = false;
    }
}

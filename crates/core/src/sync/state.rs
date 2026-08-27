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
}

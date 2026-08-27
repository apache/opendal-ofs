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

use futures::{StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::ErrorKind;
use crate::authority::{AuthorityAccess, AuthorityHead, AuthorityRoot};
use crate::format::{GcEpoch, OBJECT_PREFIX, ObjectLocator};
use crate::work::Spool;
use crate::work::{JoinItem, OrderedJoin, Unique, WorkContext, sort};

use super::open::{AccessFamily, ManagedVolume};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcOutcome {
    pub scanned: u64,
    pub deleted: u64,
    pub deleted_bytes: u64,
}

impl<A: AccessFamily> ManagedVolume<A> {
    /// Rotate the upload epoch, compact live metadata, then merge a streamed
    /// inventory against the streamed reachability set.
    pub async fn collect(&self) -> Result<GcOutcome, Error> {
        let authority = self.access().authority();
        let (fence, mut roots) = authority
            .begin_collection(self.operator(), self.multipart_part_bytes())
            .await?;
        let collection_epoch = fence.epoch;

        let workspace = WorkContext::create(self.work_budget())?;
        let mut records = workspace.writer("gc-reachable")?;
        let mut compacted = workspace.writer("gc-authority-roots")?;
        while let Some(root) = roots.try_next().await? {
            let collection_commit = self
                .compact_for_collection(
                    &workspace,
                    root.head.current_commit,
                    collection_epoch,
                    |reference| records.write(&reference),
                )
                .await?;
            compacted.write(&AuthorityRoot {
                id: root.id,
                name: root.name,
                head: AuthorityHead {
                    current_commit: collection_commit,
                    gc_epoch: collection_epoch,
                    minimum_retained_cursor: collection_commit.cursor(),
                },
            })?;
        }
        let mut compacted = compacted.finish()?.stream()?.boxed();
        if !authority
            .finish_collection(
                self.operator(),
                self.multipart_part_bytes(),
                fence,
                &mut compacted,
            )
            .await?
        {
            return Err(Error::new(
                ErrorKind::Conflict,
                "collect Managed objects",
                "namespace authority changed while publishing compacted roots",
            ));
        }

        let marks = sort(&workspace, &records.finish()?, |identity| *identity)?;
        let candidates = self
            .inventory_candidates(&workspace, collection_epoch)
            .await?;
        self.sweep_unreachable(&marks, &candidates).await
    }

    async fn inventory_candidates(
        &self,
        workspace: &WorkContext,
        current_epoch: GcEpoch,
    ) -> Result<Spool<ObjectRecord>, Error> {
        let mut records = workspace.writer("gc-inventory")?;
        let mut lister = self
            .operator()
            .lister_with(OBJECT_PREFIX)
            .recursive(true)
            .await
            .map_err(|error| Error::from_storage("list Managed objects", error))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|error| Error::from_storage("list Managed objects", error))?
        {
            if !entry.metadata().is_file() {
                continue;
            }
            let identity = ObjectLocator::parse_key(entry.path()).ok_or_else(|| {
                Error::corrupt("collect Managed objects", "object key is invalid")
            })?;
            if identity.gc_epoch.value() >= current_epoch.value() {
                continue;
            }
            records.write(&ObjectRecord {
                identity,
                length: entry.metadata().content_length(),
            })?;
        }
        sort(workspace, &records.finish()?, |record| record.identity)
    }

    async fn sweep_unreachable(
        &self,
        marks: &Spool<ObjectLocator>,
        candidates: &Spool<ObjectRecord>,
    ) -> Result<GcOutcome, Error> {
        let marks = Unique::new(marks.reader()?, |identity: &ObjectLocator| *identity);
        let candidates = candidates.reader()?;
        let mut objects = OrderedJoin::new(
            marks,
            candidates,
            |identity| *identity,
            |record: &ObjectRecord| record.identity,
        );
        let mut outcome = GcOutcome::default();
        let mut deleter = self
            .operator()
            .deleter()
            .await
            .map_err(|error| Error::from_storage("open Managed object deleter", error))?;

        while let Some(item) = objects.next()? {
            let candidate = match item {
                JoinItem::Left(_) => continue,
                JoinItem::Match(_, _) => {
                    outcome.scanned = outcome.scanned.checked_add(1).ok_or_else(|| {
                        Error::corrupt("collect Managed objects", "scanned object count overflows")
                    })?;
                    continue;
                }
                JoinItem::Right(candidate) => candidate,
            };
            outcome.scanned = outcome.scanned.checked_add(1).ok_or_else(|| {
                Error::corrupt("collect Managed objects", "scanned object count overflows")
            })?;
            deleter
                .delete(candidate.identity.key())
                .await
                .map_err(|error| Error::from_storage("delete Managed object", error))?;
            outcome.deleted = outcome.deleted.checked_add(1).ok_or_else(|| {
                Error::corrupt("collect Managed objects", "deleted object count overflows")
            })?;
            outcome.deleted_bytes = outcome
                .deleted_bytes
                .checked_add(candidate.length)
                .ok_or_else(|| {
                    Error::corrupt("collect Managed objects", "deleted byte count overflows")
                })?;
        }
        deleter
            .close()
            .await
            .map_err(|error| Error::from_storage("finish Managed object deletion", error))?;
        Ok(outcome)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct ObjectRecord {
    identity: ObjectLocator,
    length: u64,
}

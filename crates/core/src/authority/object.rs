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

//! Object-backed default authority store.

use std::num::NonZeroUsize;

use futures::{StreamExt as _, TryStreamExt as _};
use opendal::Operator;

use crate::Error;
use crate::format::{AUTHORITY_HEAD_KEY, AUTHORITY_HEAD_RECORD, ExtensionDescriptor};
use crate::storage::ControlRecord;

use super::{
    AuthorityAccess, AuthorityHead, AuthorityId, AuthorityObservation, AuthorityRoot,
    AuthorityRoots, CollectionFence, DEFAULT_AUTHORITY,
};

const HEAD_RECORD: ControlRecord<AuthorityHead> =
    ControlRecord::new(AUTHORITY_HEAD_KEY, AUTHORITY_HEAD_RECORD);

/// Core single-authority implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultAuthorityAccess;

impl AuthorityAccess for DefaultAuthorityAccess {
    fn info(&self) -> Option<ExtensionDescriptor> {
        None
    }

    async fn initialize(
        &self,
        operator: &Operator,
        _multipart_part_bytes: NonZeroUsize,
        initial: AuthorityHead,
    ) -> Result<(), Error> {
        if HEAD_RECORD.read(operator).await?.is_some() {
            self.observe(operator, DEFAULT_AUTHORITY).await?;
            return Ok(());
        }
        HEAD_RECORD.write(operator, &initial, None).await?;
        Ok(())
    }

    async fn observe(
        &self,
        operator: &Operator,
        name: &str,
    ) -> Result<AuthorityObservation, Error> {
        require_default(name)?;
        let control = HEAD_RECORD.observe(operator).await?.ok_or_else(|| {
            Error::new(
                crate::ErrorKind::NotFound,
                "open Managed volume",
                "namespace head is missing",
            )
        })?;
        let head = control.value;
        if head.minimum_retained_cursor.sequence() > head.current_commit.cursor().sequence() {
            return Err(Error::corrupt(
                "read Managed authority",
                "authority position is invalid",
            ));
        }
        Ok(AuthorityObservation {
            id: AuthorityId::from_bytes([0; 16]),
            head,
            revision: control.revision.unwrap_or_default().into_bytes(),
        })
    }

    async fn compare_exchange(
        &self,
        operator: &Operator,
        _multipart_part_bytes: NonZeroUsize,
        name: &str,
        observed: &AuthorityObservation,
        next: AuthorityHead,
    ) -> Result<bool, Error> {
        require_default(name)?;
        let revision = std::str::from_utf8(&observed.revision)
            .map_err(|_| Error::corrupt("publish Managed namespace", "head revision is invalid"))?;
        let revision = if revision.is_empty() {
            None
        } else {
            Some(revision)
        };
        HEAD_RECORD.write(operator, &next, revision).await
    }

    async fn begin_collection(
        &self,
        operator: &Operator,
        multipart_part_bytes: NonZeroUsize,
    ) -> Result<(CollectionFence, AuthorityRoots), Error> {
        let observed = self.observe(operator, DEFAULT_AUTHORITY).await?;
        let mut rotated = observed.head;
        rotated.gc_epoch = rotated.gc_epoch.next()?;
        if !self
            .compare_exchange(
                operator,
                multipart_part_bytes,
                DEFAULT_AUTHORITY,
                &observed,
                rotated,
            )
            .await?
        {
            return Err(Error::conflict(
                "collect Managed objects",
                "namespace authority changed while rotating the GC epoch",
            ));
        }
        let current = self.observe(operator, DEFAULT_AUTHORITY).await?;
        Ok((
            CollectionFence {
                epoch: rotated.gc_epoch,
                revision: current.revision,
            },
            futures::stream::iter([Ok(AuthorityRoot {
                id: current.id,
                name: DEFAULT_AUTHORITY.to_owned(),
                head: current.head,
            })])
            .boxed(),
        ))
    }

    async fn finish_collection(
        &self,
        operator: &Operator,
        multipart_part_bytes: NonZeroUsize,
        fence: CollectionFence,
        roots: &mut AuthorityRoots,
    ) -> Result<bool, Error> {
        let root = roots.try_next().await?.ok_or_else(|| {
            Error::corrupt(
                "collect Managed objects",
                "compacted authority root is missing",
            )
        })?;
        if root.name != DEFAULT_AUTHORITY || roots.try_next().await?.is_some() {
            return Err(Error::corrupt(
                "collect Managed objects",
                "compacted authority root set is invalid",
            ));
        }
        let observed = AuthorityObservation {
            id: root.id,
            head: root.head,
            revision: fence.revision,
        };
        self.compare_exchange(
            operator,
            multipart_part_bytes,
            DEFAULT_AUTHORITY,
            &observed,
            root.head,
        )
        .await
    }
}

fn require_default(name: &str) -> Result<(), Error> {
    if name == DEFAULT_AUTHORITY {
        Ok(())
    } else {
        Err(Error::new(
            crate::ErrorKind::NotFound,
            "open Managed authority",
            "the selected authority does not exist",
        ))
    }
}

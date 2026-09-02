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

use crate::{
    CommitId, Error, ErrorKind, FsVersion, HeadObservation, NodeId, Result, Storage, Tree,
};

/// Current immutable version together with the condition needed for publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Observation {
    head: HeadObservation,
    version: FsVersion,
}

impl Observation {
    pub const fn version(&self) -> &FsVersion {
        &self.version
    }

    pub const fn tree(&self) -> &Tree {
        self.version.tree()
    }
}

/// Result of an idempotent publication attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOutcome {
    Committed { version: u64 },
    Conflict { current: u64 },
}

/// Open YinYang filesystem over one storage implementation.
#[derive(Debug)]
pub struct Fs<S> {
    root: NodeId,
    storage: S,
}

impl<S: Storage> Fs<S> {
    /// Create a filesystem, or reopen the existing filesystem.
    pub async fn create(storage: S) -> Result<Self> {
        if let Some(head) = storage.observe_head().await? {
            return Self::from_head(storage, head).await;
        }

        let root = NodeId::generate();
        let genesis = FsVersion::new(Tree::genesis(root), Vec::new())?;
        let blob = storage.write_version(&genesis).await?;
        storage.create_head(&blob).await?;
        Self::open(storage).await
    }

    /// Open and validate an existing filesystem.
    pub async fn open(storage: S) -> Result<Self> {
        let head = storage.observe_head().await?.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "open YinYang filesystem",
                "head is missing",
            )
        })?;
        Self::from_head(storage, head).await
    }

    async fn from_head(storage: S, head: HeadObservation) -> Result<Self> {
        let version = storage.read_version(head.version()).await?;
        let root = version.tree().root_id()?;
        Ok(Self { root, storage })
    }

    pub const fn root(&self) -> NodeId {
        self.root
    }

    /// Observe the head and materialize its immutable version.
    pub async fn observe(&self) -> Result<Observation> {
        let head = self.storage.observe_head().await?.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "observe YinYang filesystem",
                "head is missing",
            )
        })?;
        self.read_observation(head).await
    }

    /// Publish one validated successor tree using a caller-assigned idempotency key.
    pub async fn commit(
        &self,
        observed: &Observation,
        id: CommitId,
        tree: Tree,
    ) -> Result<CommitOutcome> {
        observed.version.validate_root(self.root)?;
        if let Some(version) = committed_version(&observed.version, id) {
            return Ok(CommitOutcome::Committed { version });
        }

        let mut commits = observed.version.commits().to_vec();
        commits.push(id);
        let version = FsVersion::new(tree, commits)?;
        observed
            .version
            .tree()
            .validate_successor(version.tree(), self.root)?;
        let number = version.number();
        let blob = self.storage.write_version(&version).await?;
        match self.storage.replace_head(&observed.head, &blob).await {
            Ok(true) => Ok(CommitOutcome::Committed { version: number }),
            Ok(false) => self.resolve_conflict(id).await,
            Err(error) => match self.find_commit(id).await {
                Ok(Some(version)) => Ok(CommitOutcome::Committed { version }),
                Ok(None) | Err(_) => Err(error),
            },
        }
    }

    async fn read_observation(&self, head: HeadObservation) -> Result<Observation> {
        let version = self.storage.read_version(head.version()).await?;
        version.validate_root(self.root)?;
        Ok(Observation { head, version })
    }

    async fn resolve_conflict(&self, id: CommitId) -> Result<CommitOutcome> {
        let current = self.observe().await?;
        if let Some(version) = committed_version(&current.version, id) {
            return Ok(CommitOutcome::Committed { version });
        }
        Ok(CommitOutcome::Conflict {
            current: current.version.number(),
        })
    }

    async fn find_commit(&self, id: CommitId) -> Result<Option<u64>> {
        Ok(committed_version(&self.observe().await?.version, id))
    }
}

fn committed_version(version: &FsVersion, id: CommitId) -> Option<u64> {
    version
        .commits()
        .iter()
        .position(|candidate| *candidate == id)
        .map(|index| index as u64 + 1)
}

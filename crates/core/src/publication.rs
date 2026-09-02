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
    Commit, CommitId, Error, ErrorKind, Extension, FormatStorage, FsFormat, FsHead, FsId,
    FsVersion, FsVersionRef, GcEpoch, HeadObservation, NodeId, Result, Tree, VersionNumber,
};

/// Persisted choices made when a YinYang filesystem is created.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CreateOptions {
    decodings: Vec<Extension>,
    head_extension: Option<Extension>,
}

impl CreateOptions {
    pub const fn new() -> Self {
        Self {
            decodings: Vec::new(),
            head_extension: None,
        }
    }

    pub fn with_decodings(mut self, decodings: Vec<Extension>) -> Self {
        self.decodings = decodings;
        self
    }

    pub fn with_head_extension(mut self, extension: Extension) -> Self {
        self.head_extension = Some(extension);
        self
    }
}

/// Current immutable version together with the condition needed for publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Observation {
    head: HeadObservation,
    version: FsVersion,
}

impl Observation {
    pub const fn head(&self) -> &FsHead {
        self.head.head()
    }

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
    Committed { version: VersionNumber },
    Conflict { current: VersionNumber },
}

/// Open YinYang filesystem over one storage implementation.
#[derive(Debug)]
pub struct Fs<S> {
    format: FsFormat,
    storage: S,
}

impl<S: FormatStorage> Fs<S> {
    /// Create a filesystem, or reopen the existing filesystem with the same configuration.
    pub async fn create(storage: S, options: CreateOptions) -> Result<Self> {
        let requested = FsFormat::new(
            FsId::generate(),
            NodeId::generate(),
            options.decodings,
            options.head_extension,
        );
        let format = if storage.create_format(&requested).await? {
            requested
        } else {
            let existing = storage.read_format().await?.ok_or_else(|| {
                Error::new(
                    ErrorKind::Storage,
                    "create YinYang filesystem",
                    "format disappeared after create conflict",
                )
            })?;
            if !existing.has_same_configuration(&requested) {
                return Err(Error::conflict(
                    "create YinYang filesystem",
                    "storage contains a different format configuration",
                ));
            }
            existing
        };

        let filesystem = Self { format, storage };
        filesystem.initialize().await?;
        Ok(filesystem)
    }

    /// Open and validate an existing filesystem.
    pub async fn open(storage: S) -> Result<Self> {
        let format = storage.read_format().await?.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "open YinYang filesystem",
                "format is missing",
            )
        })?;
        let filesystem = Self { format, storage };
        filesystem.observe().await?;
        Ok(filesystem)
    }

    pub const fn format(&self) -> &FsFormat {
        &self.format
    }

    pub const fn storage(&self) -> &S {
        &self.storage
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
        self.validate_observation(observed)?;
        if let Some(committed) = observed
            .version
            .commits()
            .iter()
            .find(|commit| commit.id() == id)
        {
            return Ok(CommitOutcome::Committed {
                version: committed.version(),
            });
        }

        observed.version.tree().validate_successor(
            &tree,
            self.format.root(),
            self.format.decodings().len(),
        )?;
        let number = observed.version.number().next()?;
        let mut commits = observed.version.commits().to_vec();
        commits.push(Commit::new(id, number));
        let version = FsVersion::new(&self.format, number, tree, commits)?;
        let blob = self.storage.write_version(&version).await?;
        let reference = FsVersionRef::new(number, blob);
        let next = FsHead::new(
            reference,
            observed.head().gc_epoch(),
            observed.head().min_retained(),
        )?;

        match self.storage.replace_head(&observed.head, &next).await {
            Ok(true) => Ok(CommitOutcome::Committed { version: number }),
            Ok(false) => self.resolve_conflict(id).await,
            Err(error) => match self.find_commit(id).await {
                Ok(Some(version)) => Ok(CommitOutcome::Committed { version }),
                Ok(None) | Err(_) => Err(error),
            },
        }
    }

    async fn initialize(&self) -> Result<()> {
        if let Some(head) = self.storage.observe_head().await? {
            self.read_observation(head).await?;
            return Ok(());
        }

        let genesis = FsVersion::new(
            &self.format,
            VersionNumber::ZERO,
            Tree::genesis(self.format.root()),
            Vec::new(),
        )?;
        let blob = self.storage.write_version(&genesis).await?;
        let head = FsHead::new(
            FsVersionRef::new(VersionNumber::ZERO, blob),
            GcEpoch::ZERO,
            VersionNumber::ZERO,
        )?;
        self.storage.create_head(&head).await?;
        self.observe().await?;
        Ok(())
    }

    async fn read_observation(&self, head: HeadObservation) -> Result<Observation> {
        head.head().validate()?;
        let version = self
            .storage
            .read_version(head.head().current().blob())
            .await?;
        version.validate(&self.format)?;
        if version.number() != head.head().current().number() {
            return Err(Error::corrupt(
                "observe YinYang filesystem",
                "version number does not match the head reference",
            ));
        }
        Ok(Observation { head, version })
    }

    fn validate_observation(&self, observed: &Observation) -> Result<()> {
        observed.head().validate()?;
        observed.version.validate(&self.format)?;
        if observed.version.number() != observed.head().current().number() {
            return Err(Error::invalid(
                "commit YinYang filesystem",
                "observation does not match its head",
            ));
        }
        Ok(())
    }

    async fn resolve_conflict(&self, id: CommitId) -> Result<CommitOutcome> {
        let current = self.observe().await?;
        if let Some(commit) = current
            .version
            .commits()
            .iter()
            .find(|commit| commit.id() == id)
        {
            return Ok(CommitOutcome::Committed {
                version: commit.version(),
            });
        }
        Ok(CommitOutcome::Conflict {
            current: current.version.number(),
        })
    }

    async fn find_commit(&self, id: CommitId) -> Result<Option<VersionNumber>> {
        Ok(self
            .observe()
            .await?
            .version
            .commits()
            .iter()
            .find(|commit| commit.id() == id)
            .map(|commit| commit.version()))
    }
}

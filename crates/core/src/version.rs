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
    BlobRef, CommitId, Error, ExtensionId, FsId, GcEpoch, NodeId, Result, Tree, VersionNumber,
};

/// Persisted extension identity and configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Extension {
    id: ExtensionId,
    configuration: Vec<u8>,
}

impl Extension {
    pub fn new(id: ExtensionId, configuration: Vec<u8>) -> Self {
        Self { id, configuration }
    }

    pub const fn id(&self) -> ExtensionId {
        self.id
    }

    pub fn configuration(&self) -> &[u8] {
        &self.configuration
    }
}

/// Bootstrap record required before reading a YinYang filesystem.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsFormat {
    fs: FsId,
    root: NodeId,
    decodings: Vec<Extension>,
    head_extension: Option<Extension>,
}

impl FsFormat {
    pub fn new(
        fs: FsId,
        root: NodeId,
        decodings: Vec<Extension>,
        head_extension: Option<Extension>,
    ) -> Self {
        Self {
            fs,
            root,
            decodings,
            head_extension,
        }
    }

    pub const fn fs(&self) -> FsId {
        self.fs
    }

    pub const fn root(&self) -> NodeId {
        self.root
    }

    pub fn decodings(&self) -> &[Extension] {
        &self.decodings
    }

    pub fn head_extension(&self) -> Option<&Extension> {
        self.head_extension.as_ref()
    }

    pub(crate) fn has_same_configuration(&self, other: &Self) -> bool {
        self.decodings == other.decodings && self.head_extension == other.head_extension
    }
}

/// Reference to one immutable filesystem version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsVersionRef {
    number: VersionNumber,
    blob: BlobRef,
}

impl FsVersionRef {
    pub const fn new(number: VersionNumber, blob: BlobRef) -> Self {
        Self { number, blob }
    }

    pub const fn number(&self) -> VersionNumber {
        self.number
    }

    pub const fn blob(&self) -> &BlobRef {
        &self.blob
    }
}

/// Durable fact binding an idempotency key to a published version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Commit {
    id: CommitId,
    version: VersionNumber,
}

impl Commit {
    pub const fn new(id: CommitId, version: VersionNumber) -> Self {
        Self { id, version }
    }

    pub const fn id(self) -> CommitId {
        self.id
    }

    pub const fn version(self) -> VersionNumber {
        self.version
    }
}

/// One immutable materialized filesystem version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsVersion {
    fs: FsId,
    number: VersionNumber,
    tree: Tree,
    commits: Vec<Commit>,
}

impl FsVersion {
    pub fn new(
        format: &FsFormat,
        number: VersionNumber,
        tree: Tree,
        commits: Vec<Commit>,
    ) -> Result<Self> {
        let version = Self {
            fs: format.fs,
            number,
            tree,
            commits,
        };
        version.validate(format)?;
        Ok(version)
    }

    pub const fn fs(&self) -> FsId {
        self.fs
    }

    pub const fn number(&self) -> VersionNumber {
        self.number
    }

    pub const fn tree(&self) -> &Tree {
        &self.tree
    }

    pub fn commits(&self) -> &[Commit] {
        &self.commits
    }

    pub(crate) fn validate(&self, format: &FsFormat) -> Result<()> {
        if self.fs != format.fs {
            return Err(Error::corrupt(
                "read YinYang version",
                "filesystem identity does not match the format",
            ));
        }
        self.tree.validate(format.root, format.decodings.len())?;
        validate_commits(self.number, &self.commits)
    }
}

/// Mutable publication cell for one YinYang filesystem.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsHead {
    current: FsVersionRef,
    gc_epoch: GcEpoch,
    min_retained: VersionNumber,
}

impl FsHead {
    pub fn new(
        current: FsVersionRef,
        gc_epoch: GcEpoch,
        min_retained: VersionNumber,
    ) -> Result<Self> {
        if min_retained > current.number {
            return Err(Error::invalid(
                "construct YinYang head",
                "minimum retained version exceeds the current version",
            ));
        }
        Ok(Self {
            current,
            gc_epoch,
            min_retained,
        })
    }

    pub const fn current(&self) -> &FsVersionRef {
        &self.current
    }

    pub const fn gc_epoch(&self) -> GcEpoch {
        self.gc_epoch
    }

    pub const fn min_retained(&self) -> VersionNumber {
        self.min_retained
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.min_retained > self.current.number {
            return Err(Error::corrupt(
                "read YinYang head",
                "minimum retained version exceeds the current version",
            ));
        }
        Ok(())
    }
}

fn validate_commits(number: VersionNumber, commits: &[Commit]) -> Result<()> {
    use std::collections::BTreeSet;

    if number == VersionNumber::ZERO {
        if commits.is_empty() {
            return Ok(());
        }
        return Err(Error::corrupt(
            "read YinYang version",
            "genesis version contains commits",
        ));
    }
    let Some(first) = commits.first().copied() else {
        return Err(Error::corrupt(
            "read YinYang version",
            "non-genesis version has no retained commits",
        ));
    };
    if first.version == VersionNumber::ZERO
        || commits.last().map(|commit| commit.version) != Some(number)
    {
        return Err(Error::corrupt(
            "read YinYang version",
            "commit range does not end at the filesystem version",
        ));
    }

    let mut identities = BTreeSet::new();
    let mut previous = None;
    for commit in commits {
        if !identities.insert(commit.id)
            || previous
                .is_some_and(|previous: VersionNumber| previous.next().ok() != Some(commit.version))
        {
            return Err(Error::corrupt(
                "read YinYang version",
                "commits are not unique and contiguous",
            ));
        }
        previous = Some(commit.version);
    }
    Ok(())
}

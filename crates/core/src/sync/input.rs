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

//! Sync request, change-set, and hint types.

use crate::Error;
use crate::filesystem::{ContentRef, validate_portable_path};
use crate::format::FileRange;

/// Trusted changed ranges for one immutable staged file publication.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct FileChangeSetEntry {
    pub(crate) path: String,
    pub(crate) base: ContentRef,
    pub(crate) ranges: Vec<FileRange>,
}

impl FileChangeSetEntry {
    /// Normalize changed ranges and bind them to the previously observed content.
    pub fn new(
        path: impl Into<String>,
        base: ContentRef,
        ranges: impl IntoIterator<Item = std::ops::Range<u64>>,
    ) -> Result<Self, Error> {
        let path = path.into();
        validate_portable_path(&path)?;
        if path.is_empty() {
            return Err(Error::invalid(
                "record Managed file mutation",
                "changed file path is empty",
            ));
        }
        let mut ranges = ranges
            .into_iter()
            .map(|range| {
                let length = range.end.checked_sub(range.start).ok_or_else(|| {
                    Error::invalid("record Managed file mutation", "changed range is invalid")
                })?;
                Ok::<_, Error>(FileRange::new(range.start, length)?)
            })
            .collect::<Result<Vec<_>, _>>()?;
        ranges.sort_unstable_by_key(|range| range.offset);
        let mut normalized = Vec::<FileRange>::new();
        for range in ranges {
            if let Some(previous) = normalized.last_mut() {
                let previous_end = previous.end()?;
                if range.offset <= previous_end {
                    let end = previous_end.max(range.end()?);
                    previous.length = end - previous.offset;
                    continue;
                }
            }
            normalized.push(range);
        }
        Ok(Self {
            path,
            base,
            ranges: normalized,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Complete trusted regular-file change set for one publication.
#[derive(Clone, Debug, Default)]
pub struct FileChangeSet {
    pub(crate) entries: Vec<FileChangeSetEntry>,
}

impl FileChangeSet {
    pub fn new(entries: Vec<FileChangeSetEntry>) -> Result<Self, Error> {
        let mut seen = std::collections::BTreeSet::new();
        for entry in &entries {
            if !seen.insert(entry.path.as_str()) {
                return Err(Error::invalid(
                    "synchronize replica",
                    "one file has more than one mutation input",
                ));
            }
        }
        Ok(Self { entries })
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Non-authoritative hint that a path may have changed.
#[derive(Clone, Debug)]
pub struct LocalChangeHint {
    pub path: String,
}

/// Explicit conflict resolution by publishing the current local path.
#[derive(Clone, Debug)]
pub struct ConflictResolution {
    pub path: String,
}

/// User-visible sync request assembled by the command layer.
#[derive(Clone, Debug)]
pub struct SyncRequest {
    pub resolve: Vec<String>,
    pub change_set: Option<FileChangeSet>,
}

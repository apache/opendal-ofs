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

//! Canonical logical-file mappings over immutable extent streams.

use crate::Error;
use crate::v0::model::ContentRef;

use super::object::{ObjectClass, ObjectLocator};
use super::stream::{StreamKind, StreamRef};

/// One independently verifiable range in an immutable data segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentRangeRef {
    pub segment: ObjectLocator,
    pub offset: u64,
    pub stored_content: ContentRef,
}

super::codec::tuple_wire!(SegmentRangeRef {
    segment: ObjectLocator,
    offset: u64,
    stored_content: ContentRef,
});

/// Reference to one independently readable logical extent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentRef {
    pub stored_range: SegmentRangeRef,
    pub decoding_outputs: Vec<ContentRef>,
}

super::codec::tuple_wire!(ExtentRef {
    stored_range: SegmentRangeRef,
    decoding_outputs: Vec<ContentRef>,
});

impl ExtentRef {
    /// Logical content visible after the configured decoding chain.
    pub fn content(&self) -> ContentRef {
        self.decoding_outputs
            .last()
            .copied()
            .unwrap_or(self.stored_range.stored_content)
    }
}

/// One half-open logical byte range in a file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileRange {
    pub offset: u64,
    pub length: u64,
}

super::codec::tuple_wire!(FileRange {
    offset: u64,
    length: u64,
});

impl FileRange {
    pub fn new(offset: u64, length: u64) -> Result<Self, Error> {
        if length == 0 || offset.checked_add(length).is_none() {
            return Err(Error::invalid(
                "construct Managed file range",
                "file range is empty or overflows",
            ));
        }
        Ok(Self { offset, length })
    }

    pub fn end(self) -> Result<u64, Error> {
        self.offset
            .checked_add(self.length)
            .ok_or_else(|| Error::corrupt("read Managed file range", "file range end overflows"))
    }
}

/// Mapping from one logical file range to a slice of an immutable extent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentMapping {
    pub logical_range: FileRange,
    pub extent_offset: u64,
    pub extent: ExtentRef,
}

super::codec::tuple_wire!(ExtentMapping {
    logical_range: FileRange,
    extent_offset: u64,
    extent: ExtentRef,
});

impl ExtentMapping {
    /// Map one complete extent at the given logical file offset.
    pub fn complete(file_offset: u64, extent: ExtentRef) -> Result<Self, Error> {
        Ok(Self {
            logical_range: FileRange::new(file_offset, extent.content().length())?,
            extent_offset: 0,
            extent,
        })
    }

    pub fn end(&self) -> Result<u64, Error> {
        self.logical_range.end()
    }

    /// Select one subrange without changing the immutable extent reference.
    pub fn slice(&self, range: FileRange) -> Result<Self, Error> {
        let end = range.end()?;
        if range.offset < self.logical_range.offset || end > self.end()? {
            return Err(Error::corrupt(
                "slice Managed file extent",
                "slice falls outside its file extent",
            ));
        }
        let extent_offset = self
            .extent_offset
            .checked_add(range.offset - self.logical_range.offset)
            .ok_or_else(|| {
                Error::corrupt("slice Managed file extent", "extent offset overflows")
            })?;
        if extent_offset
            .checked_add(range.length)
            .is_none_or(|end| end > self.extent.content().length())
        {
            return Err(Error::corrupt(
                "slice Managed file extent",
                "extent slice exceeds its content",
            ));
        }
        Ok(Self {
            logical_range: range,
            extent_offset,
            extent: self.extent.clone(),
        })
    }

    pub fn validate(&self) -> Result<(), Error> {
        self.end()?;
        if self
            .extent_offset
            .checked_add(self.logical_range.length)
            .is_none_or(|end| end > self.extent.content().length())
        {
            return Err(Error::corrupt(
                "read Managed file extent",
                "extent slice exceeds its content",
            ));
        }
        Ok(())
    }
}

/// Self-contained reference to one ordered, non-overlapping extent run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentRunRef {
    pub span: FileRange,
    pub inline_extent: ExtentMapping,
    pub continuation: Option<StreamRef>,
}

super::codec::tuple_wire!(ExtentRunRef {
    span: FileRange,
    inline_extent: ExtentMapping,
    continuation: Option<StreamRef>,
});

impl ExtentRunRef {
    pub fn validate(&self) -> Result<(), Error> {
        self.inline_extent.validate()?;
        if self.inline_extent.logical_range.offset < self.span.offset
            || self.inline_extent.end()? > self.span.end()?
        {
            return Err(Error::corrupt(
                "read Managed file run",
                "inline extent falls outside its span",
            ));
        }
        if let Some(continuation) = self.continuation {
            continuation.require(StreamKind::FILE_EXTENTS, ObjectClass::FileExtentSegment)?;
        } else if self.span != self.inline_extent.logical_range {
            return Err(Error::corrupt(
                "read Managed file run",
                "inline-only run must cover its span",
            ));
        }
        Ok(())
    }
}

/// Durable file mapping: one base run plus newest-first binary-carry levels.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileExtentMap {
    pub base_run: Option<ExtentRunRef>,
    pub patch_levels: Vec<Option<ExtentRunRef>>,
}

super::codec::tuple_wire!(FileExtentMap {
    base_run: Option<ExtentRunRef>,
    patch_levels: Vec<Option<ExtentRunRef>>,
});

impl FileExtentMap {
    pub const fn empty() -> Self {
        Self {
            base_run: None,
            patch_levels: Vec::new(),
        }
    }

    /// Reference one canonical base run without patch levels.
    pub const fn from_base(base_run: ExtentRunRef) -> Self {
        Self {
            base_run: Some(base_run),
            patch_levels: Vec::new(),
        }
    }

    /// Iterate runs from newest patch to base.
    pub fn runs(&self) -> impl Iterator<Item = &ExtentRunRef> {
        self.patch_levels
            .iter()
            .flatten()
            .chain(self.base_run.iter())
    }

    /// Return the sole inline extent when this map needs no stream read.
    pub fn inline_file_extent(&self) -> Option<ExtentMapping> {
        if self.patch_levels.is_empty() {
            self.base_run.as_ref().and_then(|run| {
                run.continuation
                    .is_none()
                    .then(|| run.inline_extent.clone())
            })
        } else {
            None
        }
    }

    pub fn validate(&self, content: ContentRef) -> Result<(), Error> {
        if self.patch_levels.last().is_some_and(Option::is_none) {
            return Err(Error::corrupt(
                "read Managed file",
                "file extent levels contain trailing empty levels",
            ));
        }
        if self.patch_levels.len() > u64::BITS as usize {
            return Err(Error::corrupt(
                "read Managed file",
                "file extent levels exceed the publication sequence width",
            ));
        }
        if content.length() == 0 {
            if self.base_run.is_none() && self.patch_levels.is_empty() {
                return Ok(());
            }
            return Err(Error::corrupt(
                "read Managed file",
                "empty content must not retain extent runs",
            ));
        }
        let Some(base) = &self.base_run else {
            return Err(Error::corrupt(
                "read Managed file",
                "non-empty content requires a base run",
            ));
        };
        base.validate()?;
        for level in self.patch_levels.iter().flatten() {
            level.validate()?;
        }
        Ok(())
    }
}

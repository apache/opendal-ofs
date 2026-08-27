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

//! One file-data pipeline: partition, encode, place, and restore.

mod codec;
pub(crate) mod extent;
mod file;
mod lookup;
mod partition;
mod range;
mod segment;
pub(crate) mod write;

pub use codec::{ContentHasher, ExtentCodec, IdentityCodec};
pub use extent::{ExtentRunWriter, compact_file_extents};
pub(crate) use file::{RangeBatch, RangeBatcher};
pub use file::{ReusableFile, ReusableFileSource, logical_range, restore_file};
pub use lookup::ContentLookup;
pub use partition::{FilePartitioner, WholePartitioner};
pub use range::RangeReader;
pub use segment::DataSegmentWriter;
pub use write::{data_segments, publish_file, publish_file_patch};

use crate::Error;
use crate::format::FileRange;
use crate::format::{ExtentRef, FileExtentMap};

/// Compile-time family of file partitioning and extent encoding.
pub trait DataAccess: Clone + Send + Sync + std::fmt::Debug + Unpin + 'static {
    type Partitioner: FilePartitioner;
    type Codec: ExtentCodec;

    fn partitioner(&self) -> &Self::Partitioner;
    fn codec(&self) -> &Self::Codec;

    fn decoding_count(&self) -> usize {
        self.codec().decoding_count()
    }

    fn stored_size_bound(&self, logical_bytes: u64) -> Option<u64> {
        self.codec().stored_size_bound(logical_bytes)
    }

    fn validate_extent(&self, reference: &ExtentRef) -> Result<(), Error> {
        self.codec().validate(reference)
    }
}

/// Whole-file identity data access.
#[derive(Clone, Debug, Default)]
pub struct CoreDataAccess {
    partitioner: WholePartitioner,
    codec: IdentityCodec,
}

impl DataAccess for CoreDataAccess {
    type Partitioner = WholePartitioner;
    type Codec = IdentityCodec;

    fn partitioner(&self) -> &Self::Partitioner {
        &self.partitioner
    }

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

pub(crate) fn validate_file_map(
    data: &FileExtentMap,
    content: crate::filesystem::ContentRef,
    decoding_count: usize,
) -> Result<(), Error> {
    data.validate(content)?;
    if content.length() == 0 {
        return Ok(());
    }
    if data.patch_levels.is_empty()
        && let Some(mapping) = data.inline_file_extent()
        && (mapping.logical_range.offset != 0
            || mapping.extent_offset != 0
            || mapping.logical_range.length != content.length()
            || mapping.extent.content() != content)
    {
        return Err(Error::corrupt(
            "read Managed file",
            "single extent does not match the file content reference",
        ));
    }
    for run in data.runs() {
        if run.inline_extent.extent.decoding_outputs.len() != decoding_count {
            return Err(Error::corrupt(
                "read Managed file extent run",
                "extent decoding chain does not match the volume",
            ));
        }
    }
    Ok(())
}

pub(crate) fn file_range_end(range: FileRange) -> Result<u64, Error> {
    Ok(range.end()?)
}

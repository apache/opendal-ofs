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

//! Logical range planning and streaming restore.

use std::ops::{Bound, Range, RangeBounds};

use opendal::Operator;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _, AsyncWrite};

use crate::Error;
use crate::filesystem::ContentRef;
use crate::format::{ExtentRef, FileExtentMap, FileRange, ObjectLocator};

use super::DataAccess;
use super::codec::ExtentCodec;
use super::range::RangeReader;
use super::validate_file_map;

/// One item in a bounded range batch.
pub(crate) struct RangeBatchItem<T> {
    pub(crate) value: T,
    pub(crate) range: Range<u64>,
}

/// One storage request plan for ranges from the same immutable segment.
pub(crate) struct RangeBatch<T> {
    locator: ObjectLocator,
    items: Vec<RangeBatchItem<T>>,
    metadata_bytes: u64,
    range_bytes: u64,
    contiguous: bool,
    last_end: u64,
}

pub(crate) struct RangeBatchReader<T> {
    items: std::vec::IntoIter<RangeBatchItem<T>>,
    readers: BatchReaders,
}

enum BatchReaders {
    Stream(RangeReader),
    Fetched {
        readers: std::vec::IntoIter<RangeReader>,
        current: Option<RangeReader>,
    },
}

/// Build range batches whose retained bytes fit one transfer lane.
pub(crate) struct RangeBatcher<T> {
    maximum_bytes: u64,
    gap_bytes: u64,
    current: Option<RangeBatch<T>>,
}

impl<T> RangeBatcher<T> {
    pub(crate) fn new(maximum_bytes: usize, gap_bytes: usize) -> Self {
        Self {
            maximum_bytes: maximum_bytes as u64,
            gap_bytes: gap_bytes as u64,
            current: None,
        }
    }

    pub(crate) fn push(
        &mut self,
        locator: ObjectLocator,
        range: Range<u64>,
        metadata_bytes: usize,
        value: T,
    ) -> Result<Option<RangeBatch<T>>, Error> {
        if range.start >= range.end {
            return Err(Error::corrupt(
                "plan Managed segment ranges",
                "data segment range is empty or reversed",
            ));
        }
        let metadata_bytes = metadata_bytes as u64;
        let fits = self.current.as_ref().is_none_or(|batch| {
            batch.locator == locator
                && batch
                    .projected_bytes(&range, metadata_bytes, self.gap_bytes)
                    .is_ok_and(|bytes| bytes <= self.maximum_bytes)
        });
        let ready = (!fits).then(|| self.current.take()).flatten();
        match self.current.as_mut() {
            Some(batch) => batch.push(range, metadata_bytes, value)?,
            None => self.current = Some(RangeBatch::new(locator, range, metadata_bytes, value)),
        }
        Ok(ready)
    }

    pub(crate) fn take(&mut self) -> Option<RangeBatch<T>> {
        self.current.take()
    }
}

impl<T> RangeBatch<T> {
    fn new(locator: ObjectLocator, range: Range<u64>, metadata_bytes: u64, value: T) -> Self {
        let range_bytes = range.end - range.start;
        let last_end = range.end;
        Self {
            locator,
            items: vec![RangeBatchItem { value, range }],
            metadata_bytes,
            range_bytes,
            contiguous: true,
            last_end,
        }
    }

    fn projected_bytes(
        &self,
        range: &Range<u64>,
        metadata_bytes: u64,
        gap_bytes: u64,
    ) -> Result<u64, Error> {
        let metadata_bytes = self
            .metadata_bytes
            .checked_add(metadata_bytes)
            .ok_or_else(|| Error::corrupt("plan Managed segment ranges", "byte count overflows"))?;
        if self.contiguous && self.last_end == range.start {
            return Ok(metadata_bytes);
        }
        let range_bytes = self
            .range_bytes
            .checked_add(range.end - range.start)
            .ok_or_else(|| Error::corrupt("plan Managed segment ranges", "byte count overflows"))?;
        let gaps = gap_bytes
            .checked_mul(self.items.len() as u64)
            .ok_or_else(|| Error::corrupt("plan Managed segment ranges", "byte count overflows"))?;
        metadata_bytes
            .checked_add(range_bytes)
            .and_then(|bytes| bytes.checked_add(gaps))
            .ok_or_else(|| Error::corrupt("plan Managed segment ranges", "byte count overflows"))
    }

    fn push(&mut self, range: Range<u64>, metadata_bytes: u64, value: T) -> Result<(), Error> {
        self.metadata_bytes = self
            .metadata_bytes
            .checked_add(metadata_bytes)
            .ok_or_else(|| Error::corrupt("plan Managed segment ranges", "byte count overflows"))?;
        self.range_bytes = self
            .range_bytes
            .checked_add(range.end - range.start)
            .ok_or_else(|| Error::corrupt("plan Managed segment ranges", "byte count overflows"))?;
        self.contiguous &= self.last_end == range.start;
        self.last_end = range.end;
        self.items.push(RangeBatchItem { value, range });
        Ok(())
    }

    pub(crate) fn stream_range(&self) -> Option<Range<u64>> {
        self.contiguous.then(|| {
            self.items
                .first()
                .expect("a range batch is non-empty")
                .range
                .start..self.last_end
        })
    }

    pub(crate) async fn read(
        self,
        operator: &Operator,
        gap_bytes: usize,
    ) -> Result<RangeBatchReader<T>, Error> {
        let locator = self.locator;
        let range = self.stream_range();
        let items = self.items;
        let readers = match range {
            Some(range) => BatchReaders::Stream(RangeReader::open(operator, locator, range).await?),
            None => {
                let ranges = items.iter().map(|item| item.range.clone()).collect();
                BatchReaders::Fetched {
                    readers: RangeReader::fetch(operator, locator, ranges, gap_bytes)
                        .await?
                        .into_iter(),
                    current: None,
                }
            }
        };
        Ok(RangeBatchReader {
            items: items.into_iter(),
            readers,
        })
    }
}

impl<T> RangeBatchReader<T> {
    pub(crate) fn next(&mut self) -> Option<(T, &mut RangeReader)> {
        let item = self.items.next()?;
        let reader = match &mut self.readers {
            BatchReaders::Stream(reader) => reader,
            BatchReaders::Fetched { readers, current } => {
                *current = Some(
                    readers
                        .next()
                        .expect("a fetched reader exists for every range batch item"),
                );
                current.as_mut().expect("the fetched reader was stored")
            }
        };
        Some((item.value, reader))
    }
}

struct RemoteExtent {
    reference: ExtentRef,
    logical: Range<u64>,
}

impl RemoteExtent {
    fn metadata_bytes(&self) -> u64 {
        size_of::<Self>().saturating_add(
            self.reference
                .decoding_outputs
                .len()
                .saturating_mul(size_of::<ContentRef>()),
        ) as u64
    }
}

async fn read_remote_batch<C: ExtentCodec>(
    batch: Option<RangeBatch<RemoteExtent>>,
    codec: &C,
    operator: &Operator,
    read_gap_bytes: usize,
    destination: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Error> {
    let Some(batch) = batch else {
        return Ok(());
    };
    let mut batch = batch.read(operator, read_gap_bytes).await?;
    while let Some((extent, reader)) = batch.next() {
        codec
            .decode(reader, extent.reference, extent.logical, destination)
            .await?;
    }
    Ok(())
}

pub struct ReusableFile<'a> {
    pub reference: FileExtentMap,
    pub source: &'a mut (dyn ReusableFileSource + 'a),
}

pub trait ReusableFileSource: AsyncRead + AsyncSeek + Send + Unpin {}

impl<T> ReusableFileSource for T where T: AsyncRead + AsyncSeek + Send + Unpin {}

pub async fn restore_file<A: DataAccess>(
    access: &A,
    operator: &Operator,
    read_gap_bytes: usize,
    read_window_bytes: usize,
    reference: FileExtentMap,
    content: ContentRef,
    range: Range<u64>,
    reusable: Option<ReusableFile<'_>>,
    destination: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Error> {
    validate_file_map(&reference, content, access.decoding_count())?;
    if range.is_empty() {
        return Ok(());
    }
    let selection = FileRange::new(range.start, range.end - range.start)?;
    let mut mappings =
        super::extent::open_file_extents(&reference, operator, Some(selection)).await?;
    let (mut existing, mut reusable_source) = match reusable {
        Some(reusable) => (
            Some(
                super::extent::open_file_extents(&reusable.reference, operator, Some(selection))
                    .await?,
            ),
            Some(reusable.source),
        ),
        None => (None, None),
    };
    let mut existing_mapping = None::<crate::format::ExtentMapping>;
    let mut remote = RangeBatcher::new(read_window_bytes, read_gap_bytes);
    let mut expected_offset = range.start;
    while let Some(mapping) = mappings.next().await? {
        access.validate_extent(&mapping.extent)?;
        if mapping.logical_range.offset != expected_offset {
            return Err(Error::corrupt(
                "read Managed file",
                "file extents contain a gap or overlap",
            ));
        }
        let mapping_end = mapping.end()?;
        if let Some(existing) = existing.as_mut() {
            while existing_mapping.as_ref().is_none_or(|candidate| {
                candidate
                    .end()
                    .ok()
                    .is_some_and(|end| end <= mapping.logical_range.offset)
            }) {
                let Some(candidate) = existing.next().await? else {
                    existing_mapping = None;
                    break;
                };
                access.validate_extent(&candidate.extent)?;
                existing_mapping = Some(candidate);
            }
        }
        if mapping.logical_range.offset < range.end && range.start < mapping_end {
            let selected_start = range.start.max(mapping.logical_range.offset);
            let selected_end = range.end.min(mapping_end);
            let extent_start = mapping
                .extent_offset
                .checked_add(selected_start - mapping.logical_range.offset)
                .ok_or_else(|| Error::corrupt("read Managed file", "extent offset overflows"))?;
            let extent_end = extent_start
                .checked_add(selected_end - selected_start)
                .ok_or_else(|| Error::corrupt("read Managed file", "extent range overflows"))?;
            let reusable_offset = existing_mapping.as_ref().and_then(|existing| {
                let delta = mapping.extent_offset.checked_sub(existing.extent_offset)?;
                let end = delta.checked_add(mapping.logical_range.length)?;
                (existing.extent == mapping.extent && end <= existing.logical_range.length)
                    .then_some(existing.logical_range.offset + delta)
            });
            if let (Some(source), Some(reusable_offset)) =
                (reusable_source.as_deref_mut(), reusable_offset)
            {
                read_remote_batch(
                    remote.take(),
                    access.codec(),
                    operator,
                    read_gap_bytes,
                    destination,
                )
                .await?;
                copy_reusable(
                    source,
                    reusable_offset + selected_start - mapping.logical_range.offset,
                    selected_end - selected_start,
                    destination,
                )
                .await?;
            } else {
                let logical = extent_start..extent_end;
                let stored = access
                    .codec()
                    .stored_range(&mapping.extent, logical.clone())?;
                let physical_start = mapping
                    .extent
                    .stored_range
                    .offset
                    .checked_add(stored.start)
                    .ok_or_else(|| {
                        Error::corrupt("read Managed file", "stored range start overflows")
                    })?;
                let physical_end = mapping
                    .extent
                    .stored_range
                    .offset
                    .checked_add(stored.end)
                    .ok_or_else(|| {
                        Error::corrupt("read Managed file", "stored range end overflows")
                    })?;
                if physical_start >= physical_end {
                    return Err(Error::corrupt("read Managed file", "stored range is empty"));
                }
                let locator = mapping.extent.stored_range.segment;
                let physical = physical_start..physical_end;
                let extent = RemoteExtent {
                    reference: mapping.extent,
                    logical,
                };
                let metadata_bytes = extent.metadata_bytes() as usize;
                let ready = remote.push(locator, physical, metadata_bytes, extent)?;
                read_remote_batch(ready, access.codec(), operator, read_gap_bytes, destination)
                    .await?;
            }
        }
        expected_offset = mapping_end;
        if expected_offset >= range.end && range.end < content.length() {
            read_remote_batch(
                remote.take(),
                access.codec(),
                operator,
                read_gap_bytes,
                destination,
            )
            .await?;
            return Ok(());
        }
    }
    if expected_offset != range.end {
        return Err(Error::corrupt(
            "read Managed file",
            "file extents do not cover the logical file",
        ));
    }
    read_remote_batch(
        remote.take(),
        access.codec(),
        operator,
        read_gap_bytes,
        destination,
    )
    .await?;
    Ok(())
}

async fn copy_reusable(
    source: &mut (dyn ReusableFileSource + '_),
    offset: u64,
    length: u64,
    destination: &mut (impl AsyncWrite + Send + Unpin + ?Sized),
) -> Result<(), Error> {
    source
        .seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|error| Error::io("seek reusable Managed file", error))?;
    let copied = tokio::io::copy(&mut source.take(length), destination)
        .await
        .map_err(|error| Error::io("copy reusable Managed file", error))?;
    if copied != length {
        return Err(Error::conflict(
            "copy reusable Managed file",
            "verified local file changed during installation",
        ));
    }
    Ok(())
}

pub fn logical_range(file_size: u64, range: impl RangeBounds<u64>) -> Result<Range<u64>, Error> {
    let start = match range.start_bound() {
        Bound::Included(offset) => *offset,
        Bound::Excluded(offset) => offset
            .checked_add(1)
            .ok_or_else(|| Error::invalid("read Managed file range", "range start overflows"))?,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(offset) => offset
            .checked_add(1)
            .ok_or_else(|| Error::invalid("read Managed file range", "range end overflows"))?,
        Bound::Excluded(offset) => *offset,
        Bound::Unbounded => file_size,
    };
    if start > end || end > file_size {
        return Err(Error::invalid(
            "read Managed file range",
            "logical byte range is invalid",
        ));
    }
    Ok(start..end)
}

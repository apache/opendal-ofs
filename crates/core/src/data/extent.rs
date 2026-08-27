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

//! Canonical extent-run streaming and materialization.

use std::num::NonZeroUsize;

use opendal::Operator;

use crate::Error;
use crate::format::{
    ExtentMapping, ExtentRunRef, FileExtentMap, FileRange, ObjectClass, StreamKind,
};
use crate::storage::{RecordStreamReader, RecordStreamWriter};

use super::file_range_end;

/// Single-pass canonical view over all of one file's extent runs.
pub(crate) struct FileExtentReader {
    runs: Vec<FileExtentRunReader>,
    position: Option<u64>,
    pending: Option<ExtentMapping>,
}

/// Single-pass reader over one internally canonical extent run.
pub(crate) struct FileExtentRunReader {
    reference: ExtentRunRef,
    first: Option<ExtentMapping>,
    tail: Option<RecordStreamReader<ExtentMapping>>,
    previous_end: Option<u64>,
    head: Option<ExtentMapping>,
}

/// Forward-only writer for one canonical extent run.
pub struct ExtentRunWriter<'a> {
    operator: &'a Operator,
    gc_epoch: crate::format::GcEpoch,
    multipart_part_bytes: NonZeroUsize,
    first: Option<ExtentMapping>,
    tail: Option<RecordStreamWriter>,
    previous_end: Option<u64>,
    covered_bytes: u64,
}

/// Open the canonical newest-wins logical extent stream.
pub(crate) async fn open_file_extents(
    reference: &FileExtentMap,
    operator: &Operator,
    selection: Option<FileRange>,
) -> Result<FileExtentReader, Error> {
    let mut runs = Vec::new();
    for reference in reference.runs() {
        if let Some(selection) = selection
            && (file_range_end(reference.span)? <= selection.offset
                || file_range_end(selection)? <= reference.span.offset)
        {
            continue;
        }
        let mut run = open_extent_run(reference, operator).await?;
        run.head = run.next().await?;
        runs.push(run);
    }
    Ok(FileExtentReader {
        runs,
        position: selection.map(|range| range.offset),
        pending: None,
    })
}

/// Add one newer patch run using deterministic binary carry.
pub(crate) async fn add_extent_run(
    mut reference: FileExtentMap,
    operator: &Operator,
    gc_epoch: crate::format::GcEpoch,
    multipart_part_bytes: NonZeroUsize,
    carry: ExtentRunRef,
) -> Result<FileExtentMap, Error> {
    let mut runs = vec![carry];
    for level in 0..u64::BITS as usize {
        if reference.patch_levels.len() == level {
            reference.patch_levels.push(Some(
                materialize_runs(operator, gc_epoch, multipart_part_bytes, runs, |_| Ok(()))
                    .await?,
            ));
            return Ok(reference);
        }
        match reference.patch_levels[level].take() {
            None => {
                reference.patch_levels[level] = Some(
                    materialize_runs(operator, gc_epoch, multipart_part_bytes, runs, |_| Ok(()))
                        .await?,
                );
                while reference.patch_levels.last().is_some_and(Option::is_none) {
                    reference.patch_levels.pop();
                }
                return Ok(reference);
            }
            Some(older) => runs.push(older),
        }
    }
    runs.push(reference.base_run.take().ok_or_else(|| {
        Error::corrupt(
            "merge Managed file extent runs",
            "a patched file has no base extent run",
        )
    })?);
    reference.base_run =
        Some(materialize_runs(operator, gc_epoch, multipart_part_bytes, runs, |_| Ok(())).await?);
    reference.patch_levels.clear();
    Ok(reference)
}

/// Materialize a layered mapping as one canonical base run.
pub async fn compact_file_extents(
    reference: &FileExtentMap,
    operator: &Operator,
    gc_epoch: crate::format::GcEpoch,
    multipart_part_bytes: NonZeroUsize,
    mut visit: impl FnMut(&ExtentMapping) -> Result<(), Error>,
) -> Result<FileExtentMap, Error> {
    if reference.base_run.is_none() {
        return Ok(FileExtentMap::empty());
    }
    if reference.patch_levels.is_empty() {
        let mut extents = open_file_extents(reference, operator, None).await?;
        while let Some(mapping) = extents.next().await? {
            visit(&mapping)?;
        }
        return Ok(reference.clone());
    }
    let runs = reference.runs().cloned().collect();
    Ok(FileExtentMap::from_base(
        materialize_runs(operator, gc_epoch, multipart_part_bytes, runs, visit).await?,
    ))
}

async fn materialize_runs(
    operator: &Operator,
    gc_epoch: crate::format::GcEpoch,
    multipart_part_bytes: NonZeroUsize,
    mut runs: Vec<ExtentRunRef>,
    mut visit: impl FnMut(&ExtentMapping) -> Result<(), Error>,
) -> Result<ExtentRunRef, Error> {
    if runs.len() == 1 {
        return Ok(runs.pop().expect("one extent run remains"));
    }
    let base = runs.pop().ok_or_else(|| {
        Error::invalid(
            "merge Managed file extent runs",
            "extent-run merge is empty",
        )
    })?;
    let overlay = FileExtentMap {
        base_run: Some(base),
        patch_levels: runs.into_iter().map(Some).collect(),
    };
    let mut writer = ExtentRunWriter::new(operator, gc_epoch, multipart_part_bytes);
    let mut extents = open_file_extents(&overlay, operator, None).await?;
    while let Some(mapping) = extents.next().await? {
        visit(&mapping)?;
        writer.write(mapping).await?;
    }
    writer.close().await?.ok_or_else(|| {
        Error::corrupt(
            "merge Managed file extent runs",
            "extent-run overlay is empty",
        )
    })
}

impl<'a> ExtentRunWriter<'a> {
    /// Start an extent run without opening a remote object.
    pub(crate) const fn new(
        operator: &'a Operator,
        gc_epoch: crate::format::GcEpoch,
        multipart_part_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            operator,
            gc_epoch,
            multipart_part_bytes,
            first: None,
            tail: None,
            previous_end: None,
            covered_bytes: 0,
        }
    }

    /// Append one logical mapping in strict non-overlapping offset order.
    pub async fn write(&mut self, mapping: ExtentMapping) -> Result<(), Error> {
        mapping.validate()?;
        if self
            .previous_end
            .is_some_and(|previous| mapping.logical_range.offset < previous)
        {
            return Err(Error::invalid(
                "write Managed file extent run",
                "file extents overlap or are out of order",
            ));
        }
        self.previous_end = Some(mapping.end()?);
        self.covered_bytes = self
            .covered_bytes
            .checked_add(mapping.logical_range.length)
            .ok_or_else(|| Error::invalid("write Managed file extent run", "coverage overflows"))?;
        if self.first.is_none() {
            self.first = Some(mapping);
            return Ok(());
        }
        if self.tail.is_none() {
            self.tail = Some(
                RecordStreamWriter::open(
                    self.operator,
                    self.gc_epoch,
                    ObjectClass::FileExtentSegment,
                    StreamKind::FILE_EXTENTS,
                    self.multipart_part_bytes,
                )
                .await?,
            );
        }
        self.tail
            .as_mut()
            .expect("file extent run tail is open")
            .write(&mapping)
            .await
    }

    /// Return whether the mappings cover exactly one contiguous logical range.
    pub(crate) fn covers(&self, range: FileRange) -> bool {
        self.first.as_ref().is_some_and(|first| {
            first.logical_range.offset == range.offset
                && self.previous_end == range.end().ok()
                && self.covered_bytes == range.length
        })
    }

    /// Finish the non-empty run.
    pub(crate) async fn close(self) -> Result<Option<ExtentRunRef>, Error> {
        let Some(first) = self.first else {
            return Ok(None);
        };
        let end = self
            .previous_end
            .expect("a file extent run with a first item has an end");
        let span = FileRange::new(first.logical_range.offset, end - first.logical_range.offset)?;
        let continuation = match self.tail {
            Some(tail) => Some(tail.close().await?),
            None => None,
        };
        Ok(Some(ExtentRunRef {
            span,
            inline_extent: first,
            continuation,
        }))
    }

    pub(super) async fn abort(self) {
        if let Some(tail) = self.tail {
            tail.abort().await;
        }
    }
}

impl FileExtentReader {
    /// Read the next canonical logical mapping.
    pub(crate) async fn next(&mut self) -> Result<Option<ExtentMapping>, Error> {
        loop {
            let next = self.next_raw().await?;
            match (self.pending.take(), next) {
                (None, None) => return Ok(None),
                (Some(pending), None) => return Ok(Some(pending)),
                (None, Some(next)) => self.pending = Some(next),
                (Some(pending), Some(next)) => match coalesce_file_extents(pending, next)? {
                    Ok(combined) => self.pending = Some(combined),
                    Err((pending, next)) => {
                        self.pending = Some(next);
                        return Ok(Some(pending));
                    }
                },
            }
        }
    }

    async fn next_raw(&mut self) -> Result<Option<ExtentMapping>, Error> {
        loop {
            let Some(position) = self.position.or_else(|| {
                self.runs
                    .iter()
                    .filter_map(|run| {
                        run.head
                            .as_ref()
                            .map(|mapping| mapping.logical_range.offset)
                    })
                    .min()
            }) else {
                return Ok(None);
            };
            self.position = Some(position);
            for run in &mut self.runs {
                while run
                    .head
                    .as_ref()
                    .is_some_and(|mapping| mapping.end().ok().is_some_and(|end| end <= position))
                {
                    run.head = run.next().await?;
                }
            }
            let Some((priority, selected)) =
                self.runs.iter().enumerate().find_map(|(priority, run)| {
                    run.head.as_ref().and_then(|mapping| {
                        (mapping.logical_range.offset <= position
                            && mapping.end().ok().is_some_and(|end| position < end))
                        .then_some((priority, mapping))
                    })
                })
            else {
                self.position = self
                    .runs
                    .iter()
                    .filter_map(|run| {
                        run.head
                            .as_ref()
                            .map(|mapping| mapping.logical_range.offset)
                    })
                    .filter(|offset| *offset > position)
                    .min();
                continue;
            };
            let mut end = selected.end()?;
            for newer in &self.runs[..priority] {
                if let Some(mapping) = &newer.head
                    && mapping.logical_range.offset > position
                {
                    end = end.min(mapping.logical_range.offset);
                }
            }
            let selected = selected.slice(FileRange::new(position, end - position)?)?;
            self.position = Some(end);
            return Ok(Some(selected));
        }
    }
}

/// Open one run's ordered extent records without applying other levels.
pub(crate) async fn open_extent_run(
    reference: &ExtentRunRef,
    operator: &Operator,
) -> Result<FileExtentRunReader, Error> {
    let tail = match reference.continuation {
        Some(tail) => Some(RecordStreamReader::open(operator, tail).await?),
        None => None,
    };
    Ok(FileExtentRunReader {
        first: Some(reference.inline_extent.clone()),
        reference: reference.clone(),
        tail,
        previous_end: None,
        head: None,
    })
}

impl FileExtentRunReader {
    /// Read the next mapping in this run.
    pub(crate) async fn next(&mut self) -> Result<Option<ExtentMapping>, Error> {
        let mapping = match self.first.take() {
            Some(first) => Some(first),
            None => match self.tail.as_mut() {
                Some(tail) => tail.next().await?,
                None => None,
            },
        };
        let Some(mapping) = mapping else {
            if self.previous_end != Some(self.reference.span.end()?) {
                return Err(Error::corrupt(
                    "read Managed file extent run",
                    "extent records do not match the run span",
                ));
            }
            return Ok(None);
        };
        mapping.validate()?;
        if self
            .previous_end
            .is_some_and(|previous| mapping.logical_range.offset < previous)
            || mapping.logical_range.offset < self.reference.span.offset
            || mapping.end()? > self.reference.span.end()?
        {
            return Err(Error::corrupt(
                "read Managed file extent run",
                "extent records overlap or fall outside the run span",
            ));
        }
        self.previous_end = Some(mapping.end()?);
        Ok(Some(mapping))
    }
}

fn coalesce_file_extents(
    mut left: ExtentMapping,
    right: ExtentMapping,
) -> Result<Result<ExtentMapping, (ExtentMapping, ExtentMapping)>, Error> {
    let left_extent_end = left
        .extent_offset
        .checked_add(left.logical_range.length)
        .ok_or_else(|| Error::corrupt("read Managed file", "extent offset overflows"))?;
    if left.end()? == right.logical_range.offset
        && left_extent_end == right.extent_offset
        && left.extent == right.extent
    {
        left.logical_range.length = left
            .logical_range
            .length
            .checked_add(right.logical_range.length)
            .ok_or_else(|| Error::corrupt("read Managed file", "file range overflows"))?;
        Ok(Ok(left))
    } else {
        Ok(Err((left, right)))
    }
}

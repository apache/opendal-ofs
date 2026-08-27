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

//! Complete and range-hinted file publication.

use std::num::NonZeroUsize;

use opendal::Operator;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite};

use crate::Error;
use crate::filesystem::ContentRef;
use crate::format::{FileExtentMap, FileRange, GcEpoch};

use super::DataAccess;
use super::codec::ContentHasher;
use super::extent::ExtentRunWriter;
use super::lookup::ContentLookup;
use super::partition::FilePartitioner;
use super::segment::DataSegmentWriter;

pub async fn publish_file<A: DataAccess, K: ContentLookup + ?Sized>(
    access: &A,
    operator: &Operator,
    multipart_part_bytes: NonZeroUsize,
    placement: &mut DataSegmentWriter<'_>,
    source: &mut (dyn AsyncRead + Send + Unpin),
    logical_bytes: u64,
    known: &K,
) -> Result<(ContentRef, FileExtentMap, bool), Error> {
    let mut run = ExtentRunWriter::new(operator, placement.gc_epoch(), multipart_part_bytes);
    let content = access
        .partitioner()
        .write_run(
            access.codec(),
            placement,
            source,
            known,
            0,
            logical_bytes,
            &mut run,
        )
        .await?;
    if let Some(reference) = known.file(content)? {
        run.abort().await;
        return Ok((content, reference, true));
    }
    let complete = content.length() != 0 && run.covers(FileRange::new(0, content.length())?);
    let reference = match run.close().await? {
        Some(run) if complete => FileExtentMap::from_base(run),
        None if content.length() == 0 => FileExtentMap::empty(),
        _ => {
            return Err(Error::corrupt(
                "publish Managed file",
                "partitioned extents do not cover the source stream",
            ));
        }
    };
    Ok((content, reference, false))
}

pub async fn publish_file_patch<A: DataAccess, K: ContentLookup + ?Sized>(
    access: &A,
    operator: &Operator,
    multipart_part_bytes: NonZeroUsize,
    placement: &mut DataSegmentWriter<'_>,
    source: &mut (dyn AsyncRead + Send + Unpin),
    file_length: u64,
    previous: (ContentRef, FileExtentMap),
    ranges: &[FileRange],
    known: &K,
) -> Result<(ContentRef, FileExtentMap, bool), Error> {
    if previous.0.length() > file_length {
        return Err(Error::invalid(
            "publish Managed file changes",
            "trusted range publication does not support truncation",
        ));
    }
    let mut content = ContentHasher::default();
    let mut source = tokio_util::io::InspectReader::new(source, |bytes| content.observe(bytes));
    let mut run = ExtentRunWriter::new(operator, placement.gc_epoch(), multipart_part_bytes);
    let mut offset = 0_u64;
    for range in ranges {
        let end = range.end()?;
        if range.offset < offset || end > file_length {
            return Err(Error::invalid(
                "publish Managed file changes",
                "trusted ranges overlap, are out of order, or exceed the file",
            ));
        }
        copy_exact(
            &mut source,
            range.offset - offset,
            &mut tokio::io::sink(),
            "scan unchanged Managed file range",
        )
        .await?;
        let mut changed = (&mut source).take(range.length);
        let content = access
            .partitioner()
            .write_run(
                access.codec(),
                placement,
                &mut changed,
                known,
                range.offset,
                range.length,
                &mut run,
            )
            .await?;
        if content.length() != range.length {
            return Err(Error::conflict(
                "publish Managed file changes",
                "local file changed while publishing a trusted range",
            ));
        }
        offset = end;
    }
    copy_exact(
        &mut source,
        file_length - offset,
        &mut tokio::io::sink(),
        "scan unchanged Managed file range",
    )
    .await?;
    let mut extra = [0_u8; 1];
    if source
        .read(&mut extra)
        .await
        .map_err(|error| Error::io("finish Managed file scan", error))?
        != 0
    {
        return Err(Error::conflict(
            "publish Managed file changes",
            "local file grew while it was being published",
        ));
    }
    let content = content.complete_content().ok_or_else(|| {
        Error::conflict(
            "publish Managed file changes",
            "local file was not read completely",
        )
    })?;
    if content.length() != file_length {
        return Err(Error::conflict(
            "publish Managed file changes",
            "local file length changed while it was being published",
        ));
    }
    if let Some(data) = known.file(content)? {
        run.abort().await;
        return Ok((content, data, true));
    }
    if content == previous.0 {
        run.abort().await;
        return Ok((content, previous.1, true));
    }
    let replaces_file = ranges.len() == 1
        && ranges[0].offset == 0
        && ranges[0].length == file_length
        && run.covers(ranges[0]);
    let patch = run.close().await?.ok_or_else(|| {
        Error::invalid(
            "publish Managed file changes",
            "changed content has no trusted changed range",
        )
    })?;
    let data = if replaces_file {
        FileExtentMap::from_base(patch)
    } else {
        super::extent::add_extent_run(
            previous.1,
            operator,
            placement.gc_epoch(),
            multipart_part_bytes,
            patch,
        )
        .await?
    };
    Ok((content, data, false))
}

async fn copy_exact(
    source: &mut (impl AsyncRead + Unpin),
    length: u64,
    destination: &mut (impl AsyncWrite + Unpin),
    operation: &'static str,
) -> Result<(), Error> {
    let copied = tokio::io::copy(&mut source.take(length), destination)
        .await
        .map_err(|error| Error::io(operation, error))?;
    if copied != length {
        return Err(Error::conflict(
            operation,
            "local file changed while it was being published",
        ));
    }
    Ok(())
}

pub fn data_segments<'a>(
    operator: &'a Operator,
    gc_epoch: GcEpoch,
    target_bytes: u64,
    multipart_part_bytes: NonZeroUsize,
    stored_payload_bound: Option<u64>,
) -> DataSegmentWriter<'a> {
    let multipart_part_bytes = stored_payload_bound
        .and_then(|bytes| bytes.checked_add(crate::format::STREAM_TAIL_BYTES as u64))
        .and_then(|bytes| usize::try_from(bytes).ok())
        .and_then(NonZeroUsize::new)
        .map_or(multipart_part_bytes, |bytes| {
            bytes.max(multipart_part_bytes)
        });
    DataSegmentWriter::new(operator, gc_epoch, target_bytes, multipart_part_bytes)
}

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

//! Unique shared data-segment writer.

use std::num::NonZeroUsize;

use opendal::Operator;
use tokio::io::AsyncRead;

use crate::Error;
use crate::filesystem::ContentRef;
use crate::format::{GcEpoch, ObjectClass, ObjectLocator, StreamKind};
use crate::storage::{ImmutableWriter, finish_stream};

/// Streaming writer for one immutable data segment.
struct Writer {
    writer: ImmutableWriter,
    payload_length: u64,
}

/// Lazy sequence of immutable data segments rotated by payload target.
pub struct DataSegmentWriter<'a> {
    operator: &'a Operator,
    gc_epoch: GcEpoch,
    target_bytes: u64,
    multipart_part_bytes: NonZeroUsize,
    current: Option<Writer>,
}

impl<'a> DataSegmentWriter<'a> {
    /// Create a segment placement session without opening a remote writer.
    ///
    /// `target_bytes` rotates shared segments and does not limit logical file
    /// size.
    pub const fn new(
        operator: &'a Operator,
        gc_epoch: GcEpoch,
        target_bytes: u64,
        multipart_part_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            operator,
            gc_epoch,
            target_bytes,
            multipart_part_bytes,
            current: None,
        }
    }

    /// Append one independently verifiable stored byte stream.
    pub async fn append(
        &mut self,
        source: &mut (impl AsyncRead + Unpin + ?Sized),
    ) -> Result<(ObjectLocator, u64, ContentRef), Error> {
        let segment = self.current_writer().await?;
        let locator = segment.locator();
        let (offset, content) = segment.append(source).await?;
        Ok((locator, offset, content))
    }

    async fn current_writer(&mut self) -> Result<&mut Writer, Error> {
        if self
            .current
            .as_ref()
            .is_some_and(|segment| segment.payload_length() >= self.target_bytes)
        {
            self.current
                .take()
                .expect("current data segment exists")
                .close()
                .await?;
        }
        if self.current.is_none() {
            self.current =
                Some(Writer::open(self.operator, self.gc_epoch, self.multipart_part_bytes).await?);
        }
        Ok(self.current.as_mut().expect("data segment is open"))
    }

    pub const fn gc_epoch(&self) -> GcEpoch {
        self.gc_epoch
    }

    /// Finish the current segment, if any.
    pub async fn finish(&mut self) -> Result<(), Error> {
        if let Some(segment) = self.current.take() {
            segment.close().await?;
        }
        Ok(())
    }

    /// Abort the current incomplete segment, if any.
    pub async fn abort(&mut self) {
        if let Some(mut segment) = self.current.take() {
            let _ = segment.abort().await;
        }
    }
}

impl Writer {
    async fn open(
        operator: &Operator,
        gc_epoch: GcEpoch,
        multipart_part_bytes: NonZeroUsize,
    ) -> Result<Self, Error> {
        Ok(Self {
            writer: ImmutableWriter::open(
                operator,
                gc_epoch,
                ObjectClass::DataSegment,
                multipart_part_bytes,
            )
            .await?,
            payload_length: 0,
        })
    }

    async fn abort(&mut self) -> Result<(), Error> {
        self.writer.abort().await
    }

    async fn append(
        &mut self,
        source: &mut (impl AsyncRead + Unpin + ?Sized),
    ) -> Result<(u64, ContentRef), Error> {
        let offset = self.payload_length;
        let (length, digest) = self.writer.write_source(source).await?;
        self.payload_length = self
            .payload_length
            .checked_add(length)
            .ok_or_else(|| Error::invalid("write Managed data segment", "length overflows"))?;
        Ok((offset, ContentRef::new(digest, length)))
    }

    fn locator(&self) -> ObjectLocator {
        self.writer.locator()
    }

    const fn payload_length(&self) -> u64 {
        self.payload_length
    }

    async fn close(self) -> Result<crate::format::StreamRef, Error> {
        let digest = self.writer.digest();
        finish_stream(
            self.writer,
            StreamKind::DATA_SEGMENT,
            self.payload_length,
            digest,
        )
        .await
    }
}

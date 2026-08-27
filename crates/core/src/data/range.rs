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

//! Verified byte-range reads from immutable data segments.

use std::ops::Range;

use futures::StreamExt as _;
use opendal::{Buffer, BufferStream, Operator};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

use crate::Error;
use crate::filesystem::{ContentRef, Digest};
use crate::format::{ObjectClass, ObjectLocator};

/// One byte-range stream from a data segment.
pub struct RangeReader {
    stream: Option<BufferStream>,
    pending: Buffer,
    remaining: u64,
}

impl RangeReader {
    /// Fetch exact ranges from one segment through OpenDAL's range planner.
    pub async fn fetch(
        operator: &Operator,
        locator: ObjectLocator,
        ranges: Vec<Range<u64>>,
        gap_bytes: usize,
    ) -> Result<Vec<Self>, Error> {
        if locator.class != ObjectClass::DataSegment
            || ranges.iter().any(|range| range.start > range.end)
        {
            return Err(Error::corrupt(
                "fetch Managed data segment",
                "data segment range reference is invalid",
            ));
        }
        let expected_lengths = ranges
            .iter()
            .map(|range| range.end - range.start)
            .collect::<Vec<_>>();
        let reader = operator
            .reader_with(&locator.key())
            .gap(gap_bytes)
            .await
            .map_err(|error| Error::from_storage("open Managed data segment fetch", error))?;
        let fetched = reader
            .fetch(ranges)
            .await
            .map_err(|error| Error::from_storage("fetch Managed data segment ranges", error))?;
        if fetched.len() != expected_lengths.len() {
            return Err(Error::corrupt(
                "fetch Managed data segment ranges",
                "storage returned a different number of ranges",
            ));
        }
        fetched
            .into_iter()
            .zip(expected_lengths)
            .map(|(pending, expected)| {
                if pending.len() as u64 != expected {
                    return Err(Error::corrupt(
                        "fetch Managed data segment ranges",
                        "storage returned a truncated range",
                    ));
                }
                Ok(Self {
                    stream: None,
                    remaining: expected,
                    pending,
                })
            })
            .collect()
    }

    /// Open a streaming byte range from one immutable data segment.
    pub async fn open(
        operator: &Operator,
        locator: ObjectLocator,
        range: Range<u64>,
    ) -> Result<Self, Error> {
        if locator.class != ObjectClass::DataSegment || range.start > range.end {
            return Err(Error::corrupt(
                "read Managed data segment",
                "data segment range reference is invalid",
            ));
        }
        let remaining = range.end - range.start;
        let stream = operator
            .reader(&locator.key())
            .await
            .map_err(|error| Error::from_storage("open Managed data segment", error))?
            .into_stream(range)
            .await
            .map_err(|error| Error::from_storage("read Managed data segment", error))?;
        Ok(Self {
            stream: Some(stream),
            pending: Buffer::new(),
            remaining,
        })
    }

    async fn refill(&mut self) -> Result<(), Error> {
        if !self.pending.is_empty() {
            return Ok(());
        }
        let Some(stream) = self.stream.as_mut() else {
            return Err(Error::corrupt(
                "read Managed data segment",
                "fetched data segment range is truncated",
            ));
        };
        self.pending = stream
            .next()
            .await
            .ok_or_else(|| {
                Error::corrupt(
                    "read Managed data segment",
                    "data segment range is truncated",
                )
            })?
            .map_err(|error| Error::from_storage("read Managed data segment", error))?;
        Ok(())
    }

    /// Consume and verify one complete stored range.
    pub async fn copy_file(
        &mut self,
        content: ContentRef,
        destination: &mut (impl AsyncWrite + Unpin + ?Sized),
    ) -> Result<(), Error> {
        let expected = content.length();
        let mut hasher = blake3::Hasher::new();
        self.copy_exact(expected, destination, Some(&mut hasher))
            .await?;
        if Digest::from_bytes(hasher.finalize().into()) != content.digest() {
            return Err(Error::corrupt(
                "read Managed data segment",
                "stored range does not match its content reference",
            ));
        }
        Ok(())
    }

    pub(crate) async fn copy_bytes(
        &mut self,
        length: u64,
        destination: &mut (impl AsyncWrite + Unpin + ?Sized),
    ) -> Result<(), Error> {
        self.copy_exact(length, destination, None).await
    }

    async fn copy_exact(
        &mut self,
        length: u64,
        destination: &mut (impl AsyncWrite + Unpin + ?Sized),
        mut hasher: Option<&mut blake3::Hasher>,
    ) -> Result<(), Error> {
        if length > self.remaining {
            return Err(Error::corrupt(
                "read Managed data segment",
                "stored range exceeds the requested range",
            ));
        }
        let mut remaining = length;
        while remaining != 0 {
            if self.pending.is_empty() {
                self.refill().await?;
            }
            let take = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(self.pending.len());
            let bytes = self.pending.slice(..take);
            self.pending = self.pending.slice(take..);
            for chunk in bytes {
                if let Some(hasher) = hasher.as_deref_mut() {
                    hasher.update(&chunk);
                }
                destination
                    .write_all(&chunk)
                    .await
                    .map_err(|error| Error::io("write Managed data segment destination", error))?;
            }
            remaining -= take as u64;
            self.remaining -= take as u64;
        }
        Ok(())
    }
}

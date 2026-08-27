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

//! Extent encoding. Identity is base; Zstandard implements this port.

use std::fmt;
use std::future::Future;
use std::ops::Range;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::Error;
use crate::filesystem::ContentRef;
use crate::format::{ExtensionDescriptor, ExtentRef, ObjectClass, SegmentRangeRef};

use super::range::RangeReader;
use super::segment::DataSegmentWriter;

/// Incremental content identity used with Tokio's stream inspection adapters.
#[derive(Default)]
pub struct ContentHasher {
    hasher: blake3::Hasher,
    length: u64,
    complete: bool,
}

impl ContentHasher {
    /// Observe bytes read from a source, including the empty EOF observation.
    pub fn observe(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            self.complete = true;
            return;
        }
        self.hasher.update(bytes);
        self.length = self
            .length
            .checked_add(bytes.len() as u64)
            .expect("one byte stream cannot exceed the u64 format range");
    }

    /// Return the identity of all bytes observed so far.
    pub fn content(&self) -> ContentRef {
        ContentRef::new(
            crate::filesystem::Digest::from_bytes(self.hasher.finalize().into()),
            self.length,
        )
    }

    /// Return the content identity after the source has reached EOF.
    pub fn complete_content(&self) -> Option<ContentRef> {
        self.complete.then(|| self.content())
    }
}

/// Physical-to-logical extent encoding.
pub trait ExtentCodec: Send + Sync + Clone + fmt::Debug + Unpin + 'static {
    fn descriptor(&self) -> Option<&ExtensionDescriptor>;

    fn decoding_count(&self) -> usize;

    fn stored_size_bound(&self, logical_bytes: u64) -> Option<u64>;

    fn stored_range(&self, reference: &ExtentRef, range: Range<u64>) -> Result<Range<u64>, Error>;

    fn validate(&self, reference: &ExtentRef) -> Result<(), Error> {
        if reference.stored_range.segment.class != ObjectClass::DataSegment
            || reference.stored_range.stored_content.length() == 0
            || reference.content().length() == 0
            || reference.decoding_outputs.len() != self.decoding_count()
        {
            return Err(Error::corrupt(
                "read Managed file",
                "file extent does not match the access chain",
            ));
        }
        reference
            .stored_range
            .offset
            .checked_add(reference.stored_range.stored_content.length())
            .ok_or_else(|| Error::corrupt("read Managed file", "stored extent range overflows"))?;
        Ok(())
    }

    fn encode<'a>(
        &'a self,
        placement: &'a mut DataSegmentWriter<'_>,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
    ) -> impl Future<Output = Result<ExtentRef, Error>> + Send + 'a;

    fn decode<'a>(
        &'a self,
        source: &'a mut RangeReader,
        reference: ExtentRef,
        range: Range<u64>,
        destination: &'a mut (dyn AsyncWrite + Send + Unpin),
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;
}

/// Identity extent encoding backed by the common self-delimiting stream format.
#[derive(Clone, Copy, Debug, Default)]
pub struct IdentityCodec;

impl ExtentCodec for IdentityCodec {
    fn descriptor(&self) -> Option<&ExtensionDescriptor> {
        None
    }

    fn decoding_count(&self) -> usize {
        0
    }

    fn stored_size_bound(&self, logical_bytes: u64) -> Option<u64> {
        Some(logical_bytes)
    }

    fn stored_range(&self, reference: &ExtentRef, range: Range<u64>) -> Result<Range<u64>, Error> {
        identity_range(reference, range)
    }

    async fn encode(
        &self,
        placement: &mut DataSegmentWriter<'_>,
        source: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<ExtentRef, Error> {
        let (locator, offset, stored) = placement.append(source).await?;
        Ok(ExtentRef {
            stored_range: SegmentRangeRef {
                segment: locator,
                offset,
                stored_content: stored,
            },
            decoding_outputs: Vec::new(),
        })
    }

    async fn decode(
        &self,
        source: &mut RangeReader,
        reference: ExtentRef,
        range: Range<u64>,
        destination: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), Error> {
        let range = identity_range(&reference, range)?;
        if range.is_empty() {
            return Ok(());
        }
        if range.start == 0 && range.end == reference.stored_range.stored_content.length() {
            source
                .copy_file(reference.stored_range.stored_content, destination)
                .await
        } else {
            source
                .copy_bytes(range.end - range.start, destination)
                .await
        }
    }
}

fn identity_range(reference: &ExtentRef, range: Range<u64>) -> Result<Range<u64>, Error> {
    if !reference.decoding_outputs.is_empty()
        || reference.stored_range.segment.class != ObjectClass::DataSegment
        || range.start > range.end
        || range.end > reference.stored_range.stored_content.length()
    {
        return Err(Error::corrupt(
            "read Managed extent",
            "extent reference does not use identity encoding",
        ));
    }
    Ok(range)
}

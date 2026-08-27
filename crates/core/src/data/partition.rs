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

//! File partitioning. Whole is base; FastCDC implements this port.

use std::fmt;
use std::future::Future;

use tokio::io::AsyncRead;

use crate::Error;
use crate::filesystem::ContentRef;
use crate::format::{ExtensionDescriptor, ExtentMapping};

use super::codec::ExtentCodec;
use super::extent::ExtentRunWriter;
use super::lookup::ContentLookup;
use super::segment::DataSegmentWriter;

/// Logical file partitioning into independently encoded extents.
pub trait FilePartitioner: Send + Sync + Clone + fmt::Debug + Unpin + 'static {
    fn descriptor(&self) -> Option<&ExtensionDescriptor>;

    /// Finite maximum logical extent, when the partitioner can provide one.
    fn maximum_extent_bytes(&self) -> Option<u64>;

    fn write_run<'a, C: ExtentCodec, K: ContentLookup + ?Sized>(
        &'a self,
        codec: &'a C,
        placement: &'a mut DataSegmentWriter<'_>,
        source: &'a mut (dyn AsyncRead + Send + Unpin),
        known: &'a K,
        file_offset: u64,
        logical_bytes: u64,
        run: &'a mut ExtentRunWriter<'_>,
    ) -> impl Future<Output = Result<ContentRef, Error>> + Send + 'a;
}

/// Whole-file partitioner: one extent covers the entire logical write.
#[derive(Clone, Copy, Debug, Default)]
pub struct WholePartitioner;

impl FilePartitioner for WholePartitioner {
    fn descriptor(&self) -> Option<&ExtensionDescriptor> {
        None
    }

    fn maximum_extent_bytes(&self) -> Option<u64> {
        None
    }

    async fn write_run<C: ExtentCodec, K: ContentLookup + ?Sized>(
        &self,
        codec: &C,
        placement: &mut DataSegmentWriter<'_>,
        source: &mut (dyn AsyncRead + Send + Unpin),
        _known: &K,
        file_offset: u64,
        _logical_bytes: u64,
        run: &mut ExtentRunWriter<'_>,
    ) -> Result<ContentRef, Error> {
        let extent = codec.encode(placement, source).await?;
        let content = extent.content();
        if content.length() != 0 {
            run.write(ExtentMapping::complete(file_offset, extent)?)
                .await?;
        }
        Ok(content)
    }
}

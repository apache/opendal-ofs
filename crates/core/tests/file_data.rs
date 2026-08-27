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

use std::num::NonZeroUsize;

use ofs_core::Result;
use ofs_core::data::{
    ContentLookup, CoreDataAccess, DataSegmentWriter, publish_file, restore_file,
};
use ofs_core::filesystem::ContentRef;
use ofs_core::format::{ExtentRef, FileExtentMap, GcEpoch};
use opendal::Operator;
use opendal::services::Memory;

struct NoReachableContent;

impl ContentLookup for NoReachableContent {
    fn file(&self, _content: ContentRef) -> Result<Option<FileExtentMap>> {
        Ok(None)
    }

    fn extent(&self, _content: ContentRef) -> Result<Option<ExtentRef>> {
        Ok(None)
    }
}

fn memory_operator() -> Operator {
    Operator::new(Memory::default())
        .expect("memory service configuration is valid")
        .finish()
}

#[tokio::test]
async fn publishes_and_restores_whole_identity_files() -> Result<()> {
    let operator = memory_operator();
    let access = CoreDataAccess::default();
    let expected = b"one canonical file-data pipeline";
    let mut source = expected.as_slice();
    let part_bytes = NonZeroUsize::new(1024).unwrap();
    let mut segments = DataSegmentWriter::new(&operator, GcEpoch::ZERO, 1024, part_bytes);

    let (content, reference, reused) = publish_file(
        &access,
        &operator,
        part_bytes,
        &mut segments,
        &mut source,
        expected.len() as u64,
        &NoReachableContent,
    )
    .await?;
    segments.finish().await?;
    assert!(!reused);

    let mut actual = Vec::new();
    restore_file(
        &access,
        &operator,
        0,
        1024,
        reference,
        content,
        0..content.length(),
        None,
        &mut actual,
    )
    .await?;
    assert_eq!(actual, expected);
    Ok(())
}

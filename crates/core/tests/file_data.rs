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
    ContentLookup, CoreDataAccess, DataSegmentWriter, ReusableFile, publish_file, restore_file,
};
use ofs_core::filesystem::{ContentRef, Digest};
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

#[tokio::test]
async fn rejects_a_full_restore_with_the_wrong_file_identity() -> Result<()> {
    let operator = memory_operator();
    let access = CoreDataAccess::default();
    let expected = b"extent bytes with a mismatched file identity";
    let mut source = expected.as_slice();
    let part_bytes = NonZeroUsize::new(1024).unwrap();
    let mut segments = DataSegmentWriter::new(&operator, GcEpoch::ZERO, 1024, part_bytes);

    let (content, mut reference, _) = publish_file(
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

    reference.patch_levels.push(reference.base_run.clone());
    let mismatched = ContentRef::new(Digest::from_bytes([0; 32]), content.length());
    let mut actual = Vec::new();
    let error = restore_file(
        &access,
        &operator,
        0,
        1024,
        reference,
        mismatched,
        0..mismatched.length(),
        None,
        &mut actual,
    )
    .await
    .expect_err("a complete restore must verify the file content reference");

    assert_eq!(error.kind(), ofs_core::ErrorKind::Corrupt);
    assert_eq!(actual, expected);
    Ok(())
}

#[tokio::test]
async fn verifies_an_empty_file_identity() {
    let operator = memory_operator();
    let access = CoreDataAccess::default();
    let content = ContentRef::new(Digest::from_bytes([0; 32]), 0);
    let mut actual = Vec::new();

    let error = restore_file(
        &access,
        &operator,
        0,
        1024,
        FileExtentMap::empty(),
        content,
        0..0,
        None,
        &mut actual,
    )
    .await
    .expect_err("an empty restore must verify the empty content digest");

    assert_eq!(error.kind(), ofs_core::ErrorKind::Corrupt);
    assert!(actual.is_empty());
}

#[tokio::test]
async fn verifies_reused_bytes_during_a_full_restore() -> Result<()> {
    let operator = memory_operator();
    let access = CoreDataAccess::default();
    let expected = b"remote content whose placement is reusable";
    let mut source = expected.as_slice();
    let part_bytes = NonZeroUsize::new(1024).unwrap();
    let mut segments = DataSegmentWriter::new(&operator, GcEpoch::ZERO, 1024, part_bytes);

    let (content, reference, _) = publish_file(
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

    let mut stale = std::io::Cursor::new(vec![b'x'; expected.len()]);
    let mut actual = Vec::new();
    let error = restore_file(
        &access,
        &operator,
        0,
        1024,
        reference.clone(),
        content,
        0..content.length(),
        Some(ReusableFile {
            reference,
            source: &mut stale,
        }),
        &mut actual,
    )
    .await
    .expect_err("a complete restore must verify bytes copied from a reusable file");

    assert_eq!(error.kind(), ofs_core::ErrorKind::Corrupt);
    assert_eq!(actual, vec![b'x'; expected.len()]);
    Ok(())
}

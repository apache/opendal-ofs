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

use ofs_core::data::{DataSegmentWriter, RangeReader};
use ofs_core::format::GcEpoch;
use opendal::Operator;
use opendal::services::Memory;

fn memory_operator() -> Operator {
    Operator::new(Memory::default())
        .expect("memory service configuration is valid")
        .finish()
}

#[tokio::test]
async fn round_trips_verified_segment_ranges() {
    let operator = memory_operator();
    let expected = b"streamed Managed data";
    let mut source = expected.as_slice();
    let mut segments = DataSegmentWriter::new(
        &operator,
        GcEpoch::ZERO,
        1024,
        NonZeroUsize::new(1024).unwrap(),
    );
    let (locator, offset, content) = segments.append(&mut source).await.unwrap();
    segments.finish().await.unwrap();

    let mut reader = RangeReader::open(&operator, locator, offset..offset + content.length())
        .await
        .unwrap();
    let mut actual = Vec::new();
    reader.copy_file(content, &mut actual).await.unwrap();

    assert_eq!(actual, expected);
}

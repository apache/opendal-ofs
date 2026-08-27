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

use ofs_core::format::{GcEpoch, ObjectClass, RecordCodec, StreamKind};
use ofs_core::storage::{ControlRecord, RecordStreamReader, RecordStreamWriter};
use opendal::Operator;
use opendal::services::Memory;

fn memory_operator() -> Operator {
    Operator::new(Memory::default())
        .expect("memory service configuration is valid")
        .finish()
}

#[tokio::test]
async fn conditionally_publishes_control_records() {
    const RECORD: ControlRecord<String> =
        ControlRecord::new("managed/0/test/head", RecordCodec::new(*b"OFSTEST0", 1024));
    let operator = memory_operator();

    assert!(
        RECORD
            .write(&operator, &"first".to_owned(), None)
            .await
            .unwrap()
    );
    assert!(
        !RECORD
            .write(&operator, &"stale".to_owned(), None)
            .await
            .unwrap()
    );

    let observed = RECORD.read(&operator).await.unwrap().unwrap();
    assert_eq!(observed.value, "first");
}

#[tokio::test]
async fn streams_records_through_immutable_objects() {
    let operator = memory_operator();
    let mut writer = RecordStreamWriter::open(
        &operator,
        GcEpoch::ZERO,
        ObjectClass::NamespaceSegment,
        StreamKind::NAMESPACE_SNAPSHOT,
        NonZeroUsize::new(1024).unwrap(),
    )
    .await
    .unwrap();
    writer.write(&"alpha").await.unwrap();
    writer.write(&"beta").await.unwrap();
    let reference = writer.close().await.unwrap();

    let mut reader = RecordStreamReader::<String>::open(&operator, reference)
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().as_deref(), Some("alpha"));
    assert_eq!(reader.next().await.unwrap().as_deref(), Some("beta"));
    assert_eq!(reader.next().await.unwrap(), None);
}

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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ofs_core::ErrorKind;
use ofs_core::format::{GcEpoch, ObjectClass, RecordCodec, StreamKind};
use ofs_core::storage::{ControlRecord, RecordStreamReader, RecordStreamWriter};
use opendal::raw::*;
use opendal::services::Memory;
use opendal::{EntryMode, Metadata, Operator};

#[derive(Clone, Debug)]
struct MismatchedStatLayer {
    armed: Arc<AtomicBool>,
    body_reads: Arc<AtomicUsize>,
    stat_calls: Arc<AtomicUsize>,
    suppress_read_metadata: bool,
}

#[derive(Debug)]
struct MismatchedStatAccess<A> {
    inner: A,
    armed: Arc<AtomicBool>,
    body_reads: Arc<AtomicUsize>,
    stat_calls: Arc<AtomicUsize>,
    suppress_read_metadata: bool,
}

#[derive(Debug)]
struct ReadProbe<R> {
    inner: R,
    body_reads: Arc<AtomicUsize>,
}

impl<R: oio::Read> oio::Read for ReadProbe<R> {
    async fn read(&mut self) -> opendal::Result<opendal::Buffer> {
        self.body_reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read().await
    }
}

impl<A: Access> Layer<A> for MismatchedStatLayer {
    type LayeredAccess = MismatchedStatAccess<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        MismatchedStatAccess {
            inner,
            armed: self.armed.clone(),
            body_reads: self.body_reads.clone(),
            stat_calls: self.stat_calls.clone(),
            suppress_read_metadata: self.suppress_read_metadata,
        }
    }
}

impl<A: Access> LayeredAccess for MismatchedStatAccess<A> {
    type Inner = A;
    type Reader = ReadProbe<A::Reader>;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;
    type Copier = A::Copier;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        let (response, reader) = self.inner.read(path, args).await?;
        let response = if self.suppress_read_metadata {
            RpRead::default()
        } else if self.armed.load(Ordering::SeqCst) {
            let metadata = response
                .into_metadata()
                .unwrap_or_else(|| Metadata::new(EntryMode::FILE))
                .with_etag("\"revision-from-read\"".to_owned());
            RpRead::new(metadata)
        } else {
            response
        };
        Ok((
            response,
            ReadProbe {
                inner: reader,
                body_reads: self.body_reads.clone(),
            },
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> opendal::Result<RpStat> {
        self.stat_calls.fetch_add(1, Ordering::SeqCst);
        let response = self.inner.stat(path, args).await?;
        if self.armed.swap(false, Ordering::SeqCst) {
            Ok(response.map_metadata(|metadata| {
                metadata.with_etag("\"revision-from-another-value\"".to_owned())
            }))
        } else {
            Ok(response)
        }
    }

    async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }
}

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

    let observed = RECORD.observe(&operator).await.unwrap().unwrap();
    assert_eq!(observed.value, "first");
}

#[tokio::test]
async fn binds_control_bytes_to_their_conditional_revision() {
    const RECORD: ControlRecord<String> = ControlRecord::new(
        "managed/0/test/bound-head",
        RecordCodec::new(*b"OFSTEST0", 1024),
    );
    let armed = Arc::new(AtomicBool::new(false));
    let body_reads = Arc::new(AtomicUsize::new(0));
    let stat_calls = Arc::new(AtomicUsize::new(0));
    let operator = Operator::new(Memory::default())
        .expect("memory service configuration is valid")
        .layer(MismatchedStatLayer {
            armed: armed.clone(),
            body_reads: body_reads.clone(),
            stat_calls: stat_calls.clone(),
            suppress_read_metadata: false,
        })
        .finish();

    assert!(
        RECORD
            .write(&operator, &"first".to_owned(), None)
            .await
            .unwrap()
    );
    armed.store(true, Ordering::SeqCst);

    let observed = RECORD.observe(&operator).await.unwrap().unwrap();
    assert_eq!(observed.value, "first");
    assert_eq!(observed.revision.as_deref(), Some("\"revision-from-read\""));
    assert_eq!(stat_calls.load(Ordering::SeqCst), 0);
    assert!(body_reads.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn rejects_an_oversized_control_record_before_reading_its_body() {
    const KEY: &str = "managed/0/test/oversized-head";
    const CODEC: RecordCodec = RecordCodec::new(*b"OFSTEST0", 32);
    const RECORD: ControlRecord<String> = ControlRecord::new(KEY, CODEC);
    let armed = Arc::new(AtomicBool::new(false));
    let body_reads = Arc::new(AtomicUsize::new(0));
    let stat_calls = Arc::new(AtomicUsize::new(0));
    let operator = Operator::new(Memory::default())
        .expect("memory service configuration is valid")
        .layer(MismatchedStatLayer {
            armed,
            body_reads: body_reads.clone(),
            stat_calls: stat_calls.clone(),
            suppress_read_metadata: false,
        })
        .finish();
    operator
        .write(KEY, vec![0; CODEC.maximum_encoded_bytes() + 1])
        .await
        .unwrap();

    let error = match RECORD.read(&operator).await {
        Ok(_) => panic!("read metadata must reject an oversized object"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), ErrorKind::Corrupt);
    assert_eq!(stat_calls.load(Ordering::SeqCst), 0);
    assert_eq!(body_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn keeps_the_streaming_size_limit_without_read_metadata() {
    const KEY: &str = "managed/0/test/unreported-oversized-head";
    const CODEC: RecordCodec = RecordCodec::new(*b"OFSTEST0", 32);
    const RECORD: ControlRecord<String> = ControlRecord::new(KEY, CODEC);
    let body_reads = Arc::new(AtomicUsize::new(0));
    let stat_calls = Arc::new(AtomicUsize::new(0));
    let operator = Operator::new(Memory::default())
        .expect("memory service configuration is valid")
        .layer(MismatchedStatLayer {
            armed: Arc::new(AtomicBool::new(false)),
            body_reads: body_reads.clone(),
            stat_calls: stat_calls.clone(),
            suppress_read_metadata: true,
        })
        .finish();
    operator
        .write(KEY, vec![0; CODEC.maximum_encoded_bytes() + 1])
        .await
        .unwrap();

    let error = match RECORD.read(&operator).await {
        Ok(_) => panic!("the streaming limit must reject an oversized object"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), ErrorKind::Corrupt);
    assert_eq!(stat_calls.load(Ordering::SeqCst), 0);
    assert!(body_reads.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn reads_bounded_objects_without_read_metadata() {
    const RECORD: ControlRecord<String> = ControlRecord::new(
        "managed/0/test/unreported-head",
        RecordCodec::new(*b"OFSTEST0", 1024),
    );
    let body_reads = Arc::new(AtomicUsize::new(0));
    let stat_calls = Arc::new(AtomicUsize::new(0));
    let operator = Operator::new(Memory::default())
        .expect("memory service configuration is valid")
        .layer(MismatchedStatLayer {
            armed: Arc::new(AtomicBool::new(false)),
            body_reads: body_reads.clone(),
            stat_calls: stat_calls.clone(),
            suppress_read_metadata: true,
        })
        .finish();
    assert!(
        RECORD
            .write(&operator, &"value".to_owned(), None)
            .await
            .unwrap()
    );

    let observed = RECORD.read(&operator).await.unwrap().unwrap();

    assert_eq!(observed.value, "value");
    assert_eq!(observed.revision, None);
    assert_eq!(stat_calls.load(Ordering::SeqCst), 0);
    assert!(body_reads.load(Ordering::SeqCst) > 0);
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

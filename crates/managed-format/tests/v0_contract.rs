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

use ofs_managed_format::ErrorKind;
use ofs_managed_format::v0::model::{
    ChangeCursor, Digest, NamespaceNode, NamespaceRecord, NamespaceValue, NodeAttributes, NodeId,
    VolumeId,
};
use ofs_managed_format::v0::{
    AUTHORITY_HEAD_RECORD, AuthorityHead, COMMIT_RECORD, FileDataLayout, FileExtentMap, GcEpoch,
    NamespaceCommit, NamespaceRevision, ObjectClass, ObjectId, ObjectLocator, ObjectRef,
    StreamKind, StreamRef, VolumeFormat, encode_record_frame, encode_stream_record,
    encode_stream_tail, validate_record_frame,
};

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(encoded, "{byte:02x}").expect("write to a string");
    }
    encoded
}

fn reference() -> StreamRef {
    StreamRef {
        kind: StreamKind::NAMESPACE_SNAPSHOT,
        object: ObjectRef {
            locator: ObjectLocator {
                gc_epoch: GcEpoch::ZERO,
                class: ObjectClass::NamespaceSegment,
                id: ObjectId::from_bytes([3; 16]),
            },
            encoded_length: 120,
            digest: Digest::from_bytes([4; 32]),
        },
        payload_length: 40,
        payload_digest: Digest::from_bytes([5; 32]),
    }
}

#[test]
fn managed_v0_bytes_are_stable() {
    let volume_id = VolumeId::from_bytes([1; 16]);
    let format = VolumeFormat::new(
        volume_id,
        NodeId::from_bytes([2; 16]),
        FileDataLayout::whole_identity(8 * 1024 * 1024).expect("valid base profile"),
        None,
    );
    let volume = format.encode().expect("encode volume format");

    let snapshot = reference();
    let commit = NamespaceCommit::genesis(volume_id, snapshot);
    let commit = COMMIT_RECORD
        .encode(&commit)
        .expect("encode namespace commit");
    let revision = NamespaceRevision {
        object: ObjectRef {
            locator: ObjectLocator {
                gc_epoch: GcEpoch::ZERO,
                class: ObjectClass::NamespaceCommit,
                id: ObjectId::from_bytes([6; 16]),
            },
            encoded_length: commit.len() as u64,
            digest: Digest::from_bytes(*blake3::hash(&commit).as_bytes()),
        },
        change_cursor: ChangeCursor::GENESIS,
    };
    let head = AUTHORITY_HEAD_RECORD
        .encode(&AuthorityHead {
            current_commit: revision,
            gc_epoch: GcEpoch::ZERO,
            minimum_retained_cursor: ChangeCursor::GENESIS,
        })
        .expect("encode authority head");

    let root = NamespaceRecord {
        path: String::new(),
        value: Some(NamespaceNode {
            node_id: NodeId::from_bytes([2; 16]),
            generation: 0,
            attributes: NodeAttributes::default(),
            value: NamespaceValue::<FileExtentMap>::Directory { generation: 0 },
        }),
    };
    let encoded_root = encode_stream_record(&root).expect("encode root record");
    let frame = encode_record_frame(1, &encoded_root).expect("encode record frame");
    let tail = encode_stream_tail(
        StreamKind::NAMESPACE_SNAPSHOT,
        frame.len() as u64,
        Digest::from_bytes(*blake3::hash(&frame).as_bytes()),
    )
    .expect("encode stream tail");

    insta::assert_snapshot!("volume-format", hex(&volume));
    insta::assert_snapshot!("namespace-commit", hex(&commit));
    insta::assert_snapshot!("authority-head", hex(&head));
    insta::assert_snapshot!("namespace-frame", hex(&frame));
    insta::assert_snapshot!("namespace-tail", hex(&tail));

    assert_eq!(validate_record_frame(&frame).expect("validate frame"), 1);
    let decoded = VolumeFormat::decode(&volume).expect("decode volume format");
    assert_eq!(decoded, format);

    let mut damaged = frame;
    let last = damaged.last_mut().expect("non-empty frame");
    *last ^= 1;
    assert_eq!(
        validate_record_frame(&damaged)
            .expect_err("reject damaged frame")
            .kind(),
        ErrorKind::Corrupt
    );
}

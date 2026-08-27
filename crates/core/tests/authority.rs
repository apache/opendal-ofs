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

use ofs_core::ErrorKind;
use ofs_core::authority::{AuthorityAccess, DefaultAuthorityAccess};
use ofs_core::filesystem::{ChangeCursor, Digest};
use ofs_core::format::{
    AuthorityHead, GcEpoch, NamespaceRevision, ObjectClass, ObjectId, ObjectLocator, ObjectRef,
};
use opendal::Operator;
use opendal::services::Memory;

fn memory_operator() -> Operator {
    Operator::new(Memory::default())
        .expect("memory service configuration is valid")
        .finish()
}

fn genesis_head() -> AuthorityHead {
    AuthorityHead {
        current_commit: NamespaceRevision {
            object: ObjectRef {
                locator: ObjectLocator {
                    gc_epoch: GcEpoch::ZERO,
                    class: ObjectClass::NamespaceCommit,
                    id: ObjectId::from_bytes([1; 16]),
                },
                encoded_length: 1,
                digest: Digest::from_bytes([2; 32]),
            },
            change_cursor: ChangeCursor::GENESIS,
        },
        gc_epoch: GcEpoch::ZERO,
        minimum_retained_cursor: ChangeCursor::GENESIS,
    }
}

#[tokio::test]
async fn core_authority_owns_only_main() {
    let operator = memory_operator();
    let access = DefaultAuthorityAccess;
    let expected = genesis_head();
    access
        .initialize(&operator, NonZeroUsize::new(1024).unwrap(), expected)
        .await
        .unwrap();

    assert_eq!(
        access.observe(&operator, "main").await.unwrap().head,
        expected
    );
    assert_eq!(
        access
            .observe(&operator, "feature")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}

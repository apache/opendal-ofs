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

//! Managed v0 authority root record.

use crate::v0::model::ChangeCursor;

use super::{GcEpoch, NamespaceRevision, RecordCodec};

/// Sole v0 authority control-object key.
pub const AUTHORITY_HEAD_KEY: &str = "managed/0/head";
/// Bounded v0 authority head envelope.
pub const AUTHORITY_HEAD_RECORD: RecordCodec = RecordCodec::new(*b"OFSHED00", 64 * 1024);

/// Current namespace and reclamation position of one authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthorityHead {
    pub current_commit: NamespaceRevision,
    pub gc_epoch: GcEpoch,
    pub minimum_retained_cursor: ChangeCursor,
}

super::codec::tuple_wire!(AuthorityHead {
    current_commit: NamespaceRevision,
    gc_epoch: GcEpoch,
    minimum_retained_cursor: ChangeCursor,
});

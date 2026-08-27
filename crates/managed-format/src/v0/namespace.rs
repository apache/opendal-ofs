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

//! Snapshot, change, and receipt segment wire types.

use crate::v0::model::ChangeCursor;

use super::stream::StreamRef;

#[derive(Clone, Copy, Debug)]
pub struct NamespaceSnapshot {
    pub change_cursor: ChangeCursor,
    pub stream: StreamRef,
}

super::codec::tuple_wire!(NamespaceSnapshot {
    change_cursor: ChangeCursor,
    stream: StreamRef,
});

#[derive(Clone, Copy, Debug)]
pub struct NamespaceChangeSegment {
    pub end_cursor: ChangeCursor,
    pub compaction_weight_bytes: u64,
    pub stream: StreamRef,
}

super::codec::tuple_wire!(NamespaceChangeSegment {
    end_cursor: ChangeCursor,
    compaction_weight_bytes: u64,
    stream: StreamRef,
});

#[derive(Clone, Copy, Debug)]
pub struct OperationReceiptSegment {
    pub first_cursor: ChangeCursor,
    pub last_cursor: ChangeCursor,
    pub compaction_weight_bytes: u64,
    pub stream: StreamRef,
}

super::codec::tuple_wire!(OperationReceiptSegment {
    first_cursor: ChangeCursor,
    last_cursor: ChangeCursor,
    compaction_weight_bytes: u64,
    stream: StreamRef,
});

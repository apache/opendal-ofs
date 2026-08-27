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

//! Self-delimiting immutable stream envelope shared by metadata and file data.

use serde::Serialize;

use crate::Error;
use crate::v0::model::Digest;

use super::object::{ObjectClass, ObjectRef, checksum};

const TRAILER_MAGIC: [u8; 8] = *b"OFSSTR00";
const FOOTER_BYTES: usize = 8 + 32;
const TRAILER_BYTES: usize = 8 + 2 + 8 + 8 + 32 + 32;
/// Footer plus trailer appended to every stream object.
pub const STREAM_TAIL_BYTES: usize = FOOTER_BYTES + TRAILER_BYTES;

#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct StreamKind(u16);

impl StreamKind {
    pub const NAMESPACE_SNAPSHOT: Self = Self(1);
    pub const OPERATION_RECEIPTS: Self = Self(2);
    pub const DATA_SEGMENT: Self = Self(3);
    pub const NAMESPACE_CHANGES: Self = Self(4);
    pub const FILE_EXTENTS: Self = Self(5);

    pub const fn extension(value: u16) -> Option<Self> {
        if value >= 1024 {
            Some(Self(value))
        } else {
            None
        }
    }

    pub const fn value(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamRef {
    pub kind: StreamKind,
    pub object: ObjectRef,
    pub payload_length: u64,
    pub payload_digest: Digest,
}

super::codec::tuple_wire!(StreamRef {
    kind: StreamKind,
    object: ObjectRef,
    payload_length: u64,
    payload_digest: Digest,
});

impl StreamRef {
    pub fn require(self, kind: StreamKind, class: ObjectClass) -> Result<(), Error> {
        if self.kind != kind || self.object.locator.class != class {
            return Err(Error::corrupt(
                "read Managed stream",
                "stream reference has the wrong type",
            ));
        }
        Ok(())
    }
}

pub fn encode_stream_tail(
    kind: StreamKind,
    payload_length: u64,
    digest: Digest,
) -> Result<Vec<u8>, Error> {
    let mut tail = Vec::new();
    let footer_offset = payload_length;
    let mut footer = Vec::with_capacity(FOOTER_BYTES);
    footer.extend_from_slice(&payload_length.to_le_bytes());
    footer.extend_from_slice(digest.as_bytes());
    let footer_length = FOOTER_BYTES as u64;
    let footer_checksum = checksum(&footer);
    tail.extend_from_slice(&footer);
    let mut trailer = Vec::with_capacity(TRAILER_BYTES);
    trailer.extend_from_slice(&TRAILER_MAGIC);
    trailer.extend_from_slice(&kind.value().to_le_bytes());
    trailer.extend_from_slice(&footer_offset.to_le_bytes());
    trailer.extend_from_slice(&footer_length.to_le_bytes());
    trailer.extend_from_slice(footer_checksum.as_bytes());
    trailer.extend_from_slice(checksum(&trailer).as_bytes());
    tail.extend_from_slice(&trailer);
    Ok(tail)
}

pub fn validate_stream_tail(reference: StreamRef, tail: &[u8]) -> Result<(), Error> {
    if reference
        .payload_length
        .checked_add(STREAM_TAIL_BYTES as u64)
        != Some(reference.object.encoded_length)
    {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream length does not match its reference",
        ));
    }
    if tail.len() != STREAM_TAIL_BYTES {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream tail is truncated",
        ));
    }
    let (footer_bytes, trailer) = tail.split_at(FOOTER_BYTES);
    if trailer.len() != TRAILER_BYTES || trailer[..8] != TRAILER_MAGIC {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream trailer is invalid",
        ));
    }
    if checksum(&trailer[..TRAILER_BYTES - 32]).as_bytes() != &trailer[TRAILER_BYTES - 32..] {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream trailer checksum is invalid",
        ));
    }
    let kind = StreamKind(u16::from_le_bytes(
        trailer[8..10].try_into().expect("fixed stream kind"),
    ));
    let footer_offset = u64::from_le_bytes(trailer[10..18].try_into().expect("fixed offset"));
    let encoded_footer_length =
        u64::from_le_bytes(trailer[18..26].try_into().expect("fixed length"));
    let footer_checksum = checksum(footer_bytes);
    if kind != reference.kind
        || footer_offset != reference.payload_length
        || encoded_footer_length != FOOTER_BYTES as u64
        || &trailer[26..58] != footer_checksum.as_bytes()
    {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream trailer does not match its reference",
        ));
    }
    let payload_length = u64::from_le_bytes(footer_bytes[..8].try_into().expect("fixed length"));
    if payload_length != reference.payload_length
        || footer_bytes[8..] != *reference.payload_digest.as_bytes()
    {
        return Err(Error::corrupt(
            "read Managed stream",
            "stream footer does not match its reference",
        ));
    }
    Ok(())
}

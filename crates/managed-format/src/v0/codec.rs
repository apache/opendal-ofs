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

//! Positional tuple codec and bounded record envelope.

use std::io::Cursor;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;

use super::object::checksum;

const BODY_LENGTH_BYTES: usize = size_of::<u64>();
const CHECKSUM_BYTES: usize = size_of::<crate::v0::model::Checksum>();

macro_rules! tuple_wire {
    ($name:ty { $($field:ident : $field_type:ty),+ $(,)? }) => {
        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serde::Serialize::serialize(&($( &self.$field, )+), serializer)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let ($( $field, )+): ($( $field_type, )+) =
                    serde::Deserialize::deserialize(deserializer)?;
                Ok(Self { $( $field, )+ })
            }
        }
    };
}

pub(crate) use tuple_wire;

/// Self-validating envelope for one bounded CBOR record.
#[derive(Clone, Copy, Debug)]
pub struct RecordCodec {
    magic: [u8; 8],
    maximum_body_bytes: usize,
}

impl RecordCodec {
    /// Define a record envelope by its wire magic and maximum CBOR body size.
    pub const fn new(magic: [u8; 8], maximum_body_bytes: usize) -> Self {
        Self {
            magic,
            maximum_body_bytes,
        }
    }

    /// Largest encoded envelope accepted by this codec.
    pub const fn maximum_encoded_bytes(&self) -> usize {
        self.magic
            .len()
            .saturating_add(BODY_LENGTH_BYTES)
            .saturating_add(self.maximum_body_bytes)
            .saturating_add(CHECKSUM_BYTES)
    }

    /// Encode one value into the self-validating envelope.
    pub fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Error> {
        let mut body = Vec::new();
        ciborium::into_writer(value, &mut body)
            .map_err(|_| Error::invalid("encode Managed record", "record cannot be encoded"))?;
        if body.len() > self.maximum_body_bytes {
            return Err(Error::invalid(
                "encode Managed record",
                "record exceeds its size limit",
            ));
        }
        let body_length = u64::try_from(body.len())
            .map_err(|_| Error::invalid("encode Managed record", "record length overflows"))?;
        let mut bytes =
            Vec::with_capacity(self.magic.len() + BODY_LENGTH_BYTES + body.len() + CHECKSUM_BYTES);
        bytes.extend_from_slice(&self.magic);
        bytes.extend_from_slice(&body_length.to_le_bytes());
        bytes.extend_from_slice(&body);
        bytes.extend_from_slice(checksum(&bytes).as_bytes());
        Ok(bytes)
    }

    /// Decode and validate one complete envelope.
    pub fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Error> {
        let header_bytes = self.magic.len() + BODY_LENGTH_BYTES;
        let minimum_bytes = header_bytes + CHECKSUM_BYTES;
        if bytes.len() < minimum_bytes || bytes[..self.magic.len()] != self.magic {
            return Err(Error::corrupt(
                "decode Managed record",
                "record envelope is invalid",
            ));
        }
        let length_offset = self.magic.len();
        let body_length = usize::try_from(u64::from_le_bytes(
            bytes[length_offset..header_bytes]
                .try_into()
                .expect("length range has a fixed length"),
        ))
        .ok()
        .filter(|length| *length <= self.maximum_body_bytes)
        .ok_or_else(|| Error::corrupt("decode Managed record", "record length is invalid"))?;
        let expected_length = header_bytes
            .checked_add(body_length)
            .and_then(|length| length.checked_add(CHECKSUM_BYTES))
            .ok_or_else(|| Error::corrupt("decode Managed record", "record length overflows"))?;
        if bytes.len() != expected_length
            || checksum(&bytes[..bytes.len() - CHECKSUM_BYTES]).as_bytes()
                != &bytes[bytes.len() - CHECKSUM_BYTES..]
        {
            return Err(Error::corrupt(
                "decode Managed record",
                "record checksum is invalid",
            ));
        }
        let body = &bytes[header_bytes..header_bytes + body_length];
        let mut input = Cursor::new(body);
        let value = ciborium::from_reader(&mut input)
            .map_err(|_| Error::corrupt("decode Managed record", "record body is invalid"))?;
        if input.position() != body.len() as u64 {
            return Err(Error::corrupt(
                "decode Managed record",
                "record has trailing bytes",
            ));
        }
        Ok(value)
    }
}

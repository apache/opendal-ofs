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

//! Managed v0 record-stream framing.

use std::io::Cursor;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;

use super::checksum;

const RECORD_FRAME_MAGIC: [u8; 4] = *b"OFSF";
/// Bytes before the encoded records in every v0 record frame.
pub const RECORD_FRAME_HEADER_BYTES: usize = 4 + 8 + 4 + 32;
/// Maximum combined size of the length-prefixed records in one v0 frame.
pub const MAX_RECORD_FRAME_PAYLOAD_BYTES: usize = 64 * 1024;

/// Exact payload sizing for the v0 record-stream framing.
pub struct RecordStreamSizer {
    payload_length: u64,
    frame_length: usize,
    record: Vec<u8>,
}

impl RecordStreamSizer {
    pub const fn new() -> Self {
        Self {
            payload_length: 0,
            frame_length: 0,
            record: Vec::new(),
        }
    }

    pub fn write(&mut self, record: &impl Serialize) -> Result<(), Error> {
        encode_stream_record_into(record, &mut self.record)?;
        self.write_encoded(self.record.len())
    }

    pub fn write_encoded(&mut self, encoded_record_bytes: usize) -> Result<(), Error> {
        if encoded_record_bytes > MAX_RECORD_FRAME_PAYLOAD_BYTES {
            return Err(Error::invalid(
                "size Managed v0 stream",
                "one metadata record exceeds the frame range unit",
            ));
        }
        if self.frame_length != 0
            && self.frame_length.saturating_add(encoded_record_bytes)
                > MAX_RECORD_FRAME_PAYLOAD_BYTES
        {
            self.finish_frame()?;
        }
        self.frame_length = self
            .frame_length
            .checked_add(encoded_record_bytes)
            .ok_or_else(|| Error::invalid("size Managed v0 stream", "frame length overflows"))?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<u64, Error> {
        self.finish_frame()?;
        Ok(self.payload_length)
    }

    fn finish_frame(&mut self) -> Result<(), Error> {
        if self.frame_length == 0 {
            return Ok(());
        }
        self.payload_length = self
            .payload_length
            .checked_add(RECORD_FRAME_HEADER_BYTES as u64)
            .and_then(|length| length.checked_add(self.frame_length as u64))
            .ok_or_else(|| Error::invalid("size Managed v0 stream", "payload length overflows"))?;
        self.frame_length = 0;
        Ok(())
    }
}

impl Default for RecordStreamSizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode one CBOR value with its v0 little-endian length prefix.
pub fn encode_stream_record(record: &impl Serialize) -> Result<Vec<u8>, Error> {
    let mut encoded = Vec::new();
    encode_stream_record_into(record, &mut encoded)?;
    Ok(encoded)
}

/// Encode one CBOR value into a reusable v0 record buffer.
pub fn encode_stream_record_into(
    record: &impl Serialize,
    encoded: &mut Vec<u8>,
) -> Result<(), Error> {
    encoded.clear();
    encoded.extend_from_slice(&[0; size_of::<u32>()]);
    ciborium::into_writer(record, &mut *encoded)
        .map_err(|_| Error::invalid("write Managed v0 stream", "record cannot be encoded"))?;
    let body_length = encoded.len() - size_of::<u32>();
    let body_length = u32::try_from(body_length)
        .map_err(|_| Error::invalid("write Managed v0 stream", "one record is too large"))?;
    if encoded.len() > MAX_RECORD_FRAME_PAYLOAD_BYTES {
        return Err(Error::invalid(
            "write Managed v0 stream",
            "one metadata record exceeds the frame range unit",
        ));
    }
    encoded[..size_of::<u32>()].copy_from_slice(&body_length.to_le_bytes());
    Ok(())
}

/// Wrap concatenated length-prefixed records in one v0 frame.
pub fn encode_record_frame(record_count: u32, records: &[u8]) -> Result<Vec<u8>, Error> {
    let header = encode_record_frame_header(record_count, records)?;
    let mut frame = Vec::with_capacity(RECORD_FRAME_HEADER_BYTES + records.len());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(records);
    Ok(frame)
}

/// Encode the header for concatenated length-prefixed v0 records.
pub fn encode_record_frame_header(
    record_count: u32,
    records: &[u8],
) -> Result<[u8; RECORD_FRAME_HEADER_BYTES], Error> {
    if record_count == 0
        || records.len() > MAX_RECORD_FRAME_PAYLOAD_BYTES
        || record_count as usize > records.len() / size_of::<u32>()
    {
        return Err(Error::invalid(
            "write Managed v0 stream",
            "record frame is invalid",
        ));
    }
    let payload_length = u64::try_from(records.len())
        .map_err(|_| Error::invalid("write Managed v0 stream", "frame length overflows"))?;
    let mut header = [0; RECORD_FRAME_HEADER_BYTES];
    header[..4].copy_from_slice(&RECORD_FRAME_MAGIC);
    header[4..12].copy_from_slice(&payload_length.to_le_bytes());
    header[12..16].copy_from_slice(&record_count.to_le_bytes());
    header[16..].copy_from_slice(checksum(records).as_bytes());
    Ok(header)
}

/// Validate one complete v0 frame and return its record count.
pub fn validate_record_frame(bytes: &[u8]) -> Result<u32, Error> {
    if bytes.len() < RECORD_FRAME_HEADER_BYTES {
        return Err(Error::corrupt("read Managed v0 stream", "frame is invalid"));
    }
    let payload_length = decode_record_frame_header(&bytes[..RECORD_FRAME_HEADER_BYTES])?;
    if bytes.len() != RECORD_FRAME_HEADER_BYTES + payload_length
        || checksum(&bytes[RECORD_FRAME_HEADER_BYTES..]).as_bytes() != &bytes[16..48]
    {
        return Err(Error::corrupt(
            "read Managed v0 stream",
            "frame payload is invalid",
        ));
    }
    let record_count = u32::from_le_bytes(bytes[12..16].try_into().expect("fixed count"));
    if record_count == 0 || record_count as usize > payload_length / size_of::<u32>() {
        return Err(Error::corrupt(
            "read Managed v0 stream",
            "frame record count is invalid",
        ));
    }
    Ok(record_count)
}

/// Validate a v0 frame header and return the following payload length.
pub fn decode_record_frame_header(header: &[u8]) -> Result<usize, Error> {
    if header.len() != RECORD_FRAME_HEADER_BYTES || header[..4] != RECORD_FRAME_MAGIC {
        return Err(Error::corrupt(
            "read Managed v0 stream",
            "frame header is invalid",
        ));
    }
    usize::try_from(u64::from_le_bytes(
        header[4..12].try_into().expect("fixed frame length"),
    ))
    .ok()
    .filter(|length| *length <= MAX_RECORD_FRAME_PAYLOAD_BYTES)
    .ok_or_else(|| Error::corrupt("read Managed v0 stream", "frame length is invalid"))
}

/// Decode the next length-prefixed CBOR value from a validated v0 frame.
pub fn decode_stream_record<T: DeserializeOwned>(
    frame: &[u8],
    offset: &mut usize,
) -> Result<T, Error> {
    let length_end = offset
        .checked_add(size_of::<u32>())
        .filter(|end| *end <= frame.len())
        .ok_or_else(|| Error::corrupt("read Managed v0 stream", "record is truncated"))?;
    let length = u32::from_le_bytes(
        frame[*offset..length_end]
            .try_into()
            .expect("fixed record length"),
    ) as usize;
    let record_end = length_end
        .checked_add(length)
        .filter(|end| *end <= frame.len())
        .ok_or_else(|| Error::corrupt("read Managed v0 stream", "record is truncated"))?;
    let mut input = Cursor::new(&frame[length_end..record_end]);
    let record = ciborium::from_reader(&mut input)
        .map_err(|_| Error::corrupt("read Managed v0 stream", "record body is invalid"))?;
    if input.position() != length as u64 {
        return Err(Error::corrupt(
            "read Managed v0 stream",
            "record has trailing bytes",
        ));
    }
    *offset = record_end;
    Ok(record)
}

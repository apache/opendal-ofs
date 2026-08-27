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

//! OpenDAL adapter for Managed v0 record streams.

use std::marker::PhantomData;
use std::num::NonZeroUsize;

use futures::AsyncReadExt as _;
use opendal::Operator;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;
use crate::filesystem::Digest;
use crate::format::{
    GcEpoch, MAX_RECORD_FRAME_PAYLOAD_BYTES, ObjectClass, RECORD_FRAME_HEADER_BYTES,
    STREAM_TAIL_BYTES, StreamKind, StreamRef, decode_record_frame_header, decode_stream_record,
    encode_record_frame_header, encode_stream_record_into, encode_stream_tail,
    validate_record_frame, validate_stream_tail,
};

use super::object::ImmutableWriter;

pub struct RecordStreamReader<T> {
    reference: StreamRef,
    reader: opendal::FuturesAsyncReader,
    object_hasher: blake3::Hasher,
    offset: u64,
    frame: Vec<u8>,
    record_offset: usize,
    records_remaining: u32,
    completed: bool,
    record: PhantomData<T>,
}

impl<T: DeserializeOwned> RecordStreamReader<T> {
    pub async fn open(operator: &Operator, reference: StreamRef) -> Result<Self, Error> {
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
        Ok(Self {
            reference,
            reader: open_payload_reader(operator, reference).await?,
            object_hasher: blake3::Hasher::new(),
            offset: 0,
            frame: Vec::new(),
            record_offset: 0,
            records_remaining: 0,
            completed: false,
            record: PhantomData,
        })
    }

    pub async fn next(&mut self) -> Result<Option<T>, Error> {
        loop {
            if self.records_remaining != 0 {
                let record = decode_stream_record(&self.frame, &mut self.record_offset)?;
                self.records_remaining -= 1;
                return Ok(Some(record));
            }
            if !self.frame.is_empty() && self.record_offset != self.frame.len() {
                return Err(Error::corrupt(
                    "read Managed stream",
                    "frame record count does not match its payload",
                ));
            }
            if self.completed {
                return Ok(None);
            }
            if self.offset == self.reference.payload_length {
                if Digest::from_bytes(self.object_hasher.finalize().into())
                    != self.reference.payload_digest
                {
                    return Err(Error::corrupt(
                        "read Managed stream",
                        "stream payload does not match its reference",
                    ));
                }
                let mut tail = [0_u8; STREAM_TAIL_BYTES];
                self.reader
                    .read_exact(&mut tail)
                    .await
                    .map_err(|error| Error::io("read Managed stream tail", error))?;
                self.object_hasher.update(&tail);
                validate_stream_tail(self.reference, &tail)?;
                if Digest::from_bytes(self.object_hasher.finalize().into())
                    != self.reference.object.digest
                {
                    return Err(Error::corrupt(
                        "read Managed stream",
                        "object does not match its reference",
                    ));
                }
                self.completed = true;
                return Ok(None);
            }
            let (end, record_count) = read_next_frame(
                &mut self.reader,
                self.reference,
                self.offset,
                &mut self.frame,
            )
            .await?;
            self.object_hasher.update(&self.frame);
            self.record_offset = RECORD_FRAME_HEADER_BYTES;
            self.records_remaining = record_count;
            self.offset = end;
        }
    }
}

pub struct RecordStreamWriter {
    writer: ImmutableWriter,
    kind: StreamKind,
    payload_length: u64,
    frame: Vec<u8>,
    record: Vec<u8>,
    frame_records: u32,
}

impl RecordStreamWriter {
    pub async fn open(
        operator: &Operator,
        gc_epoch: GcEpoch,
        class: ObjectClass,
        kind: StreamKind,
        multipart_part_bytes: NonZeroUsize,
    ) -> Result<Self, Error> {
        Ok(Self {
            writer: ImmutableWriter::open(operator, gc_epoch, class, multipart_part_bytes).await?,
            kind,
            payload_length: 0,
            frame: Vec::new(),
            record: Vec::new(),
            frame_records: 0,
        })
    }

    pub async fn write(&mut self, record: &impl Serialize) -> Result<(), Error> {
        encode_stream_record_into(record, &mut self.record)?;
        if !self.frame.is_empty()
            && self.frame.len().saturating_add(self.record.len()) > MAX_RECORD_FRAME_PAYLOAD_BYTES
        {
            self.flush_frame().await?;
        }
        self.frame.extend_from_slice(&self.record);
        self.frame_records = self.frame_records.checked_add(1).ok_or_else(|| {
            Error::invalid("write Managed stream", "frame record count overflows")
        })?;
        Ok(())
    }

    async fn flush_frame(&mut self) -> Result<(), Error> {
        if self.frame_records == 0 {
            return Ok(());
        }
        let header = encode_record_frame_header(self.frame_records, &self.frame)?;
        let frame_length = header.len() + self.frame.len();
        self.payload_length = self
            .payload_length
            .checked_add(frame_length as u64)
            .ok_or_else(|| Error::invalid("write Managed stream", "payload length overflows"))?;
        self.writer.write(header.to_vec()).await?;
        self.writer.write(std::mem::take(&mut self.frame)).await?;
        self.frame_records = 0;
        Ok(())
    }

    pub async fn close(mut self) -> Result<StreamRef, Error> {
        self.flush_frame().await?;
        let payload_digest = self.writer.digest();
        finish_stream(self.writer, self.kind, self.payload_length, payload_digest).await
    }

    pub(crate) async fn abort(mut self) {
        let _ = self.writer.abort().await;
    }
}

pub async fn finish_stream(
    mut writer: ImmutableWriter,
    kind: StreamKind,
    payload_length: u64,
    digest: Digest,
) -> Result<StreamRef, Error> {
    let tail = encode_stream_tail(kind, payload_length, digest)?;
    writer.write(tail).await?;
    let object = writer.close().await?;
    Ok(StreamRef {
        kind,
        object,
        payload_length,
        payload_digest: digest,
    })
}

async fn open_payload_reader(
    operator: &Operator,
    reference: StreamRef,
) -> Result<opendal::FuturesAsyncReader, Error> {
    operator
        .reader_with(&reference.object.key())
        .content_length_hint(reference.object.encoded_length)
        .await
        .map_err(|error| Error::from_storage("read Managed stream", error))?
        .into_futures_async_read(0..reference.object.encoded_length)
        .await
        .map_err(|error| Error::from_storage("read Managed stream", error))
}

async fn read_next_frame(
    reader: &mut opendal::FuturesAsyncReader,
    reference: StreamRef,
    offset: u64,
    frame: &mut Vec<u8>,
) -> Result<(u64, u32), Error> {
    let header_end = offset
        .checked_add(RECORD_FRAME_HEADER_BYTES as u64)
        .filter(|end| *end <= reference.payload_length)
        .ok_or_else(|| Error::corrupt("read Managed stream", "frame header is truncated"))?;
    let mut header = [0_u8; RECORD_FRAME_HEADER_BYTES];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|error| Error::io("read Managed stream", error))?;
    let payload_length = decode_record_frame_header(&header)?;
    let payload_end = header_end
        .checked_add(payload_length as u64)
        .filter(|end| *end <= reference.payload_length)
        .ok_or_else(|| Error::corrupt("read Managed stream", "frame payload is truncated"))?;
    frame.clear();
    frame.reserve(RECORD_FRAME_HEADER_BYTES + payload_length);
    frame.extend_from_slice(&header);
    frame.resize(RECORD_FRAME_HEADER_BYTES + payload_length, 0);
    reader
        .read_exact(&mut frame[RECORD_FRAME_HEADER_BYTES..])
        .await
        .map_err(|error| Error::io("read Managed stream", error))?;
    let record_count = validate_record_frame(frame)?;
    Ok((payload_end, record_count))
}

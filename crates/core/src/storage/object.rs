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

//! Immutable object write and verified read.

use blake3::Hasher;
use opendal::{Buffer, Operator, Writer};
use std::num::NonZeroUsize;
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::Error;
use crate::filesystem::Digest;
use crate::format::{GcEpoch, ObjectClass, ObjectLocator, ObjectRef};

use super::control::read_bounded;

const SOURCE_BUFFER_BYTES: usize = 256 * 1024;

pub struct ImmutableWriter {
    operator: Operator,
    locator: ObjectLocator,
    writer: Writer,
    hasher: Hasher,
    encoded_length: u64,
}

impl ImmutableWriter {
    pub async fn open(
        operator: &Operator,
        gc_epoch: GcEpoch,
        class: ObjectClass,
        multipart_part_bytes: NonZeroUsize,
    ) -> Result<Self, Error> {
        let locator = ObjectLocator::generate(gc_epoch, class);
        let key = locator.key();
        let writer = operator
            .writer_with(&key)
            .if_not_exists(true)
            .chunk(multipart_part_bytes.get())
            .await
            .map_err(|error| Error::from_storage("open Managed object writer", error))?;
        Ok(Self {
            operator: operator.clone(),
            locator,
            writer,
            hasher: Hasher::new(),
            encoded_length: 0,
        })
    }

    pub async fn write(&mut self, bytes: Vec<u8>) -> Result<(), Error> {
        self.encoded_length = self
            .encoded_length
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::invalid("write Managed object", "object length overflows"))?;
        self.hasher.update(&bytes);
        if let Err(error) = self.writer.write(Buffer::from(bytes)).await {
            let error = Error::from_storage("write Managed object", error);
            let _ = self.writer.abort().await;
            return Err(error);
        }
        Ok(())
    }

    pub fn locator(&self) -> ObjectLocator {
        self.locator
    }

    pub fn digest(&self) -> Digest {
        Digest::from_bytes(self.hasher.finalize().into())
    }

    pub async fn write_source<R>(&mut self, source: &mut R) -> Result<(u64, Digest), Error>
    where
        R: AsyncRead + Unpin + ?Sized,
    {
        let mut length = 0_u64;
        let mut hasher = Hasher::new();
        loop {
            let mut bytes = vec![0; SOURCE_BUFFER_BYTES];
            let read = match source.read(&mut bytes).await {
                Ok(read) => read,
                Err(error) => {
                    let error = Error::from_io("read Managed object source", None, error);
                    let _ = self.abort().await;
                    return Err(error);
                }
            };
            if read == 0 {
                break;
            }
            bytes.truncate(read);
            length = length
                .checked_add(read as u64)
                .ok_or_else(|| Error::invalid("write Managed object", "source length overflows"))?;
            hasher.update(&bytes);
            self.write(bytes).await?;
        }
        Ok((length, Digest::from_bytes(hasher.finalize().into())))
    }

    pub async fn abort(&mut self) -> Result<(), Error> {
        self.writer
            .abort()
            .await
            .map_err(|error| Error::from_storage("abort Managed object", error))
    }

    pub async fn close(mut self) -> Result<ObjectRef, Error> {
        if let Err(error) = self.writer.close().await
            && !self.published_object_is_visible().await
        {
            let _ = self.writer.abort().await;
            return Err(Error::from_storage("finish Managed object", error));
        }
        Ok(ObjectRef {
            locator: self.locator,
            encoded_length: self.encoded_length,
            digest: Digest::from_bytes(self.hasher.finalize().into()),
        })
    }

    async fn published_object_is_visible(&self) -> bool {
        self.operator
            .stat(&self.locator.key())
            .await
            .is_ok_and(|metadata| metadata.content_length() == self.encoded_length)
    }
}

pub async fn read_immutable(
    operator: &Operator,
    reference: ObjectRef,
    maximum_bytes: usize,
) -> Result<Vec<u8>, Error> {
    let length = usize::try_from(reference.encoded_length)
        .ok()
        .filter(|length| *length <= maximum_bytes)
        .ok_or_else(|| Error::corrupt("read Managed object", "object length is invalid"))?;
    let bytes = read_bounded(operator, &reference.key(), length, "read Managed object")
        .await?
        .ok_or_else(|| Error::corrupt("read Managed object", "referenced object is missing"))?
        .bytes;
    if bytes.len() != length || blake3::hash(&bytes).as_bytes() != reference.digest.as_bytes() {
        return Err(Error::corrupt(
            "read Managed object",
            "object does not match its reference",
        ));
    }
    Ok(bytes)
}

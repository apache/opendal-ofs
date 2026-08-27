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

//! Bounded control-object reads and conditional writes.

use opendal::{ErrorKind as StorageErrorKind, Operator};
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;

use crate::Error;
use crate::format::codec::RecordCodec;

/// One decoded mutable control value and its optional storage revision.
pub struct ObservedControl<T> {
    pub value: T,
    pub revision: Option<String>,
}

/// Typed adapter for one bounded mutable control object.
pub struct ControlRecord<T> {
    key: &'static str,
    codec: RecordCodec,
    value: PhantomData<fn() -> T>,
}

impl<T> ControlRecord<T> {
    /// Bind one wire codec to its only object key.
    pub const fn new(key: &'static str, codec: RecordCodec) -> Self {
        Self {
            key,
            codec,
            value: PhantomData,
        }
    }
}

impl<T> ControlRecord<T>
where
    T: DeserializeOwned + Serialize,
{
    /// Read and decode one value together with its conditional revision.
    pub async fn read(&self, operator: &Operator) -> Result<Option<ObservedControl<T>>, Error> {
        let Some(object) = read_bounded(
            operator,
            self.key,
            self.codec.maximum_encoded_bytes(),
            "read Managed control object",
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(ObservedControl {
            value: self.codec.decode(&object.bytes)?,
            revision: object.revision,
        }))
    }

    /// Encode and conditionally write one value.
    pub async fn write(
        &self,
        operator: &Operator,
        value: &T,
        revision: Option<&str>,
    ) -> Result<bool, Error> {
        write_control(operator, self.key, self.codec.encode(value)?, revision).await
    }
}

pub(super) struct BoundedObject {
    pub(super) bytes: Vec<u8>,
    revision: Option<String>,
}

pub(super) async fn read_bounded(
    operator: &Operator,
    key: &str,
    maximum_bytes: usize,
    operation: &'static str,
) -> Result<Option<BoundedObject>, Error> {
    let buffer = match operator.read(key).await {
        Ok(buffer) => buffer,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage(operation, error)),
    };
    if buffer.len() > maximum_bytes {
        return Err(Error::corrupt(operation, "object exceeds its size limit"));
    }
    let revision = match operator.stat(key).await {
        Ok(metadata) => metadata.etag().map(str::to_owned),
        Err(error) if error.kind() == StorageErrorKind::NotFound => None,
        Err(error) if error.kind() == StorageErrorKind::Unsupported => None,
        Err(error) => return Err(Error::from_storage(operation, error)),
    };
    Ok(Some(BoundedObject {
        bytes: buffer.to_vec(),
        revision,
    }))
}

/// Conditionally replace one mutable control object.
pub(super) async fn write_control(
    operator: &Operator,
    key: &str,
    bytes: Vec<u8>,
    revision: Option<&str>,
) -> Result<bool, Error> {
    let write = operator.write_with(key, bytes);
    let result = match revision {
        None => write.if_not_exists(true).await,
        Some(revision) => write.if_match(revision).await,
    };
    match result {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(Error::from_storage("publish Managed control object", error)),
    }
}

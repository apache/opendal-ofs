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

use std::future::Future;

use crate::{BlobRef, FsFormat, FsHead, FsVersion, Result};

/// One head value and the opaque condition required to replace that observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadObservation {
    head: FsHead,
    condition: Vec<u8>,
}

impl HeadObservation {
    pub fn new(head: FsHead, condition: Vec<u8>) -> Result<Self> {
        if condition.is_empty() {
            return Err(crate::Error::invalid(
                "construct YinYang head observation",
                "head condition is empty",
            ));
        }
        Ok(Self { head, condition })
    }

    pub const fn head(&self) -> &FsHead {
        &self.head
    }

    pub fn condition(&self) -> &[u8] {
        &self.condition
    }
}

/// Persistent capabilities required by the YinYang Format state machine.
pub trait FormatStorage: Send + Sync {
    /// Create the bootstrap record when it does not exist.
    fn create_format<'a>(
        &'a self,
        format: &'a FsFormat,
    ) -> impl Future<Output = Result<bool>> + Send + 'a;

    /// Read the bootstrap record.
    fn read_format(&self) -> impl Future<Output = Result<Option<FsFormat>>> + Send + '_;

    /// Persist one immutable filesystem version and return its verifiable reference.
    fn write_version<'a>(
        &'a self,
        version: &'a FsVersion,
    ) -> impl Future<Output = Result<BlobRef>> + Send + 'a;

    /// Read and verify one immutable filesystem version.
    fn read_version<'a>(
        &'a self,
        reference: &'a BlobRef,
    ) -> impl Future<Output = Result<FsVersion>> + Send + 'a;

    /// Create the mutable head when it does not exist.
    fn create_head<'a>(
        &'a self,
        head: &'a FsHead,
    ) -> impl Future<Output = Result<bool>> + Send + 'a;

    /// Read the head and a condition bound to the same observed value.
    fn observe_head(&self) -> impl Future<Output = Result<Option<HeadObservation>>> + Send + '_;

    /// Replace the complete head when `observed` is still current.
    fn replace_head<'a>(
        &'a self,
        observed: &'a HeadObservation,
        next: &'a FsHead,
    ) -> impl Future<Output = Result<bool>> + Send + 'a;
}

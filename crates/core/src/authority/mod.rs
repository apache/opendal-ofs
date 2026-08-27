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

//! Core namespace authority and its extension boundary.

mod object;

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;

use opendal::Operator;
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::format::{ExtensionDescriptor, GcEpoch};

pub use crate::format::AuthorityHead;
pub use object::DefaultAuthorityAccess;

pub const DEFAULT_AUTHORITY: &str = "main";

/// Stable identity of one namespace authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AuthorityId([u8; 16]);

impl AuthorityId {
    pub fn generate() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// One observed authority position and its opaque conditional revision.
#[derive(Clone, Debug)]
pub struct AuthorityObservation {
    pub id: AuthorityId,
    pub head: AuthorityHead,
    pub revision: Vec<u8>,
}

/// One live root consumed and replaced during collection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorityRoot {
    pub id: AuthorityId,
    pub name: String,
    pub head: AuthorityHead,
}

/// Opaque fence established before collection roots are streamed.
#[derive(Clone, Debug)]
pub struct CollectionFence {
    pub epoch: GcEpoch,
    pub revision: Vec<u8>,
}

/// Forward-only stream of authority roots.
pub type AuthorityRoots = futures::stream::BoxStream<'static, Result<AuthorityRoot, Error>>;

/// Namespace authority access.
///
/// The core implementation always owns `main`. Authority extensions wrap an
/// existing implementation, delegate its authorities unchanged, and add their
/// own names and collection roots.
pub trait AuthorityAccess: Send + Sync + fmt::Debug + Unpin + 'static {
    fn info(&self) -> Option<ExtensionDescriptor>;

    fn initialize<'a>(
        &'a self,
        operator: &'a Operator,
        multipart_part_bytes: NonZeroUsize,
        initial: AuthorityHead,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;

    fn observe<'a>(
        &'a self,
        operator: &'a Operator,
        name: &'a str,
    ) -> impl Future<Output = Result<AuthorityObservation, Error>> + Send + 'a;

    fn compare_exchange<'a>(
        &'a self,
        operator: &'a Operator,
        multipart_part_bytes: NonZeroUsize,
        name: &'a str,
        observed: &'a AuthorityObservation,
        next: AuthorityHead,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a;

    fn begin_collection<'a>(
        &'a self,
        operator: &'a Operator,
        multipart_part_bytes: NonZeroUsize,
    ) -> impl Future<Output = Result<(CollectionFence, AuthorityRoots), Error>> + Send + 'a;

    fn finish_collection<'a>(
        &'a self,
        operator: &'a Operator,
        multipart_part_bytes: NonZeroUsize,
        fence: CollectionFence,
        roots: &'a mut AuthorityRoots,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a;
}

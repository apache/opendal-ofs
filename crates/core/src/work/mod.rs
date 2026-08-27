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

//! Bounded local work storage for streaming namespace and data algorithms.

mod compact;
mod ordered;
mod sort;
mod spool;

use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::Error;

pub(crate) use compact::RunCompactor;
pub(crate) use ordered::{
    AsyncOrderedMerge, AsyncOrderedRead, JoinItem, OrderedJoin, OrderedMerge, OrderedRead, Unique,
};
pub(crate) use sort::{merge_sorted, sort};
pub(crate) use spool::{Spool, SpoolReader, SpoolWriter};

const MEBIBYTE: usize = 1024 * 1024;

#[derive(Clone, Copy)]
pub struct WorkBudget {
    sort_run_bytes: usize,
    fan_in: usize,
}

impl WorkBudget {
    pub fn new(memory_bytes: NonZeroUsize, concurrency: NonZeroUsize) -> Result<Self, Error> {
        let fan_in = concurrency.get().checked_add(1).ok_or_else(|| {
            Error::invalid("configure local work", "stream concurrency overflows")
        })?;
        Ok(Self {
            sort_run_bytes: memory_bytes.get(),
            fan_in,
        })
    }

    pub fn from_mib(memory_mib: NonZeroUsize, concurrency: NonZeroUsize) -> Result<Self, Error> {
        let memory_bytes = memory_mib
            .get()
            .checked_mul(MEBIBYTE)
            .ok_or_else(|| Error::invalid("configure local work", "work memory overflows"))?;
        Self::new(
            NonZeroUsize::new(memory_bytes).expect("positive MiB is a positive byte count"),
            concurrency,
        )
    }

    pub const fn memory_target_bytes(self) -> usize {
        self.sort_run_bytes
    }
}

/// Operation-scoped composition of temporary storage and its resource budget.
#[derive(Clone)]
pub struct WorkContext {
    workspace: Arc<tempfile::TempDir>,
    budget: WorkBudget,
}

impl WorkContext {
    pub fn create(budget: WorkBudget) -> Result<Self, Error> {
        Self::create_in(budget, &std::env::temp_dir())
    }

    pub fn create_in(budget: WorkBudget, directory: &std::path::Path) -> Result<Self, Error> {
        Ok(Self {
            workspace: Arc::new(tempfile::TempDir::new_in(directory).map_err(|error| {
                Error::from_io("create Sync workspace", Some(directory), error)
            })?),
            budget,
        })
    }

    pub fn path(&self) -> &std::path::Path {
        self.workspace.path()
    }

    pub(super) fn sort_run_bytes(&self) -> usize {
        self.budget.sort_run_bytes
    }

    pub fn fan_in(&self) -> usize {
        self.budget.fan_in
    }

    pub fn writer<T>(&self, stem: &str) -> Result<SpoolWriter<T>, Error> {
        SpoolWriter::create(self.workspace.clone(), stem)
    }
}

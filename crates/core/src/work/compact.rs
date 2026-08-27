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

//! Resource-bounded compaction of already ordered runs.

use crate::Error;

/// Incrementally compact equivalent ordered inputs without retaining one
/// descriptor per input.
pub(crate) struct RunCompactor<T> {
    fan_in: usize,
    levels: Vec<Vec<T>>,
}

impl<T> RunCompactor<T> {
    pub(crate) fn new(fan_in: usize) -> Self {
        debug_assert!(fan_in >= 2, "run compaction must make progress");
        Self {
            fan_in,
            levels: Vec::new(),
        }
    }

    pub(crate) fn push(
        &mut self,
        mut input: T,
        mut merge: impl FnMut(&[T]) -> Result<T, Error>,
    ) -> Result<(), Error> {
        let mut level = 0;
        loop {
            if level == self.levels.len() {
                self.levels.push(Vec::new());
            }
            self.levels[level].push(input);
            if self.levels[level].len() < self.fan_in {
                return Ok(());
            }
            input = merge(&std::mem::take(&mut self.levels[level]))?;
            level += 1;
        }
    }

    pub(crate) fn finish(
        mut self,
        mut merge: impl FnMut(&[T]) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        let mut inputs = self.levels.drain(..).flatten().collect::<Vec<_>>();
        while inputs.len() > 1 {
            let mut output = Vec::with_capacity(inputs.len().div_ceil(self.fan_in));
            let mut remaining = inputs.into_iter();
            loop {
                let group = remaining.by_ref().take(self.fan_in).collect::<Vec<_>>();
                if group.is_empty() {
                    break;
                }
                if group.len() == 1 {
                    output.extend(group);
                } else {
                    output.push(merge(&group)?);
                }
            }
            inputs = output;
        }
        Ok(inputs.pop())
    }
}

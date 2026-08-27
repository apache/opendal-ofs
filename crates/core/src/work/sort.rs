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

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;

use super::compact::RunCompactor;
use super::ordered::OrderedMerge;
use super::spool::decode_record;
use super::{Spool, SpoolReader, WorkContext};

pub(crate) fn sort<T, K>(
    workspace: &WorkContext,
    source: &Spool<T>,
    key: impl Fn(&T) -> K + Copy,
) -> Result<Spool<T>, Error>
where
    T: DeserializeOwned + Serialize,
    K: Ord,
{
    let mut source = source.reader()?;
    let fan_in = workspace.fan_in();
    let mut runs = RunCompactor::new(fan_in);
    loop {
        let mut records = Vec::new();
        let mut encoded_bytes = 0_usize;
        while encoded_bytes < workspace.sort_run_bytes() {
            let Some(frame_bytes) = source.peek_frame_bytes()? else {
                break;
            };
            if encoded_bytes != 0 && frame_bytes > workspace.sort_run_bytes() - encoded_bytes {
                break;
            }
            let bytes = source
                .next_frame()?
                .expect("peeked Sync record remains available");
            let record = decode_record::<T>(&bytes)?;
            records.push((key(&record), bytes));
            encoded_bytes += frame_bytes;
        }
        if records.is_empty() {
            break;
        }
        records.sort_by(|left, right| left.0.cmp(&right.0));
        let mut run = workspace.writer("sort-run")?;
        for (_, bytes) in records {
            run.write_frame(&bytes)?;
        }
        runs.push(run.finish()?, |group| merge_runs(workspace, group, key))?;
    }
    drop(source);

    runs.finish(|runs| merge_runs(workspace, runs, key))?
        .map_or_else(|| workspace.writer("sorted")?.finish(), Ok)
}

pub(crate) fn merge_sorted<T, K>(
    workspace: &WorkContext,
    inputs: Vec<Spool<T>>,
    key: impl Fn(&T) -> K + Copy,
) -> Result<Spool<T>, Error>
where
    T: DeserializeOwned + Serialize,
    K: Ord,
{
    let mut runs = RunCompactor::new(workspace.fan_in());
    for input in inputs {
        runs.push(input, |runs| merge_runs(workspace, runs, key))?;
    }
    runs.finish(|runs| merge_runs(workspace, runs, key))?
        .map_or_else(|| workspace.writer("merged")?.finish(), Ok)
}

fn merge_runs<T, K>(
    workspace: &WorkContext,
    runs: &[Spool<T>],
    key: impl Fn(&T) -> K + Copy,
) -> Result<Spool<T>, Error>
where
    T: DeserializeOwned + Serialize,
    K: Ord,
{
    let readers = runs
        .iter()
        .map(Spool::reader)
        .collect::<Result<Vec<_>, Error>>()?;
    let mut records = OrderedMerge::new(readers, SpoolReader::next_frame, |bytes: &Vec<u8>| {
        Ok(key(&decode_record::<T>(bytes)?))
    })?;
    let mut output = workspace.writer("merge-run")?;
    while let Some(bytes) = records.next()? {
        output.write_frame(&bytes)?;
    }
    output.finish()
}

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

use std::fs;
use std::io::BufRead as _;
use std::ops::Range;

use anyhow::{Context, Result, anyhow, bail};
use ofs_core::filesystem::{ContentRef, Digest};
use ofs_core::sync::{FileChangeSetEntry, SyncEngine};

use crate::cli::SyncArgs;

pub(super) async fn run(args: SyncArgs) -> Result<()> {
    let root = fs::canonicalize(&args.replica)
        .with_context(|| format!("cannot open replica directory: {}", args.replica.display()))?;
    if !root.is_dir() {
        bail!("replica is not a directory: {}", args.replica.display());
    }
    if let Some(capability) = args
        .require
        .iter()
        .find(|capability| !capability.available())
    {
        bail!(
            "required filesystem capability is unavailable: {}",
            capability.name()
        );
    }

    let volume = super::open_named_volume(&args.volume, &args.runtime).await?;
    let volume_id = volume.id();
    let engine = SyncEngine::new(volume);
    let result = match &args.change_set {
        Some(path) => {
            let mutations = read_changes(path)?;
            engine
                .sync_with_mutations(&root, &args.state, &args.resolve, &mutations)
                .await?
        }
        None => engine.sync(&root, &args.state, &args.resolve).await?,
    };
    if !result.conflict_paths.is_empty() {
        let paths = result
            .conflict_paths
            .iter()
            .map(|path| format!("  {path}"))
            .collect::<Vec<_>>()
            .join("\n");
        bail!(
            "sync retained {} conflict(s); rerun with `--resolve <relative-path>` for each normalized relative path:\n{}",
            result.conflict_paths.len(),
            paths
        );
    }
    println!(
        "synced managed volume {} at change {}{}",
        volume_id,
        result.sequence,
        if result.published { " (published)" } else { "" }
    );
    Ok(())
}

#[derive(serde::Deserialize)]
struct ChangeInput {
    path: String,
    base: ContentInput,
    ranges: Vec<RangeInput>,
}

#[derive(serde::Deserialize)]
struct ContentInput {
    digest: String,
    length: u64,
}

#[derive(serde::Deserialize)]
struct RangeInput {
    offset: u64,
    length: u64,
}

fn read_changes(path: &std::path::Path) -> Result<Vec<FileChangeSetEntry>> {
    let file = fs::File::open(path)
        .with_context(|| format!("cannot open staged changes: {}", path.display()))?;
    std::io::BufReader::new(file)
        .lines()
        .enumerate()
        .filter_map(|(line, input)| match input {
            Ok(input) if input.trim().is_empty() => None,
            input => Some((line, input)),
        })
        .map(|(line, input)| {
            let input =
                input.with_context(|| format!("cannot read staged changes line {}", line + 1))?;
            let change: ChangeInput = serde_json::from_str(&input)
                .with_context(|| format!("invalid staged changes line {}", line + 1))?;
            let digest = decode_digest(&change.base.digest)
                .with_context(|| format!("invalid digest on staged changes line {}", line + 1))?;
            let ranges = change
                .ranges
                .into_iter()
                .map(|range| {
                    let end = range
                        .offset
                        .checked_add(range.length)
                        .ok_or_else(|| anyhow!("changed range overflows on line {}", line + 1))?;
                    Ok::<Range<u64>, anyhow::Error>(range.offset..end)
                })
                .collect::<Result<Vec<_>>>()?;
            FileChangeSetEntry::new(
                change.path,
                ContentRef::new(digest, change.base.length),
                ranges,
            )
            .map_err(anyhow::Error::msg)
        })
        .collect()
}

fn decode_digest(value: &str) -> Result<Digest> {
    if value.len() != 64 {
        bail!("digest must contain 64 hexadecimal characters");
    }
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .context("digest contains a non-hexadecimal character")?;
    }
    Ok(Digest::from_bytes(bytes))
}

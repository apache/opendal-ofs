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

use futures::{StreamExt as _, TryStreamExt as _, future};
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::data::{RangeBatch, RangeBatcher, RangeReader};
use crate::filesystem::ContentRef;
use crate::format::ExtentRef;
use crate::volume::AccessFamily;
use crate::volume::ManagedVolume;
use crate::work::{Spool, SpoolWriter, WorkContext};

use super::replica::fs::{DirectoryDurability, StoredPath};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Installation {
    pub(super) extent: ExtentRef,
    pub(super) destination: StoredPath,
    pub(super) fingerprint: ContentRef,
    pub(super) executable: bool,
}

impl Installation {
    fn memory_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.destination.memory_bytes())
            .saturating_add(
                self.extent
                    .decoding_outputs
                    .len()
                    .saturating_mul(size_of::<ContentRef>()),
            )
    }
}

pub(crate) async fn install<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    workspace: &WorkContext,
    pending: &Spool<Installation>,
    concurrency: usize,
    authoritative: bool,
    durability: &mut DirectoryDurability,
) -> Result<(), Error> {
    let pending = crate::work::sort(workspace, pending, |file| {
        (
            file.extent.stored_range.segment,
            file.extent.stored_range.offset,
        )
    })?;
    let mut batches = RangeBatcher::new(volume.transfer_window_bytes(), volume.read_gap_bytes());
    {
        let installations = pending
            .stream()?
            .map(|file| {
                file.and_then(|file| {
                    let locator = file.extent.stored_range.segment;
                    let start = file.extent.stored_range.offset;
                    let end = start
                        .checked_add(file.extent.stored_range.stored_content.length())
                        .ok_or_else(|| {
                            Error::corrupt("install Managed segment", "file range overflows")
                        })?;
                    let metadata_bytes = file.memory_bytes();
                    batches.push(locator, start..end, metadata_bytes, file)
                })
                .transpose()
            })
            .filter_map(future::ready)
            .map_ok(|batch| {
                let volume = volume.clone();
                let workspace = workspace.clone();
                async move { install_batch(&volume, &workspace, batch, authoritative).await }
            })
            .try_buffer_unordered(concurrency);
        futures::pin_mut!(installations);
        while let Some(installed) = installations.try_next().await? {
            record_installed(installed, durability)?;
        }
    }
    if let Some(batch) = batches.take() {
        let installed = install_batch(volume, workspace, batch, authoritative).await?;
        record_installed(installed, durability)?;
    }
    Ok(())
}

fn record_installed(
    paths: Spool<StoredPath>,
    durability: &mut DirectoryDurability,
) -> Result<(), Error> {
    let mut paths = paths.reader()?;
    while let Some(path) = paths.next()? {
        durability.changed_parent(&path.to_path_buf())?;
    }
    Ok(())
}

async fn install_batch<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    workspace: &WorkContext,
    batch: RangeBatch<Installation>,
    authoritative: bool,
) -> Result<Spool<StoredPath>, Error> {
    let mut installed = workspace.writer("installed-segment-files")?;
    if !authoritative {
        install_ranges(volume, batch, &mut installed).await?;
        return installed.finish();
    }
    let locator = batch.locator();
    let mut selected = RangeBatcher::new(volume.transfer_window_bytes(), volume.read_gap_bytes());
    for item in batch.into_items() {
        let file = item.value;
        if crate::sync::replica::fs::file_matches(
            &file.destination.to_path_buf(),
            file.fingerprint,
            file.executable,
        )
        .await?
        {
            continue;
        }
        let metadata_bytes = file.memory_bytes();
        if let Some(batch) = selected.push(locator, item.range, metadata_bytes, file)? {
            install_ranges(volume, batch, &mut installed).await?;
        }
    }
    if let Some(batch) = selected.take() {
        install_ranges(volume, batch, &mut installed).await?;
    }
    installed.finish()
}

async fn install_ranges<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    batch: RangeBatch<Installation>,
    installed: &mut SpoolWriter<StoredPath>,
) -> Result<(), Error> {
    let mut batch = batch
        .read(volume.operator(), volume.read_gap_bytes())
        .await?;
    while let Some((file, reader)) = batch.next() {
        install_file(volume, reader, &file).await?;
        installed.write(&file.destination)?;
    }
    Ok(())
}

async fn install_file<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    reader: &mut RangeReader,
    file: &Installation,
) -> Result<(), Error> {
    let destination = file.destination.to_path_buf();
    crate::sync::replica::fs::install_file(&destination, file.executable, async |destination| {
        volume
            .read_extent(
                reader,
                file.extent.clone(),
                0..file.fingerprint.length(),
                destination,
            )
            .await
    })
    .await?;
    Ok(())
}

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

use std::path::Path;

use tokio::fs::File;
use tokio::io::AsyncReadExt as _;

use crate::Error;
use crate::data::{ContentReuseLookup, DataSegmentWriter, ReusableFile};
use crate::filesystem::ContentRef;
use crate::format::{FileExtentMap, FileRange, GcEpoch};
use crate::volume::AccessFamily;
use crate::volume::ManagedVolume;
use serde::{Deserialize, Serialize};

use super::replica::scan::LocalFile;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) enum FilePublication {
    Complete,
    Changed {
        previous: Box<LocalFile>,
        ranges: Vec<FileRange>,
    },
}

pub(super) async fn publish_file<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    path: &Path,
    publication: &FilePublication,
    expected_length: u64,
    known: &ContentReuseLookup,
    gc_epoch: GcEpoch,
) -> Result<(ContentRef, FileExtentMap), Error> {
    let mut placement = volume.data_placement(gc_epoch, u64::MAX, None);
    let result = publish_file_into(
        volume,
        &mut placement,
        path,
        publication,
        expected_length,
        known,
    )
    .await;
    let (content, data, reused) = match result {
        Ok(result) => result,
        Err(error) => {
            placement.abort().await;
            return Err(error);
        }
    };
    if reused || content.length() == 0 {
        placement.abort().await;
    } else {
        placement.finish().await?;
    }
    Ok((content, data))
}

pub(super) async fn publish_file_into<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    placement: &mut DataSegmentWriter<'_>,
    path: &Path,
    publication: &FilePublication,
    expected_length: u64,
    known: &ContentReuseLookup,
) -> Result<(ContentRef, FileExtentMap, bool), Error> {
    let mut file = File::open(path)
        .await
        .map_err(|error| Error::from_io("publish local file", Some(path), error))?;
    let result = match publication {
        FilePublication::Complete => {
            let mut source = (&mut file).take(expected_length);
            let result = volume
                .publish_data_into(placement, &mut source, expected_length, known)
                .await?;
            drop(source);
            let mut extra = [0_u8; 1];
            if result.0.length() != expected_length
                || file.read(&mut extra).await.map_err(|error| {
                    Error::from_io("finish local file publication", Some(path), error)
                })? != 0
            {
                return Err(Error::conflict(
                    "publish local file",
                    "local file changed while it was being published",
                ));
            }
            Ok(result)
        }
        FilePublication::Changed { previous, ranges } => {
            volume
                .publish_patch_into(
                    placement,
                    &mut file,
                    expected_length,
                    (previous.content, previous.data.clone()),
                    ranges,
                    known,
                )
                .await
        }
    };
    result.map_err(|error| error.with_context("path", path.display()))
}

pub(super) async fn materialize_file<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    content: (ContentRef, FileExtentMap),
    reusable: Option<FileExtentMap>,
    destination: &Path,
    executable: bool,
) -> Result<(), Error> {
    crate::sync::replica::fs::install_file(destination, executable, async |destination_file| {
        let mut source = match reusable {
            Some(reference) => Some((
                reference,
                File::open(destination).await.map_err(|error| {
                    Error::from_io("open reusable replica file", Some(destination), error)
                })?,
            )),
            None => None,
        };
        let reusable = source.as_mut().map(|(reference, source)| ReusableFile {
            reference: reference.clone(),
            source,
        });
        volume
            .read_data(content, .., reusable, destination_file)
            .await
    })
    .await
    .map_err(|error| error.with_context("path", destination.display()))
}

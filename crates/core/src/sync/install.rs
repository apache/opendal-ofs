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

use std::cmp::Reverse;
use std::path::Path;

use crate::Error;
use crate::filesystem::NamespaceValue;
use crate::format::FileExtentMap;
use crate::volume::AccessFamily;
use crate::volume::{ManagedVolume, Namespace, NamespaceReader};
use crate::work::{Spool, SpoolReader, WorkContext};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

use super::replica::fs::{DirectoryDurability, StoredPath};
use super::segment_install::{self, Installation as SegmentInstallation};
use super::transfer::materialize_file;

pub(crate) async fn install<A: AccessFamily>(
    workspace: &WorkContext,
    root: &Path,
    target: &Namespace<FileExtentMap>,
    volume: &ManagedVolume<A>,
    current: Option<&Namespace<FileExtentMap>>,
) -> Result<(), Error> {
    let verify_existing = current.is_none();
    let transfer_concurrency = volume.stream_concurrency();
    let removals = installation_removals(root, current, target, workspace)?;
    let mut durability = DirectoryDurability::create(workspace)?;
    let removals = crate::work::sort(workspace, &removals, |path| Reverse(path.clone()))?;
    let mut removals = removals.reader()?;
    while let Some(path) = removals.next()? {
        let destination = root.join(path.to_path_buf());
        crate::sync::replica::fs::remove_path(&destination)?;
        durability.changed_parent(&destination)?;
    }

    let mut file_installations = workspace.writer("file-installations")?;
    let mut grouped = workspace.writer("grouped-segment-installations")?;
    let mut current_reader = current.map(Namespace::reader).transpose()?;
    let mut current_record = current_reader
        .as_mut()
        .map(|reader| reader.next())
        .transpose()?
        .flatten();
    let mut target_reader = target.reader()?;
    while let Some(record) = target_reader.next()? {
        while current_record
            .as_ref()
            .is_some_and(|current| current.path < record.path)
        {
            current_record = current_reader
                .as_mut()
                .expect("current record requires a reader")
                .next()?;
        }
        let matching_current = current_record
            .as_ref()
            .filter(|current| current.path == record.path);
        let Some(node) = record.value else {
            continue;
        };
        let destination = root.join(&record.path);
        let NamespaceValue::RegularFile {
            version,
            content,
            data,
        } = node.value
        else {
            if record.path.is_empty() {
                continue;
            }
            match crate::sync::replica::fs::path_metadata(&destination)? {
                Some(metadata) if metadata.is_dir() => {}
                Some(_) => {
                    crate::sync::replica::fs::remove_path(&destination)?;
                    durability.changed_parent(&destination)?;
                    crate::sync::replica::fs::create_directory(&destination, &mut durability)?;
                }
                None => {
                    crate::sync::replica::fs::create_directory(&destination, &mut durability)?;
                }
            }
            continue;
        };
        if !verify_existing
            && matching_current.is_some_and(|current| {
                current.value.as_ref().is_some_and(|current| {
                    current.attributes == node.attributes
                        && current
                            .file()
                            .is_some_and(|(current_version, _, _)| current_version == version)
                })
            })
        {
            continue;
        }
        let executable = node.attributes.executable;
        let reusable = matching_current.and_then(|current| {
            current
                .value
                .as_ref()
                .and_then(|current| current.file().map(|(_, _, data)| data.clone()))
        });
        if let Some(mapping) = data.inline_file_extent() {
            if mapping.logical_range.offset != 0
                || mapping.extent_offset != 0
                || mapping.logical_range.length != content.length()
                || mapping.extent.content() != content
            {
                return Err(Error::corrupt(
                    "install Managed file",
                    "inline extent does not match its file content",
                ));
            }
            grouped.write(&SegmentInstallation {
                extent: mapping.extent,
                destination: StoredPath::from_path(&destination)?,
                fingerprint: content,
                executable,
            })?;
        } else {
            file_installations.write(&FileInstallation {
                destination: StoredPath::from_path(&destination)?,
                fingerprint: content,
                content: data.clone(),
                reusable,
                executable,
            })?;
        }
    }
    let installations = file_installations
        .finish()?
        .stream()?
        .map_ok(|file| install_file(volume, file, verify_existing))
        .try_buffer_unordered(transfer_concurrency);
    futures::pin_mut!(installations);
    while let Some(installation) = installations.try_next().await? {
        if let Some(destination) = installation {
            durability.changed_parent(&destination)?;
        }
    }

    segment_install::install(
        volume,
        workspace,
        &grouped.finish()?,
        transfer_concurrency,
        verify_existing,
        &mut durability,
    )
    .await?;
    durability.sync(workspace)?;
    Ok(())
}

#[derive(Deserialize, Serialize)]
struct FileInstallation {
    destination: StoredPath,
    fingerprint: crate::filesystem::ContentRef,
    content: FileExtentMap,
    reusable: Option<FileExtentMap>,
    executable: bool,
}

async fn install_file<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    file: FileInstallation,
    verify_existing: bool,
) -> Result<Option<std::path::PathBuf>, Error> {
    let destination = file.destination.to_path_buf();
    if verify_existing
        && crate::sync::replica::fs::file_matches(&destination, file.fingerprint, file.executable)
            .await?
    {
        return Ok(None);
    }
    materialize_file(
        volume,
        (file.fingerprint, file.content),
        file.reusable,
        &destination,
        file.executable,
    )
    .await?;
    Ok(Some(destination))
}

fn installation_removals(
    root: &Path,
    current: Option<&Namespace<FileExtentMap>>,
    target: &Namespace<FileExtentMap>,
    workspace: &WorkContext,
) -> Result<Spool<StoredPath>, Error> {
    let mut removed = workspace.writer("removed")?;
    let current = match current {
        Some(current) => CurrentPaths::Namespace(current.reader()?),
        None => {
            let mut actual = workspace.writer("actual-paths")?;
            crate::sync::replica::fs::scan_paths(root, &mut actual, &mut removed)?;
            CurrentPaths::Filesystem(
                crate::work::sort(workspace, &actual.finish()?, String::clone)?.reader()?,
            )
        }
    };
    let mut current = current;
    let mut target = target.reader()?;
    let mut left = current.next()?;
    let mut right = target.next()?;
    while let Some(record) = left.as_ref() {
        match right.as_ref().map(|target| record.cmp(&target.path)) {
            None | Some(std::cmp::Ordering::Less) => {
                if !record.is_empty() {
                    removed.write(&StoredPath::from_path(Path::new(record))?)?;
                }
                left = current.next()?;
            }
            Some(std::cmp::Ordering::Equal) => {
                left = current.next()?;
                right = target.next()?;
            }
            Some(std::cmp::Ordering::Greater) => {
                right = target.next()?;
            }
        }
    }
    removed.finish()
}

enum CurrentPaths {
    Namespace(NamespaceReader<FileExtentMap>),
    Filesystem(SpoolReader<String>),
}

impl CurrentPaths {
    fn next(&mut self) -> Result<Option<String>, Error> {
        match self {
            Self::Namespace(reader) => Ok(reader.next()?.map(|record| record.path)),
            Self::Filesystem(reader) => reader.next(),
        }
    }
}

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

//! Ephemeral, bounded lookup for retained content.

use std::cmp::Ordering;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::{StreamExt as _, TryStreamExt as _, future};
use serde::Serialize;
use serde::de::DeserializeOwned;

use opendal::Operator;

use crate::Error;
use crate::ErrorKind;
use crate::filesystem::{ContentRef, NamespaceValue};
use crate::format::{ExtentRef, ExtentRunRef, FileExtentMap};
use crate::volume::Namespace;
use crate::work::{OrderedRead as _, Unique, WorkContext, sort};
use crate::work::{Spool, SpoolWriter};

use super::{ContentLookup, DataAccess};

const ENTRY_BYTES: u64 = 32 + 8 + 8;

/// Content already reachable from the observed or last-common namespace.
///
/// The index belongs to one publication operation and disappears with it. The
/// fixed-width sorted keys provide constant-memory lookup without creating
/// durable local state or retaining the namespace in memory.
#[derive(Clone)]
pub struct ContentReuseLookup {
    inner: Arc<KnownContentInner>,
}

struct KnownContentInner {
    _workspace: WorkContext,
    files: Lookup,
    extents: Lookup,
}

struct Lookup {
    files: Mutex<IndexFiles>,
    entries: u64,
}

struct IndexFiles {
    keys: File,
    values: File,
    keys_path: PathBuf,
    values_path: PathBuf,
}

impl fmt::Debug for ContentReuseLookup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContentReuseLookup")
            .field("files", &self.inner.files.entries)
            .field("extents", &self.inner.extents.entries)
            .finish_non_exhaustive()
    }
}

impl ContentReuseLookup {
    pub(crate) fn build(
        workspace: &WorkContext,
        files: &Spool<(ContentRef, FileExtentMap)>,
        extents: &Spool<(ContentRef, ExtentRef)>,
    ) -> Result<Self, Error> {
        Ok(Self {
            inner: Arc::new(KnownContentInner {
                _workspace: workspace.clone(),
                files: build_lookup(workspace, "known-files", files)?,
                extents: build_lookup(workspace, "known-extents", extents)?,
            }),
        })
    }
}

impl ContentLookup for ContentReuseLookup {
    fn file(&self, content: ContentRef) -> Result<Option<FileExtentMap>, Error> {
        lookup(&self.inner.files, content)
    }

    fn extent(&self, content: ContentRef) -> Result<Option<ExtentRef>, Error> {
        lookup(&self.inner.extents, content)
    }
}

pub(crate) async fn build_known_content<A: DataAccess>(
    access: &A,
    operator: &Operator,
    workspace: &WorkContext,
    stream_concurrency: usize,
    observed: &Namespace<FileExtentMap>,
    base: &Namespace<FileExtentMap>,
    index_extents: bool,
) -> Result<ContentReuseLookup, Error> {
    let mut records = workspace.writer("retained-content")?;
    let mut files = workspace.writer("retained-files")?;
    let mut runs = workspace.writer("retained-file-extent-runs")?;
    let namespaces =
        std::iter::once(observed).chain((base.cursor != observed.cursor).then_some(base));
    for namespace in namespaces {
        let mut namespace = namespace.reader()?;
        while let Some(record) = namespace.next()? {
            let Some(node) = record.value else {
                continue;
            };
            let NamespaceValue::RegularFile { content, data, .. } = node.value else {
                continue;
            };
            files.write(&(content, data.clone()))?;
            if !index_extents {
                continue;
            }
            for run in data.runs() {
                if run.continuation.is_some() {
                    runs.write(run)?;
                } else {
                    let mapping = &run.inline_extent;
                    access.validate_extent(&mapping.extent)?;
                    records.write(&(mapping.extent.content(), mapping.extent.clone()))?;
                }
            }
        }
    }
    let runs = sort(workspace, &runs.finish()?, |run: &ExtentRunRef| {
        run.continuation
            .expect("continued extent run has a stream")
            .object
            .locator
    })?;
    let mut previous: Option<ExtentRunRef> = None;
    {
        let readers = runs
            .stream()?
            .map(|run| {
                run.and_then(|run| {
                    if let Some(previous) = previous.as_ref().filter(|previous| {
                        previous.continuation.map(|tail| tail.object.locator)
                            == run.continuation.map(|tail| tail.object.locator)
                    }) {
                        if *previous != run {
                            return Err(Error::corrupt(
                                "read Managed retained content",
                                "one extent segment has conflicting stream references",
                            ));
                        }
                        return Ok(None);
                    }
                    previous = Some(run.clone());
                    Ok(Some(read_extent_run(access, operator, workspace, run)))
                })
                .transpose()
            })
            .filter_map(future::ready)
            .try_buffer_unordered(stream_concurrency);
        futures::pin_mut!(readers);
        while let Some(extents) = readers.try_next().await? {
            append_extents(&mut records, &extents)?;
        }
    }
    let records = sort(workspace, &records.finish()?, |(content, extent)| {
        (
            *content,
            extent.stored_range.segment,
            extent.stored_range.offset,
        )
    })?;
    let files = sort(workspace, &files.finish()?, |(content, _)| *content)?;
    ContentReuseLookup::build(workspace, &files, &records)
}

async fn read_extent_run<A: DataAccess>(
    access: &A,
    operator: &Operator,
    workspace: &WorkContext,
    run: ExtentRunRef,
) -> Result<Spool<(ContentRef, ExtentRef)>, Error> {
    let mut output = workspace.writer("retained-file-extents")?;
    let mut source = super::extent::open_extent_run(&run, operator).await?;
    while let Some(mapping) = source.next().await? {
        access.validate_extent(&mapping.extent)?;
        output.write(&(mapping.extent.content(), mapping.extent))?;
    }
    output.finish()
}

fn append_extents(
    output: &mut SpoolWriter<(ContentRef, ExtentRef)>,
    extents: &Spool<(ContentRef, ExtentRef)>,
) -> Result<(), Error> {
    let mut extents = extents.reader()?;
    while let Some(extent) = extents.next()? {
        output.write(&extent)?;
    }
    Ok(())
}

fn build_lookup<T: Serialize + DeserializeOwned>(
    workspace: &WorkContext,
    stem: &str,
    sorted: &Spool<(ContentRef, T)>,
) -> Result<Lookup, Error> {
    let keys_path = workspace.path().join(format!("{stem}.keys"));
    let values_path = workspace.path().join(format!("{stem}.values"));
    let mut keys = BufWriter::new(create_file(&keys_path)?);
    let mut values = BufWriter::new(create_file(&values_path)?);
    let mut source = Unique::new(sorted.reader()?, |(content, _): &(ContentRef, T)| *content);
    let mut entries = 0_u64;
    let mut value_offset = 0_u64;
    while let Some((content, value)) = source.next()? {
        let mut encoded = Vec::new();
        ciborium::into_writer(&value, &mut encoded).map_err(|_| {
            Error::invalid("build Managed content index", "value cannot be encoded")
        })?;
        let encoded_length = u32::try_from(encoded.len()).map_err(|_| {
            Error::invalid("build Managed content index", "value record is too large")
        })?;
        keys.write_all(content.digest().as_bytes())
            .and_then(|()| keys.write_all(&content.length().to_be_bytes()))
            .and_then(|()| keys.write_all(&value_offset.to_le_bytes()))
            .map_err(|error| io_error("write", &keys_path, error))?;
        values
            .write_all(&encoded_length.to_le_bytes())
            .and_then(|()| values.write_all(&encoded))
            .map_err(|error| io_error("write", &values_path, error))?;
        value_offset = value_offset
            .checked_add(4 + encoded.len() as u64)
            .ok_or_else(|| {
                Error::invalid("build Managed content index", "index length overflows")
            })?;
        entries = entries.checked_add(1).ok_or_else(|| {
            Error::invalid("build Managed content index", "entry count overflows")
        })?;
    }
    keys.flush()
        .map_err(|error| io_error("flush", &keys_path, error))?;
    values
        .flush()
        .map_err(|error| io_error("flush", &values_path, error))?;
    drop(keys);
    drop(values);
    Ok(Lookup {
        files: Mutex::new(IndexFiles {
            keys: File::open(&keys_path).map_err(|error| io_error("open", &keys_path, error))?,
            values: File::open(&values_path)
                .map_err(|error| io_error("open", &values_path, error))?,
            keys_path,
            values_path,
        }),
        entries,
    })
}

fn lookup<T: DeserializeOwned>(lookup: &Lookup, content: ContentRef) -> Result<Option<T>, Error> {
    let mut files = lookup.files.lock().map_err(|_| {
        Error::new(
            ErrorKind::Unavailable,
            "read Managed content index",
            "index lock is poisoned",
        )
    })?;
    let mut low = 0_u64;
    let mut high = lookup.entries;
    while low < high {
        let middle = low + (high - low) / 2;
        let offset = middle
            .checked_mul(ENTRY_BYTES)
            .ok_or_else(|| Error::corrupt("read Managed content index", "key offset overflows"))?;
        files
            .keys
            .seek(SeekFrom::Start(offset))
            .map_err(|error| io_error("read", &files.keys_path, error))?;
        let mut entry = [0_u8; ENTRY_BYTES as usize];
        files
            .keys
            .read_exact(&mut entry)
            .map_err(|error| io_error("read", &files.keys_path, error))?;
        match compare_key(content, &entry) {
            Ordering::Less => high = middle,
            Ordering::Greater => low = middle + 1,
            Ordering::Equal => {
                let value_offset =
                    u64::from_le_bytes(entry[40..48].try_into().expect("fixed index entry"));
                return read_value(&mut files, value_offset).map(Some);
            }
        }
    }
    Ok(None)
}

fn create_file(path: &std::path::Path) -> Result<File, Error> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| io_error("create", path, error))
}

fn compare_key(content: ContentRef, entry: &[u8; 48]) -> Ordering {
    content
        .digest()
        .as_bytes()
        .as_slice()
        .cmp(&entry[..32])
        .then_with(|| {
            content.length().cmp(&u64::from_be_bytes(
                entry[32..40].try_into().expect("fixed index entry"),
            ))
        })
}

fn read_value<T: DeserializeOwned>(files: &mut IndexFiles, offset: u64) -> Result<T, Error> {
    files
        .values
        .seek(SeekFrom::Start(offset))
        .map_err(|error| io_error("read", &files.values_path, error))?;
    let mut length = [0_u8; 4];
    files
        .values
        .read_exact(&mut length)
        .map_err(|error| io_error("read", &files.values_path, error))?;
    let mut encoded = vec![0_u8; u32::from_le_bytes(length) as usize];
    files
        .values
        .read_exact(&mut encoded)
        .map_err(|error| io_error("read", &files.values_path, error))?;
    let mut input = encoded.as_slice();
    let value = ciborium::from_reader(&mut input)
        .map_err(|_| Error::corrupt("read Managed content index", "value record is invalid"))?;
    if !input.is_empty() {
        return Err(Error::corrupt(
            "read Managed content index",
            "value record has trailing bytes",
        ));
    }
    Ok(value)
}

fn io_error(action: &'static str, path: &std::path::Path, error: std::io::Error) -> Error {
    Error::from_io("access Managed content index", Some(path), error).with_context("action", action)
}

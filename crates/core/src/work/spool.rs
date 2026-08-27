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

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read as _, Write as _};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;
use crate::filesystem::OperationId;
use crate::work::OrderedRead;

struct SpoolFile {
    _workspace: Arc<tempfile::TempDir>,
    path: PathBuf,
}

impl Drop for SpoolFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) struct Spool<T> {
    file: Arc<SpoolFile>,
    marker: PhantomData<T>,
}

impl<T> Clone for Spool<T> {
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            marker: PhantomData,
        }
    }
}

impl<T: DeserializeOwned> Spool<T> {
    pub(crate) fn reader(&self) -> Result<SpoolReader<T>, Error> {
        SpoolReader::open(self.file.clone())
    }
}

pub(crate) struct SpoolWriter<T> {
    workspace: Option<Arc<tempfile::TempDir>>,
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    marker: PhantomData<T>,
}

impl<T> SpoolWriter<T> {
    pub(super) fn create(workspace: Arc<tempfile::TempDir>, stem: &str) -> Result<Self, Error> {
        let path = workspace
            .path()
            .join(format!("{stem}-{}", OperationId::generate()));
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| Error::from_io("create Sync record stream", Some(&path), error))?;
        Ok(Self {
            workspace: Some(workspace),
            path,
            writer: Some(BufWriter::new(file)),
            marker: PhantomData,
        })
    }
}

impl<T> Drop for SpoolWriter<T> {
    fn drop(&mut self) {
        drop(self.writer.take());
        if self.workspace.is_some() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl<T: Serialize> SpoolWriter<T> {
    pub(crate) fn write(&mut self, value: &T) -> Result<(), Error> {
        self.write_sized(value).map(drop)
    }

    pub(crate) fn write_sized(&mut self, value: &T) -> Result<usize, Error> {
        let mut bytes = Vec::new();
        ciborium::into_writer(value, &mut bytes)
            .map_err(|_| Error::invalid("write Sync record stream", "record cannot be encoded"))?;
        let length = bytes.len();
        self.write_frame(&bytes)?;
        Ok(length)
    }

    pub(super) fn write_frame(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let length = u32::try_from(bytes.len())
            .map_err(|_| Error::invalid("write Sync record stream", "record is too large"))?;
        let writer = self.writer.as_mut().expect("unfinished Sync record writer");
        writer
            .write_all(&length.to_le_bytes())
            .and_then(|()| writer.write_all(bytes))
            .map_err(|error| Error::from_io("write Sync record stream", Some(&self.path), error))?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<Spool<T>, Error> {
        let mut writer = self.writer.take().expect("unfinished Sync record writer");
        writer.flush().map_err(|error| {
            Error::from_io("finish Sync record stream", Some(&self.path), error)
        })?;
        drop(writer);
        let workspace = self
            .workspace
            .take()
            .expect("unfinished Sync record writer");
        Ok(Spool {
            file: Arc::new(SpoolFile {
                _workspace: workspace,
                path: self.path.clone(),
            }),
            marker: PhantomData,
        })
    }
}

pub(crate) struct SpoolReader<T> {
    file: Arc<SpoolFile>,
    reader: BufReader<File>,
    pending_length: Option<usize>,
    marker: PhantomData<T>,
}

impl<T: DeserializeOwned> SpoolReader<T> {
    fn open(spool: Arc<SpoolFile>) -> Result<Self, Error> {
        let file = File::open(&spool.path)
            .map_err(|error| Error::from_io("open Sync record stream", Some(&spool.path), error))?;
        Ok(Self {
            file: spool,
            reader: BufReader::new(file),
            pending_length: None,
            marker: PhantomData,
        })
    }

    pub(crate) fn next(&mut self) -> Result<Option<T>, Error> {
        let Some(bytes) = self.next_frame()? else {
            return Ok(None);
        };
        decode_record(&bytes).map(Some)
    }

    pub(super) fn next_frame(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let Some(frame_bytes) = self.peek_frame_bytes()? else {
            return Ok(None);
        };
        let length = frame_bytes - size_of::<u32>();
        self.pending_length = None;
        let mut bytes = vec![0; length];
        self.reader.read_exact(&mut bytes).map_err(|error| {
            Error::from_io("read Sync record stream", Some(&self.file.path), error)
        })?;
        Ok(Some(bytes))
    }

    pub(super) fn peek_frame_bytes(&mut self) -> Result<Option<usize>, Error> {
        if let Some(length) = self.pending_length {
            return length
                .checked_add(size_of::<u32>())
                .map(Some)
                .ok_or_else(|| {
                    Error::corrupt("read Sync record stream", "record length overflows")
                });
        }
        let mut length = [0_u8; size_of::<u32>()];
        let first = self.reader.read(&mut length[..1]).map_err(|error| {
            Error::from_io("read Sync record stream", Some(&self.file.path), error)
        })?;
        if first == 0 {
            return Ok(None);
        }
        self.reader.read_exact(&mut length[1..]).map_err(|error| {
            Error::from_io("read Sync record stream", Some(&self.file.path), error)
        })?;
        let length = u32::from_le_bytes(length) as usize;
        self.pending_length = Some(length);
        length
            .checked_add(size_of::<u32>())
            .map(Some)
            .ok_or_else(|| Error::corrupt("read Sync record stream", "record length overflows"))
    }
}

impl<T: DeserializeOwned> OrderedRead for SpoolReader<T> {
    type Item = T;

    fn next(&mut self) -> Result<Option<Self::Item>, Error> {
        SpoolReader::next(self)
    }
}

pub(super) fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    let mut input = std::io::Cursor::new(bytes);
    let value = ciborium::from_reader(&mut input)
        .map_err(|_| Error::corrupt("read Sync record stream", "record is invalid"))?;
    if input.position() != bytes.len() as u64 {
        return Err(Error::corrupt(
            "read Sync record stream",
            "record has trailing bytes",
        ));
    }
    Ok(value)
}

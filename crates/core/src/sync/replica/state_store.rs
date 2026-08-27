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

//! Atomic persistence for the lightweight replica recovery record.

use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use crate::Error;
use crate::format::RecordCodec;

use crate::sync::state::ReplicaState;

const STATE_RECORD: RecordCodec = RecordCodec::new(*b"OFSSTA00", 16 * 1024);

pub(crate) struct ReplicaStateFile {
    path: PathBuf,
    exists: bool,
}

impl ReplicaStateFile {
    pub(crate) fn open(path: &Path) -> Result<(Self, Option<ReplicaState>), Error> {
        let state = load(path)?;
        Ok((
            Self {
                path: path.to_owned(),
                exists: state.is_some(),
            },
            state,
        ))
    }

    pub(crate) fn persist(&mut self, state: &ReplicaState) -> Result<(), Error> {
        write(state, &self.path, self.exists)?;
        self.exists = true;
        Ok(())
    }
}

pub(crate) fn load(path: &Path) -> Result<Option<ReplicaState>, Error> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_io("read replica state", Some(path), error)),
    };
    let state: ReplicaState = STATE_RECORD.decode(&bytes)?;
    state.validate()?;
    Ok(Some(state))
}

fn write(state: &ReplicaState, path: &Path, replace: bool) -> Result<(), Error> {
    state.validate()?;
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(directory).map_err(|error| {
        Error::from_io("create replica state directory", Some(directory), error)
    })?;
    let bytes = STATE_RECORD.encode(state)?;
    let mut temporary = tempfile::NamedTempFile::new_in(directory)
        .map_err(|error| Error::from_io("create replica state", Some(directory), error))?;
    temporary
        .write_all(&bytes)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|error| Error::from_io("persist replica state", Some(path), error))?;
    let installed = if replace {
        temporary.persist(path)
    } else {
        temporary.persist_noclobber(path)
    };
    installed.map_err(|error| {
        if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            Error::invalid(
                "synchronize replica",
                format!(
                    "cannot attach with an existing replica state: {}",
                    path.display()
                ),
            )
        } else {
            Error::from_io("install replica state", Some(path), error.error)
        }
    })?;
    sync_parent(directory)
}

#[cfg(unix)]
fn sync_parent(directory: &Path) -> Result<(), Error> {
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::from_io("persist replica state directory", Some(directory), error))
}

#[cfg(not(unix))]
fn sync_parent(_directory: &Path) -> Result<(), Error> {
    Ok(())
}

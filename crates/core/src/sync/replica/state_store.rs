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

use std::fs;
use std::path::Path;

use crate::Error;
use crate::format::RecordCodec;

use crate::sync::state::ReplicaState;

const STATE_RECORD: RecordCodec = RecordCodec::new(*b"OFSSTA00", 16 * 1024);

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

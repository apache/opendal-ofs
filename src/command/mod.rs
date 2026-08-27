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

mod gc;
mod operator;
mod status;
mod sync;
mod volume;

use anyhow::Result;
use ofs_core::{CoreAccess, ManagedVolume};

use crate::cli::{Cli, Command, RuntimeArgs};
use crate::locator::VolumeLocator;

use operator::open_storage;

pub(crate) async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Gc(args) => gc::run(args).await,
        Command::Sync(args) => sync::run(args).await,
        Command::Status(args) => status::run(args),
        Command::Volume(args) => volume::run(args).await,
    }
}

async fn open_named_volume(name: &str, runtime: &RuntimeArgs) -> Result<ManagedVolume<CoreAccess>> {
    let record = VolumeLocator::from_env()?.resolve(name)?;
    let operator = open_storage(&record.storage)?;
    let runtime = runtime.volume_runtime().map_err(anyhow::Error::msg)?;
    ManagedVolume::open(&operator, CoreAccess::default(), runtime, "main")
        .await
        .map_err(anyhow::Error::msg)
}

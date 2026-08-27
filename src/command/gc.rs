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

use anyhow::Result;

use crate::cli::GcArgs;

pub(super) async fn run(args: GcArgs) -> Result<()> {
    let volume = super::open_named_volume(&args.volume, &args.runtime).await?;
    let outcome = volume.collect().await.map_err(anyhow::Error::msg)?;
    println!(
        "collected managed volume {}: scanned {}, deleted {} object(s), {} byte(s)",
        volume.id(),
        outcome.scanned,
        outcome.deleted,
        outcome.deleted_bytes
    );
    Ok(())
}

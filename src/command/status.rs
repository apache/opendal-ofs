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

use anyhow::{Context, Result, bail};
use ofs_core::sync::load_replica_state;

use crate::cli::StatusArgs;

pub(super) fn run(args: StatusArgs) -> Result<()> {
    let root = fs::canonicalize(&args.replica)
        .with_context(|| format!("cannot open replica directory: {}", args.replica.display()))?;
    let state = load_replica_state(&args.state)
        .map_err(anyhow::Error::msg)?
        .ok_or_else(|| anyhow::anyhow!("replica state does not exist: {}", args.state.display()))?;
    if state.root() != root {
        bail!("replica state belongs to a different local directory");
    }
    if args.json {
        println!(
            "{}",
            serde_json::json!({
                "access_model": "sync",
                "base_expired": state.base_expired(),
                "conflicts": state.conflict_count(),
                "common_sequence": state.common_revision().cursor().sequence(),
                "pending": state.has_pending(),
                "remote_sequence": state.remote_cursor().sequence(),
                "volume_id": state.volume_id().to_string(),
                "volume_model": "managed",
                "capabilities": {
                    "executable": cfg!(unix),
                    "extended_attributes": false,
                    "hard_links": false,
                    "namespace_publication": "generation_cas",
                    "portable_names": true,
                    "remote_durability": "explicit_sync",
                    "stable_rename_identity": false,
                    "symbolic_links": false,
                },
            })
        );
    } else {
        print!(
            "managed sync volume {} at change {}, {} pending, {} conflict(s)",
            state.volume_id(),
            state.common_revision().cursor().sequence(),
            usize::from(state.has_pending()),
            state.conflict_count()
        );
        if state.base_expired() {
            print!(", base expiration detected");
        }
        println!();
    }
    Ok(())
}

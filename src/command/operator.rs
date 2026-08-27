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

use anyhow::{Result, anyhow};
use opendal::Operator;
use opendal::layers::{RetryLayer, TimeoutLayer};

pub(super) fn open_storage(storage: &str) -> Result<Operator> {
    Operator::from_uri(fs_uri(storage))
        .map(|operator| {
            operator
                .layer(TimeoutLayer::new())
                .layer(RetryLayer::new().with_jitter())
        })
        .map_err(|error| {
            anyhow!(
                "cannot configure --storage ({}); check its scheme, endpoint, bucket, and root",
                error.kind()
            )
        })
}

/// OpenDAL's `fs` URI parser prefixes the path with `/`.
///
/// `fs:///C:/Users/...` therefore becomes `/C:/Users/...`, which Windows cannot
/// canonicalize. Pass the drive path as `?root=` instead.
fn fs_uri(storage: &str) -> String {
    let Some(rest) = storage
        .strip_prefix("fs://")
        .or_else(|| storage.strip_prefix("file://"))
    else {
        return storage.to_owned();
    };
    if rest.contains("root=") {
        return storage.to_owned();
    }
    let path = rest
        .split_once('?')
        .map(|(path, _)| path)
        .unwrap_or(rest)
        .replace('\\', "/");
    let path = path.trim_start_matches('/');
    if path.len() >= 2 && path.as_bytes()[0].is_ascii_alphabetic() && path.as_bytes()[1] == b':' {
        return format!("fs:///?root={path}");
    }
    storage.to_owned()
}

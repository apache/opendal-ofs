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

use ofs_core::format::{DEFAULT_DATA_SEGMENT_TARGET_BYTES, FileDataLayout};
use ofs_core::{CoreAccess, CreateOptions, ErrorKind, ManagedVolume, Result, VolumeRuntime};
use opendal::Operator;
use opendal::services::Fs;

fn fs_operator(root: &std::path::Path) -> Operator {
    Operator::new(Fs::default().root(root.to_string_lossy().as_ref()))
        .expect("filesystem service configuration is valid")
        .finish()
}

#[tokio::test]
async fn rejects_storage_without_conditional_publication() -> Result<()> {
    let storage = tempfile::tempdir().unwrap();
    let operator = fs_operator(storage.path());
    let options = CreateOptions::new(FileDataLayout::whole_identity(
        DEFAULT_DATA_SEGMENT_TARGET_BYTES,
    )?);
    let error = ManagedVolume::create(
        &operator,
        options,
        CoreAccess::default(),
        VolumeRuntime::standard(),
        "main",
    )
    .await
    .err()
    .expect("filesystem storage has no conditional replace capability");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    Ok(())
}

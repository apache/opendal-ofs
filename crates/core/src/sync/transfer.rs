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

use std::path::Path;

use tokio::fs::File;

use crate::Error;
use crate::data::ContentHasher;
use crate::data::ReusableFile;
use crate::filesystem::ContentRef;
use crate::format::FileExtentMap;
use crate::volume::AccessFamily;
use crate::volume::ManagedVolume;

pub(super) async fn materialize_file<A: AccessFamily>(
    volume: &ManagedVolume<A>,
    content: (ContentRef, FileExtentMap),
    reusable: Option<FileExtentMap>,
    destination: &Path,
    executable: bool,
) -> Result<(), Error> {
    let expected = content.0;
    crate::sync::replica::fs::install_file(destination, executable, async |destination_file| {
        let mut source = match reusable {
            Some(reference) => Some((
                reference,
                File::open(destination).await.map_err(|error| {
                    Error::from_io("open reusable replica file", Some(destination), error)
                })?,
            )),
            None => None,
        };
        let reusable = source.as_mut().map(|(reference, source)| ReusableFile {
            reference: reference.clone(),
            source,
        });
        let mut materialized = ContentHasher::default();
        {
            let mut writer = tokio_util::io::InspectWriter::new(destination_file, |bytes| {
                materialized.observe(bytes)
            });
            volume.read_data(content, .., reusable, &mut writer).await?;
        }
        if materialized.content() == expected {
            Ok(())
        } else {
            Err(Error::conflict(
                "install Managed file",
                "materialized bytes do not match the target content",
            ))
        }
    })
    .await
    .map_err(|error| error.with_context("path", destination.display()))
}

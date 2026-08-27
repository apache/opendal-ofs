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

//! Local volume name to storage URL resolver.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use ofs_core::filesystem::validate_volume_name;

use crate::cli::VolumeModel;

/// One locally registered volume.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VolumeRecord {
    pub(crate) name: String,
    pub(crate) model: VolumeModel,
    pub(crate) storage: String,
}

pub(crate) struct VolumeLocator {
    home: PathBuf,
}

impl VolumeLocator {
    pub(crate) fn from_env() -> Result<Self> {
        if let Ok(home) = std::env::var("OFS_HOME") {
            return Ok(Self {
                home: PathBuf::from(home),
            });
        }
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .ok_or_else(|| anyhow!("cannot determine OFS home; set OFS_HOME"))?;
        Ok(Self {
            home: PathBuf::from(home).join(".ofs"),
        })
    }

    pub(crate) fn create(&self, name: &str, model: VolumeModel, storage: &str) -> Result<()> {
        validate_volume_name(name).map_err(anyhow::Error::msg)?;
        if storage.trim().is_empty() {
            bail!("--storage is empty");
        }
        fs::create_dir_all(self.volumes_dir()).with_context(|| {
            format!(
                "cannot create volume directory {}",
                self.volumes_dir().display()
            )
        })?;
        let path = self.record_path(name);
        if path.exists() {
            let existing = self.resolve(name)?;
            if existing.model != model || existing.storage != storage {
                bail!("volume {name} already exists with a different storage locator");
            }
            return Ok(());
        }
        let body = format!("model=managed\nstorage={storage}\n");
        fs::write(&path, body)
            .with_context(|| format!("cannot write volume locator {}", path.display()))
    }

    pub(crate) fn resolve(&self, name: &str) -> Result<VolumeRecord> {
        validate_volume_name(name).map_err(anyhow::Error::msg)?;
        let path = self.record_path(name);
        let body = fs::read_to_string(&path).with_context(|| {
            format!(
                "cannot read volume locator {}; create the volume first",
                path.display()
            )
        })?;
        parse_record(name, &body)
    }

    fn volumes_dir(&self) -> PathBuf {
        self.home.join("volumes")
    }

    fn record_path(&self, name: &str) -> PathBuf {
        self.volumes_dir().join(name)
    }
}

fn parse_record(name: &str, body: &str) -> Result<VolumeRecord> {
    let mut model = None;
    let mut storage = None;
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            bail!("volume locator has an invalid line");
        };
        match key {
            "model" if value == "managed" => model = Some(VolumeModel::Managed),
            "storage" => storage = Some(value.to_owned()),
            _ => bail!("volume locator has an unknown field {key}"),
        }
    }
    Ok(VolumeRecord {
        name: name.to_owned(),
        model: model.ok_or_else(|| anyhow!("volume locator is missing model"))?,
        storage: storage.ok_or_else(|| anyhow!("volume locator is missing storage"))?,
    })
}

pub(crate) fn model_name(_model: VolumeModel) -> &'static str {
    "managed"
}

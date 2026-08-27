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

//! Read native filesystem facts into a path-ordered record stream.

use std::path::Path;

use serde::{Deserialize, Serialize};
use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

use crate::Error;
use crate::filesystem::{ContentRef, NodeKind, validate_portable_path};
use crate::format::FileExtentMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct LocalEntry {
    pub(crate) path: String,
    pub(crate) kind: NodeKind,
    pub(crate) executable: bool,
    pub(crate) length: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct LocalFile {
    pub(crate) content: ContentRef,
    pub(crate) data: FileExtentMap,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct LocalRecord {
    pub(crate) path: String,
    pub(crate) kind: NodeKind,
    pub(crate) executable: bool,
    pub(crate) file: Option<LocalFile>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PortableName {
    parent: String,
    folded: String,
}

pub(crate) fn scan(
    workspace: &crate::work::WorkContext,
    root: &Path,
) -> Result<crate::work::Spool<LocalEntry>, Error> {
    let metadata = std::fs::symlink_metadata(root)
        .map_err(|error| Error::from_io("inspect local path", Some(root), error))?;
    if !metadata.is_dir() {
        return Err(Error::invalid(
            "scan replica",
            "local replica root is not a directory",
        ));
    }

    let mut records = workspace.writer("local-paths")?;
    records.write(&LocalEntry {
        path: String::new(),
        kind: NodeKind::Directory,
        executable: false,
        length: None,
    })?;
    let mut portable_names = workspace.writer("portable-names")?;
    for child in crate::sync::replica::fs::entries(root) {
        let child = child?;
        let name = child.file_name().to_str().ok_or_else(|| {
            Error::invalid(
                "synchronize replica",
                "local directory contains a non-Unicode name",
            )
        })?;
        let child_path = child.path();
        let relative = child_path
            .strip_prefix(root)
            .expect("walked replica path is below its root");
        let path = relative
            .to_str()
            .ok_or_else(|| {
                Error::invalid(
                    "synchronize replica",
                    "local directory contains a non-Unicode path",
                )
            })?
            .replace(std::path::MAIN_SEPARATOR, "/");
        validate_portable_path(&path)?;
        let parent = path.rsplit_once('/').map_or("", |(parent, _)| parent);
        portable_names.write(&PortableName {
            parent: parent.to_owned(),
            folded: name.case_fold().nfc().collect(),
        })?;

        let metadata = crate::sync::replica::fs::entry_metadata(&child)?;
        let (kind, executable) = local_entry(&metadata)?;
        let record = LocalEntry {
            path: path.clone(),
            kind,
            executable,
            length: (kind == NodeKind::RegularFile).then_some(metadata.len()),
        };
        records.write(&record)?;
    }
    validate_portable_names(workspace, portable_names.finish()?)?;
    let records = records.finish()?;
    crate::work::sort(workspace, &records, |record: &LocalEntry| {
        record.path.clone()
    })
}

fn validate_portable_names(
    workspace: &crate::work::WorkContext,
    names: crate::work::Spool<PortableName>,
) -> Result<(), Error> {
    let names = crate::work::sort(workspace, &names, |name: &PortableName| {
        (name.parent.clone(), name.folded.clone())
    })?;
    let mut reader = names.reader()?;
    let mut previous = None;
    while let Some(name) = reader.next()? {
        let key = (name.parent, name.folded);
        if previous.as_ref() == Some(&key) {
            return Err(Error::invalid(
                "synchronize replica",
                "directory contains a case-folding collision",
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

#[cfg(unix)]
fn local_entry(metadata: &std::fs::Metadata) -> Result<(NodeKind, bool), Error> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    if metadata.is_dir() {
        return Ok((NodeKind::Directory, false));
    }
    if metadata.is_file() {
        if metadata.nlink() > 1 {
            return Err(Error::unsupported(
                "scan replica",
                "local replica contains a hard-linked file, which Managed Sync does not support",
            ));
        }
        return Ok((
            NodeKind::RegularFile,
            metadata.permissions().mode() & 0o111 != 0,
        ));
    }
    Err(Error::unsupported(
        "scan replica",
        "local replica contains a symbolic link or special file",
    ))
}

#[cfg(not(unix))]
fn local_entry(metadata: &std::fs::Metadata) -> Result<(NodeKind, bool), Error> {
    if metadata.is_dir() {
        Ok((NodeKind::Directory, false))
    } else if metadata.is_file() {
        Ok((NodeKind::RegularFile, false))
    } else {
        Err(Error::unsupported(
            "scan replica",
            "local replica contains a symbolic link or special file",
        ))
    }
}

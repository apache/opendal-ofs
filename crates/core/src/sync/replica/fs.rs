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

//! Native local-filesystem operations used while installing a namespace.

use std::cmp::Reverse;
#[cfg(any(unix, windows))]
use std::ffi::OsString;
use std::fs::File;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs::File as AsyncFile;
use tokio::io::BufReader;

use crate::Error;
use crate::data::ContentHasher;
use crate::filesystem::ContentRef;
use crate::work::{OrderedRead as _, Unique};
use crate::work::{SpoolWriter, WorkContext};

const IO_BUFFER_BYTES: usize = 256 * 1024;

pub(crate) async fn file_matches(
    path: &Path,
    expected: ContentRef,
    executable: bool,
) -> Result<bool, Error> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(Error::from_io("inspect replica file", Some(path), error)),
    };
    if !supported_regular_file(&metadata) || is_executable(&metadata) != executable {
        return Ok(false);
    }
    if metadata.len() != expected.length() {
        return Ok(false);
    }
    Ok(hash_file(path).await? == expected)
}

async fn hash_file(path: &Path) -> Result<ContentRef, Error> {
    let file = AsyncFile::open(path)
        .await
        .map_err(|error| Error::from_io("verify local file", Some(path), error))?;
    let mut content = ContentHasher::default();
    {
        let reader = tokio_util::io::InspectReader::new(file, |bytes| content.observe(bytes));
        let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, reader);
        tokio::io::copy_buf(&mut reader, &mut tokio::io::sink())
            .await
            .map_err(|error| Error::from_io("verify local file", Some(path), error))?;
    }
    content
        .complete_content()
        .ok_or_else(|| Error::conflict("verify local file", "local file was not read completely"))
}

pub(crate) fn entries(root: &Path) -> impl Iterator<Item = Result<walkdir::DirEntry, Error>> + '_ {
    walkdir::WalkDir::new(root)
        .min_depth(1)
        .follow_links(false)
        .into_iter()
        .map(|entry| entry.map_err(|error| walk_error("walk replica directory", error)))
}

pub(crate) fn entry_metadata(entry: &walkdir::DirEntry) -> Result<std::fs::Metadata, Error> {
    entry
        .metadata()
        .map_err(|error| walk_error("inspect local path", error))
}

fn walk_error(operation: &'static str, error: walkdir::Error) -> Error {
    let path = error.path().map(Path::to_path_buf);
    let source = error
        .into_io_error()
        .unwrap_or_else(|| std::io::Error::other("directory traversal failed"));
    Error::from_io(operation, path.as_deref(), source)
}

/// One same-directory staging file committed by atomic rename.
struct StagedFile {
    destination: PathBuf,
    temporary: tempfile::TempPath,
    file: Option<AsyncFile>,
}

impl StagedFile {
    pub(crate) async fn create(destination: &Path) -> Result<Self, Error> {
        if path_metadata(destination)?.is_some_and(|metadata| metadata.is_dir()) {
            remove_replaced_directory(destination)?;
        }
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| Error::from_io("create replica directory", Some(parent), error))?;
        let staging = tempfile::NamedTempFile::new_in(parent)
            .map_err(|error| Error::from_io("create replica staging file", Some(parent), error))?;
        let (file, temporary) = staging.into_parts();
        Ok(Self {
            destination: destination.to_owned(),
            temporary,
            file: Some(AsyncFile::from_std(file)),
        })
    }

    async fn commit(mut self, executable: bool) -> Result<(), Error> {
        drop(self.file.take());
        tokio::fs::rename(&self.temporary, &self.destination)
            .await
            .map_err(|error| {
                Error::from_io("install replica file", Some(&self.destination), error)
            })?;
        if executable {
            make_executable(&self.destination)?;
        }
        Ok(())
    }
}

pub(crate) async fn install_file(
    destination: &Path,
    executable: bool,
    write: impl AsyncFnOnce(&mut AsyncFile) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut staging = StagedFile::create(destination).await?;
    write(staging.file.as_mut().expect("staging file is open")).await?;
    staging.commit(executable).await
}

pub(crate) fn scan_paths(
    root: &Path,
    actual: &mut SpoolWriter<String>,
    removed: &mut SpoolWriter<StoredPath>,
) -> Result<(), Error> {
    for child in entries(root) {
        let child = child?;
        let path = child.path();
        let relative = path
            .strip_prefix(root)
            .expect("walked installation path is below its root");
        match portable_replica_path(relative) {
            Some(path) => {
                actual.write(&path)?;
            }
            None => {
                removed.write(&StoredPath::from_path(relative)?)?;
            }
        }
    }
    Ok(())
}

fn portable_replica_path(path: &Path) -> Option<String> {
    let path = path.to_str()?;
    #[cfg(windows)]
    let path = path.replace('\\', "/");
    Some(path.to_owned())
}

pub(crate) fn create_directory(
    path: &Path,
    durability: &mut DirectoryDurability,
) -> Result<(), Error> {
    let mut missing = Vec::new();
    let mut candidate = path;
    while !candidate.exists() {
        missing.push(candidate.to_owned());
        let Some(parent) = candidate.parent() else {
            break;
        };
        candidate = parent;
    }
    std::fs::create_dir_all(path)
        .map_err(|error| Error::from_io("create replica directory", Some(path), error))?;
    for directory in missing {
        durability.record(&directory)?;
        durability.changed_parent(&directory)?;
    }
    Ok(())
}

pub(crate) struct DirectoryDurability {
    directories: SpoolWriter<StoredPath>,
}

impl DirectoryDurability {
    pub(crate) fn create(workspace: &WorkContext) -> Result<Self, Error> {
        Ok(Self {
            directories: workspace.writer("durability")?,
        })
    }

    fn record(&mut self, path: &Path) -> Result<(), Error> {
        self.directories.write(&StoredPath::from_path(path)?)?;
        Ok(())
    }

    pub(crate) fn changed_parent(&mut self, path: &Path) -> Result<(), Error> {
        if let Some(parent) = path.parent() {
            self.record(parent)?;
        }
        Ok(())
    }

    pub(crate) fn sync(self, workspace: &WorkContext) -> Result<(), Error> {
        let directories = crate::work::sort(workspace, &self.directories.finish()?, |path| {
            Reverse(path.clone())
        })?;
        let mut directories = Unique::new(directories.reader()?, Clone::clone);
        while let Some(directory) = directories.next()? {
            let directory = directory.to_path_buf();
            if path_metadata(&directory)?.is_none_or(|metadata| !metadata.is_dir()) {
                continue;
            }
            File::open(&directory)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    Error::from_io("persist replica directory", Some(&directory), error)
                })?;
        }
        Ok(())
    }
}

pub(crate) fn remove_path(path: &Path) -> Result<(), Error> {
    let Some(metadata) = path_metadata(path)? else {
        return Ok(());
    };
    if metadata.is_dir() {
        std::fs::remove_dir_all(path)
            .map_err(|error| Error::from_io("remove replica directory", Some(path), error))
    } else {
        std::fs::remove_file(path)
            .map_err(|error| Error::from_io("remove replica file", Some(path), error))
    }
}

pub(crate) fn remove_replaced_directory(path: &Path) -> Result<(), Error> {
    std::fs::remove_dir_all(path)
        .map_err(|error| Error::from_io("replace replica directory", Some(path), error))
}

pub(crate) fn path_metadata(path: &Path) -> Result<Option<std::fs::Metadata>, Error> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Error::from_io("inspect replica path", Some(path), error)),
    }
}

#[cfg(unix)]
fn supported_regular_file(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    metadata.is_file() && metadata.nlink() == 1
}

#[cfg(not(unix))]
fn supported_regular_file(metadata: &std::fs::Metadata) -> bool {
    metadata.is_file()
}

#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
const fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
pub(crate) fn make_executable(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt as _;

    let mut permissions = std::fs::metadata(path)
        .map_err(|error| Error::from_io("read replica permissions", Some(path), error))?
        .permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    std::fs::set_permissions(path, permissions)
        .map_err(|error| Error::from_io("write replica permissions", Some(path), error))
}

#[cfg(not(unix))]
pub(crate) fn make_executable(_path: &Path) -> Result<(), Error> {
    Err(Error::unsupported(
        "install replica",
        "Managed Sync executable attributes are not implemented on this platform",
    ))
}

#[cfg(unix)]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct StoredPath(Vec<u8>);

#[cfg(unix)]
impl StoredPath {
    pub(crate) fn from_path(path: &Path) -> Result<Self, Error> {
        use std::os::unix::ffi::OsStrExt as _;

        Ok(Self(path.as_os_str().as_bytes().to_vec()))
    }

    pub(crate) fn to_path_buf(&self) -> PathBuf {
        use std::os::unix::ffi::OsStringExt as _;

        PathBuf::from(OsString::from_vec(self.0.clone()))
    }

    pub(crate) const fn memory_bytes(&self) -> usize {
        self.0.len()
    }
}

#[cfg(windows)]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct StoredPath(Vec<u16>);

#[cfg(windows)]
impl StoredPath {
    pub(crate) fn from_path(path: &Path) -> Result<Self, Error> {
        use std::os::windows::ffi::OsStrExt as _;

        Ok(Self(
            path.as_os_str()
                .encode_wide()
                .map(|unit| {
                    if unit == b'\\' as u16 {
                        b'/' as u16
                    } else {
                        unit
                    }
                })
                .collect(),
        ))
    }

    pub(crate) fn to_path_buf(&self) -> PathBuf {
        use std::os::windows::ffi::OsStringExt as _;

        PathBuf::from(OsString::from_wide(&self.0))
    }

    pub(crate) const fn memory_bytes(&self) -> usize {
        self.0.len().saturating_mul(std::mem::size_of::<u16>())
    }
}

#[cfg(not(any(unix, windows)))]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct StoredPath(String);

#[cfg(not(any(unix, windows)))]
impl StoredPath {
    pub(crate) fn from_path(path: &Path) -> Result<Self, Error> {
        path.to_str()
            .map(|path| Self(path.to_owned()))
            .ok_or_else(|| {
                Error::unsupported("record replica path", "platform path is not Unicode")
            })
    }

    pub(crate) fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.0)
    }

    pub(crate) const fn memory_bytes(&self) -> usize {
        self.0.len()
    }
}

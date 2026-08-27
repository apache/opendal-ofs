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

use std::path::{Path, PathBuf};

use crate::Error;
use crate::format::{FileExtentMap, NamespaceRevision};
use crate::volume::{AccessFamily, CoreAccess, ManagedVolume, Namespace};
use crate::work::WorkContext;

use super::ReplicaState;
use super::install::install;
use super::replica::state_store::ReplicaStateFile;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncOutcome {
    pub conflict_paths: Vec<String>,
    pub published: bool,
    pub sequence: u64,
}

impl SyncOutcome {
    fn restored(revision: NamespaceRevision, published: bool) -> Self {
        Self {
            conflict_paths: Vec::new(),
            published,
            sequence: revision.cursor().sequence(),
        }
    }
}

pub struct SyncEngine<A: AccessFamily = CoreAccess> {
    volume: ManagedVolume<A>,
}

impl<A: AccessFamily> SyncEngine<A> {
    pub const fn new(volume: ManagedVolume<A>) -> Self {
        Self { volume }
    }

    /// Attach an empty local directory to a Managed volume and materialize its
    /// current namespace. Interrupted installations resume from durable state.
    pub async fn sync(
        &self,
        root: &Path,
        state_path: &Path,
        resolve_paths: &[String],
    ) -> Result<SyncOutcome, Error> {
        if !resolve_paths.is_empty() {
            return Err(Error::unsupported(
                "restore replica",
                "conflict resolution requires replica convergence",
            ));
        }
        let root = canonical_directory(root)?;
        let workspace_directory = state_path
            .parent()
            .filter(|directory| !directory.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let workspace = WorkContext::create_in(self.volume.work_budget(), workspace_directory)?;
        let (mut state_file, stored) = ReplicaStateFile::open(state_path)?;
        validate_binding(stored.as_ref(), &root, &self.volume)?;

        let requested = stored.as_ref().map(ReplicaState::resume_revision);
        let (observed, loaded) = self
            .volume
            .observe_with_base_in(&workspace, requested)
            .await?;
        if let Some(state) = stored.as_ref()
            && (state.authority_id() != observed.authority_id()
                || state.authority_name() != self.volume.authority_name())
        {
            return Err(Error::invalid(
                "restore replica",
                "replica state belongs to a different namespace authority",
            ));
        }

        if let Some(mut state) = stored {
            let Some((target, published)) = state.installation() else {
                return Err(Error::unsupported(
                    "synchronize replica",
                    "an attached replica requires convergence",
                ));
            };
            let namespace = if observed.revision() == target {
                &observed.namespace
            } else {
                loaded.as_ref().unwrap_or(&observed.namespace)
            };
            return self
                .install_and_advance(
                    &workspace,
                    &root,
                    &mut state_file,
                    &mut state,
                    namespace,
                    if observed.revision() == target {
                        target
                    } else {
                        observed.revision()
                    },
                    published,
                )
                .await;
        }

        require_empty(&root)?;
        let mut state = ReplicaState::new(
            root.clone(),
            self.volume.id(),
            observed.authority_id(),
            self.volume.authority_name().to_owned(),
            observed.revision(),
        );
        self.install_and_advance(
            &workspace,
            &root,
            &mut state_file,
            &mut state,
            &observed.namespace,
            observed.revision(),
            false,
        )
        .await
    }

    async fn install_and_advance(
        &self,
        workspace: &WorkContext,
        root: &Path,
        state_file: &mut ReplicaStateFile,
        state: &mut ReplicaState,
        namespace: &Namespace<FileExtentMap>,
        revision: NamespaceRevision,
        published: bool,
    ) -> Result<SyncOutcome, Error> {
        state.begin_install(revision, published);
        state_file.persist(state)?;
        install(workspace, root, namespace, &self.volume, None).await?;
        state.advance(revision);
        state_file.persist(state)?;
        Ok(SyncOutcome::restored(revision, published))
    }
}

fn canonical_directory(root: &Path) -> Result<PathBuf, Error> {
    let root = std::fs::canonicalize(root)
        .map_err(|error| Error::from_io("open replica directory", Some(root), error))?;
    if !root.is_dir() {
        return Err(Error::invalid(
            "restore replica",
            "replica path is not a directory",
        ));
    }
    Ok(root)
}

fn validate_binding<A: AccessFamily>(
    state: Option<&ReplicaState>,
    root: &Path,
    volume: &ManagedVolume<A>,
) -> Result<(), Error> {
    let Some(state) = state else {
        return Ok(());
    };
    if state.root() != root {
        return Err(Error::invalid(
            "restore replica",
            "replica state belongs to a different local directory",
        ));
    }
    if state.volume_id() != volume.id() {
        return Err(Error::invalid(
            "restore replica",
            "replica state belongs to a different volume",
        ));
    }
    Ok(())
}

fn require_empty(root: &Path) -> Result<(), Error> {
    let mut entries = std::fs::read_dir(root)
        .map_err(|error| Error::from_io("read replica directory", Some(root), error))?;
    if entries.next().is_some() {
        return Err(Error::unsupported(
            "restore replica",
            "initial publication requires local observation",
        ));
    }
    Ok(())
}

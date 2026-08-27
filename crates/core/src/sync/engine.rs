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

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::Error;
use crate::filesystem::ChangeCursor;
use crate::format::{FileExtentMap, NamespaceRevision};
use crate::volume::{AccessFamily, CoreAccess, ManagedVolume, Namespace};
use crate::work::WorkContext;

use super::FileChangeSetEntry;
use super::ReplicaState;
use super::install::install;
use super::replica::state_store::ReplicaStateFile;
use super::scan::ScannedTree;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncOutcome {
    pub conflict_paths: Vec<String>,
    pub published: bool,
    pub sequence: u64,
}

impl SyncOutcome {
    pub(super) fn complete(revision: NamespaceRevision, published: bool) -> Self {
        Self {
            conflict_paths: Vec::new(),
            published,
            sequence: revision.cursor().sequence(),
        }
    }
}

pub struct SyncEngine<A: AccessFamily = CoreAccess> {
    pub(super) volume: ManagedVolume<A>,
}

impl<A: AccessFamily> SyncEngine<A> {
    pub const fn new(volume: ManagedVolume<A>) -> Self {
        Self { volume }
    }

    pub async fn sync(
        &self,
        root: &Path,
        state_path: &Path,
        resolve_paths: &[String],
    ) -> Result<SyncOutcome, Error> {
        self.sync_inner(root, state_path, resolve_paths, None).await
    }

    /// Synchronize immutable staged files using an exhaustive, base-bound set
    /// of regular-file mutations.
    pub async fn sync_with_mutations(
        &self,
        root: &Path,
        state_path: &Path,
        resolve_paths: &[String],
        mutations: &[FileChangeSetEntry],
    ) -> Result<SyncOutcome, Error> {
        self.sync_inner(root, state_path, resolve_paths, Some(mutations))
            .await
    }

    async fn sync_inner(
        &self,
        root: &Path,
        state_path: &Path,
        resolve_paths: &[String],
        mutations: Option<&[FileChangeSetEntry]>,
    ) -> Result<SyncOutcome, Error> {
        validate_inputs(resolve_paths, mutations.unwrap_or_default())?;
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
        validate_authority(stored.as_ref(), &observed, &self.volume)?;

        if let Some((target, published)) = stored.as_ref().and_then(ReplicaState::installation) {
            let state = stored.expect("checked interrupted installation state");
            let namespace = if observed.revision() == target {
                &observed.namespace
            } else {
                loaded.as_ref().unwrap_or(&observed.namespace)
            };
            let revision = if observed.revision() == target {
                target
            } else {
                observed.revision()
            };
            return self
                .install_and_advance(
                    &workspace,
                    &root,
                    &mut state_file,
                    state,
                    namespace,
                    revision,
                    None,
                    published,
                )
                .await;
        }

        let mut state = match stored {
            Some(state) => state,
            None => {
                let state = ReplicaState::new(
                    root.clone(),
                    self.volume.id(),
                    observed.authority_id(),
                    self.volume.authority_name().to_owned(),
                    observed.revision(),
                );
                if observed.namespace.cursor == ChangeCursor::GENESIS && !directory_is_empty(&root)?
                {
                    state_file.persist(&state)?;
                    state
                } else {
                    require_empty(&root)?;
                    return self
                        .install_and_advance(
                            &workspace,
                            &root,
                            &mut state_file,
                            state,
                            &observed.namespace,
                            observed.revision(),
                            None,
                            false,
                        )
                        .await;
                }
            }
        };
        if state.has_pending() {
            return Err(Error::unsupported(
                "synchronize replica",
                "pending publication requires recovery",
            ));
        }
        if state.common_revision().cursor().sequence() == observed.revision().cursor().sequence()
            && state.common_revision() != observed.revision()
        {
            return Err(Error::unsupported(
                "synchronize replica",
                "equivalent namespace rebasing requires convergence",
            ));
        }
        if !observed.can_read_revision(state.common_revision()) {
            return Err(Error::unsupported(
                "synchronize replica",
                "an expired replica base requires conservative reconciliation",
            ));
        }

        let base = loaded.as_ref().unwrap_or(&observed.namespace);
        let local = self
            .scan(&workspace, &root, base, &observed, mutations)
            .await?;
        let remote_changed = state.common_revision() != observed.revision();
        match (local, remote_changed) {
            (ScannedTree::Unchanged, false) => {
                Ok(SyncOutcome::complete(state.common_revision(), false))
            }
            (ScannedTree::Changed(target), false) => {
                let target = self
                    .publish_planned_files(&workspace, &root, &observed, &target, mutations)
                    .await?;
                let revision = self
                    .prepare_and_commit(&workspace, &mut state_file, &mut state, &observed, &target)
                    .await?;
                Self::advance(&mut state_file, state, revision, true)
            }
            (ScannedTree::Unchanged, true) => {
                self.install_and_advance(
                    &workspace,
                    &root,
                    &mut state_file,
                    state,
                    &observed.namespace,
                    observed.revision(),
                    Some(base),
                    false,
                )
                .await
            }
            (ScannedTree::Changed(_), true) => Err(Error::unsupported(
                "synchronize replica",
                "concurrent local and remote changes require reconciliation",
            )),
        }
    }

    async fn install_and_advance(
        &self,
        workspace: &WorkContext,
        root: &Path,
        state_file: &mut ReplicaStateFile,
        mut state: ReplicaState,
        namespace: &Namespace<FileExtentMap>,
        revision: NamespaceRevision,
        current: Option<&Namespace<FileExtentMap>>,
        published: bool,
    ) -> Result<SyncOutcome, Error> {
        state.begin_install(revision, published);
        state_file.persist(&state)?;
        install(workspace, root, namespace, &self.volume, current).await?;
        Self::advance(state_file, state, revision, published)
    }

    fn advance(
        state_file: &mut ReplicaStateFile,
        mut state: ReplicaState,
        revision: NamespaceRevision,
        published: bool,
    ) -> Result<SyncOutcome, Error> {
        state.advance(revision);
        state_file.persist(&state)?;
        Ok(SyncOutcome::complete(revision, published))
    }
}

fn validate_inputs(
    resolve_paths: &[String],
    mutations: &[FileChangeSetEntry],
) -> Result<(), Error> {
    if !resolve_paths.is_empty() {
        return Err(Error::unsupported(
            "synchronize replica",
            "conflict resolution requires reconciliation",
        ));
    }
    let paths = mutations
        .iter()
        .map(|mutation| mutation.path.as_str())
        .collect::<BTreeSet<_>>();
    if paths.len() != mutations.len() {
        return Err(Error::invalid(
            "synchronize replica",
            "one file has more than one mutation input",
        ));
    }
    Ok(())
}

fn canonical_directory(root: &Path) -> Result<PathBuf, Error> {
    let root = std::fs::canonicalize(root)
        .map_err(|error| Error::from_io("open replica directory", Some(root), error))?;
    if !root.is_dir() {
        return Err(Error::invalid(
            "synchronize replica",
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
            "synchronize replica",
            "replica state belongs to a different local directory",
        ));
    }
    if state.volume_id() != volume.id() {
        return Err(Error::invalid(
            "synchronize replica",
            "replica state belongs to a different volume",
        ));
    }
    Ok(())
}

fn validate_authority<A: AccessFamily>(
    state: Option<&ReplicaState>,
    observed: &crate::volume::ManagedObservation,
    volume: &ManagedVolume<A>,
) -> Result<(), Error> {
    if let Some(state) = state
        && (state.authority_id() != observed.authority_id()
            || state.authority_name() != volume.authority_name())
    {
        return Err(Error::invalid(
            "synchronize replica",
            "replica state belongs to a different namespace authority",
        ));
    }
    Ok(())
}

fn require_empty(root: &Path) -> Result<(), Error> {
    if !directory_is_empty(root)? {
        return Err(Error::invalid(
            "synchronize replica",
            "a replica without state must use an empty local directory",
        ));
    }
    Ok(())
}

fn directory_is_empty(root: &Path) -> Result<bool, Error> {
    let mut entries = std::fs::read_dir(root)
        .map_err(|error| Error::from_io("read replica directory", Some(root), error))?;
    Ok(entries.next().is_none())
}

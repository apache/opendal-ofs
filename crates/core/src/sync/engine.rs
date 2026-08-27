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
use std::path::Path;

use crate::Error;
use crate::filesystem::ChangeCursor;
use crate::format::{FileExtentMap, NamespaceRevision};
use crate::volume::{AccessFamily, CoreAccess};
use crate::volume::{ManagedVolume, Namespace};
use crate::work::WorkContext;

use super::FileChangeSetEntry;
use super::ReplicaState;
use super::install::install;
use super::plan::{ConvergencePlan, SyncPlan};
use super::replica::state_store::ReplicaStateFile;

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

    pub(super) fn conflicted(conflict_paths: Vec<String>, sequence: u64) -> Self {
        Self {
            conflict_paths,
            published: false,
            sequence,
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
    ///
    /// Existing regular files omitted from `mutations` are trusted to retain
    /// their previously synchronized content. Namespace additions, removals,
    /// renames, type changes, and attributes are still observed from the local
    /// directory.
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
        let mutation_input = mutations;
        let mutations = mutations.unwrap_or_default();
        let mutation_paths = mutations
            .iter()
            .map(|mutation| mutation.path.as_str())
            .collect::<BTreeSet<_>>();
        if mutation_paths.len() != mutations.len() {
            return Err(Error::invalid(
                "synchronize replica",
                "one file has more than one mutation input",
            ));
        }
        let resolved = resolve_paths.iter().cloned().collect::<BTreeSet<_>>();
        if resolved.len() != resolve_paths.len() {
            return Err(Error::invalid(
                "synchronize replica",
                "a conflict resolution path was provided more than once",
            ));
        }
        let root = std::fs::canonicalize(root)
            .map_err(|error| Error::from_io("open replica directory", Some(root), error))?;
        if !root.is_dir() {
            return Err(Error::invalid(
                "synchronize replica",
                "replica path is not a directory",
            ));
        }
        let workspace_directory = state_path
            .parent()
            .filter(|directory| !directory.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let workspace = WorkContext::create_in(self.volume.work_budget(), workspace_directory)?;

        let (mut state_file, stored) = ReplicaStateFile::open(state_path)?;
        if let Some(state) = &stored {
            if state.root() != root {
                return Err(Error::invalid(
                    "synchronize replica",
                    "replica state belongs to a different local directory",
                ));
            }
            if state.volume_id() != self.volume.id() {
                return Err(Error::invalid(
                    "synchronize replica",
                    "replica state belongs to a different volume",
                ));
            }
        }
        let requested_revision = stored.as_ref().map(ReplicaState::resume_revision);
        let (observed, loaded_revision) = self
            .volume
            .observe_with_base_in(&workspace, requested_revision)
            .await?;
        if let Some(state) = &stored
            && (state.authority_id() != observed.authority_id()
                || state.authority_name() != self.volume.authority_name())
        {
            return Err(Error::invalid(
                "synchronize replica",
                "replica state belongs to a different namespace authority",
            ));
        }
        if let Some((target, published)) = stored.as_ref().and_then(ReplicaState::installation) {
            return self
                .recover_install(
                    &workspace,
                    &root,
                    &mut state_file,
                    stored.expect("checked interrupted installation state"),
                    observed,
                    loaded_revision,
                    target,
                    published,
                )
                .await;
        }

        let mut state = match stored {
            Some(state) => state,
            None => {
                if !resolved.is_empty() {
                    return Err(Error::invalid(
                        "synchronize replica",
                        "--resolve requires an unresolved conflict in replica state",
                    ));
                }
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
        if state.pending_publication().is_some() {
            return self
                .recover_pending(
                    &workspace,
                    &root,
                    &mut state_file,
                    state,
                    observed,
                    loaded_revision,
                )
                .await;
        }

        if state.common_revision() != observed.revision()
            && state.common_revision().cursor().sequence()
                == observed.revision().cursor().sequence()
        {
            state.rebase_equivalent(observed.revision())?;
            state_file.persist(&state)?;
        }
        if !observed.can_read_revision(state.common_revision()) {
            return self
                .conservative_rebase(
                    &workspace,
                    &root,
                    &mut state_file,
                    state,
                    observed,
                    &resolved,
                )
                .await;
        }

        let base = loaded_revision.as_ref().unwrap_or(&observed.namespace);
        let local = self
            .scan(&workspace, &root, base, &observed, mutation_input)
            .await?;
        match SyncPlan::build(
            base,
            local,
            &observed.namespace,
            observed.revision(),
            &resolved,
            &workspace,
        )? {
            SyncPlan::Unchanged => Ok(SyncOutcome::complete(state.common_revision(), false)),
            SyncPlan::Conflicted(paths) => {
                Self::retain_conflicts(&mut state_file, state, paths, observed.revision(), false)
            }
            SyncPlan::Converge(plan) => {
                self.converge(
                    &workspace,
                    &root,
                    &mut state_file,
                    state,
                    &observed,
                    plan,
                    mutation_input,
                )
                .await
            }
        }
    }

    pub(super) async fn converge(
        &self,
        workspace: &WorkContext,
        root: &Path,
        state_file: &mut ReplicaStateFile,
        mut state: ReplicaState,
        observed: &crate::volume::ManagedObservation,
        plan: ConvergencePlan,
        mutations: Option<&[FileChangeSetEntry]>,
    ) -> Result<SyncOutcome, Error> {
        let published = plan.committed.is_none();
        let target = if published {
            self.publish_planned_files(workspace, root, observed, &plan.target, mutations)
                .await?
        } else {
            plan.target
        };
        let revision = match plan.committed {
            Some(revision) => revision,
            None => {
                self.prepare_and_commit(workspace, state_file, &mut state, observed, &target)
                    .await?
            }
        };
        if let Some(current) = plan.install_from {
            return self
                .install_and_advance(
                    workspace,
                    root,
                    state_file,
                    state,
                    &target,
                    revision,
                    Some(&current),
                    published,
                )
                .await;
        }
        Self::advance(state_file, state, revision, published)
    }

    pub(super) async fn install_and_advance(
        &self,
        workspace: &WorkContext,
        root: &Path,
        state_file: &mut ReplicaStateFile,
        mut state: ReplicaState,
        target: &Namespace<FileExtentMap>,
        target_revision: NamespaceRevision,
        current: Option<&Namespace<FileExtentMap>>,
        published: bool,
    ) -> Result<SyncOutcome, Error> {
        state.begin_install(target_revision, published);
        state_file.persist(&state)?;
        install(workspace, root, target, &self.volume, current).await?;
        Self::advance(state_file, state, target_revision, published)
    }

    pub(super) fn advance(
        state_file: &mut ReplicaStateFile,
        mut state: ReplicaState,
        revision: NamespaceRevision,
        published: bool,
    ) -> Result<SyncOutcome, Error> {
        state.advance(revision);
        state_file.persist(&state)?;
        Ok(SyncOutcome::complete(revision, published))
    }

    pub(super) fn retain_conflicts(
        state_file: &mut ReplicaStateFile,
        mut state: ReplicaState,
        paths: Vec<String>,
        remote: NamespaceRevision,
        base_expired: bool,
    ) -> Result<SyncOutcome, Error> {
        state.retain_conflicts(paths.len(), remote, base_expired);
        state_file.persist(&state)?;
        Ok(SyncOutcome::conflicted(
            paths,
            state.common_revision().cursor().sequence(),
        ))
    }
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

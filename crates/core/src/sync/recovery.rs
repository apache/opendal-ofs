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

//! Recovery paths for interrupted or no-longer-readable Sync state.

use std::collections::BTreeSet;
use std::path::Path;

use crate::Error;
use crate::format::{FileExtentMap, NamespaceRevision};
use crate::volume::AccessFamily;
use crate::volume::{ManagedObservation, Namespace};
use crate::work::WorkContext;

use super::ReplicaState;
use super::engine::{SyncEngine, SyncOutcome};
use super::plan::ConvergencePlan;
use super::reconcile::changed_paths;
use super::replica::state_store::ReplicaStateFile;
use super::scan::ScannedTree;

impl<A: AccessFamily> SyncEngine<A> {
    pub(super) async fn recover_install(
        &self,
        workspace: &WorkContext,
        root: &Path,
        state_file: &mut ReplicaStateFile,
        state: ReplicaState,
        observed: ManagedObservation,
        loaded_target: Option<Namespace<FileExtentMap>>,
        target: NamespaceRevision,
        published: bool,
    ) -> Result<SyncOutcome, Error> {
        let (target_namespace, target_revision) = if observed.revision() == target {
            (&observed.namespace, target)
        } else if let Some(target_namespace) = loaded_target.as_ref() {
            (target_namespace, target)
        } else {
            (&observed.namespace, observed.revision())
        };
        self.install_and_advance(
            workspace,
            root,
            state_file,
            state,
            target_namespace,
            target_revision,
            None,
            published,
        )
        .await
    }

    pub(super) async fn recover_pending(
        &self,
        workspace: &WorkContext,
        root: &Path,
        state_file: &mut ReplicaStateFile,
        mut state: ReplicaState,
        observed: ManagedObservation,
        loaded_target: Option<Namespace<FileExtentMap>>,
    ) -> Result<SyncOutcome, Error> {
        let (expected, target, operation, gc_epoch) = state
            .pending_publication()
            .expect("pending state has publication references");
        if observed.revision() == target
            || self
                .volume
                .operation_committed(operation, target.cursor(), &observed)
                .await?
        {
            return self
                .install_committed_publication(
                    workspace,
                    root,
                    state_file,
                    state,
                    observed,
                    loaded_target,
                    expected,
                    target,
                )
                .await;
        }
        if observed.gc_epoch() != gc_epoch {
            state.cancel_pending(observed.revision());
            state_file.persist(&state)?;
            return Err(Error::invalid(
                "synchronize replica",
                "pending publication was invalidated by data collection; repeat sync to prepare it again",
            ));
        }
        if observed.revision() != expected {
            state.cancel_pending(observed.revision());
            state_file.persist(&state)?;
            return Err(Error::invalid(
                "synchronize replica",
                "pending publication conflicted with a newer remote change; repeat sync to reconcile",
            ));
        }
        self.volume
            .commit_publication(&observed, target, operation)
            .await?;
        self.install_committed_publication(
            workspace,
            root,
            state_file,
            state,
            observed,
            loaded_target,
            expected,
            target,
        )
        .await
    }

    async fn install_committed_publication(
        &self,
        workspace: &WorkContext,
        root: &Path,
        state_file: &mut ReplicaStateFile,
        state: ReplicaState,
        observed: ManagedObservation,
        loaded_expected: Option<Namespace<FileExtentMap>>,
        expected: NamespaceRevision,
        target: NamespaceRevision,
    ) -> Result<SyncOutcome, Error> {
        let current = if observed.revision() == expected {
            Some(&observed.namespace)
        } else {
            loaded_expected.as_ref()
        };
        if observed.revision().cursor() >= target.cursor() {
            return self
                .install_and_advance(
                    workspace,
                    root,
                    state_file,
                    state,
                    &observed.namespace,
                    observed.revision(),
                    current,
                    true,
                )
                .await;
        }
        let target_namespace = self.volume.read_namespace_in(workspace, target).await?;
        self.install_and_advance(
            workspace,
            root,
            state_file,
            state,
            &target_namespace,
            target,
            current,
            true,
        )
        .await
    }

    pub(super) async fn conservative_rebase(
        &self,
        workspace: &WorkContext,
        root: &Path,
        state_file: &mut ReplicaStateFile,
        state: ReplicaState,
        observed: ManagedObservation,
        resolved: &BTreeSet<String>,
    ) -> Result<SyncOutcome, Error> {
        let local = match self
            .scan(workspace, root, &observed.namespace, &observed, None)
            .await?
        {
            ScannedTree::Unchanged => {
                return Self::advance(state_file, state, observed.revision(), false);
            }
            ScannedTree::Changed(namespace) => namespace,
        };
        let ambiguous = changed_paths(&observed.namespace, &local)?;
        if resolved != &ambiguous {
            let conflict_paths = ambiguous.into_iter().collect::<Vec<_>>();
            return Self::retain_conflicts(
                state_file,
                state,
                conflict_paths,
                observed.revision(),
                true,
            );
        }
        self.converge(
            workspace,
            root,
            state_file,
            state,
            &observed,
            ConvergencePlan {
                target: local,
                committed: None,
                install_from: None,
            },
            None,
        )
        .await
    }
}

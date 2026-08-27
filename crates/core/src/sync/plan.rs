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

//! Deterministic projection from observed trees to one convergence plan.

use std::collections::BTreeSet;

use crate::Error;
use crate::format::{FileExtentMap, NamespaceRevision};
use crate::volume::Namespace;
use crate::work::WorkContext;

use super::reconcile::{ReconcilePlan, reconcile};
use super::scan::ScannedTree;

pub(super) enum SyncPlan {
    Unchanged,
    Conflicted(Vec<String>),
    Converge(ConvergencePlan),
}

pub(super) struct ConvergencePlan {
    pub(super) target: Namespace<FileExtentMap>,
    pub(super) committed: Option<NamespaceRevision>,
    pub(super) install_from: Option<Namespace<FileExtentMap>>,
}

impl SyncPlan {
    pub(super) fn build(
        base: &Namespace<FileExtentMap>,
        local: ScannedTree,
        remote: &Namespace<FileExtentMap>,
        remote_revision: NamespaceRevision,
        resolved: &BTreeSet<String>,
        workspace: &WorkContext,
    ) -> Result<Self, Error> {
        let remote_changed = remote_revision.cursor() != base.cursor;
        match (local, remote_changed) {
            (ScannedTree::Unchanged, false) => Ok(Self::Unchanged),
            (ScannedTree::Changed(target), false) => {
                require_no_resolutions(resolved)?;
                Ok(Self::Converge(ConvergencePlan {
                    target,
                    committed: None,
                    install_from: None,
                }))
            }
            (ScannedTree::Unchanged, true) => {
                require_no_resolutions(resolved)?;
                Ok(Self::Converge(ConvergencePlan {
                    target: remote.clone(),
                    committed: Some(remote_revision),
                    install_from: Some(base.clone()),
                }))
            }
            (ScannedTree::Changed(local), true) => {
                match reconcile(base, &local, remote, resolved, workspace)? {
                    ReconcilePlan::Conflicted(conflicts) => Ok(Self::Conflicted(conflicts)),
                    ReconcilePlan::Publish(target) => Ok(Self::Converge(ConvergencePlan {
                        target,
                        committed: None,
                        install_from: Some(local),
                    })),
                    ReconcilePlan::Remote => Ok(Self::Converge(ConvergencePlan {
                        target: remote.clone(),
                        committed: Some(remote_revision),
                        install_from: Some(local),
                    })),
                }
            }
        }
    }
}

fn require_no_resolutions(resolved: &BTreeSet<String>) -> Result<(), Error> {
    if !resolved.is_empty() {
        return Err(Error::invalid(
            "synchronize replica",
            "--resolve requires a current local and remote conflict",
        ));
    }
    Ok(())
}

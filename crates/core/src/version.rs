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

use crate::{CommitId, Error, NodeId, Result, Tree};

/// One immutable materialized filesystem version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsVersion {
    tree: Tree,
    commits: Vec<CommitId>,
}

impl FsVersion {
    pub fn new(tree: Tree, commits: Vec<CommitId>) -> Result<Self> {
        let root = tree.root_id()?;
        tree.validate(root)?;
        validate_commits(&commits)?;
        Ok(Self { tree, commits })
    }

    pub fn number(&self) -> u64 {
        self.commits.len() as u64
    }

    pub const fn tree(&self) -> &Tree {
        &self.tree
    }

    pub fn commits(&self) -> &[CommitId] {
        &self.commits
    }

    pub(crate) fn validate_root(&self, root: NodeId) -> Result<()> {
        if self.tree.root_id()? != root {
            return Err(Error::corrupt(
                "read YinYang version",
                "root identity changed",
            ));
        }
        Ok(())
    }
}

fn validate_commits(commits: &[CommitId]) -> Result<()> {
    use std::collections::BTreeSet;

    if commits.iter().copied().collect::<BTreeSet<_>>().len() != commits.len() {
        return Err(Error::corrupt(
            "read YinYang version",
            "commit identities are not unique",
        ));
    }
    Ok(())
}

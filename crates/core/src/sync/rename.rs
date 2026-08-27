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

//! Correlate unique file fingerprints without materializing the namespace.

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::filesystem::{
    ContentRef, FileVersionId, NamespaceNode, NamespaceRecord, NamespaceValue, NodeAttributes,
    NodeId,
};
use crate::format::FileExtentMap;
use crate::work::OrderedMerge;

use super::replica::scan::LocalRecord;
use super::scan::next_generation;

#[derive(Clone, Debug, Deserialize, Serialize)]
enum RenameCandidate {
    Local {
        path: String,
        executable: bool,
        fingerprint: ContentRef,
        data: FileExtentMap,
    },
    Base {
        path: String,
        fingerprint: ContentRef,
        node: NamespaceNode<FileExtentMap>,
    },
}

impl RenameCandidate {
    const fn fingerprint(&self) -> ContentRef {
        match self {
            Self::Local { fingerprint, .. } | Self::Base { fingerprint, .. } => *fingerprint,
        }
    }

    fn path(&self) -> &str {
        match self {
            Self::Local { path, .. } | Self::Base { path, .. } => path,
        }
    }
}

pub(super) struct RenameCandidates {
    records: crate::work::SpoolWriter<RenameCandidate>,
}

impl RenameCandidates {
    pub(super) fn new(workspace: &crate::work::WorkContext) -> Result<Self, Error> {
        Ok(Self {
            records: workspace.writer("rename-candidates")?,
        })
    }

    pub(super) fn local(&mut self, local: LocalRecord) -> Result<(), Error> {
        self.records.write(&RenameCandidate::Local {
            path: local.path,
            executable: local.executable,
            fingerprint: local
                .file
                .as_ref()
                .expect("a local regular file has published content")
                .content,
            data: local
                .file
                .expect("a local regular file has published content")
                .data,
        })
    }

    pub(super) fn removed_record(
        &mut self,
        record: NamespaceRecord<FileExtentMap>,
    ) -> Result<(), Error> {
        let node = record
            .value
            .ok_or_else(|| Error::corrupt("scan replica", "base namespace contains a deletion"))?;
        self.removed_node(record.path, node)
    }

    pub(super) fn removed_node(
        &mut self,
        path: String,
        node: NamespaceNode<FileExtentMap>,
    ) -> Result<(), Error> {
        let Some((_, fingerprint, _)) = node.file() else {
            return Ok(());
        };
        self.records.write(&RenameCandidate::Base {
            path,
            fingerprint,
            node,
        })
    }

    pub(super) fn resolve(
        self,
        workspace: &crate::work::WorkContext,
    ) -> Result<crate::work::Spool<NamespaceRecord<FileExtentMap>>, Error> {
        let candidates = crate::work::sort(
            workspace,
            &self.records.finish()?,
            |candidate: &RenameCandidate| (candidate.fingerprint(), candidate.path().to_owned()),
        )?;
        let mut output = workspace.writer("resolved-renames")?;
        let mut groups = OrderedMerge::from_readers(
            vec![candidates.reader()?],
            |candidate: &RenameCandidate| Ok(candidate.fingerprint()),
        )?;
        while let Some((_, group)) = groups
            .reduce_next_group(CandidateGroup::default(), |group, _, candidate| {
                group.push(candidate, &mut output)
            })?
        {
            group.finish(&mut output)?;
        }
        crate::work::sort(
            workspace,
            &output.finish()?,
            |record: &NamespaceRecord<FileExtentMap>| record.path.clone(),
        )
    }
}

#[derive(Default)]
struct CandidateGroup {
    local: Option<RenameCandidate>,
    base: Option<RenameCandidate>,
    ambiguous_local: bool,
    ambiguous_base: bool,
}

impl CandidateGroup {
    fn push(
        &mut self,
        candidate: RenameCandidate,
        output: &mut crate::work::SpoolWriter<NamespaceRecord<FileExtentMap>>,
    ) -> Result<(), Error> {
        match candidate {
            RenameCandidate::Local { .. } if self.ambiguous_local => {
                write_new_file(output, candidate)
            }
            RenameCandidate::Local { .. } => match self.local.replace(candidate) {
                Some(previous) => {
                    self.ambiguous_local = true;
                    write_new_file(output, previous)?;
                    write_new_file(
                        output,
                        self.local
                            .take()
                            .expect("second local candidate was stored"),
                    )
                }
                None => Ok(()),
            },
            RenameCandidate::Base { .. } if self.ambiguous_base => Ok(()),
            RenameCandidate::Base { .. } => {
                if self.base.replace(candidate).is_some() {
                    self.base = None;
                    self.ambiguous_base = true;
                }
                Ok(())
            }
        }
    }

    fn finish(
        self,
        output: &mut crate::work::SpoolWriter<NamespaceRecord<FileExtentMap>>,
    ) -> Result<(), Error> {
        match (self.local, self.base) {
            (Some(local), Some(base)) => write_renamed_file(output, local, base),
            (Some(local), None) => write_new_file(output, local),
            (None, _) => Ok(()),
        }
    }
}

fn write_new_file(
    output: &mut crate::work::SpoolWriter<NamespaceRecord<FileExtentMap>>,
    local: RenameCandidate,
) -> Result<(), Error> {
    let RenameCandidate::Local {
        path,
        executable,
        fingerprint,
        data,
    } = local
    else {
        unreachable!("new files are local rename candidates")
    };
    output.write(&NamespaceRecord {
        path,
        value: Some(NamespaceNode {
            node_id: NodeId::generate(),
            generation: 1,
            attributes: NodeAttributes { executable },
            value: NamespaceValue::RegularFile {
                version: FileVersionId::generate(),
                content: fingerprint,
                data,
            },
        }),
    })
}

fn write_renamed_file(
    output: &mut crate::work::SpoolWriter<NamespaceRecord<FileExtentMap>>,
    local: RenameCandidate,
    base: RenameCandidate,
) -> Result<(), Error> {
    let RenameCandidate::Local {
        path, executable, ..
    } = local
    else {
        unreachable!("a renamed file has one local candidate")
    };
    let RenameCandidate::Base { node, .. } = base else {
        unreachable!("a renamed file has one base candidate")
    };
    let NamespaceValue::RegularFile {
        version,
        content,
        data,
    } = node.value
    else {
        return Err(Error::corrupt(
            "scan replica",
            "rename candidate is not a regular file",
        ));
    };
    let attributes = NodeAttributes { executable };
    let generation = if attributes == node.attributes {
        node.generation
    } else {
        next_generation(node.generation)?
    };
    output.write(&NamespaceRecord {
        path,
        value: Some(NamespaceNode {
            node_id: node.node_id,
            generation,
            attributes,
            value: NamespaceValue::RegularFile {
                version,
                content,
                data,
            },
        }),
    })
}

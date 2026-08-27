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

//! Merge a path-ordered local scan with the last common namespace.

use std::path::Path;

use crate::Error;
use crate::filesystem::{
    ChangeCursor, FileVersionId, NamespaceNode, NamespaceRecord, NamespaceValue, NodeAttributes,
    NodeId, NodeKind,
};
use crate::format::FileExtentMap;
use crate::volume::AccessFamily;
use crate::volume::{ManagedObservation, Namespace};
use crate::work::{JoinItem, OrderedJoin};

use super::FileChangeSetEntry;
use super::SyncEngine;
use super::rename::RenameCandidates;
use super::replica::scan::LocalRecord;

pub(crate) enum ScannedTree {
    Unchanged,
    Changed(Namespace<FileExtentMap>),
}

impl<A: AccessFamily> SyncEngine<A> {
    pub(crate) async fn scan(
        &self,
        workspace: &crate::work::WorkContext,
        root: &Path,
        base: &Namespace<FileExtentMap>,
        _observed: &ManagedObservation,
        mutations: Option<&[FileChangeSetEntry]>,
    ) -> Result<ScannedTree, Error> {
        let entries = crate::sync::replica::scan::scan(workspace, root)?;
        let local = self
            .observe_local_files(workspace, root, base, entries, mutations)
            .await?;
        let next_cursor = base
            .cursor
            .sequence()
            .checked_add(1)
            .map(ChangeCursor::from_sequence);
        let mut output = workspace.writer("scanned-namespace")?;
        let mut renames = RenameCandidates::new(workspace)?;
        let mut changed_directories = workspace.writer("changed-directories")?;
        let mut records = OrderedJoin::new(
            local.reader()?,
            base.reader()?,
            |record: &LocalRecord| record.path.clone(),
            |record: &NamespaceRecord<FileExtentMap>| record.path.clone(),
        );
        let mut changed = false;
        let mut root_seen = false;

        while let Some(record) = records.next()? {
            match record {
                JoinItem::Left(local) => {
                    changed = true;
                    write_parent(&mut changed_directories, &local.path)?;
                    write_new_local(&mut output, &mut renames, local)?;
                }
                JoinItem::Right(base_record) => {
                    changed = true;
                    write_parent(&mut changed_directories, &base_record.path)?;
                    renames.removed_record(base_record)?;
                }
                JoinItem::Match(local, base_record) => {
                    let path = local.path.clone();
                    let base_node = base_record.value.ok_or_else(|| {
                        Error::corrupt("scan replica", "base namespace contains a deletion")
                    })?;
                    if path.is_empty() {
                        if base_node.node_id != base.root || base_node.kind() != NodeKind::Directory
                        {
                            return Err(Error::corrupt(
                                "scan replica",
                                "base namespace root is invalid",
                            ));
                        }
                        root_seen = true;
                    }
                    if local.kind == base_node.kind() {
                        changed |= !same_base_entry(&local, &base_node);
                        let node = reuse_same_path(&local, base_node)?;
                        output.write(&NamespaceRecord {
                            path,
                            value: Some(node),
                        })?;
                    } else {
                        changed = true;
                        write_parent(&mut changed_directories, &path)?;
                        renames.removed_node(path.clone(), base_node)?;
                        write_new_local(&mut output, &mut renames, local)?;
                    }
                }
            }
        }
        if !root_seen {
            return Err(Error::corrupt(
                "scan replica",
                "base namespace root is missing",
            ));
        }

        let renames = renames.resolve(workspace)?;
        if !changed {
            return Ok(ScannedTree::Unchanged);
        }
        let cursor = next_cursor
            .ok_or_else(|| Error::corrupt("scan replica", "Managed change sequence overflows"))?;
        let entries = merge_path_records(workspace, &output.finish()?, &renames)?;
        let changed_directories =
            crate::work::sort(workspace, &changed_directories.finish()?, String::clone)?;
        let entries =
            advance_directory_generations(workspace, base, &entries, &changed_directories)?;
        Ok(ScannedTree::Changed(Namespace {
            volume_id: base.volume_id,
            cursor,
            root: base.root,
            entries,
        }))
    }
}

fn merge_path_records(
    workspace: &crate::work::WorkContext,
    main: &crate::work::Spool<NamespaceRecord<FileExtentMap>>,
    renames: &crate::work::Spool<NamespaceRecord<FileExtentMap>>,
) -> Result<crate::work::Spool<NamespaceRecord<FileExtentMap>>, Error> {
    let mut records = OrderedJoin::new(
        main.reader()?,
        renames.reader()?,
        |record: &NamespaceRecord<FileExtentMap>| record.path.clone(),
        |record: &NamespaceRecord<FileExtentMap>| record.path.clone(),
    );
    let mut output = workspace.writer("scanned-namespace-ordered")?;
    while let Some(record) = records.next()? {
        let record = match record {
            JoinItem::Left(record) | JoinItem::Right(record) => record,
            JoinItem::Match(_, _) => {
                return Err(Error::corrupt(
                    "scan replica",
                    "one path has conflicting scan records",
                ));
            }
        };
        output.write(&record)?;
    }
    output.finish()
}

fn write_new_local(
    output: &mut crate::work::SpoolWriter<NamespaceRecord<FileExtentMap>>,
    renames: &mut RenameCandidates,
    local: LocalRecord,
) -> Result<(), Error> {
    match local.kind {
        NodeKind::Directory => output.write(&NamespaceRecord {
            path: local.path,
            value: Some(new_directory()),
        }),
        NodeKind::RegularFile => renames.local(local),
    }
}

fn write_parent(output: &mut crate::work::SpoolWriter<String>, path: &str) -> Result<(), Error> {
    if path.is_empty() {
        return Ok(());
    }
    output.write(
        &path
            .rsplit_once('/')
            .map_or("", |(parent, _)| parent)
            .to_owned(),
    )
}

fn advance_directory_generations(
    workspace: &crate::work::WorkContext,
    base: &Namespace<FileExtentMap>,
    target: &crate::work::Spool<NamespaceRecord<FileExtentMap>>,
    changed_directories: &crate::work::Spool<String>,
) -> Result<crate::work::Spool<NamespaceRecord<FileExtentMap>>, Error> {
    let mut base = base.reader()?;
    let mut target = target.reader()?;
    let mut changed = changed_directories.reader()?;
    let mut base_record = base.next()?;
    let mut changed_path = changed.next()?;
    let mut output = workspace.writer("directory-generations")?;

    while let Some(mut record) = target.next()? {
        while base_record
            .as_ref()
            .is_some_and(|base| base.path < record.path)
        {
            base_record = base.next()?;
        }
        while changed_path
            .as_ref()
            .is_some_and(|changed| changed < &record.path)
        {
            let previous_changed = changed_path.take();
            changed_path = changed.next()?;
            while changed_path == previous_changed {
                changed_path = changed.next()?;
            }
        }
        if changed_path.as_deref() == Some(record.path.as_str())
            && let (Some(base), Some(node)) = (base_record.as_ref(), record.value.as_mut())
            && base.path == record.path
            && base
                .value
                .as_ref()
                .is_some_and(|base| base.node_id == node.node_id)
            && let NamespaceValue::Directory { generation } = &mut node.value
        {
            *generation = next_generation(*generation)?;
        }
        output.write(&record)?;
    }
    output.finish()
}

fn reuse_same_path(
    local: &LocalRecord,
    base: NamespaceNode<FileExtentMap>,
) -> Result<NamespaceNode<FileExtentMap>, Error> {
    let attributes = NodeAttributes {
        executable: local.executable,
    };
    match base.value {
        NamespaceValue::Directory { generation } => Ok(NamespaceNode {
            node_id: base.node_id,
            generation: if base.attributes == attributes {
                base.generation
            } else {
                next_generation(base.generation)?
            },
            attributes,
            value: NamespaceValue::Directory { generation },
        }),
        NamespaceValue::RegularFile {
            version,
            content,
            data,
        } => {
            let local_file = local
                .file
                .as_ref()
                .expect("a local regular file has published content");
            let local_fingerprint = local_file.content;
            let unchanged_content = content == local_fingerprint;
            Ok(NamespaceNode {
                node_id: base.node_id,
                generation: if unchanged_content && base.attributes == attributes {
                    base.generation
                } else {
                    next_generation(base.generation)?
                },
                attributes,
                value: if unchanged_content {
                    NamespaceValue::RegularFile {
                        version,
                        content,
                        data,
                    }
                } else {
                    NamespaceValue::RegularFile {
                        version: FileVersionId::generate(),
                        content: local_fingerprint,
                        data: local_file.data.clone(),
                    }
                },
            })
        }
    }
}

fn same_base_entry(local: &LocalRecord, node: &NamespaceNode<FileExtentMap>) -> bool {
    if node.kind() != local.kind || node.attributes.executable != local.executable {
        return false;
    }
    match &node.value {
        NamespaceValue::Directory { .. } => local.kind == NodeKind::Directory,
        NamespaceValue::RegularFile { content, .. } => {
            local.kind == NodeKind::RegularFile
                && local.file.as_ref().map(|file| file.content) == Some(*content)
        }
    }
}

fn new_directory() -> NamespaceNode<FileExtentMap> {
    NamespaceNode {
        node_id: NodeId::generate(),
        generation: 1,
        attributes: NodeAttributes::default(),
        value: NamespaceValue::Directory { generation: 1 },
    }
}

pub(super) fn next_generation(generation: u64) -> Result<u64, Error> {
    generation
        .checked_add(1)
        .ok_or_else(|| Error::corrupt("scan replica", "node generation overflows"))
}

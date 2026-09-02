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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

use crate::{Digest, Error, FileVersionId, Generation, NodeId, Result};

const MAX_COMPONENT_BYTES: usize = 255;
const MAX_PATH_BYTES: usize = 4096;

/// Canonical relative filesystem path. The empty path is the root.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Path(String);

impl Path {
    pub const fn root() -> Self {
        Self(String::new())
    }

    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_portable_path(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn parent(&self) -> Option<Self> {
        if self.is_root() {
            return None;
        }
        Some(match self.0.rsplit_once('/') {
            Some((parent, _)) => Self(parent.to_owned()),
            None => Self::root(),
        })
    }

    pub fn name(&self) -> Option<&str> {
        (!self.is_root()).then(|| {
            self.0
                .rsplit('/')
                .next()
                .expect("a non-root path has a name")
        })
    }
}

impl fmt::Display for Path {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Stable identity of one logical byte sequence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContentId {
    digest: Digest,
    length: u64,
}

impl ContentId {
    pub const fn new(digest: Digest, length: u64) -> Self {
        Self { digest, length }
    }

    pub const fn digest(self) -> Digest {
        self.digest
    }

    pub const fn length(self) -> u64 {
        self.length
    }
}

/// Opaque persistent reference to one verifiable immutable byte region.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BlobRef {
    reference: Vec<u8>,
    content: ContentId,
}

impl BlobRef {
    pub fn new(reference: impl Into<Vec<u8>>, content: ContentId) -> Result<Self> {
        let reference = reference.into();
        if reference.is_empty() {
            return Err(Error::invalid(
                "construct YinYang blob reference",
                "blob reference is empty",
            ));
        }
        Ok(Self { reference, content })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.reference
    }

    pub const fn content(&self) -> ContentId {
        self.content
    }
}

/// One half-open logical byte range in a file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileRange {
    offset: u64,
    length: u64,
}

impl FileRange {
    pub fn new(offset: u64, length: u64) -> Result<Self> {
        if length == 0 || offset.checked_add(length).is_none() {
            return Err(Error::invalid(
                "construct YinYang file range",
                "file range is empty or overflows",
            ));
        }
        Ok(Self { offset, length })
    }

    pub const fn offset(self) -> u64 {
        self.offset
    }

    pub const fn length(self) -> u64 {
        self.length
    }

    pub fn end(self) -> Result<u64> {
        self.offset
            .checked_add(self.length)
            .ok_or_else(|| Error::corrupt("read YinYang file range", "file range end overflows"))
    }
}

/// Independently verifiable and decodable source bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileSource {
    stored: BlobRef,
    decoded: Vec<ContentId>,
}

impl FileSource {
    pub fn new(stored: BlobRef, decoded: Vec<ContentId>) -> Self {
        Self { stored, decoded }
    }

    pub const fn stored(&self) -> &BlobRef {
        &self.stored
    }

    pub fn decoded(&self) -> &[ContentId] {
        &self.decoded
    }

    pub fn content(&self) -> ContentId {
        self.decoded
            .last()
            .copied()
            .unwrap_or_else(|| self.stored.content())
    }
}

/// Mapping from one logical file range into a source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilePart {
    range: FileRange,
    source_offset: u64,
    source: FileSource,
}

impl FilePart {
    pub fn new(range: FileRange, source_offset: u64, source: FileSource) -> Result<Self> {
        if source_offset
            .checked_add(range.length())
            .is_none_or(|end| end > source.content().length())
        {
            return Err(Error::invalid(
                "construct YinYang file part",
                "file part exceeds its source content",
            ));
        }
        Ok(Self {
            range,
            source_offset,
            source,
        })
    }

    pub const fn range(&self) -> FileRange {
        self.range
    }

    pub const fn source_offset(&self) -> u64 {
        self.source_offset
    }

    pub const fn source(&self) -> &FileSource {
        &self.source
    }
}

/// One published logical file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct File {
    version: FileVersionId,
    content: ContentId,
    parts: Vec<FilePart>,
}

impl File {
    pub fn new(version: FileVersionId, content: ContentId, parts: Vec<FilePart>) -> Result<Self> {
        let file = Self {
            version,
            content,
            parts,
        };
        file.validate(None)?;
        Ok(file)
    }

    pub const fn version(&self) -> FileVersionId {
        self.version
    }

    pub const fn content(&self) -> ContentId {
        self.content
    }

    pub fn parts(&self) -> &[FilePart] {
        &self.parts
    }

    fn validate(&self, decoding_count: Option<usize>) -> Result<()> {
        if self.content.length() == 0 {
            if self.parts.is_empty() {
                return Ok(());
            }
            return Err(Error::invalid(
                "validate YinYang file",
                "empty file contains parts",
            ));
        }

        let mut expected_offset = 0;
        for part in &self.parts {
            if part.range.offset() != expected_offset {
                return Err(Error::invalid(
                    "validate YinYang file",
                    "file parts contain a gap or overlap",
                ));
            }
            if decoding_count.is_some_and(|count| part.source.decoded.len() != count) {
                return Err(Error::invalid(
                    "validate YinYang file",
                    "file source decoding count does not match the format",
                ));
            }
            expected_offset = part.range.end()?;
        }
        if expected_offset != self.content.length() {
            return Err(Error::invalid(
                "validate YinYang file",
                "file parts do not exactly cover the logical content",
            ));
        }
        Ok(())
    }
}

/// Filesystem attributes owned by one node generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeAttrs {
    pub executable: bool,
}

/// Directory membership state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Dir {
    entries_generation: Generation,
}

impl Dir {
    pub const fn new(entries_generation: Generation) -> Self {
        Self { entries_generation }
    }

    pub const fn entries_generation(self) -> Generation {
        self.entries_generation
    }
}

/// Node-specific filesystem state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeBody {
    Dir(Dir),
    File(File),
}

/// One stable filesystem node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Node {
    id: NodeId,
    generation: Generation,
    attrs: NodeAttrs,
    body: NodeBody,
}

impl Node {
    pub const fn dir(
        id: NodeId,
        generation: Generation,
        attrs: NodeAttrs,
        entries_generation: Generation,
    ) -> Self {
        Self {
            id,
            generation,
            attrs,
            body: NodeBody::Dir(Dir::new(entries_generation)),
        }
    }

    pub const fn file(id: NodeId, generation: Generation, attrs: NodeAttrs, file: File) -> Self {
        Self {
            id,
            generation,
            attrs,
            body: NodeBody::File(file),
        }
    }

    pub const fn id(&self) -> NodeId {
        self.id
    }

    pub const fn generation(&self) -> Generation {
        self.generation
    }

    pub const fn attrs(&self) -> NodeAttrs {
        self.attrs
    }

    pub const fn body(&self) -> &NodeBody {
        &self.body
    }

    pub const fn dir_body(&self) -> Option<Dir> {
        match self.body {
            NodeBody::Dir(dir) => Some(dir),
            NodeBody::File(_) => None,
        }
    }

    pub const fn file_body(&self) -> Option<&File> {
        match &self.body {
            NodeBody::Dir(_) => None,
            NodeBody::File(file) => Some(file),
        }
    }
}

/// Materialized ordered filesystem tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Tree {
    entries: BTreeMap<Path, Node>,
}

impl Tree {
    pub fn genesis(root: NodeId) -> Self {
        Self {
            entries: BTreeMap::from([(
                Path::root(),
                Node::dir(
                    root,
                    Generation::FIRST,
                    NodeAttrs::default(),
                    Generation::FIRST,
                ),
            )]),
        }
    }

    pub fn from_entries(entries: impl IntoIterator<Item = (Path, Node)>) -> Result<Self> {
        let mut tree = Self {
            entries: BTreeMap::new(),
        };
        for (path, node) in entries {
            if tree.entries.insert(path, node).is_some() {
                return Err(Error::invalid(
                    "construct YinYang tree",
                    "tree contains a duplicate path",
                ));
            }
        }
        Ok(tree)
    }

    pub fn get(&self, path: &Path) -> Option<&Node> {
        self.entries.get(path)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Path, &Node)> {
        self.entries.iter()
    }

    pub fn insert(&mut self, path: Path, node: Node) -> Option<Node> {
        self.entries.insert(path, node)
    }

    pub fn remove(&mut self, path: &Path) -> Option<Node> {
        self.entries.remove(path)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn validate(&self, root: NodeId, decoding_count: usize) -> Result<()> {
        let root_node = self
            .entries
            .get(&Path::root())
            .ok_or_else(|| Error::invalid("validate YinYang tree", "tree root is missing"))?;
        if root_node.id != root || root_node.dir_body().is_none() {
            return Err(Error::invalid(
                "validate YinYang tree",
                "tree root does not match the format",
            ));
        }

        let mut identities = BTreeSet::new();
        for (path, node) in &self.entries {
            if node.generation == Generation::ZERO {
                return Err(Error::invalid(
                    "validate YinYang tree",
                    "node generation is zero",
                ));
            }
            if !identities.insert(node.id) {
                return Err(Error::invalid(
                    "validate YinYang tree",
                    "node identity appears at more than one path",
                ));
            }
            if let Some(dir) = node.dir_body()
                && dir.entries_generation() == Generation::ZERO
            {
                return Err(Error::invalid(
                    "validate YinYang tree",
                    "directory entries generation is zero",
                ));
            }
            if let Some(file) = node.file_body() {
                file.validate(Some(decoding_count))?;
            }
            if let Some(parent) = path.parent()
                && self.entries.get(&parent).and_then(Node::dir_body).is_none()
            {
                return Err(Error::invalid(
                    "validate YinYang tree",
                    "tree entry parent is missing or is not a directory",
                ));
            }
        }
        directory_membership(self)?;
        Ok(())
    }

    pub(crate) fn validate_successor(
        &self,
        successor: &Self,
        root: NodeId,
        decoding_count: usize,
    ) -> Result<()> {
        self.validate(root, decoding_count)?;
        successor.validate(root, decoding_count)?;

        let previous_by_id = nodes_by_id(self);
        let successor_by_id = nodes_by_id(successor);
        for (id, (_, next)) in &successor_by_id {
            let Some((_, previous)) = previous_by_id.get(id) else {
                if next.generation != Generation::FIRST
                    || next
                        .dir_body()
                        .is_some_and(|dir| dir.entries_generation() != Generation::FIRST)
                {
                    return Err(Error::invalid(
                        "validate YinYang tree successor",
                        "new node does not start at the first generation",
                    ));
                }
                continue;
            };

            if matches!(previous.body, NodeBody::Dir(_)) != matches!(next.body, NodeBody::Dir(_)) {
                return Err(Error::invalid(
                    "validate YinYang tree successor",
                    "node kind changed without a new identity",
                ));
            }
            if let (Some(previous_file), Some(next_file)) = (previous.file_body(), next.file_body())
                && previous_file.version == next_file.version
                && (previous_file.content != next_file.content
                    || previous_file.parts != next_file.parts)
            {
                return Err(Error::invalid(
                    "validate YinYang tree successor",
                    "file content changed without a new file version",
                ));
            }

            let payload_changed = previous.attrs != next.attrs
                || match (&previous.body, &next.body) {
                    (NodeBody::Dir(_), NodeBody::Dir(_)) => false,
                    (NodeBody::File(previous), NodeBody::File(next)) => previous != next,
                    _ => unreachable!("node kind was checked above"),
                };
            let expected = if payload_changed {
                previous.generation.next()?
            } else {
                previous.generation
            };
            if next.generation != expected {
                return Err(Error::invalid(
                    "validate YinYang tree successor",
                    "node generation does not match its content change",
                ));
            }
        }

        let previous_membership = directory_membership(self)?;
        let successor_membership = directory_membership(successor)?;
        for (id, (_, next)) in successor_by_id {
            let Some(next_dir) = next.dir_body() else {
                continue;
            };
            let Some((_, previous)) = previous_by_id.get(&id) else {
                continue;
            };
            let previous_dir = previous
                .dir_body()
                .expect("a stable directory identity remains a directory");
            let changed = previous_membership.get(&id) != successor_membership.get(&id);
            let expected = if changed {
                previous_dir.entries_generation().next()?
            } else {
                previous_dir.entries_generation()
            };
            if next_dir.entries_generation() != expected {
                return Err(Error::invalid(
                    "validate YinYang tree successor",
                    "directory generation does not match its membership change",
                ));
            }
        }
        Ok(())
    }
}

fn nodes_by_id(tree: &Tree) -> BTreeMap<NodeId, (&Path, &Node)> {
    tree.entries
        .iter()
        .map(|(path, node)| (node.id, (path, node)))
        .collect()
}

fn directory_membership(tree: &Tree) -> Result<BTreeMap<NodeId, BTreeMap<String, NodeId>>> {
    let mut membership = tree
        .entries
        .values()
        .filter_map(|node| node.dir_body().map(|_| (node.id, BTreeMap::new())))
        .collect::<BTreeMap<_, _>>();
    let mut folded_names = BTreeMap::<NodeId, BTreeSet<String>>::new();
    for (path, node) in &tree.entries {
        let Some(parent) = path.parent() else {
            continue;
        };
        let parent_id = tree
            .entries
            .get(&parent)
            .and_then(|node| node.dir_body().map(|_| node.id))
            .ok_or_else(|| {
                Error::invalid(
                    "validate YinYang tree",
                    "tree entry parent is missing or is not a directory",
                )
            })?;
        let name = path.name().expect("a non-root path has a name");
        if !folded_names
            .entry(parent_id)
            .or_default()
            .insert(name.case_fold().nfc().collect())
        {
            return Err(Error::invalid(
                "validate YinYang tree",
                "directory contains a case-folding name collision",
            ));
        }
        membership
            .get_mut(&parent_id)
            .expect("every directory has a membership set")
            .insert(name.to_owned(), node.id);
    }
    Ok(membership)
}

fn validate_portable_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Ok(());
    }
    if path.len() > MAX_PATH_BYTES
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains("//")
    {
        return Err(Error::invalid(
            "construct YinYang path",
            "path is not canonical and portable",
        ));
    }
    for component in path.split('/') {
        validate_portable_component(component)?;
    }
    Ok(())
}

fn validate_portable_component(component: &str) -> Result<()> {
    if component.len() > MAX_COMPONENT_BYTES
        || component == "."
        || component == ".."
        || component.ends_with([' ', '.'])
        || component.chars().any(|character| {
            character.is_control()
                || matches!(character, '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*')
        })
        || !component.nfc().eq(component.chars())
    {
        return Err(Error::invalid(
            "construct YinYang path",
            "path component is not canonical and portable",
        ));
    }
    let folded = component.case_fold().nfc().collect::<String>();
    let stem = folded.split('.').next().unwrap_or_default();
    if matches!(stem, "con" | "prn" | "aux" | "nul")
        || stem.len() == 4
            && (stem.starts_with("com") || stem.starts_with("lpt"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9')
        || matches!(stem, "com¹" | "com²" | "com³" | "lpt¹" | "lpt²" | "lpt³")
    {
        return Err(Error::invalid(
            "construct YinYang path",
            "path component is reserved",
        ));
    }
    Ok(())
}

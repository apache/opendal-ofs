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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use yinyang_core::{
    BlobRef, CommitId, CommitOutcome, ContentId, CreateOptions, Digest, Error, ErrorKind,
    Extension, ExtensionId, File, FilePart, FileRange, FileSource, FormatStorage, Fs, FsFormat,
    FsHead, FsVersion, Generation, HeadObservation, Node, NodeAttrs, NodeId, Path, Result, Tree,
    VersionNumber,
};

#[derive(Clone, Debug, Default)]
struct MemoryStorage {
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    format: Option<FsFormat>,
    head: Option<FsHead>,
    head_revision: u64,
    versions: BTreeMap<BlobRef, FsVersion>,
    next_blob: u64,
    fail_after_replace: bool,
}

impl MemoryStorage {
    fn fail_next_replace_after_success(&self) {
        self.state.lock().unwrap().fail_after_replace = true;
    }

    fn remove_current_version(&self) {
        let mut state = self.state.lock().unwrap();
        let reference = state
            .head
            .as_ref()
            .expect("the test filesystem has a head")
            .current()
            .blob()
            .clone();
        state.versions.remove(&reference);
    }
}

impl FormatStorage for MemoryStorage {
    async fn create_format(&self, format: &FsFormat) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        if state.format.is_some() {
            return Ok(false);
        }
        state.format = Some(format.clone());
        Ok(true)
    }

    async fn read_format(&self) -> Result<Option<FsFormat>> {
        Ok(self.state.lock().unwrap().format.clone())
    }

    async fn write_version(&self, version: &FsVersion) -> Result<BlobRef> {
        let mut state = self.state.lock().unwrap();
        state.next_blob += 1;
        let identity = state.next_blob;
        let mut digest = [0; 32];
        digest[..8].copy_from_slice(&identity.to_le_bytes());
        let reference = BlobRef::new(
            identity.to_le_bytes(),
            ContentId::new(Digest::from_bytes(digest), 1),
        )?;
        state.versions.insert(reference.clone(), version.clone());
        Ok(reference)
    }

    async fn read_version(&self, reference: &BlobRef) -> Result<FsVersion> {
        self.state
            .lock()
            .unwrap()
            .versions
            .get(reference)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Corrupt,
                    "read in-memory YinYang version",
                    "referenced version is missing",
                )
            })
    }

    async fn create_head(&self, head: &FsHead) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        if state.head.is_some() {
            return Ok(false);
        }
        state.head = Some(head.clone());
        state.head_revision = 1;
        Ok(true)
    }

    async fn observe_head(&self) -> Result<Option<HeadObservation>> {
        let state = self.state.lock().unwrap();
        state
            .head
            .clone()
            .map(|head| HeadObservation::new(head, state.head_revision.to_le_bytes().to_vec()))
            .transpose()
    }

    async fn replace_head(&self, observed: &HeadObservation, next: &FsHead) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        if state.head.is_none() || observed.condition() != state.head_revision.to_le_bytes() {
            return Ok(false);
        }
        state.head = Some(next.clone());
        state.head_revision += 1;
        if state.fail_after_replace {
            state.fail_after_replace = false;
            return Err(Error::new(
                ErrorKind::Storage,
                "replace in-memory YinYang head",
                "publication response was lost",
            ));
        }
        Ok(true)
    }
}

fn add_directory(tree: &Tree, name: &str, id: NodeId) -> Tree {
    let mut successor = tree.clone();
    advance_root_membership(&mut successor);
    successor.insert(
        Path::new(name).unwrap(),
        Node::dir(
            id,
            Generation::FIRST,
            NodeAttrs::default(),
            Generation::FIRST,
        ),
    );
    successor
}

fn advance_root_membership(tree: &mut Tree) {
    let root_path = Path::root();
    let root = tree
        .get(&root_path)
        .expect("a valid tree has a root")
        .clone();
    let root_dir = root.dir_body().expect("the root is a directory");
    tree.insert(
        root_path,
        Node::dir(
            root.id(),
            root.generation(),
            root.attrs(),
            root_dir.entries_generation().next().unwrap(),
        ),
    );
}

#[tokio::test]
async fn creates_and_reopens_one_filesystem() {
    let storage = MemoryStorage::default();
    let (left, right) = tokio::join!(
        Fs::create(storage.clone(), CreateOptions::new()),
        Fs::create(storage.clone(), CreateOptions::new()),
    );
    let left = left.unwrap();
    let right = right.unwrap();

    assert_eq!(left.format(), right.format());
    let observed = Fs::open(storage).await.unwrap().observe().await.unwrap();
    assert_eq!(observed.version().number(), VersionNumber::ZERO);
    assert_eq!(observed.tree().len(), 1);
    assert_eq!(
        observed.tree().get(&Path::root()).unwrap().id(),
        left.format().root()
    );
}

#[tokio::test]
async fn rejects_a_different_create_configuration() {
    let storage = MemoryStorage::default();
    Fs::create(storage.clone(), CreateOptions::new())
        .await
        .unwrap();
    let options = CreateOptions::new().with_decodings(vec![Extension::new(
        ExtensionId::from_bytes([1; 16]),
        Vec::new(),
    )]);

    let error = Fs::create(storage, options).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
}

#[tokio::test]
async fn publishes_and_retries_one_commit() {
    let storage = MemoryStorage::default();
    let filesystem = Fs::create(storage.clone(), CreateOptions::new())
        .await
        .unwrap();
    let observed = filesystem.observe().await.unwrap();
    let commit = CommitId::generate();
    let successor = add_directory(observed.tree(), "dir", NodeId::generate());

    assert_eq!(
        filesystem
            .commit(&observed, commit, successor.clone())
            .await
            .unwrap(),
        CommitOutcome::Committed {
            version: VersionNumber::from_value(1)
        }
    );
    assert_eq!(
        filesystem
            .commit(&observed, commit, successor)
            .await
            .unwrap(),
        CommitOutcome::Committed {
            version: VersionNumber::from_value(1)
        }
    );

    let reopened = Fs::open(storage).await.unwrap();
    let current = reopened.observe().await.unwrap();
    assert!(current.tree().get(&Path::new("dir").unwrap()).is_some());
    assert_eq!(current.version().commits().len(), 1);
    assert_eq!(current.version().commits()[0].id(), commit);
}

#[tokio::test]
async fn reports_conflict_for_competing_observations() {
    let filesystem = Fs::create(MemoryStorage::default(), CreateOptions::new())
        .await
        .unwrap();
    let first = filesystem.observe().await.unwrap();
    let second = filesystem.observe().await.unwrap();

    let first_tree = add_directory(first.tree(), "first", NodeId::generate());
    let second_tree = add_directory(second.tree(), "second", NodeId::generate());
    assert!(matches!(
        filesystem
            .commit(&first, CommitId::generate(), first_tree)
            .await
            .unwrap(),
        CommitOutcome::Committed { .. }
    ));
    assert_eq!(
        filesystem
            .commit(&second, CommitId::generate(), second_tree)
            .await
            .unwrap(),
        CommitOutcome::Conflict {
            current: VersionNumber::from_value(1)
        }
    );
}

#[tokio::test]
async fn resolves_a_lost_publication_response() {
    let storage = MemoryStorage::default();
    let filesystem = Fs::create(storage.clone(), CreateOptions::new())
        .await
        .unwrap();
    let observed = filesystem.observe().await.unwrap();
    let successor = add_directory(observed.tree(), "durable", NodeId::generate());
    storage.fail_next_replace_after_success();

    assert_eq!(
        filesystem
            .commit(&observed, CommitId::generate(), successor)
            .await
            .unwrap(),
        CommitOutcome::Committed {
            version: VersionNumber::from_value(1)
        }
    );
}

#[tokio::test]
async fn requires_directory_generation_for_membership_changes() {
    let filesystem = Fs::create(MemoryStorage::default(), CreateOptions::new())
        .await
        .unwrap();
    let observed = filesystem.observe().await.unwrap();
    let mut invalid = observed.tree().clone();
    invalid.insert(
        Path::new("child").unwrap(),
        Node::dir(
            NodeId::generate(),
            Generation::FIRST,
            NodeAttrs::default(),
            Generation::FIRST,
        ),
    );

    let error = filesystem
        .commit(&observed, CommitId::generate(), invalid)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Invalid);
}

#[tokio::test]
async fn rejects_duplicate_nodes_and_missing_parents() {
    let filesystem = Fs::create(MemoryStorage::default(), CreateOptions::new())
        .await
        .unwrap();
    let observed = filesystem.observe().await.unwrap();
    let node = NodeId::generate();
    let mut duplicate = add_directory(observed.tree(), "first", node);
    duplicate.insert(
        Path::new("second").unwrap(),
        Node::dir(
            node,
            Generation::FIRST,
            NodeAttrs::default(),
            Generation::FIRST,
        ),
    );
    assert_eq!(
        filesystem
            .commit(&observed, CommitId::generate(), duplicate)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );

    let mut missing_parent = observed.tree().clone();
    missing_parent.insert(
        Path::new("missing/child").unwrap(),
        Node::dir(
            NodeId::generate(),
            Generation::FIRST,
            NodeAttrs::default(),
            Generation::FIRST,
        ),
    );
    assert_eq!(
        filesystem
            .commit(&observed, CommitId::generate(), missing_parent)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
}

#[tokio::test]
async fn rejects_case_folding_collisions_within_a_directory() {
    let filesystem = Fs::create(MemoryStorage::default(), CreateOptions::new())
        .await
        .unwrap();
    let observed = filesystem.observe().await.unwrap();
    let mut successor = observed.tree().clone();
    advance_root_membership(&mut successor);
    successor.insert(
        Path::new("Readme").unwrap(),
        Node::dir(
            NodeId::generate(),
            Generation::FIRST,
            NodeAttrs::default(),
            Generation::FIRST,
        ),
    );
    successor.insert(
        Path::new("README").unwrap(),
        Node::dir(
            NodeId::generate(),
            Generation::FIRST,
            NodeAttrs::default(),
            Generation::FIRST,
        ),
    );

    let error = filesystem
        .commit(&observed, CommitId::generate(), successor)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Invalid);
}

#[tokio::test]
async fn requires_a_new_file_version_for_content_changes() {
    let filesystem = Fs::create(MemoryStorage::default(), CreateOptions::new())
        .await
        .unwrap();
    let genesis = filesystem.observe().await.unwrap();
    let node = NodeId::generate();
    let file_version = yinyang_core::FileVersionId::generate();
    let mut first = genesis.tree().clone();
    advance_root_membership(&mut first);
    first.insert(
        Path::new("file").unwrap(),
        Node::file(
            node,
            Generation::FIRST,
            NodeAttrs::default(),
            File::new(
                file_version,
                ContentId::new(Digest::from_bytes([1; 32]), 0),
                Vec::new(),
            )
            .unwrap(),
        ),
    );
    filesystem
        .commit(&genesis, CommitId::generate(), first)
        .await
        .unwrap();

    let observed = filesystem.observe().await.unwrap();
    let mut changed = observed.tree().clone();
    changed.insert(
        Path::new("file").unwrap(),
        Node::file(
            node,
            Generation::from_value(2),
            NodeAttrs::default(),
            File::new(
                file_version,
                ContentId::new(Digest::from_bytes([2; 32]), 0),
                Vec::new(),
            )
            .unwrap(),
        ),
    );
    assert_eq!(
        filesystem
            .commit(&observed, CommitId::generate(), changed)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
}

#[tokio::test]
async fn requires_each_configured_decoding_identity() {
    let filesystem = Fs::create(
        MemoryStorage::default(),
        CreateOptions::new().with_decodings(vec![Extension::new(
            ExtensionId::from_bytes([1; 16]),
            Vec::new(),
        )]),
    )
    .await
    .unwrap();
    let observed = filesystem.observe().await.unwrap();
    let content = ContentId::new(Digest::from_bytes([3; 32]), 1);
    let source = FileSource::new(
        BlobRef::new(b"encoded".to_vec(), content).unwrap(),
        Vec::new(),
    );
    let part = FilePart::new(FileRange::new(0, 1).unwrap(), 0, source).unwrap();
    let mut tree = observed.tree().clone();
    advance_root_membership(&mut tree);
    tree.insert(
        Path::new("file").unwrap(),
        Node::file(
            NodeId::generate(),
            Generation::FIRST,
            NodeAttrs::default(),
            File::new(yinyang_core::FileVersionId::generate(), content, vec![part]).unwrap(),
        ),
    );

    assert_eq!(
        filesystem
            .commit(&observed, CommitId::generate(), tree)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
}

#[tokio::test]
async fn preserves_node_generation_across_rename() {
    let filesystem = Fs::create(MemoryStorage::default(), CreateOptions::new())
        .await
        .unwrap();
    let genesis = filesystem.observe().await.unwrap();
    let node_id = NodeId::generate();
    filesystem
        .commit(
            &genesis,
            CommitId::generate(),
            add_directory(genesis.tree(), "before", node_id),
        )
        .await
        .unwrap();
    let observed = filesystem.observe().await.unwrap();
    let mut renamed = observed.tree().clone();
    let node = renamed
        .remove(&Path::new("before").unwrap())
        .expect("the committed directory exists");
    renamed.insert(Path::new("after").unwrap(), node.clone());
    let root = renamed.get(&Path::root()).unwrap().clone();
    let dir = root.dir_body().unwrap();
    renamed.insert(
        Path::root(),
        Node::dir(
            root.id(),
            root.generation(),
            root.attrs(),
            dir.entries_generation().next().unwrap(),
        ),
    );

    filesystem
        .commit(&observed, CommitId::generate(), renamed)
        .await
        .unwrap();
    let current = filesystem.observe().await.unwrap();
    assert_eq!(
        current
            .tree()
            .get(&Path::new("after").unwrap())
            .unwrap()
            .generation(),
        Generation::FIRST
    );
}

#[test]
fn validates_file_part_coverage_and_sources() {
    let source_content = ContentId::new(Digest::from_bytes([1; 32]), 8);
    let source = FileSource::new(
        BlobRef::new(b"blob".to_vec(), source_content).unwrap(),
        Vec::new(),
    );
    let first = FilePart::new(FileRange::new(0, 4).unwrap(), 0, source.clone()).unwrap();
    let second = FilePart::new(FileRange::new(5, 3).unwrap(), 4, source.clone()).unwrap();
    let content = ContentId::new(Digest::from_bytes([2; 32]), 8);

    assert_eq!(
        File::new(
            yinyang_core::FileVersionId::generate(),
            content,
            vec![first, second]
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Invalid
    );
    assert_eq!(
        FilePart::new(FileRange::new(0, 5).unwrap(), 4, source)
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
}

#[test]
fn accepts_only_canonical_portable_paths() {
    assert_eq!(Path::new("").unwrap(), Path::root());
    assert!(Path::new("dir/file").is_ok());
    for invalid in ["/rooted", "trailing/", "double//slash", ".", "CON", "bad?"] {
        assert_eq!(Path::new(invalid).unwrap_err().kind(), ErrorKind::Invalid);
    }
}

#[tokio::test]
async fn rejects_a_missing_referenced_version() {
    let storage = MemoryStorage::default();
    let filesystem = Fs::create(storage.clone(), CreateOptions::new())
        .await
        .unwrap();
    storage.remove_current_version();

    let error = filesystem.observe().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Corrupt);
}

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

use futures_util::TryStreamExt as _;
use opendal::{ErrorKind as StorageErrorKind, Operator};

use crate::{
    BlobRef, CommitId, ContentId, Error, File, FilePart, FsVersion, Generation, Node, NodeBody,
    NodeId, Path, Result, Tree,
};

const HEAD_PATH: &str = ".yinyang/head";
const VERSION_PREFIX: &str = ".yinyang/versions/";
const HEAD_MAGIC: &[u8; 8] = b"YYHEAD01";
const VERSION_MAGIC: &[u8; 8] = b"YYVER001";
const MAX_HEAD_BYTES: usize = 4 * 1024;
const MAX_VERSION_BYTES: usize = 64 * 1024 * 1024;
const CHECKSUM_BYTES: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HeadObservation {
    version: BlobRef,
    etag: String,
}

impl HeadObservation {
    pub(crate) const fn version(&self) -> &BlobRef {
        &self.version
    }
}

pub(crate) fn validate_operator(operator: &Operator) -> Result<()> {
    let capability = operator.info().capability();
    if !capability.read
        || !capability.write
        || !capability.write_with_if_match
        || !capability.write_with_if_not_exists
    {
        return Err(Error::unsupported(
            "use YinYang filesystem",
            "OpenDAL backend must support read, write, create-if-absent, and ETag if-match",
        ));
    }
    Ok(())
}

pub(crate) async fn write_version(operator: &Operator, version: &FsVersion) -> Result<BlobRef> {
    let bytes = encode_version(version)?;
    let content = content_id(&bytes);
    let path = version_path(content);
    let already_exists = match operator.write_with(&path, bytes).if_not_exists(true).await {
        Ok(_) => false,
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            true
        }
        Err(error) => return Err(Error::from_storage("write YinYang version", error)),
    };
    let reference = BlobRef::new(path, content);
    if already_exists {
        read_version(operator, &reference).await?;
    }
    Ok(reference)
}

pub(crate) async fn read_version(operator: &Operator, reference: &BlobRef) -> Result<FsVersion> {
    let expected_path = version_path(reference.content());
    if reference.as_bytes() != expected_path.as_bytes()
        || reference.content().length() > MAX_VERSION_BYTES as u64
    {
        return Err(Error::corrupt(
            "read YinYang version",
            "version reference is invalid",
        ));
    }
    let object = read_bounded(
        operator,
        &expected_path,
        MAX_VERSION_BYTES,
        "read YinYang version",
    )
    .await?
    .ok_or_else(|| Error::corrupt("read YinYang version", "referenced version is missing"))?;
    if content_id(&object.bytes) != reference.content() {
        return Err(Error::corrupt(
            "read YinYang version",
            "version does not match its reference",
        ));
    }
    decode_version(&object.bytes)
}

pub(crate) async fn create_head(operator: &Operator, version: &BlobRef) -> Result<()> {
    let bytes = encode_head(version)?;
    match operator
        .write_with(HEAD_PATH, bytes)
        .if_not_exists(true)
        .await
    {
        Ok(_) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(Error::from_storage("create YinYang head", error)),
    }
}

pub(crate) async fn observe_head(operator: &Operator) -> Result<Option<HeadObservation>> {
    let Some(object) =
        read_bounded(operator, HEAD_PATH, MAX_HEAD_BYTES, "observe YinYang head").await?
    else {
        return Ok(None);
    };
    let etag = object.etag.ok_or_else(|| {
        Error::unsupported(
            "observe YinYang head",
            "OpenDAL backend did not return an ETag with the head read",
        )
    })?;
    Ok(Some(HeadObservation {
        version: decode_head(&object.bytes)?,
        etag,
    }))
}

pub(crate) async fn replace_head(
    operator: &Operator,
    observed: &HeadObservation,
    next: &BlobRef,
) -> Result<bool> {
    let bytes = encode_head(next)?;
    match operator
        .write_with(HEAD_PATH, bytes)
        .if_match(&observed.etag)
        .await
    {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::ConditionNotMatch | StorageErrorKind::NotFound
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(Error::from_storage("replace YinYang head", error)),
    }
}

struct StoredObject {
    bytes: Vec<u8>,
    etag: Option<String>,
}

async fn read_bounded(
    operator: &Operator,
    path: &str,
    maximum_bytes: usize,
    operation: &'static str,
) -> Result<Option<StoredObject>> {
    let reader = match operator.reader(path).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage(operation, error)),
    };
    let mut stream = match reader.into_stream(..).await {
        Ok(stream) => stream,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage(operation, error)),
    };
    let (capacity, etag) = match stream.metadata().await {
        Ok(metadata) => {
            let length = usize::try_from(metadata.content_length())
                .ok()
                .filter(|length| *length <= maximum_bytes)
                .ok_or_else(|| Error::corrupt(operation, "object exceeds its size limit"))?;
            (length, metadata.etag().map(str::to_owned))
        }
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) if error.kind() == StorageErrorKind::Unsupported => (0, None),
        Err(error) => return Err(Error::from_storage(operation, error)),
    };

    let mut bytes = Vec::with_capacity(capacity);
    while let Some(buffer) = stream
        .try_next()
        .await
        .map_err(|error| Error::from_storage(operation, error))?
    {
        if buffer.len() > maximum_bytes.saturating_sub(bytes.len()) {
            return Err(Error::corrupt(operation, "object exceeds its size limit"));
        }
        for chunk in buffer {
            bytes.extend_from_slice(&chunk);
        }
    }
    Ok(Some(StoredObject { bytes, etag }))
}

fn encode_version(version: &FsVersion) -> Result<Vec<u8>> {
    let wire = WireVersion::from(version);
    let mut bytes = VERSION_MAGIC.to_vec();
    let body = bincode::encode_to_vec(
        wire,
        bincode::config::standard()
            .with_fixed_int_encoding()
            .with_limit::<MAX_VERSION_BYTES>(),
    )
    .map_err(|_| Error::invalid("encode YinYang version", "version cannot be encoded"))?;
    bytes.extend_from_slice(&body);
    if bytes.len() > MAX_VERSION_BYTES {
        return Err(Error::invalid(
            "encode YinYang version",
            "version exceeds its size limit",
        ));
    }
    Ok(bytes)
}

fn decode_version(bytes: &[u8]) -> Result<FsVersion> {
    if bytes.len() > MAX_VERSION_BYTES || !bytes.starts_with(VERSION_MAGIC) {
        return Err(Error::corrupt(
            "decode YinYang version",
            "version envelope is invalid",
        ));
    }
    let body = &bytes[VERSION_MAGIC.len()..];
    let (wire, consumed) = bincode::decode_from_slice::<WireVersion, _>(
        body,
        bincode::config::standard()
            .with_fixed_int_encoding()
            .with_limit::<MAX_VERSION_BYTES>(),
    )
    .map_err(|_| Error::corrupt("decode YinYang version", "version body is invalid"))?;
    if consumed != body.len() {
        return Err(Error::corrupt(
            "decode YinYang version",
            "version contains trailing bytes",
        ));
    }
    wire.into_version()
        .map_err(|error| Error::corrupt("decode YinYang version", error.message()))
}

fn encode_head(reference: &BlobRef) -> Result<Vec<u8>> {
    let wire = WireBlobRef::from(reference);
    let mut bytes = HEAD_MAGIC.to_vec();
    let body = bincode::encode_to_vec(
        wire,
        bincode::config::standard()
            .with_fixed_int_encoding()
            .with_limit::<MAX_HEAD_BYTES>(),
    )
    .map_err(|_| Error::invalid("encode YinYang head", "head cannot be encoded"))?;
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    if bytes.len() > MAX_HEAD_BYTES {
        return Err(Error::invalid(
            "encode YinYang head",
            "head exceeds its size limit",
        ));
    }
    Ok(bytes)
}

fn decode_head(bytes: &[u8]) -> Result<BlobRef> {
    if bytes.len() < HEAD_MAGIC.len() + CHECKSUM_BYTES
        || bytes.len() > MAX_HEAD_BYTES
        || !bytes.starts_with(HEAD_MAGIC)
    {
        return Err(Error::corrupt(
            "decode YinYang head",
            "head envelope is invalid",
        ));
    }
    let checksum_offset = bytes.len() - CHECKSUM_BYTES;
    if blake3::hash(&bytes[..checksum_offset]).as_bytes() != &bytes[checksum_offset..] {
        return Err(Error::corrupt(
            "decode YinYang head",
            "head checksum is invalid",
        ));
    }
    let body = &bytes[HEAD_MAGIC.len()..checksum_offset];
    let (wire, consumed) = bincode::decode_from_slice::<WireBlobRef, _>(
        body,
        bincode::config::standard()
            .with_fixed_int_encoding()
            .with_limit::<MAX_HEAD_BYTES>(),
    )
    .map_err(|_| Error::corrupt("decode YinYang head", "head body is invalid"))?;
    if consumed != body.len() {
        return Err(Error::corrupt(
            "decode YinYang head",
            "head contains trailing bytes",
        ));
    }
    Ok(wire.into_blob_ref())
}

fn content_id(bytes: &[u8]) -> ContentId {
    ContentId::new(blake3::hash(bytes).into(), bytes.len() as u64)
}

fn version_path(content: ContentId) -> String {
    format!(
        "{VERSION_PREFIX}{}",
        blake3::Hash::from_bytes(*content.digest()).to_hex()
    )
}

#[derive(bincode::Encode, bincode::Decode)]
struct WireVersion {
    entries: Vec<WireEntry>,
    commits: Vec<[u8; 16]>,
}

impl From<&FsVersion> for WireVersion {
    fn from(version: &FsVersion) -> Self {
        Self {
            entries: version
                .tree()
                .iter()
                .map(|(path, node)| WireEntry {
                    path: path.as_str().to_owned(),
                    node: WireNode::from(node),
                })
                .collect(),
            commits: version
                .commits()
                .iter()
                .map(|commit| *commit.as_bytes())
                .collect(),
        }
    }
}

impl WireVersion {
    fn into_version(self) -> Result<FsVersion> {
        let entries = self
            .entries
            .into_iter()
            .map(|entry| Ok((Path::new(entry.path)?, entry.node.into_node()?)))
            .collect::<Result<Vec<_>>>()?;
        let commits = self.commits.into_iter().map(CommitId::from_bytes).collect();
        FsVersion::new(Tree::from_entries(entries)?, commits)
    }
}

#[derive(bincode::Encode, bincode::Decode)]
struct WireEntry {
    path: String,
    node: WireNode,
}

#[derive(bincode::Encode, bincode::Decode)]
struct WireNode {
    id: [u8; 16],
    generation: u64,
    executable: bool,
    body: WireNodeBody,
}

impl From<&Node> for WireNode {
    fn from(node: &Node) -> Self {
        Self {
            id: *node.id().as_bytes(),
            generation: node.generation().value(),
            executable: node.executable(),
            body: WireNodeBody::from(node.body()),
        }
    }
}

impl WireNode {
    fn into_node(self) -> Result<Node> {
        let id = NodeId::from_bytes(self.id);
        let generation = Generation::from_value(self.generation);
        match self.body {
            WireNodeBody::Dir { entries_generation } => Ok(Node::dir(
                id,
                generation,
                self.executable,
                Generation::from_value(entries_generation),
            )),
            WireNodeBody::File(file) => Ok(Node::file(
                id,
                generation,
                self.executable,
                file.into_file()?,
            )),
        }
    }
}

#[derive(bincode::Encode, bincode::Decode)]
enum WireNodeBody {
    Dir { entries_generation: u64 },
    File(WireFile),
}

impl From<&NodeBody> for WireNodeBody {
    fn from(body: &NodeBody) -> Self {
        match body {
            NodeBody::Dir { entries_generation } => Self::Dir {
                entries_generation: entries_generation.value(),
            },
            NodeBody::File(file) => Self::File(WireFile::from(file)),
        }
    }
}

#[derive(bincode::Encode, bincode::Decode)]
struct WireFile {
    content: WireContentId,
    parts: Vec<WireFilePart>,
}

impl From<&File> for WireFile {
    fn from(file: &File) -> Self {
        Self {
            content: WireContentId::from(file.content()),
            parts: file.parts().iter().map(WireFilePart::from).collect(),
        }
    }
}

impl WireFile {
    fn into_file(self) -> Result<File> {
        let parts = self
            .parts
            .into_iter()
            .map(WireFilePart::into_part)
            .collect::<Result<Vec<_>>>()?;
        File::new(self.content.into_content_id(), parts)
    }
}

#[derive(bincode::Encode, bincode::Decode)]
struct WireFilePart {
    start: u64,
    end: u64,
    blob_offset: u64,
    blob: WireBlobRef,
}

impl From<&FilePart> for WireFilePart {
    fn from(part: &FilePart) -> Self {
        Self {
            start: part.range().start,
            end: part.range().end,
            blob_offset: part.blob_offset(),
            blob: WireBlobRef::from(part.blob()),
        }
    }
}

impl WireFilePart {
    fn into_part(self) -> Result<FilePart> {
        FilePart::new(
            self.start..self.end,
            self.blob_offset,
            self.blob.into_blob_ref(),
        )
    }
}

#[derive(bincode::Encode, bincode::Decode)]
struct WireBlobRef {
    reference: Vec<u8>,
    content: WireContentId,
}

impl From<&BlobRef> for WireBlobRef {
    fn from(reference: &BlobRef) -> Self {
        Self {
            reference: reference.as_bytes().to_vec(),
            content: WireContentId::from(reference.content()),
        }
    }
}

impl WireBlobRef {
    fn into_blob_ref(self) -> BlobRef {
        BlobRef::new(self.reference, self.content.into_content_id())
    }
}

#[derive(bincode::Encode, bincode::Decode)]
struct WireContentId {
    digest: [u8; 32],
    length: u64,
}

impl From<ContentId> for WireContentId {
    fn from(content: ContentId) -> Self {
        Self {
            digest: *content.digest(),
            length: content.length(),
        }
    }
}

impl WireContentId {
    const fn into_content_id(self) -> ContentId {
        ContentId::new(self.digest, self.length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_wire_contract_is_stable() {
        let root = NodeId::from_bytes([1; 16]);
        let version = FsVersion::new(Tree::genesis(root), Vec::new()).unwrap();

        let actual = encode_version(&version).unwrap();
        let mut expected = VERSION_MAGIC.to_vec();
        expected.extend_from_slice(&1_u64.to_le_bytes());
        expected.extend_from_slice(&0_u64.to_le_bytes());
        expected.extend_from_slice(&[1; 16]);
        expected.extend_from_slice(&1_u64.to_le_bytes());
        expected.push(0);
        expected.extend_from_slice(&0_u32.to_le_bytes());
        expected.extend_from_slice(&1_u64.to_le_bytes());
        expected.extend_from_slice(&0_u64.to_le_bytes());

        assert_eq!(actual, expected);
        assert_eq!(decode_version(&actual).unwrap(), version);
    }

    #[test]
    fn head_wire_contract_is_stable() {
        let reference = BlobRef::new(b"v".to_vec(), ContentId::new([2; 32], 3));

        let actual = encode_head(&reference).unwrap();
        let mut expected = HEAD_MAGIC.to_vec();
        expected.extend_from_slice(&1_u64.to_le_bytes());
        expected.push(b'v');
        expected.extend_from_slice(&[2; 32]);
        expected.extend_from_slice(&3_u64.to_le_bytes());
        let checksum = blake3::hash(&expected);
        expected.extend_from_slice(checksum.as_bytes());

        assert_eq!(actual, expected);
        assert_eq!(decode_head(&actual).unwrap(), reference);
    }
}

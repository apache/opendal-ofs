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

//! Experimental Managed volume format v0.

use serde::Serialize;

use crate::Error;
use crate::v0::model::{NodeId, VolumeId};

use super::codec::RecordCodec;
use super::fixed::identity;

/// Control object that stores the volume descriptor.
pub const FORMAT_KEY: &str = "managed/0/format";
const MAX_FORMAT_BODY_BYTES: usize = 64 * 1024;
/// Envelope for the volume descriptor.
pub const FORMAT_RECORD: RecordCodec = RecordCodec::new(*b"OFSFMT00", MAX_FORMAT_BODY_BYTES);
/// Default shared data-segment rotation target.
pub const DEFAULT_DATA_SEGMENT_TARGET_BYTES: u64 = 8 * 1024 * 1024;

identity!(
    /// Stable identity of one persisted extension type.
    pub ExtensionId,
    16
);

/// Self-description stored in the volume format for one active extension.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtensionDescriptor {
    id: ExtensionId,
    configuration: Vec<u8>,
}

impl ExtensionDescriptor {
    /// Describe an extension without persisted parameters.
    pub const fn empty(id: ExtensionId) -> Self {
        Self {
            id,
            configuration: Vec::new(),
        }
    }

    /// Encode one fixed extension configuration.
    pub fn encode(id: ExtensionId, value: &impl Serialize) -> Self {
        let mut configuration = Vec::new();
        ciborium::into_writer(value, &mut configuration)
            .expect("a fixed extension configuration is encodable");
        Self { id, configuration }
    }

    pub const fn id(&self) -> ExtensionId {
        self.id
    }

    pub fn configuration(&self) -> &[u8] {
        &self.configuration
    }

    /// Decode the exact configuration of the expected extension.
    pub fn decode<T: serde::de::DeserializeOwned>(
        &self,
        expected: ExtensionId,
    ) -> Result<T, Error> {
        let mut input = self.require(expected)?;
        let value = ciborium::from_reader(&mut input).map_err(|_| {
            Error::corrupt("open Managed volume", "extension configuration is invalid")
        })?;
        if !input.is_empty() {
            return Err(Error::corrupt(
                "open Managed volume",
                "extension configuration has trailing bytes",
            ));
        }
        Ok(value)
    }

    /// Require one extension identity and return its raw configuration.
    pub fn require(&self, expected: ExtensionId) -> Result<&[u8], Error> {
        if self.id != expected {
            return Err(Error::unsupported(
                "open Managed volume",
                "extension identity does not match this build",
            ));
        }
        Ok(&self.configuration)
    }
}

super::codec::tuple_wire!(ExtensionDescriptor {
    id: ExtensionId,
    configuration: Vec<u8>,
});

/// Persistent file-data organization for one volume.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileDataLayout {
    data_segment_target_bytes: u64,
    partitioning: Option<ExtensionDescriptor>,
    decodings: Vec<ExtensionDescriptor>,
}

super::codec::tuple_wire!(FileDataLayout {
    data_segment_target_bytes: u64,
    partitioning: Option<ExtensionDescriptor>,
    decodings: Vec<ExtensionDescriptor>,
});

impl FileDataLayout {
    pub fn new(
        data_segment_target_bytes: u64,
        partitioning: Option<ExtensionDescriptor>,
        decodings: Vec<ExtensionDescriptor>,
    ) -> Result<Self, Error> {
        if data_segment_target_bytes == 0 {
            return Err(Error::invalid(
                "construct file data layout",
                "data segment target must be positive",
            ));
        }
        Ok(Self {
            data_segment_target_bytes,
            partitioning,
            decodings,
        })
    }

    /// Whole-file identity layout with shared data-segment placement.
    pub fn whole_identity(data_segment_target_bytes: u64) -> Result<Self, Error> {
        Self::new(data_segment_target_bytes, None, Vec::new())
    }

    pub const fn data_segment_target_bytes(&self) -> u64 {
        self.data_segment_target_bytes
    }

    pub fn partitioning(&self) -> Option<&ExtensionDescriptor> {
        self.partitioning.as_ref()
    }

    pub fn decodings(&self) -> &[ExtensionDescriptor] {
        &self.decodings
    }
}

/// The sole Managed storage format understood by this build.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeFormat {
    volume_id: VolumeId,
    root_node_id: NodeId,
    file_data_layout: FileDataLayout,
    authority: Option<ExtensionDescriptor>,
}

super::codec::tuple_wire!(VolumeFormat {
    volume_id: VolumeId,
    root_node_id: NodeId,
    file_data_layout: FileDataLayout,
    authority: Option<ExtensionDescriptor>,
});

impl VolumeFormat {
    pub fn new(
        volume_id: VolumeId,
        root_node_id: NodeId,
        file_data_layout: FileDataLayout,
        authority: Option<ExtensionDescriptor>,
    ) -> Self {
        Self {
            volume_id,
            root_node_id,
            file_data_layout,
            authority,
        }
    }

    pub const fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub const fn root_node_id(&self) -> NodeId {
        self.root_node_id
    }

    pub const fn file_data_layout(&self) -> &FileDataLayout {
        &self.file_data_layout
    }

    pub fn authority(&self) -> Option<&ExtensionDescriptor> {
        self.authority.as_ref()
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        FORMAT_RECORD.encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        FORMAT_RECORD.decode(bytes)
    }
}

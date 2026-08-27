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

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::v0::fixed::identity;

identity!(
    /// Stable identity of one Managed volume.
    pub VolumeId,
    16
);
identity!(
    /// Stable identity of one filesystem node.
    pub NodeId,
    16
);
identity!(
    /// Content identity used by immutable Managed records and file data.
    pub Digest,
    32
);
identity!(
    /// Stable identity of one immutable logical file version.
    pub FileVersionId,
    16
);
identity!(
    /// Integrity value used to verify one independently readable record.
    pub Checksum,
    32
);
identity!(
    /// Idempotency identity of one publication attempt.
    pub OperationId,
    16
);

macro_rules! generated_identity {
    ($($name:ident),+ $(,)?) => {
        $(
            impl $name {
                pub fn generate() -> Self {
                    Self::from_bytes(*uuid::Uuid::new_v4().as_bytes())
                }
            }
        )+
    };
}

generated_identity!(VolumeId, NodeId, FileVersionId, OperationId);

/// Stable identity of one logical byte sequence.
///
/// The reference is independent of a file version, path, and physical layout,
/// so the same content can be compared and reused across namespace entries.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContentRef {
    digest: Digest,
    length: u64,
}

impl Serialize for ContentRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        (self.digest, self.length).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ContentRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (digest, length) = Deserialize::deserialize(deserializer)?;
        Ok(Self { digest, length })
    }
}

impl ContentRef {
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

macro_rules! display_identity {
    ($($name:ident),+ $(,)?) => {
        $(
            impl fmt::Display for $name {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    for byte in self.as_bytes() {
                        write!(formatter, "{byte:02x}")?;
                    }
                    Ok(())
                }
            }
        )+
    };
}

display_identity!(VolumeId, FileVersionId, OperationId);

/// A position in a Managed volume's ordered change stream.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChangeCursor(u64);

impl Serialize for ChangeCursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ChangeCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        u64::deserialize(deserializer).map(Self)
    }
}

impl ChangeCursor {
    pub const GENESIS: Self = Self(0);

    pub const fn sequence(self) -> u64 {
        self.0
    }

    pub const fn from_sequence(sequence: u64) -> Self {
        Self(sequence)
    }
}

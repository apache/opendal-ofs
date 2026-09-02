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

use std::fmt;

use crate::{Error, Result};

macro_rules! identity {
    ($name:ident, $bytes:literal) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; $bytes]);

        impl $name {
            pub const fn from_bytes(bytes: [u8; $bytes]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; $bytes] {
                &self.0
            }
        }
    };
}

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

identity!(FsId, 16);
identity!(NodeId, 16);
identity!(FileVersionId, 16);
identity!(CommitId, 16);
identity!(ExtensionId, 16);
identity!(Digest, 32);

generated_identity!(FsId, NodeId, FileVersionId, CommitId);

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

display_identity!(FsId, NodeId, FileVersionId, CommitId, ExtensionId, Digest);

macro_rules! counter {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u64);

        impl $name {
            pub const ZERO: Self = Self(0);

            pub const fn from_value(value: u64) -> Self {
                Self(value)
            }

            pub const fn value(self) -> u64 {
                self.0
            }

            pub fn next(self) -> Result<Self> {
                self.0
                    .checked_add(1)
                    .map(Self)
                    .ok_or_else(|| Error::corrupt("advance YinYang counter", "counter overflows"))
            }
        }
    };
}

counter!(Generation);
counter!(VersionNumber);
counter!(GcEpoch);

impl Generation {
    pub const FIRST: Self = Self(1);
}

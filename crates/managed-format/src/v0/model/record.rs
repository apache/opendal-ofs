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

use serde::de::{Error as _, SeqAccess, Visitor};
use serde::ser::SerializeTuple as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

use crate::Error;

use super::{ContentRef, FileVersionId, NodeAttributes, NodeId, NodeKind};

/// One path-ordered row in a Managed namespace stream.
///
/// `data` is supplied by the volume implementation. A durable Managed
/// namespace uses its immutable data reference; a local scan uses `()`
/// until publication attaches that reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceRecord<C> {
    pub path: String,
    pub value: Option<NamespaceNode<C>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceNode<C> {
    pub node_id: NodeId,
    pub generation: u64,
    pub attributes: NodeAttributes,
    pub value: NamespaceValue<C>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamespaceValue<C> {
    Directory {
        generation: u64,
    },
    RegularFile {
        version: FileVersionId,
        content: ContentRef,
        data: C,
    },
}

impl<C: Serialize> Serialize for NamespaceRecord<C> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(2)?;
        tuple.serialize_element(&self.path)?;
        tuple.serialize_element(&self.value)?;
        tuple.end()
    }
}

impl<'de, C: Deserialize<'de>> Deserialize<'de> for NamespaceRecord<C> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (path, value) = Deserialize::deserialize(deserializer)?;
        Ok(Self { path, value })
    }
}

impl<C: Serialize> Serialize for NamespaceNode<C> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(4)?;
        tuple.serialize_element(&self.node_id)?;
        tuple.serialize_element(&self.generation)?;
        tuple.serialize_element(&self.attributes)?;
        tuple.serialize_element(&self.value)?;
        tuple.end()
    }
}

impl<'de, C: Deserialize<'de>> Deserialize<'de> for NamespaceNode<C> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (node_id, generation, attributes, value) = Deserialize::deserialize(deserializer)?;
        Ok(Self {
            node_id,
            generation,
            attributes,
            value,
        })
    }
}

impl<C: Serialize> Serialize for NamespaceValue<C> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Directory { generation } => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&0_u8)?;
                tuple.serialize_element(generation)?;
                tuple.end()
            }
            Self::RegularFile {
                version,
                content,
                data,
            } => {
                let mut tuple = serializer.serialize_tuple(4)?;
                tuple.serialize_element(&1_u8)?;
                tuple.serialize_element(version)?;
                tuple.serialize_element(content)?;
                tuple.serialize_element(data)?;
                tuple.end()
            }
        }
    }
}

impl<'de, C: Deserialize<'de>> Deserialize<'de> for NamespaceValue<C> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ValueVisitor<C>(std::marker::PhantomData<C>);

        impl<'de, C: Deserialize<'de>> Visitor<'de> for ValueVisitor<C> {
            type Value = NamespaceValue<C>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a positional namespace value")
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let kind = sequence
                    .next_element::<u8>()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let value = match kind {
                    0 => NamespaceValue::Directory {
                        generation: sequence
                            .next_element()?
                            .ok_or_else(|| A::Error::invalid_length(1, &self))?,
                    },
                    1 => NamespaceValue::RegularFile {
                        version: sequence
                            .next_element()?
                            .ok_or_else(|| A::Error::invalid_length(1, &self))?,
                        content: sequence
                            .next_element()?
                            .ok_or_else(|| A::Error::invalid_length(2, &self))?,
                        data: sequence
                            .next_element()?
                            .ok_or_else(|| A::Error::invalid_length(3, &self))?,
                    },
                    _ => return Err(A::Error::custom("unknown namespace value kind")),
                };
                if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    return Err(A::Error::custom("namespace value has trailing fields"));
                }
                Ok(value)
            }
        }

        deserializer.deserialize_seq(ValueVisitor(std::marker::PhantomData))
    }
}

impl<C> NamespaceNode<C> {
    pub const fn kind(&self) -> NodeKind {
        match self.value {
            NamespaceValue::Directory { .. } => NodeKind::Directory,
            NamespaceValue::RegularFile { .. } => NodeKind::RegularFile,
        }
    }

    pub const fn file(&self) -> Option<(FileVersionId, ContentRef, &C)> {
        match &self.value {
            NamespaceValue::RegularFile {
                version,
                content,
                data,
            } => Some((*version, *content, data)),
            NamespaceValue::Directory { .. } => None,
        }
    }

    pub fn map_data<D>(self, map: impl FnOnce(C) -> D) -> NamespaceNode<D> {
        NamespaceNode {
            node_id: self.node_id,
            generation: self.generation,
            attributes: self.attributes,
            value: match self.value {
                NamespaceValue::Directory { generation } => {
                    NamespaceValue::Directory { generation }
                }
                NamespaceValue::RegularFile {
                    version,
                    content,
                    data,
                } => NamespaceValue::RegularFile {
                    version,
                    content,
                    data: map(data),
                },
            },
        }
    }
}

impl<C> NamespaceRecord<C> {
    pub fn map_data<D>(self, map: impl FnOnce(C) -> D + Copy) -> NamespaceRecord<D> {
        NamespaceRecord {
            path: self.path,
            value: self.value.map(|value| value.map_data(map)),
        }
    }
}

/// Validate one RFC volume name without retaining a catalog entry.
pub fn validate_volume_name(name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::invalid(
            "validate volume name",
            "volume name is empty",
        ));
    }
    validate_portable_component(name).map_err(|_| {
        Error::invalid(
            "validate volume name",
            "volume name is not a portable locator",
        )
    })
}

/// Validate one canonical path without retaining the namespace.
pub fn validate_portable_path(path: &str) -> Result<(), Error> {
    if path.is_empty() {
        return Ok(());
    }
    if path.len() > MAX_PATH_BYTES
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains("//")
    {
        return Err(Error::invalid(
            "validate filesystem path",
            "path is not portable",
        ));
    }
    for name in path.split('/') {
        validate_portable_component(name)?;
    }
    Ok(())
}

fn validate_portable_component(name: &str) -> Result<(), Error> {
    if name.len() > MAX_COMPONENT_BYTES
        || name == "."
        || name == ".."
        || name.ends_with([' ', '.'])
        || name.chars().any(|character| {
            character.is_control()
                || matches!(character, '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*')
        })
        || !name.nfc().eq(name.chars())
    {
        return Err(Error::invalid(
            "validate filesystem path",
            "path component is not portable",
        ));
    }
    let folded_name = name.case_fold().nfc().collect::<String>();
    let stem = folded_name.split('.').next().unwrap_or_default();
    if matches!(stem, "con" | "prn" | "aux" | "nul")
        || stem.len() == 4
            && (stem.starts_with("com") || stem.starts_with("lpt"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9')
        || matches!(stem, "com¹" | "com²" | "com³" | "lpt¹" | "lpt²" | "lpt³")
    {
        return Err(Error::invalid(
            "validate filesystem path",
            "path component is reserved",
        ));
    }
    Ok(())
}

const MAX_COMPONENT_BYTES: usize = 255;
const MAX_PATH_BYTES: usize = 4096;

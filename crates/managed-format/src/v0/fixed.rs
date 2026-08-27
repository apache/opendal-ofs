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

//! Canonical serde decoding for fixed-width byte identities.

use std::fmt;

macro_rules! identity {
    ($(#[$meta:meta])* $visibility:vis $name:ident, $length:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        $visibility struct $name([u8; $length]);

        impl $name {
            $visibility const fn from_bytes(bytes: [u8; $length]) -> Self {
                Self(bytes)
            }

            $visibility const fn as_bytes(&self) -> &[u8; $length] {
                &self.0
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_bytes(&self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                crate::v0::fixed::deserialize(deserializer).map(Self)
            }
        }
    };
}

pub(crate) use identity;

pub(crate) fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct FixedBytesVisitor<const N: usize>;

    impl<const N: usize> serde::de::Visitor<'_> for FixedBytesVisitor<N> {
        type Value = [u8; N];

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "a {N}-byte value")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            value
                .try_into()
                .map_err(|_| E::invalid_length(value.len(), &self))
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.visit_bytes(&value)
        }
    }

    deserializer.deserialize_bytes(FixedBytesVisitor::<N>)
}

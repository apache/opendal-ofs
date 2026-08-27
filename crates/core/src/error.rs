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
use std::path::Path;

/// Stable failure categories based on the caller's next action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    Invalid,
    NotFound,
    Unsupported,
    PermissionDenied,
    Conflict,
    Corrupt,
    Unavailable,
}

/// An OFS failure with a stable category and safe diagnostic context.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    operation: &'static str,
    message: String,
    context: Vec<(&'static str, String)>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Error {
    pub fn new(kind: ErrorKind, operation: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            operation,
            message: message.into(),
            context: Vec::new(),
            source: None,
        }
    }

    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn with_context(mut self, key: &'static str, value: impl ToString) -> Self {
        self.context.push((key, value.to_string()));
        self
    }

    /// Attach the original implementation failure without changing its stable kind.
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    pub(crate) fn invalid(operation: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, operation, message)
    }

    pub(crate) fn conflict(operation: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, operation, message)
    }

    pub(crate) fn unsupported(operation: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, operation, message)
    }

    pub(crate) fn corrupt(operation: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Corrupt, operation, message)
    }

    pub(crate) fn from_io(
        operation: &'static str,
        path: Option<&Path>,
        source: std::io::Error,
    ) -> Self {
        let kind = match source.kind() {
            std::io::ErrorKind::NotFound => ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            std::io::ErrorKind::AlreadyExists => ErrorKind::Conflict,
            std::io::ErrorKind::Unsupported => ErrorKind::Unsupported,
            _ => ErrorKind::Unavailable,
        };
        let mut error = Self::new(kind, operation, "local filesystem operation failed");
        if let Some(path) = path {
            error = error.with_context("path", path.display());
        }
        error.source = Some(Box::new(source));
        error
    }

    pub(crate) fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self::from_io(operation, None, source)
    }

    pub(crate) fn from_storage(operation: &'static str, source: opendal::Error) -> Self {
        let kind = match source.kind() {
            opendal::ErrorKind::ConfigInvalid => ErrorKind::Invalid,
            opendal::ErrorKind::Unsupported => ErrorKind::Unsupported,
            opendal::ErrorKind::NotFound => ErrorKind::NotFound,
            opendal::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::ConditionNotMatch => {
                ErrorKind::Conflict
            }
            _ => ErrorKind::Unavailable,
        };
        let message = match kind {
            ErrorKind::Invalid => "object storage configuration is invalid",
            ErrorKind::NotFound => "object storage entry does not exist",
            ErrorKind::Unsupported => "object storage operation is unsupported",
            ErrorKind::PermissionDenied => "object storage permission denied",
            ErrorKind::Conflict => "object storage condition changed",
            ErrorKind::Corrupt | ErrorKind::Unavailable => "object storage is unavailable",
        };
        let status = if source.is_temporary() {
            "temporary"
        } else if source.is_persistent() {
            "persistent"
        } else {
            "permanent"
        };
        Self::new(kind, operation, message)
            .with_context("storage_kind", source.kind())
            .with_context("storage_status", status)
            .with_source(source)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.operation, self.message)?;
        if !self.context.is_empty() {
            write!(formatter, " (")?;
            for (index, (key, value)) in self.context.iter().enumerate() {
                if index != 0 {
                    write!(formatter, ", ")?;
                }
                write!(formatter, "{key}: {value}")?;
            }
            write!(formatter, ")")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl From<ofs_managed_format::Error> for Error {
    fn from(source: ofs_managed_format::Error) -> Self {
        let kind = match source.kind() {
            ofs_managed_format::ErrorKind::Invalid => ErrorKind::Invalid,
            ofs_managed_format::ErrorKind::Unsupported => ErrorKind::Unsupported,
            ofs_managed_format::ErrorKind::Corrupt => ErrorKind::Corrupt,
            _ => ErrorKind::Corrupt,
        };
        let operation = source.operation();
        let message = source.message().to_owned();
        Self::new(kind, operation, message).with_source(source)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

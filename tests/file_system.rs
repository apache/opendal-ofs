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

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::ffi::OsStr;

use fuse3::path::prelude::*;
use opendal::Operator;
use opendal::services;

#[tokio::test]
async fn test_release_commits_pending_write() {
    let root = tempfile::tempdir().unwrap();
    let root_path = root.path().to_string_lossy().to_string();
    let operator = Operator::new(services::Fs::default().root(&root_path))
        .unwrap()
        .finish();
    let filesystem = fuse3_opendal::Filesystem::new(operator.clone(), 0, 0);

    let path = OsStr::new("test.txt");
    let flags = (libc::O_WRONLY | libc::O_TRUNC) as u32;
    let created = filesystem
        .create(Request::default(), OsStr::new(""), path, 0o644, flags)
        .await
        .unwrap();
    filesystem
        .write(
            Request::default(),
            Some(path),
            created.fh,
            0,
            b"hello",
            0,
            flags,
        )
        .await
        .unwrap();

    filesystem
        .release(Request::default(), Some(path), created.fh, flags, 0, false)
        .await
        .unwrap();

    let content = operator.read("test.txt").await.unwrap().to_bytes();
    assert_eq!(content.as_ref(), b"hello");
}

#[tokio::test]
async fn test_flush_keeps_file_handle_open() {
    let root = tempfile::tempdir().unwrap();
    let root_path = root.path().to_string_lossy().to_string();
    let operator = Operator::new(services::Fs::default().root(&root_path))
        .unwrap()
        .finish();
    operator.write("test.txt", "hello").await.unwrap();

    let filesystem = fuse3_opendal::Filesystem::new(operator, 0, 0);
    let path = OsStr::new("test.txt");
    let flags = libc::O_RDONLY as u32;
    let opened = filesystem
        .open(Request::default(), path, flags)
        .await
        .unwrap();

    filesystem
        .flush(Request::default(), Some(path), opened.fh, 0)
        .await
        .unwrap();

    let reply = filesystem
        .read(Request::default(), Some(path), opened.fh, 0, 5)
        .await
        .unwrap();
    assert_eq!(reply.data.as_ref(), b"hello");

    filesystem
        .release(Request::default(), Some(path), opened.fh, flags, 0, false)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_read_honors_requested_range() {
    let root = tempfile::tempdir().unwrap();
    let root_path = root.path().to_string_lossy().to_string();
    let operator = Operator::new(services::Fs::default().root(&root_path))
        .unwrap()
        .finish();
    operator.write("source.txt", "hello world").await.unwrap();

    let filesystem = fuse3_opendal::Filesystem::new(operator, 0, 0);
    let path = OsStr::new("source.txt");
    let flags = libc::O_RDONLY as u32;
    let opened = filesystem
        .open(Request::default(), path, flags)
        .await
        .unwrap();

    let reply = filesystem
        .read(Request::default(), Some(path), opened.fh, 1, 4)
        .await
        .unwrap();
    assert_eq!(reply.data.as_ref(), b"ello");

    let reply = filesystem
        .read(Request::default(), Some(path), opened.fh, 8, 8)
        .await
        .unwrap();
    assert_eq!(reply.data.as_ref(), b"rld");

    let reply = filesystem
        .read(Request::default(), Some(path), opened.fh, 11, 4)
        .await
        .unwrap();
    assert!(reply.data.is_empty());

    filesystem
        .release(Request::default(), Some(path), opened.fh, flags, 0, false)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_copy_file_range_honors_requested_length() {
    let root = tempfile::tempdir().unwrap();
    let root_path = root.path().to_string_lossy().to_string();
    let operator = Operator::new(services::Fs::default().root(&root_path))
        .unwrap()
        .finish();
    operator.write("source.txt", "hello world").await.unwrap();

    let filesystem = fuse3_opendal::Filesystem::new(operator.clone(), 0, 0);
    let source_path = OsStr::new("source.txt");
    let source_flags = libc::O_RDONLY as u32;
    let source = filesystem
        .open(Request::default(), source_path, source_flags)
        .await
        .unwrap();

    let target_path = OsStr::new("target.txt");
    let target_flags = (libc::O_WRONLY | libc::O_TRUNC) as u32;
    let target = filesystem
        .create(
            Request::default(),
            OsStr::new(""),
            target_path,
            0o644,
            target_flags,
        )
        .await
        .unwrap();

    let reply = filesystem
        .copy_file_range(
            Request::default(),
            Some(source_path),
            source.fh,
            1,
            Some(target_path),
            target.fh,
            0,
            4,
            0,
        )
        .await
        .unwrap();
    assert_eq!(reply.copied, 4);

    filesystem
        .release(
            Request::default(),
            Some(target_path),
            target.fh,
            target_flags,
            0,
            false,
        )
        .await
        .unwrap();
    filesystem
        .release(
            Request::default(),
            Some(source_path),
            source.fh,
            source_flags,
            0,
            false,
        )
        .await
        .unwrap();

    let content = operator.read("target.txt").await.unwrap().to_bytes();
    assert_eq!(content.as_ref(), b"ello");
}

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

//! Profile-driven black-box acceptance for Managed volumes.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use serde::Deserialize;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Parser)]
#[clap(name = "acceptance")]
pub(crate) struct CommandAcceptance {
    #[arg(long, value_enum, default_value_t = ProfileName::Smoke)]
    profile: ProfileName,

    #[arg(long, default_value_t = 825)]
    seed: u64,

    #[arg(long)]
    files: Option<u64>,

    #[arg(long)]
    file_bytes: Option<u64>,

    #[arg(long)]
    directory_fanout: Option<u64>,

    #[arg(long)]
    generations: Option<u64>,

    #[arg(long)]
    changes: Option<u64>,

    #[arg(long, help = "Keep generated files after the fixture stops.")]
    keep: bool,
}

impl CommandAcceptance {
    pub(crate) fn run(self) {
        run(self).unwrap_or_else(|error| panic!("Managed acceptance failed: {error}"));
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProfileName {
    Smoke,
    TinyFiles,
    LargeFiles,
}

impl ProfileName {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::TinyFiles => "tiny-files",
            Self::LargeFiles => "large-files",
        }
    }

    const fn defaults(self, seed: u64) -> Profile {
        match self {
            Self::Smoke => Profile {
                name: self,
                seed,
                files: 64,
                file_bytes: 64 * 1024,
                directory_fanout: 16,
                generations: 2,
                changes: 8,
            },
            Self::TinyFiles => Profile {
                name: self,
                seed,
                files: 1_000_000,
                file_bytes: 4 * 1024,
                directory_fanout: 1_000,
                generations: 1,
                changes: 4_096,
            },
            Self::LargeFiles => Profile {
                name: self,
                seed,
                files: 3,
                file_bytes: 10 * 1024 * 1024 * 1024,
                directory_fanout: 3,
                generations: 1,
                changes: 1,
            },
        }
    }
}

#[derive(Clone, Copy)]
struct Profile {
    name: ProfileName,
    seed: u64,
    files: u64,
    file_bytes: u64,
    directory_fanout: u64,
    generations: u64,
    changes: u64,
}

impl Profile {
    fn resolve(args: &CommandAcceptance) -> Result<Self, String> {
        let mut profile = args.profile.defaults(args.seed);
        profile.files = args.files.unwrap_or(profile.files);
        profile.file_bytes = args.file_bytes.unwrap_or(profile.file_bytes);
        profile.directory_fanout = args.directory_fanout.unwrap_or(profile.directory_fanout);
        profile.generations = args.generations.unwrap_or(profile.generations);
        profile.changes = args.changes.unwrap_or(profile.changes);
        if profile.files == 0
            || profile.file_bytes == 0
            || profile.directory_fanout == 0
            || profile.generations == 0
            || profile.changes == 0
        {
            return Err("profile counts and sizes must be positive".into());
        }
        Ok(profile)
    }

    fn logical_bytes(self) -> Result<u64, String> {
        self.files
            .checked_mul(self.file_bytes)
            .ok_or_else(|| "profile logical size overflows".into())
    }
}

#[derive(Deserialize)]
struct Status {
    volume_id: String,
    common_sequence: u64,
    remote_sequence: u64,
    pending: bool,
    conflicts: u64,
}

#[derive(serde::Serialize)]
struct Evidence<'a> {
    schema: u32,
    profile: &'a str,
    seed: u64,
    files: u64,
    file_bytes: u64,
    logical_bytes: u64,
    initial_publish_ms: u128,
    cold_restore_ms: u128,
    reconcile_ms: u128,
    post_gc_restore_ms: u128,
    initial_storage: StorageShape,
    final_storage: StorageShape,
    replica_state_bytes: u64,
    volume_id: &'a str,
    result: &'static str,
}

fn run(args: CommandAcceptance) -> Result<(), String> {
    let profile = Profile::resolve(&args)?;
    build_product()?;
    let run_root = env::var_os("OFS_ACCEPTANCE_ROOT")
        .map_or_else(|| workspace().join(".local/acceptance/runs"), PathBuf::from);
    fs::create_dir_all(&run_root)
        .map_err(|error| format!("create acceptance run root {}: {error}", run_root.display()))?;
    let root = tempfile::Builder::new()
        .prefix("opendal-ofs-acceptance-")
        .tempdir_in(&run_root)
        .map_err(|error| format!("create acceptance root: {error}"))?;
    let paths = Paths::new(root.path());
    fs::create_dir_all(&paths.replica_a).map_err(|error| format!("create replica A: {error}"))?;
    fs::create_dir_all(&paths.replica_b).map_err(|error| format!("create replica B: {error}"))?;
    fs::create_dir_all(&paths.replica_c).map_err(|error| format!("create replica C: {error}"))?;
    seed_dataset(&paths.replica_a, profile)?;

    let fixture = Fixture::start()?;
    fs::create_dir_all(paths.home.join("tmp"))
        .map_err(|error| format!("create product work directory: {error}"))?;
    let product = Product::new(&paths.home, fixture.storage_url());
    product.create_volume()?;

    let initial_publish_ms = timed(|| product.sync(&paths.replica_a, &paths.state_a))?;
    let initial = product.status(&paths.replica_a, &paths.state_a)?;
    require_clean(&initial)?;
    let initial_storage = fixture.storage_shape()?;

    let cold_restore_ms = timed(|| product.sync(&paths.replica_b, &paths.state_b))?;
    require_same_tree(&paths.replica_a, &paths.replica_b, "cold restore")?;

    let reconcile_started = Instant::now();
    for generation in 0..profile.generations {
        mutate_dataset(&paths.replica_a, profile, generation)?;
        product.sync(&paths.replica_a, &paths.state_a)?;
        product.sync(&paths.replica_b, &paths.state_b)?;
        require_same_tree(&paths.replica_a, &paths.replica_b, "reconciliation")?;
    }
    let reconcile_ms = reconcile_started.elapsed().as_millis();

    let before_noop = product.status(&paths.replica_a, &paths.state_a)?;
    product.sync(&paths.replica_a, &paths.state_a)?;
    let after_noop = product.status(&paths.replica_a, &paths.state_a)?;
    if before_noop.common_sequence != after_noop.common_sequence
        || before_noop.remote_sequence != after_noop.remote_sequence
    {
        return Err("no-op synchronization advanced the durable sequence".into());
    }
    require_clean(&after_noop)?;

    product.gc()?;
    let post_gc_restore_ms = timed(|| product.sync(&paths.replica_c, &paths.state_c))?;
    require_same_tree(&paths.replica_a, &paths.replica_c, "post-GC restore")?;
    let final_status = product.status(&paths.replica_c, &paths.state_c)?;
    require_clean(&final_status)?;
    if initial.volume_id != final_status.volume_id {
        return Err("volume identity changed during its lifecycle".into());
    }

    let evidence = Evidence {
        schema: 1,
        profile: profile.name.as_str(),
        seed: profile.seed,
        files: profile.files,
        file_bytes: profile.file_bytes,
        logical_bytes: profile.logical_bytes()?,
        initial_publish_ms,
        cold_restore_ms,
        reconcile_ms,
        post_gc_restore_ms,
        initial_storage,
        final_storage: fixture.storage_shape()?,
        replica_state_bytes: [&paths.state_a, &paths.state_b, &paths.state_c]
            .into_iter()
            .try_fold(0_u64, |total, path| {
                fs::metadata(path)
                    .map(|metadata| total.saturating_add(metadata.len()))
                    .map_err(|error| format!("inspect replica state {}: {error}", path.display()))
            })?,
        volume_id: &final_status.volume_id,
        result: "passed",
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&evidence).expect("serialize acceptance evidence")
    );

    drop(fixture);
    if args.keep {
        let kept = root.keep();
        println!("kept acceptance files at {}", kept.display());
    }
    Ok(())
}

fn require_clean(status: &Status) -> Result<(), String> {
    if status.pending || status.conflicts != 0 || status.common_sequence != status.remote_sequence {
        return Err("replica status is not clean and converged".into());
    }
    Ok(())
}

fn timed(operation: impl FnOnce() -> Result<(), String>) -> Result<u128, String> {
    let started = Instant::now();
    operation()?;
    Ok(started.elapsed().as_millis())
}

struct Paths {
    home: PathBuf,
    replica_a: PathBuf,
    replica_b: PathBuf,
    replica_c: PathBuf,
    state_a: PathBuf,
    state_b: PathBuf,
    state_c: PathBuf,
}

impl Paths {
    fn new(root: &Path) -> Self {
        Self {
            home: root.join("home"),
            replica_a: root.join("replica-a"),
            replica_b: root.join("replica-b"),
            replica_c: root.join("replica-c"),
            state_a: root.join("state-a"),
            state_b: root.join("state-b"),
            state_c: root.join("state-c"),
        }
    }
}

struct Product {
    binary: PathBuf,
    home: PathBuf,
    storage: String,
}

impl Product {
    fn new(home: &Path, storage: String) -> Self {
        Self {
            binary: workspace().join("target/release/ofs"),
            home: home.to_owned(),
            storage,
        }
    }

    fn create_volume(&self) -> Result<(), String> {
        self.run([
            "volume",
            "create",
            "acceptance",
            "--model",
            "managed",
            "--storage",
            &self.storage,
        ])
        .map(drop)
    }

    fn sync(&self, replica: &Path, state: &Path) -> Result<(), String> {
        let mut command = self.command();
        command
            .args(["sync", "acceptance"])
            .arg(replica)
            .arg("--state")
            .arg(state);
        require_success(command, "synchronize replica").map(drop)
    }

    fn status(&self, replica: &Path, state: &Path) -> Result<Status, String> {
        let mut command = self.command();
        command
            .arg("status")
            .arg(replica)
            .arg("--state")
            .arg(state)
            .arg("--json");
        let output = require_success(command, "read replica status")?;
        serde_json::from_slice(&output.stdout).map_err(|error| format!("decode status: {error}"))
    }

    fn gc(&self) -> Result<(), String> {
        self.run(["gc", "acceptance"]).map(drop)
    }

    fn run<const N: usize>(&self, args: [&str; N]) -> Result<Output, String> {
        let mut command = self.command();
        command.args(args);
        require_success(command, "run OFS")
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        command
            .env("OFS_HOME", &self.home)
            .env("TMPDIR", self.home.join("tmp"))
            .env("AWS_ACCESS_KEY_ID", "minioadmin")
            .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true");
        command
    }
}

fn build_product() -> Result<(), String> {
    let mut command = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .current_dir(workspace())
        .args(["build", "--release", "--bin", "ofs"]);
    require_success(command, "build release product").map(drop)
}

fn require_success(mut command: Command, action: &str) -> Result<Output, String> {
    let output = command
        .output()
        .map_err(|error| format!("{action}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{action}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(output)
}

struct Fixture {
    runtime: String,
    project: String,
    port: u16,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
struct StorageShape {
    data_objects: u64,
    data_bytes: u64,
    metadata_objects: u64,
    metadata_bytes: u64,
}

impl Fixture {
    fn start() -> Result<Self, String> {
        let runtime = env::var("OFS_CONTAINER_RUNTIME").unwrap_or_else(|_| "podman".into());
        let port = env::var("OFS_ACCEPTANCE_MINIO_PORT")
            .ok()
            .map(|value| value.parse::<u16>())
            .transpose()
            .map_err(|error| format!("parse OFS_ACCEPTANCE_MINIO_PORT: {error}"))?
            .unwrap_or(19_000);
        let fixture = Self {
            runtime,
            project: format!("ofs-managed-acceptance-{}", std::process::id()),
            port,
        };
        let mut command = fixture.compose();
        command.args(["up", "--detach", "minio"]);
        require_success(command, "start MinIO fixture")?;
        fixture.create_bucket()?;
        Ok(fixture)
    }

    fn create_bucket(&self) -> Result<(), String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            let mut command = self.compose();
            command.args([
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "minio-client",
                "mb",
                "--ignore-existing",
                "local/managed-sync",
            ]);
            if command.output().is_ok_and(|output| output.status.success()) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("MinIO fixture did not become ready within 30 seconds".into());
            }
            thread::sleep(Duration::from_millis(250));
        }
    }

    fn storage_url(&self) -> String {
        format!(
            "s3://managed-sync/acceptance?endpoint=http%3A%2F%2F127.0.0.1%3A{}&region=us-east-1",
            self.port
        )
    }

    fn storage_shape(&self) -> Result<StorageShape, String> {
        let mut command = self.compose();
        command.args([
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "minio-client",
            "du",
            "--recursive",
            "--depth",
            "3",
            "--json",
            "local/managed-sync/acceptance/managed/0/objects",
        ]);
        let output = require_success(command, "inspect Managed storage shape")?;
        let mut shape = StorageShape::default();
        for line in output.stdout.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let document: serde_json::Value = serde_json::from_slice(line)
                .map_err(|error| format!("decode Managed storage shape: {error}"))?;
            let Some(prefix) = document["prefix"].as_str() else {
                return Err("Managed storage shape prefix is missing".into());
            };
            let Some(relative) = prefix.strip_prefix("managed-sync/acceptance/managed/0/objects/")
            else {
                continue;
            };
            let mut parts = relative.split('/');
            let (Some(_epoch), Some(class), None) = (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            let objects = document["objects"]
                .as_u64()
                .ok_or_else(|| "Managed storage object count is missing".to_owned())?;
            let bytes = document["size"]
                .as_u64()
                .ok_or_else(|| "Managed storage byte count is missing".to_owned())?;
            if class == "04-data-segment" {
                shape.data_objects = shape.data_objects.saturating_add(objects);
                shape.data_bytes = shape.data_bytes.saturating_add(bytes);
            } else {
                shape.metadata_objects = shape.metadata_objects.saturating_add(objects);
                shape.metadata_bytes = shape.metadata_bytes.saturating_add(bytes);
            }
        }
        Ok(shape)
    }

    fn compose(&self) -> Command {
        let mut command = Command::new(&self.runtime);
        command
            .current_dir(workspace())
            .arg("compose")
            .arg("--file")
            .arg(workspace().join("fixtures/managed-acceptance/compose.yaml"))
            .arg("--project-name")
            .arg(&self.project)
            .env("OFS_ACCEPTANCE_MINIO_PORT", self.port.to_string());
        command
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let mut command = self.compose();
        let _ = command
            .args(["down", "--volumes", "--remove-orphans"])
            .output();
    }
}

fn seed_dataset(root: &Path, profile: Profile) -> Result<(), String> {
    for index in 0..profile.files {
        write_profile_file(
            &dataset_path(root, profile.directory_fanout, index),
            profile,
            index,
        )?;
    }
    Ok(())
}

fn mutate_dataset(root: &Path, profile: Profile, generation: u64) -> Result<(), String> {
    for ordinal in 0..profile.changes {
        let index = profile
            .seed
            .wrapping_add(generation.wrapping_mul(profile.changes))
            .wrapping_add(ordinal)
            % profile.files;
        write_profile_file(
            &dataset_path(root, profile.directory_fanout, index),
            profile,
            profile.files + generation * profile.changes + ordinal,
        )?;
    }
    let created = root.join(format!("created/generation-{generation:04}.bin"));
    write_profile_file(&created, profile, u64::MAX.wrapping_sub(generation))
}

fn dataset_path(root: &Path, fanout: u64, index: u64) -> PathBuf {
    root.join(format!("d{:06}/f{index:09}.bin", index / fanout))
}

fn write_profile_file(path: &Path, profile: Profile, identity: u64) -> Result<(), String> {
    fs::create_dir_all(path.parent().expect("dataset path has a parent"))
        .map_err(|error| format!("create dataset directory: {error}"))?;
    let mut output = fs::File::create(path)
        .map_err(|error| format!("create dataset file {}: {error}", path.display()))?;
    let mut remaining = profile.file_bytes;
    let mut offset = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    while remaining != 0 {
        let length = remaining.min(buffer.len() as u64) as usize;
        fill_bytes(
            &mut buffer[..length],
            profile.seed ^ identity.rotate_left(17) ^ offset,
        );
        output
            .write_all(&buffer[..length])
            .map_err(|error| format!("write dataset file {}: {error}", path.display()))?;
        remaining -= length as u64;
        offset += length as u64;
    }
    Ok(())
}

fn fill_bytes(bytes: &mut [u8], seed: u64) {
    let mut state = seed;
    for byte in bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
}

fn require_same_tree(left: &Path, right: &Path, context: &str) -> Result<(), String> {
    let left = tree(left)?;
    let right = tree(right)?;
    if left != right {
        return Err(format!("replicas differ after {context}"));
    }
    Ok(())
}

fn tree(root: &Path) -> Result<BTreeMap<PathBuf, blake3::Hash>, String> {
    let mut files = BTreeMap::new();
    visit(root, root, &mut files)?;
    Ok(files)
}

fn visit(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<PathBuf, blake3::Hash>,
) -> Result<(), String> {
    let mut entries = fs::read_dir(directory)
        .map_err(|error| format!("read replica tree {}: {error}", directory.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read replica entry: {error}"))?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let metadata = entry
            .metadata()
            .map_err(|error| format!("inspect replica path {}: {error}", path.display()))?;
        if metadata.is_dir() {
            visit(root, &path, files)?;
        } else if metadata.is_file() {
            let mut input = fs::File::open(&path)
                .map_err(|error| format!("open replica file {}: {error}", path.display()))?;
            let mut hasher = blake3::Hasher::new();
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let length = input
                    .read(&mut buffer)
                    .map_err(|error| format!("read replica file {}: {error}", path.display()))?;
                if length == 0 {
                    break;
                }
                hasher.update(&buffer[..length]);
            }
            files.insert(
                path.strip_prefix(root)
                    .expect("visited path belongs to root")
                    .to_owned(),
                hasher.finalize(),
            );
        } else {
            return Err(format!("unsupported replica entry {}", path.display()));
        }
    }
    Ok(())
}

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_WORKSPACE_DIR"))
}

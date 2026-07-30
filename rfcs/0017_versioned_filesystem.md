- Proposal Name: `versioned_filesystem`
- Start Date: 2026-07-29
- RFC PR: [apache/opendal-ofs#17](https://github.com/apache/opendal-ofs/pull/17)
- Tracking Issue: [apache/opendal-ofs#0000](https://github.com/apache/opendal-ofs/issues/0000)

# Summary

This RFC proposes evolving ofs into a local-first, automatically persisted,
versioned filesystem with point-in-time recovery.

After mounting, applications read and write ordinary files in a local working
directory. They do not wait for remote storage on every filesystem operation.
In the background, ofs detects settled changes, uploads whole files through
OpenDAL, and publishes the resulting filesystem tree as a checkpoint. A new
node can restore the latest published state, while a user can mount an earlier
checkpoint read-only to recover an accidentally deleted or corrupted file.

Version 1 uses a single-writer model. A Blob Store holds file contents. A
replaceable Metadata Store holds checkpoints, a linear timeline, and its head.
The Metadata Store may be implemented with metadata objects, SQLite, or a
remote database.

# Motivation

Agent sessions, skills, and MCP configuration are mutable working state. Users
need this state to:

- behave like ordinary local files;
- survive replacement of a development environment or container;
- save automatically without an explicit commit or push;
- recover from accidental deletion or modification; and
- use different storage services through OpenDAL.

Today, ofs forwards filesystem operations directly to an OpenDAL `Operator`.
This model is useful for accessing objects, but it exposes object-storage
latency and semantics directly to applications. A read cache can improve
performance, but it cannot by itself provide local writes, atomic publication
of a filesystem tree, cross-node recovery, or point-in-time recovery.

Git provides version history, but branches, commits, and pushes are not
necessary concepts for preserving agent working state. ofs should instead
behave like an automatic Time Machine for a filesystem.

# Guide-level explanation

## The aha moment

A user keeps agent state in ofs:

1. They mount ofs on one node and work with sessions, skills, and MCP
   configuration normally.
2. ofs automatically saves settled changes and eventually reports `clean`.
3. The node is destroyed.
4. The user mounts the same filesystem on another node and resumes from the
   latest published state.
5. An agent accidentally deletes a skill.
6. The user mounts the state from ten minutes earlier read-only and copies the
   skill back with ordinary filesystem tools.

The version 1 acceptance loop is:

```text
write
  -> autosave
  -> clean
  -> remount on another node
  -> mount at timestamp
  -> recover a file
```

## Mental model

```text
Application
    |
    v
Working Directory
    |
    v
Change Tracker --Change Set--> Publisher
                                   |
                          +--------+--------+
                          |                 |
                          v                 v
                     Blob Store       Metadata Store
                     file bytes     timeline and head
```

The Working Directory is an ordinary local directory on the active writer.
The Change Tracker identifies settled changes. The Publisher performs remote
publication. Only a checkpoint reachable from `head` is recoverable on another
node.

## User-visible state

- `dirty`: the Working Directory contains changes that have not been
  published.
- `syncing`: the Publisher is publishing a checkpoint.
- `clean`: the most recent reconciliation found the Working Directory equal
  to the checkpoint at `head`.

Ordinary `write`, `close`, and `fsync` calls retain the semantics of the
underlying local filesystem. `ofs sync` waits until current changes are part of
a shared checkpoint. If a node is permanently lost, another node can recover
only through the last state reported as `clean`.

Version 1 allows one writable mount for a filesystem at a time. Other clients
may mount a fixed checkpoint read-only.

# Reference-level explanation

## Scope

Version 1 supports:

- `create`, `read`, `write`, `truncate`, `mkdir`, `readdir`, `rename`,
  `remove`, and `stat` for regular files and directories;
- low-latency access to ordinary local files;
- automatic saving and explicit `ofs sync`;
- filesystem-wide checkpoints on a linear timeline;
- restoring `head` on another node; and
- mounting historical state read-only by checkpoint or timestamp.

Version 1 does not include:

- concurrent-writer reconciliation;
- full POSIX semantics such as hard links, file locks, and extended
  attributes;
- content chunking, lazy hydration, or Foyer caching;
- Git compatibility;
- destructive in-place rollback;
- a metadata server; or
- automatic garbage collection.

To make point-in-time recovery complete, version 1 retains every published
checkpoint. Retention policies and garbage collection can be added later.

## Components and boundaries

| Component | Responsibility | Not responsible for |
| --- | --- | --- |
| Working Directory | Current filesystem tree and unpublished changes | Cross-node recovery |
| Change Tracker | Watching, scanning, and producing a stable Change Set | Remote writes |
| Publisher | Uploading content and publishing checkpoints | Filesystem event handling |
| Blob Store | Immutable file contents | Filesystem trees and history |
| Metadata Store | Checkpoints, timeline, and head | File contents |

The Working Directory is node-local state. After a process restart, ofs scans
the directory again and does not depend on a retained event stream.

The Metadata Store and Blob Store together form shared state. A checkpoint is
authoritative across nodes only after it has been committed to the Metadata
Store and all referenced content exists in the Blob Store.

The Metadata Store must be accessible from a new node. An embedded SQLite
implementation provides cross-node recovery only if its committed metadata is
replicated to shared storage. A database that exists only on the writer does
not.

## Mounting

When a filesystem is created, the Metadata Store records the format version,
filesystem ID, and an initial empty checkpoint.

A new node mounts the filesystem by:

1. reading and validating the format version;
2. reading `head` and its checkpoint;
3. downloading every file referenced by the checkpoint manifest;
4. materializing the Working Directory; and
5. starting the Change Tracker and Publisher.

Version 1 downloads complete files and does not hydrate them lazily. When an
existing Working Directory is mounted again, ofs scans it and reconciles it
with `head`.

## Change tracking

The Change Tracker encapsulates platform-specific filesystem notifications,
debouncing, directory scans, and stability checks.

Filesystem notifications are scan hints, not a source of truth. Notifications
may be coalesced, repeated, or lost. The following operations therefore
reconcile the directory against the checkpoint at `head`:

- daemon startup;
- periodic autosave; and
- explicit `ofs sync`.

The Change Tracker emits a Change Set containing at least:

```text
base_checkpoint
changed_entries
deleted_paths
file_metadata
content_sources
```

Content referenced by a Change Set must remain valid while it is being
published. An implementation may use stable file handles, temporary staging,
or validation before and after publication. A file that is still changing is
not included in the current Change Set.

## Checkpoint timeline

Version 1 stores each file as a whole, content-addressed blob. A checkpoint
contains a complete filesystem manifest:

```text
id
parent
generation
committed_at
writer_id
manifest
```

A file entry in the manifest records its path, type, blob ID, size, mode, and
modification time. Directory entries do not reference blobs.

Checkpoints form an append-only linear timeline:

- `parent` identifies the preceding checkpoint;
- `generation` defines the authoritative commit order;
- `committed_at` supports user-facing time queries; and
- `head` identifies the newest visible checkpoint.

The Metadata Store provides these logical operations:

- read `head` and a checkpoint by ID;
- list the timeline by generation or timestamp; and
- commit a checkpoint against an expected head, then advance `head`.

A database implementation can use tables indexed by `generation` and
`committed_at`. When the Blob Store and Metadata Store share an object
backend, the repository can use this private layout:

```text
.ofs/
  format
  blobs/<content-id>
  checkpoints/<checkpoint-id>
  refs/head
```

In the object-storage implementation, checkpoint parent links represent the
timeline. A time index may be generated as an optimization, but it is not
authoritative state.

## Publishing

After the Change Tracker produces a Change Set, the Publisher:

1. reads and validates changed files;
2. uploads blobs that are missing from the Blob Store;
3. constructs an immutable checkpoint that references those blobs;
4. commits the checkpoint to the Metadata Store, using `base_checkpoint` as
   the expected head;
5. asks the Change Tracker to reconcile again; and
6. reports `clean` if the Working Directory matches the new head.

Committing the checkpoint and advancing `head` is one logical transaction. A
database implementation uses a transaction. An object-storage implementation
writes the immutable checkpoint first and then performs a compare-and-set on
`refs/head`. If the compare-and-set fails, the checkpoint remains invisible.

Publication preserves these invariants:

- a checkpoint references only complete blobs;
- `head` references only a complete checkpoint;
- a visible checkpoint points to the preceding generation as its parent;
- a new mount sees either the complete old tree or the complete new tree;
- retrying the same publication has the same logical result; and
- a crash may leave unreferenced objects but never exposes an incomplete tree.

## Durability and failure recovery

ofs distinguishes two durability boundaries:

- **Local durability:** the filesystem containing the Working Directory has
  persisted the change.
- **Shared durability:** the change is part of the checkpoint at `head` and
  can be recovered without the local directory.

| Failure point | Shared state | Recovery |
| --- | --- | --- |
| Before committing a checkpoint | Old head | Scan again and retry |
| Blobs or checkpoint written, head not advanced | Old head | Retry or leave unreferenced objects |
| Head advanced, local status still syncing | New head | Reconcile and update local status |
| Checkpoint references a missing blob | Corrupt | Fail the mount and report the error |

## Point-in-time recovery

Recovering to time `T` means:

> Select the newest retained checkpoint on the timeline whose `committed_at`
> is not later than `T`.

A database implementation can use a time index. An object-storage
implementation can walk parent links backwards from `head`. Both return the
same logical result.

Version 1 materializes a historical checkpoint as a separate read-only
directory. Users recover files with ordinary tools such as `cp`, without
overwriting the current state.

Native object versioning preserves versions of individual objects. It does not
replace a filesystem-wide checkpoint.

## Single-writer semantics

The Publisher uses the Change Set's `base_checkpoint` as the expected head:

- if `head` is unchanged, it commits the new checkpoint;
- if `head` has changed, it returns a stale-writer error instead of silently
  overwriting shared state.

The Metadata Store should update `head` atomically through a compare-and-set or
transaction. A deployment without atomic head updates must enforce a single
writer externally.

Each checkpoint retains its parent, generation, and complete manifest. Path
changes can be derived by comparing the checkpoint with its parent. A future
multi-writer implementation can rebase disjoint path changes onto a new head
and retry without changing the recovery model.

# Drawbacks

- There is a data-loss window between a completed local write and publication
  of a shared checkpoint.
- Without a local filesystem snapshot, the Change Tracker must detect changes
  that occur during scanning and publication.
- Large directories require repeated scans and hashing of complete files.
- Full manifests and whole-file uploads do not scale to every workload.
- Retaining all checkpoints continuously increases storage use.
- A single writer cannot support several agents modifying one filesystem
  concurrently.

# Rationale and alternatives

## Metadata Store implementations

Metadata objects, SQLite, D1, Neon, and other databases can implement the
Metadata Store. Each implementation stores the same checkpoint timeline and
provides the same commit semantics.

An object-storage implementation requires the least deployment machinery. A
database implementation offers more direct transactions and time queries. The
choice does not change the mount, autosave, or recovery experience.

## Git

Git is appropriate when code collaboration requires branches, merges, and
ecosystem interoperability. Agent working state requires automatic
persistence, cross-node recovery, and point-in-time recovery, so Git is not a
requirement.

## Direct object mapping

Mapping OpenDAL objects directly to files does not provide local-write
semantics, filesystem-wide checkpoints, or a consistent historical point in
time.

# Prior art

- [TensorLake `tl fs`](https://docs.tensorlake.ai/filesystems/architecture)
  provides autosave checkpoints, a shared timeline, and time travel. ofs
  version 1 adopts a single-writer model.
- [ObjectiveFS](https://objectivefs.com/features) demonstrates that a
  filesystem can be built directly on object storage without a separate
  metadata server.

# Unresolved questions

- How should a Change Set retain a stable reference to a file that may still
  be changing?
- What should the autosave settle window and maximum interval be?
- What encoding and compatibility rules should checkpoints and manifests use?
- Should version 1 support symbolic links, and which additional POSIX metadata
  should it preserve?
- Which Metadata Store implementation should be the default?

# Future possibilities

- Foyer-backed blob caching and lazy hydration.
- Incremental trees, content chunking, and delta uploads.
- Checkpoint pinning, tiered retention, and garbage collection.
- Multi-writer checkpoint rebasing.

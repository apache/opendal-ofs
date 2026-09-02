# YinYang Format Core

## Scope

The YinYang Format core defines the filesystem values and publication state
machine independently of storage representation. It owns the materialized
`Tree`, immutable `FsVersion` values, durable commit identities, and the mutable
head. A storage implementation supplies immutable blobs and conditional head
replacement. The core defines no wire representation.

The current implementation stores complete materialized versions and retains
all commits.

## Filesystem values

`Tree` is an ordered map from canonical relative `Path` values to `Node` values.
The empty path is the root. Every other path has an existing directory parent.
Each `NodeId` occurs at exactly one path. The root is a directory whose identity
remains stable across filesystem versions.

A node contains its stable identity, node generation, executable flag, and
either a directory body with a membership generation or a `File` body. A file
contains a `ContentId` and ordered `FilePart` values. Parts are non-empty,
non-overlapping, and exactly cover
`[0, File.content.length)`. An empty file has no parts. Every part fits within
the content identified by its `BlobRef`.

`BlobRef` is an opaque persistent reference plus the identity of its referenced
bytes. The storage implementation constructs and verifies it. The core does
not interpret object keys, byte-range syntax, framing, or integrity
metadata inside the reference.

Paths use normalized portable components. They are relative, NFC-normalized,
at most 4096 bytes, and contain components of at most 255 bytes. Empty,
dot-relative, control-character, Windows-reserved, and trailing-space or
trailing-dot components are rejected. A directory cannot contain two names with
the same Unicode case-folded NFC form.

## Successor tree contract

New nodes start at node generation 1; new directories also start at membership
generation 1. A stable `NodeId` cannot change between directory and file.

Rename preserves the node identity and node generation. A change to node
executable state or file state advances the node generation by exactly one.
Unchanged node state preserves its generation.

Directory membership is compared as `name -> NodeId`. A create, remove,
replacement, or rename affecting that mapping advances the directory membership
generation by exactly one. Unchanged membership preserves its generation.

## Versions and commits

`FsVersion.number` is its commit count and therefore its publication order.
Genesis is version 0 and contains no commits. Version N contains exactly one
commit for every version from 1 through N. The ordered position of each unique,
caller-assigned `CommitId` is its published version number.

The head contains the current version's `BlobRef`. Every version reached through
one head history uses the same root `NodeId`.

## Storage contract

A `Storage` implementation provides five operations:

1. write an immutable `FsVersion` and return its `BlobRef`;
2. read and verify an `FsVersion` through its `BlobRef`;
3. create the initial head;
4. observe the head together with an opaque condition bound to that exact read;
5. replace the head when the observed condition remains current.

Head creation is idempotent and never replaces an existing value.
Compare-exchange reports whether it replaced the observed head. An observed
condition is never reconstructed through a later metadata request. The storage
implementation rejects a missing, truncated, or unverifiable blob as corrupt
data.

## Lifecycle

Creation writes an immutable genesis version and creates the head if it does not
exist. Concurrent creators reopen the winner.

Observation reads the head and its condition, reads the referenced immutable
version, and verifies that its root identity belongs to the opened filesystem.

Commit accepts an observation, caller-assigned `CommitId`, and complete successor
tree. It validates the successor, constructs version `current + 1`, persists the
immutable version, then conditionally replaces the head. A successful replacement
returns `Committed`.

When replacement loses a race, the core observes the new current version. The
same `CommitId` in its commits means the earlier attempt succeeded and also
returns `Committed`; otherwise the result is `Conflict`. When replacement
returns an error after the storage system may have accepted it, the core
performs the same commit lookup before returning the storage error. This makes
retry safe when the publication response is lost.

# YinYang Format Core

## Scope

The YinYang Format core defines the filesystem values and publication state
machine shared by all persistent encodings. It owns `FsFormat`, the materialized
`Tree`, immutable `FsVersion` values, durable `Commit` facts, and the mutable
`FsHead`. A storage implementation supplies create-once records, immutable
blobs, and conditional head replacement.

The current implementation stores complete materialized versions and retains
all commits. Persistent encodings may introduce snapshots, deltas, streams,
packing, or compaction while preserving the values and transitions in this
specification.

## Filesystem values

`FsFormat` is created once and contains the filesystem identity, root node
identity, ordered content-decoding extensions, and an optional head extension.
The order of content-decoding extensions is significant.

`Tree` is an ordered map from canonical relative `Path` values to `Node` values.
The empty path is the root. Every other path has an existing directory parent.
Each `NodeId` occurs at exactly one path. The root is a directory whose identity
equals `FsFormat.root`.

A node contains its stable identity, node generation, attributes, and either a
`Dir` or `File` body. Directory membership has an independent generation. A
file contains a publication identity, a `ContentId`, and ordered `FilePart`
values. Parts are non-empty, non-overlapping, and exactly cover
`[0, File.content.length)`. An empty file has no parts. Every part fits within
the logical content of its `FileSource`, and each source contains one decoded
identity for every decoding extension in `FsFormat`.

`BlobRef` is an opaque persistent reference plus the identity of its referenced
bytes. The storage implementation constructs and verifies it. The
core does not interpret object keys, byte-range syntax, framing, or integrity
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
attributes or file state advances the node generation by exactly one. Unchanged
node state preserves its generation. File content or parts cannot change while
retaining the same `FileVersionId`; publishing the same logical bytes under a
new file version remains valid.

Directory membership is compared as `name -> NodeId`. A create, remove,
replacement, or rename affecting that mapping advances the directory membership
generation by exactly one. Unchanged membership preserves its generation.

## Versions and commits

`FsVersion.number` is the publication order. Genesis is version 0 and contains
no commits. Every non-genesis version retains a contiguous commit sequence that
ends at its own version number. Commit identities are unique within the retained
sequence, and each `Commit` binds one caller-assigned `CommitId` to one published
version.

`FsHead.current.number` equals the referenced version number. The referenced
version uses the same `FsId` as `FsFormat`. `FsHead.min_retained` never exceeds
the current version. The initial implementation keeps `min_retained` at version
0 and carries every commit into its successor.

## Storage contract

A `FormatStorage` implementation provides seven operations:

1. create and read the create-once `FsFormat`;
2. write and read a verified immutable `FsVersion` blob;
3. create the initial `FsHead`;
4. observe `FsHead` together with an opaque condition bound to that exact read;
5. replace the complete head when the observed condition remains current.

Create and compare-exchange report whether they won the conditional operation.
An observed condition is never reconstructed through a later metadata request.
The storage implementation rejects a missing, truncated, or unverifiable blob
as corrupt data.

## Lifecycle

Creation first publishes `FsFormat`. The winner initializes an empty root tree,
writes genesis as an immutable version, and creates `FsHead`. Concurrent
creators that observe the same format configuration reopen the winner. A
different persisted configuration returns a conflict.

Observation reads `FsHead` and its condition, reads the referenced immutable
version, and validates the head, filesystem identity, version number, tree, and
retained commits before returning the value.

Commit accepts an observation, caller-assigned `CommitId`, and complete successor
tree. It validates the successor, constructs version `current + 1`, persists the
immutable version, then conditionally replaces `FsHead`. A successful replacement
returns `Committed`.

When replacement loses a race, the core observes the new current version. The
same `CommitId` in retained commits means the earlier attempt succeeded and also
returns `Committed`; otherwise the result is `Conflict`. When replacement
returns an error after the storage system may have accepted it, the core performs
the same commit lookup before returning the storage error. This makes retry safe
when the publication response is lost.

## Encoding boundary

This specification defines no wire representation. An encoding owns record
envelopes, field layout, object locations, snapshots, deltas, streams, packing,
and compaction. Decoding must produce values accepted by the core validators;
encoding must preserve every identity, generation, reference, ordering rule,
and publication result defined here.

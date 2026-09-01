- Proposal Name: `managed_core_model`
- Start Date: 2026-09-01
- RFC PR: [apache/opendal-yinyang#41](https://github.com/apache/opendal-yinyang/pull/41)
- Tracking Issue: [apache/opendal-yinyang#19](https://github.com/apache/opendal-yinyang/issues/19)

# Summary

Define the Apache OpenDAL™ YinYang Managed filesystem core as a materialized
logical tree, immutable filesystem versions, idempotent commit facts, and
verifiable references to immutable bytes. The model separates those stable
semantics from snapshots, delta batches, patch layers, record streams, and
other persistence structures.

This model applies to the `Managed` Volume Model defined by RFC-0016. It does
not change the user-facing Volume Model vocabulary, the `Direct` model, runtime
behavior, or any persistent encoding.

# Problem

A Managed filesystem needs stable names for the state observed by filesystem
operations. Persistent formats may encode that state as compacted snapshots,
delta streams, layered file mappings, and immutable objects. Those structures
answer how state is stored; they do not define what a file, filesystem version,
or successful commit means.

Treating persistence structures as domain objects makes the core depend on one
format's compaction and framing strategy. It also obscures three independent
boundaries: logical identity versus path, a successful publication versus the
resulting filesystem version, and logical content versus its physical
placement.

# Proposed Design

## Model boundaries

The Managed core has one bootstrap record and three semantic layers:

```text
Bootstrap   FsFormat
Logical     Tree / Node / File
Versioning  FsHead / FsVersion / Commit
Physical    BlobRef
```

`FsFormat` is created once. `FsHead` is the only mutable publication cell.
`FsVersion` and every byte region reached through a `BlobRef` are immutable.

## Bootstrap

```text
FsFormat {
  fs: FsId,
  root: NodeId,
  decodings: [Extension],
  head_extension: Extension?,
}
```

`FsFormat` contains information required before a reader can locate the head or
interpret file content. Writer packing targets and other operational policy do
not belong to the core model.

## Logical state

```text
Tree = ordered map Path -> Node

Node {
  id: NodeId,
  generation: Generation,
  attrs: NodeAttrs,
  body: Dir | File,
}

Dir {
  entries_generation: Generation,
}

File {
  version: FileVersionId,
  content: ContentId,
  parts: [FilePart],
}

FilePart {
  range: FileRange,
  source_offset: u64,
  source: FileSource,
}

FileSource {
  stored: BlobRef,
  decoded: [ContentId],
}

ContentId {
  digest: Digest,
  length: u64,
}
```

`Path` identifies a location while `NodeId` identifies a node across rename.
`Node.generation` protects node content and attributes;
`Dir.entries_generation` independently protects the directory membership set.
`FileVersionId` identifies one content publication, including a publication
whose logical bytes equal an older version. `ContentId` identifies the complete
logical byte sequence independently of its placement.

`FilePart` is a mapping from a file range into a source. `FileSource` is the
larger verification and decoding unit, so multiple parts may reuse one source
through different offsets. Parts are ordered, non-overlapping, and exactly
cover `[0, File.content.length)`; an empty file has no parts. The logical source
content is the last decoded identity, or the stored identity when the decoding
list is empty.

## Versioning and publication

```text
FsHead {
  current: FsVersionRef,
  gc_epoch: GcEpoch,
  min_retained: VersionNumber,
}

FsVersionRef {
  number: VersionNumber,
  blob: BlobRef,
}

FsVersion {
  fs: FsId,
  number: VersionNumber,
  tree: Tree,
  commits: [Commit],
}

Commit {
  id: CommitId,
  version: VersionNumber,
}
```

An observation returns `FsHead` and the condition token needed to replace the
same head state. Publication first makes the new immutable version and all of
its referenced bytes durable, then conditionally replaces the complete head.
Advancing the garbage-collection fence uses the same conditional cell.

`VersionNumber` is the publication order. `FsVersion.fs` must equal
`FsFormat.fs`, `FsVersionRef.number` must equal the referenced version number,
and `FsHead.min_retained` must not exceed the current number.

A caller assigns `CommitId` before attempting publication. A `Commit` exists
only after that attempt succeeds and binds the identifier to the published
version. Retained commits cover a contiguous version range ending at the
current version, contain one commit per non-genesis version in that range, and
contain no duplicate `CommitId`. This allows a retry to distinguish a committed
attempt from a conflict or an outcome older than the retained range.

## Physical boundary

`BlobRef` is the only physical concept exposed to the core:

> A persistent, verifiable reference to a contiguous byte region in one
> immutable blob.

The reference encapsulates location, byte range, and the integrity information
required by its persistent representation. The core does not depend on object
keys, framing, checksums, stream kinds, object classes, or garbage-collection
sharding.

## Persistence projection

A persistent format may compact or partition domain values without changing
their meaning:

| Domain value | Possible persistence representation |
| --- | --- |
| `FsVersion.tree` | Tree snapshot plus ordered delta batches |
| `FsVersion.commits` | One or more commit batches |
| `File.parts` | Base and patch layers with inline and continued mappings |
| `BlobRef` | Whole-object, framed-payload, or stored-range reference |

Snapshot cursors, batch boundaries, compaction weights, patch levels, bounding
ranges, streams, and continuation records belong to the format implementation.
Materializing them must produce the domain values and invariants defined above.

Core naming follows the semantic boundary: `Fs*` names bootstrap and
publication state; `Tree`, `Node`, `Dir`, and `File` name logical state;
`Commit*` names idempotent publication facts; and `BlobRef` names the physical
reference. Persistence-specific terms such as `Snapshot`, `Batch`, `Layer`,
`Stream`, and `Locator` remain inside format implementations.

# Compatibility and Migration

This RFC changes no API behavior or wire bytes. Existing and in-progress
Managed formats may retain their current tuple fields, type tags, object keys,
and integrity checks. Their readers materialize the core model at the format
boundary, and their writers lower the model back into the format's snapshots,
batches, layers, and references.

RFC-0016 continues to use `Volume` for the user-facing choice between `Direct`
and `Managed`. The `Fs*` vocabulary introduced here applies to the internal
Managed filesystem state and does not rename that architecture-level concept.

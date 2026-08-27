- Proposal Name: `managed_format_v0`
- Start Date: 2026-08-27
- RFC PR: [apache/opendal-ofs#29](https://github.com/apache/opendal-ofs/pull/29)
- Tracking PR: [apache/opendal-ofs#26](https://github.com/apache/opendal-ofs/pull/26)

# Summary

This RFC defines the first persistent storage format for the managed filesystem
described by [RFC-0016](0016_filesystem_architecture.md). Managed format v0
stores file data, file extent maps, namespace revisions, operation receipts, and
garbage-collection state above an OpenDAL object store.

The format has two kinds of state:

- immutable objects under `managed/0/objects/`; and
- small control records under `managed/0/` that select the volume format and
  the current namespace revision.

All stored values have explicit framing, length bounds, and BLAKE3 digests.
File contents may share data segments, while a file version describes its
logical bytes through an ordered set of extents. Namespace publication writes
immutable objects first and changes the visible revision with one conditional
write to the authority head.

This RFC documents the format introduced by
[PR #29](https://github.com/apache/opendal-ofs/pull/29). Format v0 remains
experimental until the project makes an explicit stability decision.

# Motivation

RFC-0016 separates a filesystem into two axes. The data axis turns logical file
bytes into stored objects. The namespace axis turns paths into directories and
file versions. A persistent managed volume needs a precise contract at the
point where those axes meet the object store.

Without such a contract, implementations can agree at the Rust API boundary
and still disagree about byte order, tuple shape, object keys, publication, or
recovery. Those disagreements are especially costly once multiple processes,
older readers, and garbage collection operate on the same volume.

Managed format v0 therefore specifies:

- the byte-level envelopes used for bounded records and framed streams;
- the key space and immutable object identity model;
- the representation of data segments and file extent maps;
- the namespace snapshot, change, receipt, commit, and authority records;
- the validation rules required before stored state is accepted; and
- the ordering constraints for publication and garbage collection.

The format is intended to make a volume independently readable. It does not
standardize a local replica database, a synchronization policy, a command-line
interface, or a particular OpenDAL service.

# Guide-level explanation

## From filesystem operations to objects

A managed volume is not stored as one object per file. It is an immutable
object graph selected by a small mutable head:

```text
managed/0/format                         managed/0/head
        |                                      |
        | fixes volume identity and layout     | selects current revision
        v                                      v
   VolumeFormat                         NamespaceCommit
                                             / | \
                                            /  |  \
                              snapshot stream  |  receipt streams
                                               |
                                          change streams
                                               |
                                      NamespaceNode values
                                               |
                                        FileExtentMap
                                           /       \
                                  extent streams  DataSegments
```

An **object** is an immutable byte sequence at a key derived from its garbage
collection epoch, object class, and object identifier. An **object reference**
adds the encoded length and whole-object digest needed to verify a read.

A **record** is a CBOR value with a fixed positional schema. Small control
records use a bounded record envelope. Collections use framed record streams.
A **stream reference** identifies both the containing immutable object and the
logical stream payload inside it.

An **extent** maps a logical file range to stored bytes in a data segment. A
file extent map overlays newer patch runs on an older base run. The newest
mapping wins when runs overlap. This lets a publication reuse unchanged data
without rewriting a complete file object.

## Content identity and placement

The format keeps byte identity separate from physical placement:

```text
logical bytes
    |
    +-- ContentRef = (BLAKE3 digest, logical length)
    |
    +-- ExtentRef
           |
           +-- SegmentRangeRef
                   |
                   +-- ObjectLocator = (epoch, class, object id)
                   +-- offset in the data segment
                   +-- ContentRef of the stored range
```

`ContentRef` answers "which bytes are these?" `ObjectLocator` and the range
answer "where are these bytes stored?" The separation allows several file
versions to share a data segment and leaves room for future placement schemes
without changing the meaning of the content.

Format v0 implements the whole-file, identity-decoding profile. It does not yet
define built-in chunking or compression algorithms. The wire model carries
partitioning and decoding extension descriptors so later profiles can describe
those choices explicitly.

## Publication

Publication follows a prepare-then-commit rule:

```text
writer                         object store                    reader
  |                                  |                            |
  |-- write data segments ---------->|                            |
  |-- write extent streams --------->|                            |
  |-- write namespace streams ------>|                            |
  |-- write commit object ---------->|                            |
  |                                  |<------- read old head ------|
  |-- compare-and-swap head -------->|                            |
  |                                  |<------- read new head ------|
  |                                  |-------- immutable graph --->|
```

The writer publishes every referenced immutable object before it attempts the
head update. The conditional head write is the visibility boundary. A failed
compare-and-swap does not expose a partial namespace revision; it leaves only
unreferenced immutable objects, which garbage collection may later reclaim.

An operation receipt binds an operation identifier to the cursor at which it
was committed. Writers can use receipts to distinguish a lost response from an
operation that was never published.

## Reading a file

A reader starts from a validated authority head, loads the selected namespace
commit, and resolves the path against the snapshot plus ordered changes. For a
regular file it reads the newest extent runs first:

```text
requested logical range
          |
          v
  newest patch level ---- mapped? ---- yes ---> read referenced extent
          |
          no
          v
   older patch level ----- mapped? ---- yes ---> read referenced extent
          |
          no
          v
       base run -----------------------> read referenced extent
```

The reader copies only the covered stored ranges and applies the declared
decodings in order. In the v0 core profile the stored bytes are the logical
bytes, so decoding is the identity operation.

When fully consumed, an object read through an `ObjectRef` is verified against
its encoded length and whole-object digest. The current identity codec verifies
a complete stored extent against its own `ContentRef`. Reading only part of an
extent cannot prove that digest. The Bao-derived index discussed in Future
possibilities addresses that gap without changing the base extent
representation.

## Updating a file

A patch publication writes new bytes for changed logical ranges and reuses old
extent references elsewhere. The implementation scans the resulting logical
file to compute its complete `ContentRef`, then constructs a new
`FileExtentMap`.

Patch levels use deterministic binary carry. Inserting a new patch at an
occupied level merges the two runs and carries the result to the next level.
This bounds the number of runs a reader must consult while avoiding a full base
rewrite for every update:

```text
new run R       levels before       levels after
---------       -------------       ------------
R               [A, empty, C]       [empty, A+R, C]
R               [A, B, empty]       [empty, empty, A+B+R]
```

The exact merge output is an implementation concern, but it must preserve the
newest-wins logical view and the structural invariants in the reference-level
specification.

## Garbage collection

Each immutable object key includes a garbage collection epoch. Collection first
advances the epoch under an authority fence. It then compacts each authority
root into the new epoch and records every referenced metadata and data object.
Only unmarked objects in older epochs are eligible for deletion.

```text
rotate epoch -> compact authority roots -> mark referenced objects -> sweep old epochs
```

The epoch is a placement and collection boundary, not part of content identity.
A live reference may point to an object written in an older epoch.

# Reference-level explanation

## Conventions

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**,
and **MAY** are to be interpreted as described in BCP 14 when, and only when,
they appear in all capitals.

All integer fields outside CBOR are unsigned and little-endian. Sizes and
offsets are measured in bytes. `BLAKE3(x)` is the 32-byte BLAKE3 digest of the
exact byte sequence `x`. Concatenation is written as `||`.

CBOR schemas below use array notation. The position of each element is part of
the schema. A decoder MUST reject a tuple with missing or trailing elements,
unless a later format version explicitly changes that rule. Fixed-width
identities are CBOR byte strings of the width listed below. Extension
configuration is an opaque byte vector at the v0 layer.

## Key space

The v0 key space is rooted at `managed/0/`:

| Key | Contents | Mutation rule |
| --- | --- | --- |
| `managed/0/format` | `VolumeFormat` bounded record | Create once |
| `managed/0/head` | `AuthorityHead` bounded record | Conditional replacement |
| `managed/0/objects/{epoch}/{class}/{shard}/{id}` | Immutable object | Create once |

An immutable object key is formatted as:

```text
managed/0/objects/{epoch:020}/{class-segment}/{id[0]:02x}/{id:lower-hex}
```

`epoch` is a zero-padded 20-digit decimal `u64`. `id` is a 16-byte object ID
encoded as 32 lowercase hexadecimal digits. `shard` is the first object ID byte
encoded as two lowercase hexadecimal digits.

Object classes are fixed as follows:

| Code | Class | Key segment |
| ---: | --- | --- |
| 1 | NamespaceCommit | `01-namespace-commit` |
| 2 | NamespaceSegment | `02-namespace-segment` |
| 3 | OperationReceiptSegment | `03-operation-receipt-segment` |
| 4 | DataSegment | `04-data-segment` |
| 5 | FileExtentSegment | `05-file-extent-segment` |
| 6 | Extension | `06-extension` |

Writers MUST use create-if-absent semantics and MUST NOT intentionally reuse an
immutable-object key for different bytes.

## Scalar identities and references

The following types have fixed widths:

| Type | Width | Meaning |
| --- | ---: | --- |
| `VolumeId` | 16 | Stable volume identity |
| `NodeId` | 16 | Stable namespace-node identity |
| `FileVersionId` | 16 | Identity of one file version |
| `OperationId` | 16 | Idempotent publication identity |
| `ObjectId` | 16 | Immutable object identity within its key tuple |
| `ExtensionId` | 16 | Extension implementation identity |
| `Digest` | 32 | BLAKE3 content digest |
| `Checksum` | 32 | BLAKE3 envelope checksum |
| `GcEpoch` | `u64` | Garbage-collection placement epoch |
| `ChangeCursor` | `u64` | Namespace revision cursor; zero is genesis |

The core reference tuples are:

```text
ContentRef       = [digest: Digest, length: u64]
ObjectLocator    = [gc_epoch: GcEpoch, class: u8, id: ObjectId]
ObjectRef        = [locator: ObjectLocator, encoded_length: u64, digest: Digest]
StreamRef        = [kind: u16, object: ObjectRef,
                    payload_length: u64, payload_digest: Digest]
```

`ObjectRef.digest` covers the complete encoded object. `StreamRef.payload_digest`
covers only the stream payload before its footer and trailer. A reader MUST
validate both when it reads the complete object.

Stream kind codes are:

| Code | Stream kind | Required object class |
| ---: | --- | --- |
| 1 | NamespaceSnapshot | NamespaceSegment |
| 2 | OperationReceipts | OperationReceiptSegment |
| 3 | DataSegment | DataSegment |
| 4 | NamespaceChanges | NamespaceSegment |
| 5 | FileExtents | FileExtentSegment |
| 1024 through 65535 | Extension-defined | Defined by the extension |

Codes 6 through 1023 are reserved. A reader MUST reject an unknown core stream
kind when the enclosing record requires its semantics.

## Bounded records

A bounded record has this envelope:

```text
+----------------------+----------------------+-------------------+------------------+
| magic: 8 bytes       | body length: u64 LE  | CBOR body: N      | checksum: 32     |
+----------------------+----------------------+-------------------+------------------+

checksum = BLAKE3(magic || encoded_body_length || body)
```

The decoder MUST reject a wrong magic value, a body over the applicable limit,
an encoded-length mismatch, a checksum mismatch, invalid CBOR, or trailing
bytes. The limits and magic values are:

| Record | Magic | Maximum CBOR body |
| --- | --- | ---: |
| Volume format | `OFSFMT00` | 64 KiB |
| Authority head | `OFSHED00` | 64 KiB |
| Namespace commit | `OFSCMT00` | 4 MiB |

The format and authority records occupy their control keys. A namespace commit
uses the same envelope as the encoded bytes of a NamespaceCommit immutable
object.

## Framed record streams

Namespace, receipt, and extent collections use frames. Each frame is:

```text
+-----------+----------------+----------------+----------------------+----------------+
| `OFSF`    | records length | record count   | records digest       | records area   |
| 4 bytes   | u64 LE         | u32 LE         | 32 bytes              | N bytes        |
+-----------+----------------+----------------+----------------------+----------------+
```

The 48-byte frame header is followed by `record_count` records. Each record is
`body_length:u32-le || CBOR body`. The combined records area MUST NOT exceed
64 KiB. A frame MUST contain at least one record. Its digest is the BLAKE3 digest
of the exact records area. Decoders MUST consume exactly the declared number of
records and bytes.

The payload of a record-stream object is the concatenation of its frames. A
data-segment payload is instead the concatenation of stored byte ranges. Both
forms end with the same 130-byte stream tail:

```text
footer (40 bytes)
  payload_length       u64 LE
  payload_digest       32 bytes

trailer (90 bytes)
  magic                `OFSSTR00` (8 bytes)
  stream_kind          u16 LE
  footer_offset        u64 LE, equal to payload_length
  footer_length        u64 LE, equal to 40
  footer_checksum      BLAKE3(footer), 32 bytes
  trailer_checksum     BLAKE3(first 58 trailer bytes), 32 bytes
```

For an object containing a stream:

```text
encoded_length = payload_length + 130
object_digest   = BLAKE3(payload || footer || trailer)
```

A decoder MUST validate the stream kind, both footer coordinates, both tail
checksums, the payload digest, the encoded length, and the whole-object digest
before accepting a complete stream object.

## Volume format and extensions

The volume record is:

```text
VolumeFormat = [
  volume_id: VolumeId,
  root_node_id: NodeId,
  file_data_layout: FileDataLayout,
  authority: ExtensionDescriptor?
]

FileDataLayout = [
  data_segment_target_bytes: u64,
  partitioning: ExtensionDescriptor?,
  decodings: [ExtensionDescriptor]
]

ExtensionDescriptor = [id: ExtensionId, configuration: [u8]]
```

`data_segment_target_bytes` MUST be greater than zero. It is a rotation target,
not a hard object-size limit: one stored range may make a segment larger than
the target.

The format-v0 core profile has no partitioning descriptor and an empty decoding
list. Its default data-segment target is 8 MiB. A reader MUST reject a volume
whose required partitioning, decoding, or authority extension it does not
support. The second descriptor element is an opaque byte vector, encoded as a
CBOR array of `u8` values. The current `ExtensionDescriptor::encode` helper
stores one exact CBOR value in that vector, while `ExtensionDescriptor::empty`
stores zero bytes. The extension ID determines how an implementation interprets
the bytes.

## Data segments and file extents

A data segment is a stream of kind 3 whose payload contains stored byte ranges.
The references used to construct a logical file are:

```text
SegmentRangeRef = [
  segment: ObjectLocator,
  offset: u64,
  stored_content: ContentRef
]

ExtentRef = [
  stored_range: SegmentRangeRef,
  decoding_outputs: [ContentRef]
]

FileRange = [offset: u64, length: u64]

ExtentMapping = [
  logical_range: FileRange,
  extent_offset: u64,
  extent: ExtentRef
]
```

`FileRange.length` MUST be greater than zero, and `offset + length` MUST fit in
`u64`. A writer MUST place `offset .. offset + stored_content.length` within the
data-segment payload. `SegmentRangeRef` does not carry the payload length, so a
reader cannot establish that bound from the locator alone. The logical content
of an extent is the last decoding output, or its stored content when the
decoding list is empty. An extent mapping MUST fit within that logical content
after applying `extent_offset`.

The stored segment class MUST be DataSegment. Stored and logical content lengths
MUST be positive, and `decoding_outputs` MUST have one entry per decoding in
`FileDataLayout`. In the current core profile that list is empty, so stored and
logical extent content are identical.

Extent mappings are encoded in streams of kind 5. Within one run they MUST be
ordered by logical offset and MUST NOT overlap. A continuation, when present,
MUST reference another FileExtentSegment stream. The first mapping is stored in
`inline_extent`; the continuation contains the remaining mappings. Every
mapping MUST fall within `span`, and the final mapping MUST end at the end of
`span`. Without a continuation, `span` MUST equal the inline mapping's logical
range.

```text
ExtentRunRef = [
  span: FileRange,
  inline_extent: ExtentMapping,
  continuation: StreamRef?
]

FileExtentMap = [
  base_run: ExtentRunRef?,
  patch_levels: [ExtentRunRef?]
]
```

An empty file has no base run and no patch levels. A non-empty file MUST have a
base run. The last patch level MUST NOT be absent. There may be at most 64 patch
levels. A reader evaluates patch levels from newest to oldest, followed by the
base run. For every requested byte, the first mapping found is authoritative.

Every byte in the declared file content MUST be covered exactly once after
overlay resolution. Holes, out-of-bounds mappings, and unresolved ambiguity are
format errors.

## Namespace records

The namespace record schemas are:

```text
NamespaceRecord = [path: string, value: NamespaceNode?]

NamespaceNode = [
  node_id: NodeId,
  generation: u64,
  attributes: NodeAttributes,
  value: NamespaceValue
]

NodeAttributes = [executable: bool]

NamespaceValue =
  [0, generation: u64]
  or
  [1, version: FileVersionId,
      content: ContentRef,
      data: FileExtentMap]
```

A `null` value is a deletion marker and is valid in a change stream. A snapshot
stream MUST contain materialized nodes, not deletion markers. Records in each
namespace stream MUST be strictly ordered by the UTF-8 bytes of `path`.

The root path is the empty string. A valid materialized namespace MUST contain
one root directory whose node ID equals `VolumeFormat.root_node_id`. A writer
MUST compute a regular file's `ContentRef` from its complete logical byte
sequence. The current restore path checks extent coverage and the available
extent digests, but does not recompute the file-level digest over its output.

The namespace value tag is closed in format v0. A decoder MUST reject an
unknown tag or a tuple with an unexpected element count.

## Paths

A path is UTF-8 and MUST be in Unicode Normalization Form C. The root is the
empty string. A non-root path:

- MUST contain at most 4096 UTF-8 bytes;
- MUST NOT start or end with `/`, and MUST NOT contain `//`;
- MUST have components of at most 255 UTF-8 bytes;
- MUST NOT contain `.` or `..` components;
- MUST NOT have a component ending in a space or dot;
- MUST NOT contain control characters or `<`, `>`, `:`, `"`, `\\`, `|`, `?`,
  or `*`; and
- MUST NOT use a case-insensitive Windows reserved stem such as `CON`, `PRN`,
  `AUX`, `NUL`, `COM1` through `COM9`, or `LPT1` through `LPT9`, including the
  recognized superscript digit forms.

## Namespace revisions and commits

Revision records are:

```text
NamespaceSnapshot = [change_cursor: ChangeCursor, stream: StreamRef]

NamespaceChangeSegment = [
  end_cursor: ChangeCursor,
  compaction_weight_bytes: u64,
  stream: StreamRef
]

OperationReceipt = [change_cursor: ChangeCursor, operation_id: OperationId]

OperationReceiptSegment = [
  first_cursor: ChangeCursor,
  last_cursor: ChangeCursor,
  compaction_weight_bytes: u64,
  stream: StreamRef
]

NamespaceCommit = [
  volume_id: VolumeId,
  change_cursor: ChangeCursor,
  namespace_snapshot: NamespaceSnapshot,
  namespace_changes: [NamespaceChangeSegment],
  operation_receipts: [OperationReceiptSegment]
]

NamespaceRevision = [object: ObjectRef, change_cursor: ChangeCursor]

AuthorityHead = [
  current_commit: NamespaceRevision,
  gc_epoch: GcEpoch,
  minimum_retained_cursor: ChangeCursor
]
```

`AuthorityHead.minimum_retained_cursor` MUST NOT exceed
`current_commit.change_cursor`.

Snapshot streams have kind 1, change streams kind 4, receipt streams kind 2,
and commit objects class 1. The commit volume ID and cursor MUST match their
enclosing revision and volume. A snapshot cursor MUST NOT exceed the commit
cursor.

Every change segment MUST have a positive compaction weight. Its end cursor
MUST be greater than the preceding snapshot or segment cursor and no greater
than the commit cursor. The last change segment, or the snapshot when no change
segments exist, MUST end at the commit cursor.

Every receipt segment MUST have a positive compaction weight and an inclusive
range with `first_cursor <= last_cursor <= commit cursor`. Adjacent receipt
segments MUST be cursor-contiguous, and the final segment MUST end at the commit
cursor. A genesis commit MUST have no receipt segments. Each receipt within a
segment MUST identify a committed cursor covered by that segment. Receipt
records MUST be ordered from newest cursor to oldest, and the stream MUST
contain every cursor in its declared inclusive range.

To materialize a namespace at cursor `C`, a reader starts with
`namespace_snapshot` and applies change segments whose `end_cursor` is no
greater than `C`, in increasing `end_cursor` order. For repeated paths, the
latest applicable record wins. Compaction MAY replace snapshot, change, or
receipt segments only when the materialized view and retained receipt semantics
remain unchanged.

## Publication and authority

The default authority is the `managed/0/head` record. The authority extension
in `VolumeFormat`, when present, may define another authority mechanism.

A writer publishing revision `C + 1` MUST:

1. Read and validate the current head and its selected commit at cursor `C`.
2. Write every new data, extent, namespace, receipt, and commit object with
   create-if-absent semantics.
3. Replace the head using a condition tied to the version observed in step 1,
   such as an ETag match.
4. If the condition fails, read the current commit and use the operation receipt
   to distinguish an already committed operation from a conflict.

A reader MUST treat the authority head as the visibility boundary. It MUST NOT
discover a revision by listing immutable objects.

The format record MUST be created before the initial head and MUST NOT be
replaced in place. Opening a volume MUST validate both records and confirm that
all embedded volume and root identities agree.

## Garbage collection

Garbage collection MUST coordinate with the authority so that it cannot delete
an object that a concurrent successful publication may reference.

The current implementation uses an epoch fence:

1. Conditionally advance `AuthorityHead.gc_epoch`.
2. Materialize and compact each fenced authority root into the new epoch.
3. Mark the new commit, its namespace and receipt streams, its extent streams,
   and every referenced data segment.
4. List objects only in epochs older than the fenced epoch.
5. Delete an old object only if it is not in the reachable set.

An implementation MUST fail closed on malformed references or incomplete
listing. It MUST NOT infer liveness from object age alone.

## Validation and error handling

Readers MUST validate data before making it visible to higher layers. At a
minimum this includes:

- canonical key spelling for an object locator;
- envelope magic, lengths, checksums, and exact byte consumption;
- fixed tuple shape, enum tag, scalar range, and extension support;
- stream kind and object-class agreement;
- object, payload, and complete stored-range digests when their full bytes are
  read;
- path syntax and namespace ordering;
- extent arithmetic, ordering, coverage, and overlay invariants; and
- volume, cursor, and authority consistency across references.

A checksum or digest mismatch is corruption. A failed conditional write is a
conflict. An unsupported required extension is an incompatibility. Implementations
SHOULD preserve these distinctions in their error surfaces.

## Compatibility and conformance

The `0` in `managed/0/` is the storage-format version. Format v0 uses closed,
fixed-length CBOR tuples. Adding a field to an existing tuple, changing a tag's
meaning, or adding a required union arm is not backward compatible with a v0
reader.

Implementations MUST reject structures they cannot interpret safely. They MUST
NOT silently ignore an unknown tuple suffix or treat an unknown required stream
kind as empty. A new implementation MAY add stricter checks that reject states
which already violated the invariants in this RFC.

Golden byte snapshots in `crates/managed-format/tests/snapshots/` are
conformance fixtures for the current encoding. Passing those fixtures is
necessary but not sufficient: implementations must also test malformed lengths,
trailing fields, digest failures, wrong classes and kinds, invalid paths,
overlapping extents, cursor gaps, and conditional-publication conflicts.

# Drawbacks

The format has more metadata than a direct path-to-object mapping. Extent maps,
commits, and receipts introduce additional reads and validation work, especially
for small files.

Fixed positional tuples keep v0 compact and unambiguous but make compatible
field addition difficult. Until a new encoding is adopted, even an optional
field changes the tuple shape and therefore requires an explicit format
transition.

Whole-object and whole-extent digests do not authenticate arbitrary partial
reads. A client must fetch the complete authenticated unit or rely on an
additional verified-range structure.

The shared-segment model also makes reclamation indirect. Deleting a file
version does not immediately delete its bytes; the collector must trace
reachability and may need later segment cleaning to recover partially live
objects.

# Rationale and alternatives

## Atomic publication

Publishing immutable objects before one conditional head update gives readers
a single visibility boundary. An interrupted writer may leave unreachable
objects, but it cannot expose a commit whose head was not published.

## Content identity and shared placement

A BLAKE3 digest and length identify bytes independently of their object key.
Extents provide placement separately, so file versions can reuse stored ranges
and future locators can change placement without changing content identity.
Shared data segments reduce the number of small objects at the cost of extent
metadata and reachability-based collection.

## Positional tuple encoding

Fixed tuples have small encodings and make every accepted shape explicit. They
also match the PR #29 implementation. Their main cost is schema evolution: an
additional tuple element is incompatible with current v0 readers. The
integer-key map proposal below defines the rules needed before adopting a more
extensible encoding.

# Prior art

[Apache Parquet](https://github.com/apache/parquet-format/blob/master/README.md)
separates its logical hierarchy from byte-level layout and writes metadata after
data so a file can be produced in one pass. Managed format v0 follows the same
discipline of naming physical units and placing validating metadata at a known
tail, although its unit is an object-store stream rather than a columnar file.
Parquet's numbered Thrift fields and
[protocol extensions](https://github.com/apache/parquet-format/blob/master/BinaryProtocolExtensions.md)
also inform the integer-key map proposal in Future possibilities.

[Apache Iceberg](https://iceberg.apache.org/spec/) uses immutable metadata trees,
snapshots, and an atomic current-metadata pointer. It also reserves format
version changes for features that older readers cannot safely interpret.
Managed publication and the compatibility rules in this RFC follow those two
principles, adapted to a filesystem namespace.

[Lance](https://github.com/lance-format/lance/blob/main/docs/src/format/file/index.md)
describes its file format as a set of physical units with footer metadata, while
its [index format](https://lance.org/format/index/) treats search structures as
separate, redundant data. This RFC similarly separates authoritative streams
from derived accelerators.

[Bao](https://github.com/oconnor663/bao/blob/master/docs/spec.md) defines verified
streaming and slice proofs whose root is the ordinary BLAKE3 hash. That property
makes Bao a candidate derived index for `ContentRef` without replacing the
content identity already stored by v0.

[CBOR](https://www.rfc-editor.org/rfc/rfc8949.html) supplies the underlying data
model and deterministic-encoding guidance. Format v0 narrows it to fixed tuple
schemas and exact-byte validation.

# Unresolved questions

- At what project milestone does format v0 stop being experimental, and what
  cross-version test matrix is required before that decision?
- Which limits should be fixed by the format, rather than by an implementation,
  for total object size, stream frame count, extent count, and commit fan-out?
- How long must operation receipts be retained, and how should that retention
  interact with `minimum_retained_cursor`?
- Should a future external locator encode ownership per locator, or inherit an
  immutable volume-level ownership and export policy?

# Future possibilities

## Integer-key CBOR maps

A future encoding may replace selected fixed tuples with CBOR maps whose keys
are stable unsigned integers. Moving from v0 tuples to that encoding is itself
a format transition. Once adopted, the map rules can allow an older reader to
skip a newly added optional field without another format-version change. They
do not make every schema change compatible.

Such an encoding needs explicit rules:

- field IDs are permanent and MUST NOT be reused;
- missing optional fields have specified defaults;
- unknown optional fields may be skipped;
- unknown required fields and union arms fail before any mutation;
- deterministic map-key ordering is required wherever encoded bytes contribute
  to identity or golden fixtures; and
- a read-modify-write implementation must preserve unknown authoritative fields,
  or open the value read-only, rather than silently dropping them.

Breaking semantic changes, required new features, and new closed-union arms
would still require a new format version or an explicit required-feature gate.

## Locator generalization

Format v0 has one locator:

```text
Internal(gc_epoch, object_class, object_id)
```

A future format may introduce a tagged locator:

```text
Locator = Internal(gc_epoch, object_class, object_id)
        | Key(object_key, object_pin)
```

`Key` would let a file extent refer to an existing ordinary object without
copying it into `managed/0/objects/`. The pin must make the reference immutable.
A service version ID is preferable; an ETag is sufficient only where the
service gives it stable conditional-read semantics.

This enables zero-copy ingest, representation of very large pre-existing
objects, and zero-copy export. A first managed patch could place new internal
extents over an unchanged `Key`-backed base extent.

The ownership contract must be explicit. Garbage collection must never delete
an external `Key` object. If managed volumes can own ordinary-key objects, their
ownership must come from immutable format state, not mutable process
configuration. A key locator must not alias format, authority, or immutable
metadata objects. A pin mismatch is a conflict or corruption error; a reader
must never fall back to the latest object at that key. Compaction may materialize
bytes internally and need not preserve their original locator.

Because an old reader cannot interpret a `Key` union arm, writing the first such
locator requires a new format version or required-feature gate. New readers can
still read existing Internal-only volumes.

## Bao-derived authenticated range indexes

A Bao outboard tree can authenticate a byte range against the same BLAKE3 root
already stored in `ContentRef`. A future implementation could build an outboard
index during the full logical-file scan required for publication, or generate
it later from authoritative bytes.

The index should be a derived, rebuildable object keyed by content identity and,
where necessary, decoding profile. Readers that do not understand it must be
able to read the base extents. Missing or stale indexes must cause fallback, not
data loss. The design also needs a clear cache, reachability, and garbage
collection contract.

[Bao's README](https://github.com/oconnor663/bao) describes the implementation
as beta cryptography that has not been formally audited. Adoption therefore
requires a prototype, interoperable test vectors, and a security review before
its proofs are treated as an integrity boundary.

- Proposal Name: `filesystem_architecture`
- Start Date: 2026-07-30
- RFC PR: [apache/opendal-ofs#16](https://github.com/apache/opendal-ofs/pull/16)
- Tracking Issue: [apache/opendal-ofs#19](https://github.com/apache/opendal-ofs/issues/19)

# Summary

Define ofs as a cross-platform filesystem engine with two independent choices.
A **Volume Model** selects either `Direct`, where an existing storage namespace
is authoritative, or `Managed`, where ofs metadata owns the filesystem
namespace and storage holds immutable file data. An **Access Model** selects
either `Mount`, which presents an online remote filesystem, or `Sync`, which
reconciles a local native filesystem with a volume.

The same filesystem contract is implemented by OS-specific mount and sync
frontends. FUSE, filesystem extensions, user-mode drivers, placeholder APIs,
and network filesystem protocols are transports for that contract rather than
separate ofs storage modes.

# Motivation

An unmodified application can access remote files only through an interface
recognized by its operating system or through files materialized on a native
filesystem. Linux, macOS, and Windows expose different extension points and
different naming, handle, permission, and deletion behavior. A portable
filesystem can isolate these differences behind frontends, but cannot remove
them.

Object storage and filesystems also expose different semantics. A key and its
value do not provide a stable inode, transactional directory entry, atomic
rename, open-unlink behavior, hard links, random overwrite, or file locking.
Deriving those concepts from object names is useful for browsing existing data,
but it cannot provide the same guarantees as a namespace backed by filesystem
metadata.

ofs users need both outcomes:

- access existing storage without importing or rewriting it;
- use storage as the data plane for a reliable filesystem;
- choose an online mount or an offline-capable local replica; and
- know which operations, durability boundaries, and conflict behavior apply on
  every supported platform.

The architecture must preserve these differences instead of reporting a broad
POSIX label for behavior that only some combinations can enforce.

# Guide-level explanation

## The two-axis model

Every ofs volume has one Volume Model and can be accessed through either Access
Model:

| Volume Model | Mount | Sync |
| --- | --- | --- |
| `Direct` | Storage View over existing objects | Local replica of an object namespace |
| `Managed` | Remote filesystem with an authoritative namespace | Offline-capable local filesystem |

The Volume Model answers, "Where is the authoritative namespace?" The Access
Model answers, "What state does an application read and write immediately?"
They are configured independently.

## Direct volumes

A Direct volume maps storage paths to filesystem paths without creating
authoritative remote metadata:

```text
ofs volume create archive \
  --model direct \
  --storage <storage-url>

ofs mount archive /mnt/archive --read-only
```

Objects remain readable by existing storage tools. Direct volumes are suitable
for browsing, ingest, export, and whole-file workflows. Writable access stages
a complete file locally and replaces the corresponding object when it is
committed. Backend versions or ETags are used to detect conflicting changes
when available.

Directories and inode values in this view are derived. Rename can require copy
and delete. A Direct volume therefore does not claim stable identity across
rename, atomic rename, hard links, open-unlink, distributed locks, or efficient
random writes. Read-only mount is the default; writable Direct access must be
enabled explicitly.

## Managed volumes

A Managed volume stores files through a metadata namespace:

```text
ofs volume create workspace \
  --model managed \
  --storage <storage-url>

ofs mount workspace /mnt/workspace
```

Applications see stable files and directories. ofs metadata owns node
identity, directory entries, attributes, content versions, and generations.
Storage contains immutable file data referenced by metadata. The storage
prefix is private to ofs and must not be modified by external writers.

The metadata provider may use an embedded database, a remote database, or
metadata objects. The deployment choice is hidden behind one transaction and
change-feed contract. Every Managed volume provides stable node identity,
generation-checked mutations, and atomic create, unlink, and same-volume
rename. A metadata provider or frontend that cannot provide this baseline
cannot back or expose a Managed volume. Hard links, extended attributes,
advisory locks, and other operations remain optional capabilities.

## Mount and Sync

Mount and Sync have deliberately different acknowledgement semantics:

| Contract | Mount | Sync |
| --- | --- | --- |
| Immediate application view | Remote volume or a coherent cache | Local native filesystem |
| Successful `fsync` | Data and metadata published remotely | Data persisted locally |
| Offline writes | Not supported by default | Supported |
| Conflict reporting | Filesystem error | Conflict record and retained content |
| Local disk usage | Evictable cache and staging | Materialized tree and state database |

`ofs mount` selects a frontend supported by the host. A user asks for a mount,
not for FUSE as a product mode:

```text
ofs mount <volume> <mount-path>
```

A Linux FUSE frontend, a platform filesystem extension, a user-mode driver, or
a loopback network filesystem can implement the same Mount contract. Selecting
a different frontend cannot strengthen the volume's capabilities.

`ofs sync` reconciles a volume with an ordinary local directory:

```text
ofs sync <volume> <local-directory>
ofs status <local-directory>
```

Local writes remain available while disconnected. Remote publication has a
separate status and explicit wait operation. When both sides change from the
same base generation and cannot be merged, ofs retains both contents and
reports a conflict instead of choosing the last writer.

## Required contracts and optional capabilities

Selecting `Direct` or `Managed` and `Mount` or `Sync` is the complete
user-facing mode selection. Each choice contributes the mandatory contract
described above. ofs does not add another semantic bundle that weakens or
rebundles those guarantees; a combination that cannot satisfy its selected
models fails to start.

Operations outside the baseline are reported individually. A user can require
them when starting an access model:

```text
ofs mount workspace /mnt/workspace \
  --require hard-link \
  --require xattr
```

Missing requirements fail before the mount becomes visible. `ofs status`
reports effective capabilities, pending writes, conflicts, and the local and
remote durability positions.

# Reference-level explanation

## Architecture boundaries

ofs is divided into four responsibilities:

```text
OS frontend
    |
    v
Filesystem core
    |
    +---- Volume implementation
    |        |
    |        +---- Direct namespace
    |        |
    |        +---- Managed metadata
    |
    v
OpenDAL data storage
```

The filesystem core owns common file operations, handles, generation checks,
baseline validation, capability reporting, cache policy, and error semantics.
A Volume implementation owns namespace authority and publication. An OS
frontend translates native operations without changing their guarantees.
OpenDAL provides storage primitives and their native capabilities; it does not
emulate filesystem transactions for ofs.

The core contract includes these logical identities:

```text
VolumeId
NodeId
FileHandle
FileVersion
Generation
DirectoryEntry
Capabilities
ChangeCursor
```

It exposes lookup, enumeration, open, close, read, staged write, commit, abort,
create, unlink, rename, attribute operations, and change enumeration. Mutations
that can replace observed state include an expected `Generation`. A generation
mismatch is a conflict and never silently overwrites newer state.

`FileHandle` remains bound to the node and content semantics established by
open, independent of later path changes. Managed `NodeId` values are stable
across rename. Direct `NodeId` values may be path-derived and advertise that
they are not stable across namespace changes.

Effective capabilities are the intersection of all participating layers:

```text
volume model
  ∩ storage backend
  ∩ access model
  ∩ OS frontend
  = effective capabilities
```

Each capability defines its atomicity scope, durability boundary, multi-client
visibility, and unsupported error. Frontends cannot simulate and advertise a
stronger operation when a crash or competing writer can observe an intermediate
state. Before access starts, the intersection must contain every operation
required by the selected Volume and Access Models. Remaining operations are
reported individually and can be asserted with `--require`.

## Direct namespace contract

The storage namespace is the only remote authority for a Direct volume:

- a reversible path codec maps object keys to path components;
- object content is file content;
- an object version, ETag, or equivalent token becomes its generation;
- directories, inode numbers, and unsupported attributes are derived;
- external storage writers are valid concurrent writers; and
- local caches, indexes, staging files, and recovery journals are allowed, but
  no remote ofs metadata becomes namespace authority.

Pure storage means that ofs does not require an additional remote metadata
model. It does not require stateless clients. Pending local writes and
multi-step operations must be journaled until they are committed, cancelled,
or recovered.

A writable Direct file is committed as follows:

```text
stage complete local file
    -> upload replacement object
    -> condition publication on observed generation when supported
    -> expose success or conflict
```

Random application writes can update the staging file, but remote publication
remains a whole-object replacement. A rename implemented by copy and delete is
reported as non-atomic. Recovery may complete or compensate that workflow, but
does not retroactively make it atomic.

If the backend cannot enforce a conditional replacement, writable Direct
access is unavailable and the volume remains read-only. Direct mode never
silently substitutes an unprotected last-writer-wins commit.

Existing object names may be illegal or colliding on the local OS. Direct mode
uses a deterministic, reversible escape representation for those entries and
reports the mapping. It never hides an object or maps two object keys to one
local path.

## Managed namespace contract

The Managed metadata plane is the sole authority for:

- volume format and identity;
- stable nodes and directory entries;
- attributes and link relationships;
- node and directory generations;
- file manifests and content versions; and
- a cursor-based change log.

The data plane stores immutable blobs, chunks, or extents. A `FileVersion`
manifest references only durable data. Publishing a file follows this order:

```text
local staging
    -> upload immutable data
    -> verify successful data publication
    -> atomically publish FileVersion and namespace metadata
    -> acknowledge commit
    -> reclaim unreachable data asynchronously
```

The metadata transaction includes an operation ID and expected generations.
Retrying a committed operation returns the committed result. A failure before
the transaction may leave unreferenced data, but metadata never references
incomplete data. Garbage collection treats only metadata-reachable data as
live.

Namespace mutations are linearizable within their advertised transaction
scope. Open handles continue to reference their opened node and version after
rename or unlink. Multiple clients observe changes through generations and the
change log; cache leases or invalidations are implementation mechanisms, not
additional semantics.

Managed storage is an internal format. Direct writes to its data prefix are
corruption, not concurrent filesystem changes.

## Mount contract

A Mount frontend presents the remote volume as the authority. Local data is an
evictable cache or recoverable staging state.

`write` may acknowledge data accepted into a local writeback cache. `flush`
attempts publication. `fsync` succeeds only after ofs has completed remote data
publication and the required metadata commit according to the backend's
acknowledged durability contract. If the network or remote commit is
unavailable, `fsync` fails.

Asynchronous writeback errors remain attached to the handle and volume. They
are returned by a later `flush`, `fsync`, or `close` where the frontend permits,
and are always visible through `ofs status`.

Managed mounts use generations and invalidation to provide their declared
multi-client visibility. Direct mounts use storage versions and conditional
operations. A detected conflict returns a stale or conflict error; it is not
converted into a background conflict file.

Long-lived offline mutation is outside the Mount contract. A successful
filesystem call cannot later be revoked when an offline change conflicts after
reconnection.

## Sync contract

Sync materializes a local native filesystem and maintains a durable local state
database. That database records the last common generations, pending
mutations, rename information, tombstones, and the remote change cursor.
Filesystem notifications are scan hints, not authoritative history; startup,
periodic reconciliation, and explicit sync rescan the local tree.

A local `fsync` has only local durability. Remote durability is reached when
the corresponding mutations are committed to the volume and the sync status
advances. A user can wait for that point explicitly before replacing a machine.

Reconciliation compares local and remote state against the last common
generation. Non-overlapping changes can be applied independently. Conflicting
file updates, delete-versus-edit, and incompatible renames retain all available
content and create a conflict record.

Direct Sync discovers remote changes from listings and object generations.
Rename detection is an optimization and is published as create plus delete.
Managed Sync consumes stable node identities and `ChangeCursor`, so it can
preserve rename and deletion identity across clients.

## Naming and portability

The default Managed naming policy defines a normalized Unicode representation,
component and path length bounds, and a set of names valid on supported desktop
platforms. It rejects case-folding collisions and platform-reserved names at
creation time.

A Managed volume can opt into a platform-specific naming policy, such as
Unix-only names. A frontend that cannot represent that policy rejects the
mount or sync instead of changing names. Direct volumes cannot reject existing
objects, so they use the reversible path codec described above.

## Compatibility and migration

The existing two-positional-argument CLI remains a compatibility form of a
Direct Mount while the volume-oriented CLI is introduced. Existing object
layouts and OpenDAL URLs keep their current interpretation.

The current FUSE implementation becomes one Mount frontend; this RFC does not
require an immediate implementation change. New frontends must implement the
same core contract rather than fork Direct or Managed behavior.

A volume's model is fixed when it is created. Direct-to-Managed conversion is
an explicit import that assigns stable identities and initial generations.
Managed-to-Direct conversion is an explicit export that materializes ordinary
objects. Neither operation changes the source volume in place.

Managed metadata, manifests, and change cursors are versioned. A client that
does not understand the format can mount read-only only when that format
explicitly defines backward-readable behavior; otherwise it fails before
mutation.

## Conformance

A `Volume Model × Access Model × OS frontend × backend` combination can ship
only after the mandatory contracts of both selected models pass their
conformance suites. Each optional capability requires targeted coverage before
it is advertised. Tests include crash points during publication, unknown
commit results, concurrent generation changes, `fsync` followed by restart,
name collisions, open-handle rename and unlink, cache invalidation, and offline
sync conflicts.

# Drawbacks

Two independent axes create more concepts than a single storage-backed mount.
Users and maintainers must understand which durability and conflict contract
they selected.

Managed volumes require a transactional metadata implementation, format
versioning, repair tools, and garbage collection. They also prevent users from
treating the underlying objects as the filesystem namespace.

Direct volumes retain weaker filesystem semantics. Making those limitations
visible may reject workloads that appeared to work under low contention.

Multiple OS frontends increase packaging, signing, installation, and
conformance work. Sync consumes local disk and needs user-visible conflict
handling, while Mount depends on online availability for mutation.

# Rationale and alternatives

## One POSIX abstraction

Making POSIX the internal model would still require silent translation for
Windows names, sharing modes, ACLs, and deletion behavior. Explicit
model contracts and individual capabilities preserve POSIX operations where
they can be enforced and reject unsupported combinations elsewhere. A future
POSIX conformance label may expand to a versioned set of required capabilities,
but it does not change the selected Volume or Access Model.

## One cross-platform mount implementation

FUSE-compatible libraries and user-mode drivers solve an OS transport problem.
They do not define storage authority, remote durability, or offline conflict
semantics. Keeping them as frontends allows ofs to adopt the best supported
mechanism on each platform without changing volume behavior.

## Direct storage only

Direct mapping preserves existing data and is operationally simple, but cannot
provide stable identity or transactional namespace semantics. Emulating those
features in local caches fails with multiple clients and after cache loss.

## Managed storage only

Always requiring metadata would make reliable filesystem semantics simpler,
but would force users to import existing object namespaces and stop using
storage-native tools. Direct remains a deliberate compatibility and data-access
model.

## Mount only

Mount offers transparent paths and avoids a full local copy, but it cannot
provide reliable long-lived offline writes because conflicts occur after the
originating filesystem call has returned.

## Sync only

Sync provides native local behavior and offline access, but consumes local
storage and does not provide an immediately consistent remote namespace.
Applications that need a shared online view still require Mount.

## Network filesystem protocols

Serving SMB, NFS, or WebDAV can avoid installing a custom filesystem driver on
some hosts. It remains a Mount frontend: protocol semantics and the host client
still participate in the effective capability intersection.

# Prior art

[Linux FUSE](https://www.kernel.org/doc/html/latest/filesystems/fuse/fuse.html)
separates the kernel filesystem interface from a userspace daemon.

Apple exposes both
[FSKit filesystem extensions](https://developer.apple.com/documentation/fskit)
and a
[replicated File Provider model](https://developer.apple.com/documentation/fileprovider/replicated-file-provider-extension).
The former presents a filesystem implementation; the latter synchronizes
system-managed local copies with remote storage.

Windows similarly separates
[projected filesystems](https://learn.microsoft.com/en-us/windows/win32/projfs/projected-file-system)
from
[Cloud Files sync providers](https://learn.microsoft.com/en-us/windows/win32/cfapi/build-a-cloud-file-sync-engine).
These APIs validate the distinction between presenting a remote view and
reconciling local files, while their platform-specific contracts reinforce the
need for shared ofs semantics above the frontend.

# Unresolved questions

- What is the minimum metadata transaction interface required for the first
  Managed implementation?
- Which exact path encoding and Unicode normalization form define the portable
  and Direct escape codecs?
- Which Mount and Sync frontends form the first cross-platform release target?
- Which cache lease and invalidation policy is required before Managed mounts
  can advertise multi-client coherence?

# Future possibilities

Managed volumes can add filesystem-wide checkpoints, snapshots, retention, and
point-in-time recovery without changing the two-axis model. Their history is
metadata over immutable `FileVersion` values.

Content-defined chunking, deduplication, sparse hydration, and delta upload can
optimize Managed data publication. Multi-writer Sync can add structured merge
policies over stable nodes and generations. Additional network and native
frontends can be introduced independently after passing the same capability
conformance suites.

- Proposal Name: `filesystem_components`
- Start Date: 2026-07-29
- RFC PR: [apache/opendal-ofs#17](https://github.com/apache/opendal-ofs/pull/17)
- Tracking Issue: [apache/opendal-ofs#0000](https://github.com/apache/opendal-ofs/issues/0000)

# Summary

Define the component architecture used to implement ofs.

An ofs instance consists of a volume catalog, an Access Model implementation,
the filesystem core, a Volume Model implementation, and provider adapters.
Access and Volume implementations meet only at the core. This lets the four
model selections share capability admission, publication, recovery, and status
without sharing namespace authority.

# Motivation

A Mount frontend can call OpenDAL directly. A Sync engine can depend on one
Managed metadata schema. These shortcuts are easy to build, but they tie an
Access Model to one Volume or provider. They also duplicate capability checks,
recovery, error handling, and status.

Explicit component boundaries keep those dependencies local. They also assign
durable state to the component that can validate and recover it after a crash.

# Guide-level explanation

## Implementation architecture

```text
                    CLI and Volume Catalog
                              |
                              v
                         Access instance
                              |
                              v
                  Access Model implementation
                       /                 \
              Mount frontend          Sync engine
                       \                 /
                        v               v
                         Filesystem core
                  (includes publication coordinator)
                       /                 \
              Direct volume         Managed volume
                   |                 /            \
          OpenDAL operator    Metadata Store    Data Store
                                                   |
                                           OpenDAL operator
```

Each Access Model implementation can pair with each Volume Model
implementation because both depend on the filesystem core. The four selections
reuse one set of boundaries, not four stacks.

The catalog creates the access binding, and provider adapters satisfy the
selected volume's external dependencies. The component that creates durable
state must also validate and recover it.

The publication coordinator is a core component shared by both Access
implementations. It accepts prepared changes from Access code and delegates the
authority-specific write to the selected Volume implementation.

These components may run in one process or be split across processes. Their
placement does not change the dependency direction or ownership.

## Assemble an access instance

Every access instance starts through the same sequence:

1. Resolve and validate the volume definition.
2. Select the Access Model implementation and provider adapters.
3. Compose their required and provided capabilities.
4. Reject the instance before activation if a mandatory contract is missing.
5. Bind versioned local access and recovery state to the volume identity.
6. Recover pending work and establish access-local state against the current
   Volume state.
7. Expose the access path and derive status from durable state.

## Share publication coordination

The shared publication coordinator follows this lifecycle:

```text
observe authoritative state
    -> prepare stable input
    -> persist publication intent
    -> publish with a concurrency precondition
    -> resolve success, absence, or conflict
    -> advance durable access state
```

The coordinator owns stable input, operation identity, recovery, and status.
The Volume implementation supplies the publication primitive. A Direct adapter
protects a storage mutation. A Managed adapter publishes data and then commits
metadata. These primitives stay separate.

The Access implementation hands the coordinator a stable, replayable input. If
the source changes before that handoff completes, Access prepares it again
instead of publishing the stale input.

## Replaceable mechanisms

Component contracts cover identities, durable state, generation checks,
conflicts, and recovery. They do not fix a database, object layout, integrity
algorithm, serialization, cache, scan strategy, or operating-system
integration.

A component may replace any of these mechanisms if it still satisfies its
contract and can read or migrate existing durable state.

# Reference-level explanation

## Component ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| Catalog and control plane | Stable volume definitions and access selection | Filesystem operation semantics |
| Access implementation | Host integration, acknowledgement, and access-local state | Remote namespace authority |
| Filesystem core | Common operations, handles, capabilities, errors, and status | Provider protocols and layouts |
| Publication coordinator | Prepared changes, publication intent, and result recovery | Namespace authority and provider requests |
| Volume implementation | Namespace authority, generations, and publication | Host-specific transport behavior |
| Provider adapter | Provider primitives and native guarantees | Portable filesystem semantics |

Put each check or mutation at the narrowest boundary that has enough
information and authority. Components may share a module without sharing
ownership.

## Volume catalog and access binding

A volume definition contains a stable `VolumeId`, its Volume Model, adapter
configuration, and format identifiers. Adapters resolve credentials
separately; credentials are not part of volume identity.

An access binding records the `VolumeId`, Access Model, durable-state version,
and admitted capabilities. This identity prevents an implementation from
opening the state for a different volume or contract.

The Access implementation validates the binding and recovers pending work
before it becomes active. It then establishes its local state against the
current Volume state. Only after that point may it expose a mount point, scan
for new local changes, or accept filesystem mutations. Catalog and binding
formats are private to the components that own them.

## Access implementation boundary

A Mount frontend adapts a host filesystem API to the core. A Sync engine adapts
a native tree and its reconciliation loop to the same interface. Both send
filesystem operations through the core.

Host translation, access-local cache, staging, and recovery state stay behind
the Access implementation boundary. The access binding supplies the Volume
identity and admitted capabilities, so Access code has no reason to inspect a
provider.

## Filesystem core boundary

The filesystem core carries the architecture's logical operations and types
between Access and Volume implementations. Expected generations, operation
IDs, capabilities, and errors pass through unchanged.

The core dispatches operations and keeps handle bindings and error categories
intact. Access code translates host identifiers. Volume code translates
authority identifiers. Provider types stop at provider adapters, and the core
has no code path dedicated to one Access and Volume selection.

## Volume implementation boundary

A Volume implementation maps the core contract to the selected authority. It
provides observation, publication, recovery, change discovery, and capability
operations.

Direct and Managed implement this boundary independently. Code shared above
the boundary can coordinate publication and recovery, but cannot assume that
both models expose the same transaction or namespace primitive.

## Direct volume implementation

A Direct Volume implementation maps the core contract to one OpenDAL operator.
The adapter owns path encoding, generation tokens, preconditions, and result
resolution.

It reports the publication operations that the operator can protect and
supplies Direct observation and recovery to the publication coordinator.
Object keys, provider versions, and response types stop at this adapter.

A Direct implementation may keep local indexes, caches, and recovery journals.
This RFC uses `Metadata Store` only for the authoritative namespace component
of a Managed Volume, not for this access-local or derived metadata.

## Managed volume implementation

A Managed Volume implementation has two dependencies:

```text
                         Managed volume
                       /                \
              Metadata Store          Data Store
          namespace and generations   immutable file data
                                            |
                                     OpenDAL operator
```

`ManagedVolume` composes a provider-neutral Metadata Store and Data Store. The
Metadata Store accepts namespace, generation, change-position, and operation
ID requests. The Data Store accepts content and returns an opaque data
reference.

The two stores are logical interfaces. They may run in one process or use the
same physical provider; a separately deployed metadata service is not
required.

`ManagedVolume` sequences the two stores and passes only durable data
references to metadata publication. Physical data locations and provider
transaction types stay inside the corresponding store adapter.

The Data Store layout is not a transparent object namespace. An implementation
may provide an explicit export or read-only projection, but that view is
derived from Managed metadata. External changes to the projection or Data Store
do not change the Managed namespace.

The durability and visibility of a Managed Volume are bounded by both stores.
A Metadata Store confined to one node cannot provide cross-node recovery,
regardless of the Data Store's durability. Replication may widen that scope,
but it must be part of the admitted Metadata Store contract.

## Publication and recovery

Before starting an external write, the publication coordinator records its
target, stable input, expected authoritative state, and operation ID.

Access owns preparation until it hands a stable, replayable input to the
coordinator. The coordinator owns that input and its recovery record until the
result is resolved. Volume owns the authoritative precondition and write.

Publication has six requirements:

1. The recorded input stays unchanged until the attempt is resolved.
2. A replacement uses the generation or transaction position observed when the
   operation was prepared.
3. Recovery resolves an unknown result before submitting a new intent for the
   same operation.
4. The Access implementation advances durable state only after it knows the
   publication result.
5. A conflict keeps competing state and all available content.
6. Clearing the recovery record is durable.

The component that can finish the operation owns its recovery record. The
record is outside the user-visible namespace.

## Reconciliation

The Sync engine runs reconciliation in three steps: observation, planning, and
execution.

Observation produces an immutable input containing the local view, remote
view, and common position. Planning produces changes with their expected
generations but performs no remote mutation. Before execution, the Sync engine
stabilizes every content source referenced by the plan. Execution hands that
replayable input to the publication coordinator and returns to observation
when the source changes or a precondition is stale.

The Sync engine owns the plan and durable common position. The Volume
implementation owns revalidation and publication. Notifications and provider
events can schedule observation, but they cannot replace it.

## Capability admission

Each component declares `provides`, `requires`, and `limits`. Admission combines
these declarations and checks the selected model contracts before constructing
an active access instance.

The admitted result is retained with the access binding and passed to the core,
Access implementation, and Volume implementation. Components branch on that
result, not on implementation or provider names.

Managed admission includes declarations from both the Metadata Store and Data
Store. A capability is limited to the weaker store's durability and visibility
scope.

## Durable formats and provider isolation

Each component versions the durable state it owns. It validates, recovers, and
migrates that state before activation. A compatible migration retains
outstanding publication records.

Provider adapters translate provider paths, sessions, transactions, versions,
and responses into the core and volume boundary types. No provider structure
reaches a higher layer unless the adapter carries it as an opaque value.

## Validation

Tests target component contracts and observable failures. Module structure,
database tables, storage layouts, and callback order are private details.

Tests cover crashes around external writes, unknown results, competing
generations, restart from durable state, and invalid formats. A component test
may replace either neighbor with a contract test double.

# Drawbacks

The component split adds interfaces between Access, core, Volume, and provider
code. A small implementation may find those interfaces heavier than direct
calls.

Recoverable publication also requires durable records and repeated checks
around remote writes. Reconciliation may repeat observation when a generation
changes.

# Rationale and alternatives

## Duplicate the stack for each selection

A separate stack for each selection avoids shared interfaces at first, but
duplicates Access behavior, recovery, and capability checks. Specialized code
can still sit behind a shared boundary.

## Define one generic remote transaction

Direct and Managed use different concurrency and namespace primitives. A
single transaction interface would either expose provider details or claim
atomicity that one model cannot provide. They share publication coordination,
not a transaction API.

## Retry every unknown result

Blind retries can duplicate effects or overwrite a competing change. Recovery
first resolves the recorded attempt using its operation ID and precondition.

## Standardize physical state and data layouts

A common physical layout would couple Access implementations and providers.
Each owning component can version its own format while keeping the component
contracts stable.

# Prior art

OpenDAL separates native provider capabilities from the behavior exposed by a
composed operator. ofs uses the same approach when it admits Access, Volume,
and provider components.

[ObjectiveFS](https://objectivefs.com/features) coordinates a filesystem
through an object store without a separately deployed metadata server. This is
one way to implement a Metadata Store adapter; it does not remove the logical
Metadata Store boundary.

Write-ahead records preserve intent across a crash. Operation IDs make retries
identifiable, and generation checks reject writes prepared from stale state.
The publication coordinator combines these techniques.

# Unresolved questions

- Which implementation contracts should become reusable public interfaces, and
  which should remain internal boundaries?
- Should durable access state be portable between implementations, or only
  versioned and recoverable by the implementation that created it?
- How should capability definitions themselves be versioned as additional
  frontends and providers are introduced?
- Which conflict information must have a portable representation across access
  implementations?

# Future possibilities

The publication coordinator and reconciliation pipeline may become reusable
libraries once their interfaces have been tested by more than one
implementation.

A common contract harness could test new frontends, Volume implementations,
and provider adapters. Fault injection would cover crashes between durable
state changes and external writes.

Incremental observation, content chunking, caching, and multi-writer
coordination fit behind the existing boundaries. Each optimization still has
to satisfy the publication and recovery requirements.

# Appendix A: Managed Sync implementation path

This appendix applies the component model to a Managed Sync implementation at
`0b0d112`. It adds a named Managed Volume and a native local replica, without
introducing another model combination.

## Expected use case

A user keeps agent sessions, skills, MCP configuration, and other memory as
ordinary files in a short-lived agent environment. At startup, they synchronize
the last published state into a local directory and use normal file tools.

Changes remain local until the user explicitly publishes them. Other
environments continue to read the last published state and receive the new
state on their next synchronization.

## Evolution from `0b0d112`

At `0b0d112`, ofs exposes one OpenDAL operator through FUSE. There is no
Managed namespace, local replica state, or reconciliation path. Managed Sync
adds these components alongside the existing path rather than adapting FUSE.

```text
0b0d112

CLI -> FUSE frontend -> OpenDAL operator

RFC-0017 path

CLI + volume catalog
        |
Sync engine -> filesystem core -> Managed volume
     |                |            /          \
local Sync state  publication   Metadata    Data Store
                  coordinator     Store          |
                                           OpenDAL operator
```

The Managed Volume is established first. Its Metadata Store owns the namespace,
generations, publisher fencing, and atomic commits. Its Data Store writes
immutable file content through OpenDAL.

The Sync engine then observes a native local tree, its common base, and the
current Managed state. It stabilizes changed content, reconciles the three
views, and gives the publication coordinator a replayable change set. Durable
Sync state records the replica position and pending work outside the
synchronized directory.

Finally, the CLI binds the local replica to a named volume. One `ofs sync`
invocation performs one reconciliation and exits. Recovery runs before
reconciliation, and `ofs status` reports the replica's durable state.

## Usage

```shell
ofs volume create agent-home \
  --model managed \
  --storage <storage-url> \
  --metadata <metadata-url>

ofs sync agent-home ./agent-home \
  --state /var/lib/ofs/agent-home

ofs status ./agent-home \
  --state /var/lib/ofs/agent-home
```

Synchronizing an empty directory materializes the published state. Later local
changes remain private until the user runs the same command again. Reusing the
state directory after restart resumes the same replica.

## Scope

The path covers regular files and directories, native local mutations,
explicit whole-tree publication, restart from durable Sync state, and one
fenced publisher at a time. Local filesystem operations do not publish remote
state, and the path does not require FUSE.

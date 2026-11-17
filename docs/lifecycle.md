# Retention, archival and storage lifecycle

This milestone implements versioned retention policies, reviewable plans, legal holds,
verified POSIX array tiering, read-through of archived versions, explicit hot-copy eviction,
restoration and non-destructive observation revision exports. No production retention job
is enabled automatically. The PostgreSQL catalogue, history, scenario pins, derived cache
references and quality evidence remain intact.

## Configure disjoint storage roots

Set MATRIX_DB_ARRAY_ROOT for hot storage and MATRIX_DB_ARCHIVE_ROOT for the archive.
They must be disjoint directories (neither may contain the other). All API, CLI, worker
and simulation processes reading arrays must use the same roots. Use a separate durable
filesystem for cold storage if the goal is to reclaim capacity on the hot device. Moving
bytes between directories on one device does not free that device's capacity.

Archive storage remains directly readable POSIX storage, not an offline/object-store tier.
Both tiers need backups. An archive copy on the same machine is not a complete disaster
recovery strategy. Keep the paired PostgreSQL-plus-array backup procedure in operations.md;
a backup must now include the configured archive root as well.

## Plan and apply

```
matrix-db db-migrate
matrix-db-lifecycle --policy examples/retention-policy.json
matrix-db-lifecycle --plan demo-weather
```

Policy registration is idempotent for identical content and creates a new revision when
changed. A plan persists its version IDs, manifest checksums, action and protection reason,
and never moves or deletes data. Plans support at most 1,000 versions per dataset; an
over-budget plan fails rather than silently truncating. Eligibility uses publication age,
not source valid time. At least one newest version is always protected. An active hold
prevents archival and hot eviction. `keep_pinned` defaults true and also protects explicit
scenario, simulation output, derived-cache input/output and comparison references.

After reviewing the plan ID, an operator can run:

```
matrix-db-lifecycle --apply PLAN_UUID
```

Apply commits each version separately and revalidates the current policy, checksum,
holds and explicit pins. Changed policy revisions require a new plan. Busy readers cause
a safe failure that can be retried. A partial plan application is resumable; earlier commits
are not rolled back if a later version fails. No background timer applies plans implicitly.

Archival copies immutable version files into a staging directory under the archive tier,
verifies every file against the catalogue manifest, fsyncs files/directories, then atomically
renames the copy and switches the separate routing record. The immutable version/manifest
is not edited. Filesystem publication precedes the database commit: a rollback/crash can
leave a verified extra copy, which a retry can reuse. Each version is limited to 1 GiB and
100,000 files for lifecycle operations; larger versions need an explicit future extension.
Symlinks, special files, extra files, unsafe paths and incomplete/checksum-invalid copies
are rejected. Operators must protect the storage roots from unrelated filesystem writers.

## Read, reclaim hot bytes and restore

Existing array subset, alignment, derivation and simulation read paths follow the routing
record automatically. Version IDs and checksums stay stable. Archived reads still verify
checksums and preserve masks, coverage, quality and provenance. A missing archive root or
corrupt copy fails; the reader does not silently substitute another version or a hot cache.

Archival retains the hot copy initially. Reclamation is a separate explicit command:

```
matrix-db-lifecycle --evict-hot VERSION_UUID
matrix-db-lifecycle --restore VERSION_UUID
```

Eviction rechecks current age/latest/pin/hold protections and independently verifies that
the archive routing record was already committed. It verifies the entire cold copy before
renaming the hot copy into a local eviction directory and removing it. This cannot delete
the sole committed copy. Readers hold shared transaction locks; lifecycle operations take
an exclusive lock, preventing hot eviction during a read. The same protections also apply
when ArrayStore is used on an autocommit connection.

An interrupted removal can leave `.evicted` remnants. The same eviction command verifies
remaining known files against the manifest and finishes removal on retry. `db-array-inventory`
reports these remnants without deleting them. Restore stages/verifies a hot copy and switches
routing back to hot. It never removes the cold copy, so rollback remains safe. If a restore
finishes before interrupted-eviction cleanup, remnants remain listed for operator review.

Library lifecycle/read methods own a transaction when none is active; use an explicit
`connection.transaction()` block when several calls must share a rollback or reader-lock
lifetime. A connection context alone opens a transaction lazily and is not that boundary.

All readers must run the lifecycle-aware code before any archival cutover. Long-lived
read transactions can delay tiering; avoid holding them idle. A lifecycle copy can also
make new readers wait or hit their normal SQL timeout. Prefer maintenance windows for
large versions. Locks coordinate this application's database users, not out-of-band file edits.

## Holds and audit

```
matrix-db-lifecycle --hold VERSION_UUID --reason 'Investigation reference' --actor operator
matrix-db-lifecycle --release-hold VERSION_UUID --reason 'Investigation closed' --actor operator
```

Holds block new archival/eviction, not reads or restoration. Setting a hold on an already
archived version does not automatically restore it. Releasing a hold does not execute an
old plan. Lifecycle events preserve the actor, action and integrity details. Retention
policies, plans, events and revision-export manifests are append-only at the database level.
Operator CLI access uses database privileges, not a browser credential; no destructive
storage operation is exposed through the read-only GeoSentinel gateway.

## Observation revision archives

```
matrix-db-lifecycle --export-revisions demo-weather --before 2026-12-01T00:00:00Z
matrix-db-lifecycle --verify-export ARCHIVE_UUID
```

This writes a checksummed gzip JSON-lines copy containing full records, IDs, revision
numbers, recorded times, withdrawal flags, reasons, actors and import references. Exports
select `recorded_at < before`, never pretend data was known at its source valid time,
and are bounded to 10,000 rows and a 64 MiB record-payload preflight. Export verification
checks the compressed-file checksum, JSON parsing and row count. Filesystem-first publication
can leave an unreferenced export after a database rollback; it must not be treated as a
catalogued archive until its database manifest exists.

PostgreSQL history is not pruned, partition-detached or rewritten: scenario pins and quality
snapshots still resolve against original revisions, and as-of queries keep their behavior.
Revision exports therefore do not reduce PostgreSQL storage. Reimport/database restore from
these exports is not implemented; use the qualified database backup/restore procedure.
No current observation, historical revision, scenario, quality evaluation, cold array or
unreferenced staging directory is automatically deleted. TTL database pruning and object
storage lifecycle integration remain future extensions.

## Qualification

CI checks read-through and restoration, hot eviction, corruption rejection, legal holds,
policy changes, explicit pins, transaction rollback, shared reader exclusion, interrupted
removal recovery, revision export verification and unchanged append-only history. Full
production backup/restore and capacity/load qualification are operator gates. No production
files are archived or removed as part of implementing this feature.

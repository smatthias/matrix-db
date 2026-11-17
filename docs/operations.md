# Release qualification and recovery

This branch is a release candidate, not a deployed release. The baseline uses synthetic
observations on CI hardware; it does not establish production throughput or latency.

## Query baseline

On a migrated disposable database, set MATRIX_DB_DATABASE_URL and run:

```
python scripts/benchmark.py --rows 2000 --repeats 5 > benchmark.json
```

The JSON records ingestion duration, median/max query latency, result counts and actual
PostgreSQL plans with buffers for time paging, viewport and nearest-radius queries.
Rows use a unique dataset and are rolled back even on failure. Run larger baselines
on a dedicated staging database. ANALYZE still takes normal database locks; do not run
this qualification workload on a busy production database. Compare the same dataset
size, hardware, PostGIS/PostgreSQL versions and warm-cache conditions. No arbitrary
shared-runner timing threshold is treated as a release guarantee.

## Worker crash recovery

Only one worker executes jobs at a time, enforced by a dedicated PostgreSQL session
advisory lock. Additional workers skip work while the owner is alive. Every successful
lock acquisition reconciles abandoned running jobs before claiming queued work:
`failed/worker_interrupted`, or `cancelled` if cancellation was requested. Recovery
never retries a write or publishes a result. A healthy but hung owner is not evicted;
use the process supervisor to stop it, then restart `matrix-db-worker`. Database
session cleanup controls when the lock is released after a network partition.

Inspect failed jobs through the authenticated API. Check array inventory for orphaned
files and scenario state before submitting a replacement job. Files renamed before a
transaction rollback can remain unreferenced; inventory is read-only and does not
prove that a directory is safe to delete. Do not manually change terminal jobs back to
queued. Queued work can survive a restart; completed job results remain unchanged.

## Consistent backup and restore drill

1. Stop API ingress, ingestion writers and workers; verify that writes have drained.
2. Take a PostgreSQL custom-format backup with `pg_dump --format=custom`, and a copy
   of MATRIX_DB_ARRAY_ROOT while writers remain stopped. Record package version,
   migration level, PostgreSQL/PostGIS versions and checksums for both artifacts.
3. Restart the original services only after both copies finish. Protect API key
   configuration separately; do not include credentials in published artifacts.
4. Restore into an isolated database and a separate array root. Use `pg_restore`
   against the empty database and preserve the complete directory structure.
5. Run `matrix-db db-ready`, catalogue/query checks, `db-array-inventory` and checksum-
   validating `db-array-read` on representative versions. Check scenario references,
   job recovery and scoped API access against the restored copy.
6. Record restore duration, tested counts and missing/corrupt objects. A backup is
   unqualified until this drill succeeds. Never test a restore over the source DB.

No automatic backup, file deletion, database downgrade or production restore is provided.

## Package gate

CI builds wheel and source distribution, installs the wheel in a clean virtualenv,
checks packaged SQL migrations and imports the API/worker. Keep both artifacts and
benchmark JSON with the successful workflow run. Before tagging: complete the isolated
backup/restore drill, staging load test and full GeoSentinel browser workflow; review
service keys and deployment configuration. These operator gates are not asserted by CI.

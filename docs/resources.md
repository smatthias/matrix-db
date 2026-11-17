# Quotas and resource controls

Resource controls are application-level admission/capacity limits, not operating-system
CPU/RAM/disk isolation, a billing system or a hard physical storage quota. They preserve
existing data and never delete records/messages to make room. Configure identical limits
on APIs, workers, ingestion and operator processes; restart them after configuration changes.

Set MATRIX_DB_RESOURCE_LIMITS to a JSON object (omit fields to keep defaults):

```
{"requests_per_minute":600,"concurrent_http_requests":64,"active_jobs_per_credential":50,"active_jobs_global":500,"observations_per_batch":10000,"observations_per_dataset":1000000,"revisions_per_dataset":5000000,"array_versions_per_dataset":10000,"logical_array_bytes_per_dataset":0,"subscriptions_per_owner":50,"inbox_items_per_owner":10000}
```

Invalid/unknown settings fail startup. requests_per_minute:0 disables request-rate admission;
logical_array_bytes_per_dataset:0 disables only cumulative logical byte enforcement. Other
limits require positive values. Existing API body, query, chunk/subset, graph and lifecycle
budgets remain in force. Limits are service-wide settings, not user-editable preferences or
per-project entitlement policies. No credential gets an implicit administrator exemption.

## Request admission

Authenticated operations entering a database-backed API path consume a PostgreSQL fixed
minute window per credential. The counter commits separately from the operation; a later
SQL/validation/transaction failure does not refund it. Atomic upserts serialize concurrent
servers, retain only the current window per key and never move a window backwards. Header
scheme casing does not bypass admission. This is a fixed-window counter, not a sliding
window/token bucket: bursts can straddle boundaries. Shared gateway keys aggregate users;
notification inbox/subscription ownership remains per delegated owner.

Authentication failures, pre-database validation/authorization failures, OpenAPI/docs and
liveness do not consume that counter. Limits do not protect those paths against network
traffic; normal ingress controls remain separate. One request is charged once even if its
handler opens multiple database contexts. Rate admission has a five-second SQL timeout.

A nonblocking per-app execution-slot limit rejects excess work before buffering its body.
It covers request processing through response creation, not the full lifetime of streaming
socket output. It is per process/app instance, not a distributed concurrency pool. The
GET /health/live endpoint remains available when slots are full and does not query PostgreSQL.
Existing SQL timeouts and bounded read/working-memory checks still apply.

Rate/capacity rejection uses HTTP 429 with a sanitized quota_exceeded envelope naming the
resource/limit and request ID. Transient rate/job pressure includes Retry-After. Persistent
storage/batch/inbox capacity does not imply that waiting alone frees space. Execution-slot
pressure uses HTTP 503/server_busy with Retry-After:1. The Python/TypeScript clients expose
Retry-After but do not automatically replay HTTP capacity errors or mutations. GeoSentinel's
reader/admin/notification gateways preserve status and safe numeric retry headers.

## Jobs, observations and revisions

Job admission takes a global transaction lock, checks queued+running counts globally and
per credential, and inserts in the same transaction. Completion/failure/cancellation frees
active capacity; retained terminal history is not counted. Claiming queued work does not
create extra capacity. The single execution-worker rule remains separate.

Observation insertion holds the dataset update lock through count checks and writes.
Batches above the configured count are rejected before insertion. Counts include withdrawn
current rows because they still occupy storage. Every initial observation consumes one
revision, and corrections/withdrawals check the revision budget under the same dataset lock.
All normal API, provider and import storage paths share these guards. Failed writes roll
back together. These count checks can scan large datasets; locks also serialize writers for
a dataset. The checks are correctness-oriented controls, not a production throughput claim.
Manually writing SQL outside these application paths bypasses these logical guards.

## Array accounting

Every new published version records its actual manifest/chunk file bytes in a separate
resource row. A preflight catches already-exhausted version capacity, and final publication
rechecks count/bytes under the dataset lock before rename/catalogue commit. Rejection cleans
its staging directory and does not expose a version. Published version count includes both
hot and archived versions. Logical bytes count one immutable version payload, not extra hot/
archive copies, staging files, eviction remnants, database metadata or backups. Moving tiers
or evicting a hot cache does not reduce logical bytes. Staging/FS capacity is not reserved by
this quota, so filesystem capacity/OS limits still matter.

Older versions may lack byte accounting. When the optional byte limit is enabled, any
untracked version blocks new publication rather than guessing zero usage. To verify and
populate those resource rows (bounded to 1,000 versions, using lifecycle integrity budgets):

```
matrix-db-resources --dataset demo-weather --reconcile-arrays
matrix-db-resources --dataset demo-weather
```

Reconciliation follows hot/archive routing, verifies all files/checksums under reader locks
and commits accounting together. It never edits version identities or payloads. Both roots
must be configured for archived versions. Integrity/budget failure leaves accounting unchanged.
An existing dataset already above a new limit is retained; only further growth is rejected.

## Subscriptions and inbox backpressure

The subscription cap counts non-deleted preferences per owner (active and paused), with a
hard maximum of 50 for bounded listing. Soft-deleted version history remains retained. The
inbox cap counts all retained deliveries per owner, including read items. Workers serialize
owner-capacity checks, deduplicate overlapping matches and stop before the first event that
cannot fit. Prior events/checkpoints in the batch may commit; the blocked event is not skipped.
The subscription reports inbox_capacity and remains active. Raising the limit allows replay
from that checkpoint without a new subscription. Mark-read does not delete messages or free
capacity. No notification purging/expiration is implemented here.

Provider ingestion reports quota_exceeded and preserves its prior watermark on rejection.
Operational failures use the existing source backoff. Quality evaluation/retention workflows
retain their existing sample/version budgets; request-rate admission does not grant extra
permission or remove their hard bounds.

## Inspection and qualification

GET /v1/admin/datasets/{id}/resources returns configured limits and observed dataset usage,
including untracked array versions and global active jobs. It requires the Matrix DB admin
credential. GeoSentinel's admin page displays this read-only capacity summary; limits are
changed by operator configuration, not a browser write. The operator CLI is also available
when an API key has exhausted its request window. Neither view returns service credentials.

CI tests concurrent request/job/observation admission, revision/batch rejection, array count/
byte caps and reconciliation, unrefunded failed work, case-insensitive bearer admission,
process-slot backpressure/liveness and inbox stop/resume without dropped events. Gateways
and both clients test Retry-After behavior; live HTTP checks verify capacity responses.
Production load/fairness testing, OS hard limits, distributed execution slots, physical disk
accounting and per-project budget allocation remain unimplemented or unverified.

# Administration and explorer

The Matrix DB API exposes bounded operational administration under `/v1/admin/datasets/{id}`.
Every endpoint requires an administrative service credential, including overview and activity
reads. Existing dataset-scoped read APIs remain available to reader/ingestor credentials.
No browser receives a Matrix DB service key.

## Dataset overview and edits

`GET /v1/admin/datasets/{id}` returns the catalogue entry and its checksum, observation
counts, current array-tier counts, sanitized provider status and active quality/retention
policies. It does not return provider configuration, environment credential names, job
request/result bodies, source hashes or raw import error text. Counts can incur a database
scan and share the API's statement timeout; they are exact results, not invented estimates.

`POST .../metadata` accepts expected_checksum, name, license, status, attribution and
contact. It takes a dataset lock and rejects a changed checksum with HTTP 409/stale_edit.
Reload before saving; ingestion freshness changes can also invalidate an edit. Variable
contracts, origin, provider, source identity, coverage and measurement records are not edited
by this form. Successful edits and their prior/current fields are audited atomically.
Existing legacy dataset-update APIs retain their existing authorization and semantics.

## Monitoring and retention

`POST .../quality-policy` and `POST .../retention-policy` accept `{expected_revision, policy}`.
Use null for a policy that does not yet exist. Dataset mismatch, invalid policy and stale
revision checks prevent accidental overwrite; saves retain version history and create an
administration audit record. Policy equality keeps the same revision.

`POST .../retention-plan` generates/audits a non-destructive preview of the active retention
policy. It never applies a plan, archives a version, evicts a hot copy or deletes a file.
Those actions remain operator-only lifecycle commands. Existing quality evaluation and
incident acknowledgement endpoints require admin credentials and retain their quality
incident/event lifecycle. GeoSentinel exposes those two actions only to admin sessions.

`POST .../jobs/{job}/cancel` verifies that the job's dataset or target grid belongs to the
selected dataset, requests cooperative cancellation and records the action. Terminal jobs
reject cancellation. Running work is not claimed to stop before its next checkpoint.

## Activity pagination

`GET .../activity?kind=providers|imports|jobs|storage|audit&limit=50&after=CURSOR` returns
operational summaries in descending timestamp/identity order. Cursors are bounded, typed
and bound to dataset/kind. They paginate existing rows, not a snapshot-isolated subscription;
refresh from the beginning for newly inserted rows. Pages cap at 100. Jobs are grouped by
their observation dataset or target grid, not every input dataset. Storage events include
array lifecycle and revision-export events. Import summaries omit raw error/rejection text.
Administrative audit actors are authenticated Matrix DB service subjects; a shared gateway
credential does not provide per-human attribution in Matrix DB's audit records.

## GeoSentinel

The reader explorer supports text search (ID/name/provider/variable), origin/status filters,
50-item catalogue pages, observation maps/playback, quality health and local notifications.
Array versions show publication time and hot/archive tier; users can inspect manifest axes,
choose a variable and request explicit index slices. The sample view displays persisted
values, masks, units, coverage and quality as JSON, not raster tiles. Initial slices use at
most two cells per selected dimension; server subset size/access limits still apply.

The separate `/admin/matrix-db` page requires the app's admin role. Backend admin routes
also require an authenticated admin JWT session; read-only app API keys are rejected.
Set a separate server-side MATRIX_DB_ADMIN_TOKEN with Matrix DB admin privileges; there is
no fallback to MATRIX_DB_TOKEN. Only fixed routes are proxied. Missing configuration fails
closed. MATRIX_DB_TOKEN remains the scoped reader used by the explorer/health panels.
Admins can choose only catalogue entries returned by that reader on the page; the admin
service credential itself authorizes all datasets. This is not per-project entitlement isolation.

The page supports metadata edits, policy JSON editing, retention preview, quality evaluation,
incident acknowledgement, job cancellation and operational activity paging. A stale edit
requires reload. JSON fields are rendered as escaped text; raw upstream errors and service
credentials are never displayed. No mutation is automatically retried after a transport error;
reload status before deciding whether to repeat an uncertain action.

CI verifies authorization, sanitized views, cursor binding, conflicts, audit rollback and
record/contract preservation. GeoSentinel checks role gating/fixed routes, component
compilation and actual HTTP reads/admin actions against the pinned Matrix DB service.
Full GeoSentinel application/browser E2E and deployment remain unverified.

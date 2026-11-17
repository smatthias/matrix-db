# Data quality and operational health

Quality assessment is separate from source measurements. It never overwrites upstream
flags, values or history. Versioned policies produce immutable evaluations with bounded
sample evidence referencing observation IDs, revisions and recorded-at times. A policy
change creates a new revision; identical registration is idempotent. Superseded incidents
close with a `policy_superseded` event, not a claim that the data recovered.

## Rules

All rules select one variable. The policy window is inclusive, ends at evaluation time,
and is at most 30 days. Withdrawn and future-valid records are excluded.

| Kind | Parameters | Meaning |
| --- | --- | --- |
| range | minimum and/or maximum | Flag nonmissing values outside inclusive bounds, including upstream bad values |
| rate | max_rate_per_second | Absolute change per second between adjacent usable samples at the exact same location, vertical coordinate and forecast issue; conflicting values at equal times fail |
| completeness | expected_count, min_fraction | Usable distinct time/location/vertical/issue keys divided by expected count; fraction capped at 1 |
| freshness | max_age_seconds | Age of latest usable nonfuture sample, including samples before the policy window; no sample fails |
| coverage | points, min_fraction | Fraction of expected exact coordinate pairs with usable data in the window; not polygon/raster coverage |
| consistency | other_variable, tolerance | Each usable left value must be at most right value plus tolerance at the identical time/location/vertical/issue; missing or ambiguous right counterparts fail |

Usable means nonmissing and upstream quality other than bad. Suspect/unknown flags are
preserved; passing a rule does not certify them as good. Consistency requires the same
unit string; it does not implicitly convert units. Rate units are the variable's unit
per second. Coverage is horizontal; expected stations at different vertical levels are
not distinguished. Completeness expectations are explicit counts, not an inferred regular
sampling cadence. Model estimates and observation sources require suitable separate policies.

Range/rate/consistency with no checkable input return unknown. Unknown does not resolve
an existing incident. Completeness/coverage/freshness can fail for absence of data.
`failed_count` counts failing sample records; aggregate-rule failures instead explain their
result through metric and checked fields. Sample evidence is capped at 20 per finding;
the persisted evaluation snapshot pins all inspected window samples and freshness inputs.
No array-grid scanning or cross-dataset consistency is provided in this milestone.

The evaluator uses a repeatable-read transaction and locks the dataset through policy,
incident and notification updates. A window above 10,000 records fails atomically; it is
never silently sampled or treated as passing. Shorten the lookback or partition datasets.
Database operations have a 30-second statement timeout. Concurrent serialization failures
can require an operator/API retry; evaluations are not automatically retried as mutations.

## Run a monitor

Create/register the target dataset first, then edit `examples/quality-policy.json`:

```
matrix-db-quality --policy examples/quality-policy.json --once
matrix-db-quality --dataset demo-weather --interval 300
```

The monitor evaluates the persisted active policy periodically until stopped. Keep its
interval shorter than the policy evaluation expiry. Invalid policy/budget failures exit
nonzero. Supervise/restart the operator process on failures. It is separate from provider
ingestion so stale data can still be detected while a provider is down.

## API and incident lifecycle

Administrative service credentials can register policies (`POST /v1/quality/policies`),
evaluate (`POST /v1/quality/{dataset}/evaluate`) and acknowledge incidents
(`POST /v1/quality/{dataset}/incidents/{id}/acknowledge`). The acknowledgement actor comes
from the authenticated principal. Repeated acknowledgements are idempotent. Resolved
incidents cannot be acknowledged again.

Dataset-scoped readers can read `GET /v1/quality/{dataset}/health` and the durable
notification feed `GET /v1/quality/{dataset}/events?after=0&limit=100`. Notifications
are local, persisted incident transition events: opened, acknowledged, resolved and
policy_superseded. Pages return a next cursor when more events remain; a consumer should
retain the last sequence it read for subsequent polling. Sequence gaps are normal.
No email, Slack or webhook is sent; external delivery/subscriptions remain later work.

An ongoing failure reuses its open/acknowledged incident and updates the linked evaluation
without spamming transition events. A subsequent pass resolves it. A later failure opens
a new incident, preserving the earlier lifecycle. Acknowledgement does not mean recovery.

Health is unknown without an active/current evaluation or when a rule has insufficient
input. Open or acknowledged incidents, or persisted provider failures, make it degraded.
Healthy requires current, fully passing findings and no active incidents/provider failures.
Evaluation expiry never silently resolves incidents. Provider last-poll and latest-data
timestamps are separate; a successful empty poll does not prove the data is current.
A raw database/provider outage appears as request failure, not fabricated healthy status.

## GeoSentinel integration

The authenticated read-only gateway exposes health and notification reads for the selected
dataset. The explorer shows evaluation age, rule outcomes, incident status, provider poll
and data times, plus local notifications. It refreshes every minute and supports manual
refresh/paging. Failures display unavailable; missing/expired evaluations display unknown.
Browser credentials never receive Matrix DB service keys. The gateway shares its reader
allowlist across authenticated users; it does not grant per-user administrative actions.

CI covers rule semantics, incident lifecycle/deduplication, acknowledgement, policy
supersession, revision evidence, rollback, stale evaluations and scope enforcement. The
GeoSentinel workflow compiles the Vue component and checks real HTTP health/notification
reads. Full application/browser E2E and deployment are still unverified.

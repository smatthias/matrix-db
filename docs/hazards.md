# Configured hazard and event detection

This milestone turns persisted observations into configurable analytical signals with
versioned policies, revision-linked evidence, lifecycle history and a separate hazard
notification topic. The included thresholds/data are synthetic examples, not validated
hazard forecasts, official warnings or recommended operational thresholds. Scientific
calibration, physical hazard models, native raster detection and exposure/compound-event
analysis are not supplied by this generic threshold engine.

## Rule semantics

A policy selects an observation-origin dataset and a rolling lookback (up to 30 days), with
at most 20 rules. Each rule declares its variable/unit, hazard label (wildfire, seismic,
flood, heat, air_quality or custom), optional WGS84 region, maximum/minimum/mean aggregation,
above/below trigger and release thresholds, optional critical threshold, minimum sample
count, minimum qualifying-sample time span and maximum input age. Units must match exactly;
there is no implicit conversion. Forecast/simulation datasets are rejected.

One active event is tracked per rule and policy revision for the configured dataset/region.
This is regional rolling-window aggregation, not spatial fire clustering, event identity
matching, perimeter extraction or a claim that several detections form one physical hazard.
Samples may cover multiple locations. Minimum samples counts distinct persisted record IDs;
minimum_duration_seconds is the span between qualifying records, not proof of continuous
threshold exceedance. Bad/missing values, withdrawn records and future valid times are excluded.
Suspect/unknown source flags are retained in their original revisions rather than certified.

Above rules require release < trigger <= critical (if configured); below rules reverse the
ordering. Enough fresh input crossing the trigger and persistence criteria detects a signal.
A fresh aggregate crossing the release threshold resolves it. Between thresholds it holds an
existing signal without opening a new one. Insufficient/stale input is unknown and does not
resolve an event. Low/corrected current observations can resolve a signal, while withdrawal
or disappearance of all supporting input leaves it unknown pending review.

The representative point is an observed extremum, not a centroid/epicenter/perimeter estimate.
The bbox encloses available usable samples, including longitude wrap around the antimeridian;
it is an evidence extent, not a physical hazard boundary. UI/API fields distinguish last
valid data, last assessment and lifecycle status. A retained metric/location on unknown input
is last-known evidence, not a fresh measurement.

## Versioning and event lifecycle

Policies are immutable numbered revisions with optimistic expected_revision checks. Identical
registration is idempotent. A changed policy marks active prior-version events superseded,
with an explicit transition rather than pretending the hazard cleared. Assessments preserve
policy, evaluated/window times, findings and foreign-key pins to every selected observation
revision. Source values/upstream flags are never rewritten. API findings show up to 20
supporting references; full selected revisions remain pinned internally.

Detected events open once, update when evidence/state changes and retain their identity while
active. Severity changes produce an explicit transition. Repeat assessments with unchanged
signals do not spam transition notifications. Resolving then detecting again opens a new ID.
Acknowledgement records review but does not resolve the signal. Administrator retraction
requires a reason and expected event revision. Identical supporting evidence will not reopen
a retracted signal; changed source revisions/record identities or a new policy can generate a
new signal. Retraction is not a permanent mute of a geographic region.

Live evaluations reject backdated evaluation times. They use a repeatable-read transaction,
a dataset lock and at most 2,000 window records. An over-budget/failed assessment rolls back
its evidence, events and feed together. Concurrent snapshot conflicts may require an explicit
operator/API retry. No mutation is automatically replayed by the client. Evaluations/history
remain retained; no physical database size budget or automatic pruning is added here.

## API, operator process and review

Dataset-scoped readers can use:

- GET /v1/hazards/{dataset}?status=all|active|resolved|retracted|superseded&limit=50&after=UUID
- GET /v1/hazards/{dataset}/policy (includes the latest assessment)
- GET /v1/hazards/{dataset}/events/{event} (current event, assessment evidence and up to 100 recent transitions)

Event pages are UUID-ordered bounded live lists, not a snapshot or newest-first stream.
Administrative endpoints use /v1/admin/datasets/{dataset}/hazard-policy and hazard-evaluate.
Policy changes accept {expected_revision, policy}; acknowledgement/retraction use
hazard-events/{event}/acknowledge|retract with {expected_revision, reason}. Actors come from
service credentials. Shared GeoSentinel admin keys retain the service-identity audit limitation.

```
matrix-db-detect --policy examples/hazard-policy.json --once
matrix-db-detect --dataset demo-weather --interval 300
```

A supervised detection process evaluates independently of source ingestion. Source freshness
and quality monitors remain separate. CLI arguments do not authorize external notifications
or publish any warning. Existing API rate/execution controls apply to HTTP assessments.

## Hazard subscriptions

Subscriptions now accept topic:quality (default, preserving existing behavior) or topic:hazard.
Hazard kinds are opened, updated, severity_changed, resolved, acknowledged, retracted and
policy_superseded. Quality subscriptions reject hazard-only kinds. Topic changes reset the
checkpoint according to start:now|beginning. Deduplication is owner+topic+event sequence, so
sequences from different feeds never collide. Both topics share retained inbox capacity and
keep blocked checkpoints under backpressure. Hazard severity is captured at transition time;
a later escalation cannot relabel earlier warning notifications as critical.

Run matrix-db-notify to deliver these transitions into the existing private in-app inboxes.
No email/webhook/SMS/push is sent. Hazard and quality messages have distinct labels and event
identifiers. The GeoSentinel signal explorer shows the evidence map, current assessment,
status-filtered event pages, revision-linked findings and lifecycle history. The admin page
edits policies, evaluates rules and acknowledges/retracts active signals with reasons.

## Qualification

CI uses synthetic fixtures to cover thresholds, sample-span persistence, hysteresis, stale/
missing/bad/withdrawn data, unit checks, antimeridian evidence bounds, deduplication, escalation,
resolution/reopening, correction pins, policy supersession/conflicts, retraction suppression,
atomic rollback, dataset scope and notification topic/severity behavior. GeoSentinel compiles
four Vue components and checks fixed routes plus real HTTP workflows. Full app/browser E2E,
production load, scientific model validation and deployment remain unverified.

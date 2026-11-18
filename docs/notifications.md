# Subscriptions and in-app notifications

This milestone delivers persisted quality-incident transitions into private in-app inboxes.
It does not send email, Slack messages, SMS, webhooks, browser push or external traffic.
The existing dataset-wide quality event feed remains available separately.

## Ownership and authorization

Normal service credentials own their subscriptions/inbox by credential hash. Each request
checks current dataset read scope; removing a dataset hides its existing messages and
subscriptions and prevents read acknowledgement. Other credentials cannot inspect, modify
or acknowledge those resources, even when they share dataset access. Admin credentials do
not bypass ownership. Rotating a token creates a new owner namespace; ownership migration
is not supplied automatically.

GeoSentinel uses a separate server-side MATRIX_DB_NOTIFICATION_TOKEN whose Matrix DB
configuration has role reader, the intended dataset allowlist and notification_gateway:true.
That key is trusted to delegate notification ownership. The backend derives the header
X-Matrix-Notification-Owner from the authenticated user's numeric ID as gs-user:ID and never
accepts the owner from browser input. Both the key hash and delegated ID form the namespace.
Only JWT user sessions may use these routes. Read-only app API keys are rejected. There is
no fallback to the ordinary reader or admin service key. Use distinct notification keys per
deployment. Direct clients with an ordinary credential cannot send an owner header; gateway
credentials must send a valid one. A compromised gateway key could impersonate users in
that key's namespace, so keep it server-side. Full OAuth/project permission isolation remains
outside this milestone; dataset access is still the shared gateway allowlist.

Configuration example (placeholder, not a usable credential):

```
{"token":"REPLACE_WITH_RANDOM_32_BYTE_TOKEN","subject":"geosentinel-notifications","role":"reader","datasets":["demo-weather"],"notification_gateway":true}
```

## Subscription API

- GET /v1/subscriptions lists the caller's accessible non-deleted subscriptions (maximum 50).
- POST /v1/subscriptions creates name, dataset_id, kinds, minimum_severity, rule_ids, active
  and start. Kinds are opened, acknowledged, resolved and policy_superseded; severity is
  warning or critical. Empty rule_ids means all rules. Dataset identity is immutable.
- PUT /v1/subscriptions/{id} accepts expected_revision plus subscription. A stale revision
  returns HTTP 409. Updates are recorded as append-only versions.
- DELETE /v1/subscriptions/{id}?expected_revision=N soft-deletes the subscription, retaining
  its history and already-delivered messages.

The default start:now excludes already committed incident history. Explicit start:beginning
replays retained history. Creation/update takes the dataset lock used by incident writers,
so the checkpoint boundary is consistent with committed quality events. Updates and resume
also start from now unless replay is explicitly selected. Pausing does not produce messages;
resume-from-now skips transitions during the pause. Already delivered events remain deduplicated
and read state is never reset by replay. Unsubscribe does not erase prior inbox items.

## Durable fanout and retries

Run the separate supervised process after configuring the database, service keys and array root:

```
matrix-db db-migrate
matrix-db-notify
matrix-db-notify --once --batch-size 100
```

Each transaction locks one active subscription with SKIP LOCKED, checks its credential and
scope, processes up to a bounded event batch, inserts inbox/match rows and advances the
checkpoint atomically. Overlapping subscriptions for one owner produce one inbox item per
event, with separate retained match references. A crash rolls back both delivery and checkpoint;
a retry does not duplicate or lose delivered events. Filters that do not match still advance
the checkpoint. Revoked credentials/scope pause a subscription with authorization_revoked.
An operator/user must explicitly update or recreate it when authorization is restored.

The long-running worker retries database failures with bounded exponential delays (five retries,
then nonzero exit for supervisor intervention). One-shot failures exit nonzero immediately;
rerun after recovery. SQL statements have a 15-second timeout. Batch size is 1–1000; default
100. Fair rotation includes idle subscriptions, so latency depends on active subscription
count and worker availability. --once polls one subscription, not a full queue drain. This
is database-to-inbox delivery, not an exactly-once external transport guarantee.

Quality events are ordered by sequence within a dataset because normal incident writers
serialize on its dataset lock. Out-of-band SQL inserts must not bypass that protocol. Source
provider/data changes do not themselves generate subscriptions events: use quality rules to
turn freshness/completeness/other failures into incident transitions.

## Inbox and GeoSentinel

GET /v1/notifications?limit=50&before=ID&unread=true reads newest-first owned messages with
bounded cursor pagination. POST /v1/notifications/{id}/read idempotently marks one owned,
currently authorized message as read. Read state is separate from incident acknowledgement;
reading a notification does not acknowledge or resolve the incident. Payloads contain dataset,
rule, severity, transition/time and incident/evaluation IDs, not provider configuration,
credentials, actor identities or raw source records.

The /matrix-db/notifications page provides dataset search, subscription filters, edit/pause/
resume/unsubscribe, explicit historical replay, inbox paging, unread filtering and mark-read.
It polls every 30 seconds while mounted, aborts obsolete requests and displays failures
rather than fabricating an empty successful inbox. Mutation transport errors are not
replayed automatically; refresh current state before repeating an uncertain action.

CI covers filtering, credential/delegated-user isolation, scope revocation, cursor/read state,
replay/pause/update/delete, overlap deduplication and rollback after delivery insertion before
checkpoint commit. Gateway tests enforce server-derived identity and fixed routes; live HTTP
checks exercise two delegated users and worker delivery. Full GeoSentinel browser E2E,
external transports and deployment are not implemented/verified by these checks.

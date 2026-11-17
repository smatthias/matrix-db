# Provider ingestion

`matrix-db-ingest` is an operator-only process. It uses the database connection role,
not browser/service API credentials. It never fetches provider data in response to an
end-user URL. All HTTP endpoints, request fields and query templates are fixed.

## Adapters and semantics

| Provider | Stored data | Credential / limitation |
| --- | --- | --- |
| USGS FDSN GeoJSON | Event magnitude at event time/location, stable event identity, corrections and explicit deleted-event withdrawals | No key; magnitude scales are provider scales, not converted energy or a universal scale; depth is not imported |
| NASA FIRMS VIIRS SNPP NRT CSV | Fire radiative power in MW at UTC detection time/location | Environment MAP_KEY; low-confidence detections marked suspect; no fire perimeter or spread inference |
| Open-Meteo archive | Hourly reanalysis temperature, relative humidity and wind at returned grid coordinates | No key on public endpoint; archive has a conservative seven-day availability lag; this adapter does not produce live forecasts or invent issue times |
| Sentinel Hub Statistics / Sentinel-2 L2A | Cloud/no-data-masked daily area-mean NDVI, sampled on a 256×256 grid, represented at AOI bbox center | Environment bearer token and account access required; area statistic is not a point measurement or native-resolution raster; interval end retained in source reference; all-missing values flagged bad |
| OSM Overpass | Building-presence snapshots, node coordinates or enclosing-bbox centers for ways/relations | No key; ODbL attribution required; value 1 means one returned OSM element, not residents, occupancy or unique physical buildings; elements can overlap |

Official protocol references:
[USGS](https://earthquake.usgs.gov/fdsnws/event/1/),
[FIRMS](https://firms.modaps.eosdis.nasa.gov/content/academy/data_api/firms_api_use.html),
[Open-Meteo archive](https://open-meteo.com/en/docs/historical-weather-api),
[Sentinel Statistics](https://docs.sentinel-hub.com/api/latest/api/statistical/),
[Overpass JSON](https://dev.overpass-api.de/output_formats.html) and
[Overpass centers](https://dev.overpass-api.de/overpass-doc/en/targets/formats.html).

The example configuration declares licensing/attribution; operators must verify their
source/account terms and commercial service access before enabling a source. No real
provider credentials are included, and CI uses protocol fixtures, not live accounts.

## Configure and run

Edit `examples/providers.json`: choose the sources, coverage, start times and dedicated
dataset IDs. Set MATRIX_DB_DATABASE_URL. For credentialed sources set
MATRIX_DB_PROVIDER_FIRMS_KEY or MATRIX_DB_PROVIDER_SENTINEL_TOKEN externally, then run:

```
matrix-db db-migrate
matrix-db-ingest examples/providers.json --once --source usgs-berlin
matrix-db-ingest examples/providers.json --status --source usgs-berlin
matrix-db-ingest examples/providers.json
```

The last command polls due sources every ten seconds until stopped. Each source has a
minimum 60-second schedule. Configuration is validated before polling; invalid or failed
one-shot runs exit nonzero. Source IDs and dedicated dataset IDs bind an immutable config
hash: changes require a new source/dataset identity. Retiring sources means removing
them from the config; persisted history is retained. Schedule edits are not silently
applied to an existing watermark. Start dates in the example are illustrative, not a
promise that public endpoints still retain those dates. FIRMS NRT is a recent-data
service; historical archives require a separate adapter.

No credentials are persisted in state, run reports or observations; only environment
variable names are stored. FIRMS keys necessarily appear in outgoing provider URL paths,
so configure infrastructure/proxy logs accordingly. The CLI uses sanitized error codes;
it never prints upstream exception text or a URL. Sentinel bearer-token renewal is an
operator responsibility. Credentials are loaded afresh on each poll.

## Checkpoints, overlap and backfill

Successful polls commit data, revisions, catalogue freshness, run counts and watermark
in one database transaction. A failed fetch/schema check/write leaves the prior checkpoint
unchanged. Duplicate records ignore ingestion timestamp changes. Corrections append
history; only explicit upstream deletions withdraw observations. Missing results do not
imply deletion, including in current Overpass snapshots. Older exposure snapshots remain.

Each forward window overlaps the last checkpoint to collect late arrivals/corrections.
Overlap must be smaller than the window. Vegetation windows/overlap use complete UTC days.
Late corrections older than the overlap need explicit backfill; full historical correction
tracking is not claimed. A source that is catching up still respects its configured
poll interval; operators can adjust a new source's starting window rather than overload
a public service. Sources do not share a global provider quota, so coordinate schedules
across source configurations and deployments.

For a bounded historical refresh (at most the configured window size):

```
matrix-db-ingest examples/providers.json --once --source usgs-berlin \
  --from 2026-11-14T00:00:00Z --to 2026-11-16T00:00:00Z
```

Backfill updates observations/history/freshness but never advances the forward watermark.
Split larger backfills explicitly. Overpass is current-snapshot only and rejects backfill.
The weather adapter rejects windows newer than its conservative archive cutoff. Antimeridian
provider bounds are rejected: use separate non-crossing sources with separate datasets.
No source is allowed to take ownership of a manually populated dataset.

## Failure and operational status

Each source holds its own PostgreSQL session lock across fetch/publication. Concurrent
pollers skip the live owner. The next owner marks interrupted runs failed before starting
again; their uncommitted writes and checkpoint were rolled back. Unchanged IDs make replay
safe. A completion is conditional on the run still being active, preventing an interrupted
poll from publishing after recovery. A hung live session is not evicted automatically.

Readers cap decoded responses at 8 MiB and a batch at 10,000 records. USGS responses at
the configured event cap, partial Sentinel responses, Overpass runtime remarks, duplicate
identities, invalid units and incompatible schema fail the whole poll. There is no silent
truncation or fabricated fallback. Split dense areas/windows if limits are reached.

HTTP reads have a 30-second inactivity timeout, no redirects, at most three attempts,
and bounded exponential delay. Retry-After up to 30 seconds is honored; longer rate-limit
responses defer the poll. Persisted failures use exponential backoff (at least the source
interval). Authorization/schema failures also back off; operators must correct credentials
or configuration. There is no permanent automatic disable or unbounded in-request retry.

Status reports distinguish last successful poll, latest nonwithdrawn valid time,
forward watermark, next attempt and sanitized last error. Last success alone does not
prove the data is current: empty responses can be successful. Quality alerts and UI health
indicators belong to Step 14. Provider downtime, quota exhaustion and unavailable archive
ranges never generate synthetic observations.

# Matrix DB

Spatial and temporal environmental data infrastructure for GeoSentinel, retaining the original Matrix DB name.

## Step 1: implemented scope

Validated dataset, variable, observation, vertical-coordinate and JSON bundle contracts; a synthetic weather example; a validation/inspection CLI; and contract tests. Python 3.11 or later is required.

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -e .
matrix-db validate examples/weather.json
matrix-db inspect examples/weather.json
python -m unittest discover -s tests -v
```

The example contains synthetic temperature, humidity and wind observations, not live weather. It requires no credentials, database, or environment configuration.

## Contract conventions

- Schema version 1 rejects unknown fields. IDs are nonempty stable strings.
- Locations and coverage use EPSG:4326, longitude then latitude, in degrees. West greater than east denotes antimeridian-crossing coverage. Coverage edges are inclusive.
- Timestamps require an explicit timezone and normalize to UTC. Valid time and ingestion time are independent. Forecasts also require an issue time no later than valid time.
- Numeric values reject strings, booleans, NaN and infinities. Missing measurements use JSON null, permitted only for nullable variables. Integer variables require integer values.
- Units must match the variable exactly; automatic conversion is outside Step 1.
- Vertical coordinates require metres, positive direction and an explicit reference. No vertical datum transformation is performed.
- Bundles validate dataset/variable references, location coverage, ranges, unit consistency and unique observation IDs. Cross-bundle identity and persistence are future work.
- Quality flags describe supplied quality information; validation does not establish scientific accuracy.

`src/matrix_db/contracts.py` contains contracts; `cli.py` exposes validation and inspection; `examples/` holds fixtures and `tests/` verifies behaviour. Validation errors include field paths; cross-record failures identify the observation index and ID. Invalid input exits with status 1.

## Step 2: catalogue registration and discovery

`Catalogue` provides in-memory registration, retrieval, replacement updates, and combined filtering. Duplicate dataset IDs and duplicate `(provider, source_id)` registrations are rejected; updates preserve dataset identity. Returned entries are defensive copies. Entries support attribution, contact, source URLs, update intervals, lifecycle status, optional vertical coverage, and independently declared/ingested time coverage and last successful ingestion time.

```sh
matrix-db catalogue examples/catalogue.json --variable humidity --status active
matrix-db catalogue examples/catalogue.json --bbox 170 -10 -170 10
matrix-db catalogue examples/catalogue.json --start 2026-11-17T08:00:00Z --end 2026-11-17T09:00:00Z --availability ingested
```

The catalogue fixture registers three synthetic sources. Filters combine with AND; string matching is exact; region and time overlap boundaries are inclusive. Unknown time coverage does not satisfy a time query. All statuses are included unless filtered. Ingested time coverage is a reported envelope, not proof that every sample inside it exists. Last successful ingestion is supplied independently, never inferred from declared coverage.

Programmatic registration and updates use `CatalogueEntry`, `Catalogue.register(entry)`, and `Catalogue.update(dataset_id, replacement)`. The CLI loads a catalogue fixture for discovery; writes are currently in-memory only and do not survive process exit.

## Next milestone

Step 3 adds the PostgreSQL backend described below. Live ingestion, APIs, array storage, authentication and GeoSentinel integration remain planned. The broader roadmap described in the planning conversation is not a claim of available functionality.

## Step 3: PostgreSQL/PostGIS storage

Copy `.env.example` to `.env`, choose a local password and set the matching connection URL. Compose reads `.env`; the CLI requires the URL exported into its environment. Use URL encoding for password characters in the connection URL.

```sh
cp .env.example .env
# Edit .env before starting the local database.
docker compose up -d --wait
export MATRIX_DB_DATABASE_URL='postgresql://matrix:YOUR_LOCAL_PASSWORD@127.0.0.1:5432/matrix'
matrix-db db-migrate
matrix-db db-ready
matrix-db db-seed examples/weather.json
matrix-db db-inspect demo-weather
docker compose restart db
matrix-db db-inspect demo-weather
```

The named Compose volume persists across restarts. `db-seed` registers and writes its bundle in one transaction; repeating a seed fails on duplicate identity without partially writing. Connections require an explicit URL and time out after five seconds. CLI database failures suppress connection details to avoid printing credentials. Use a dedicated development database; the Compose role owns its database and installs PostGIS during migration. Production role separation is future work.

Two append-only, checksummed migrations create the sources/datasets/variables/observations tables, foreign keys, dataset/source uniqueness, generated PostGIS points, and dataset-variable-time/time/GiST indexes. Migration execution is transactionally locked. Re-running migrations is safe; changed migration checksums, non-contiguous histories and unsupported downgrades are rejected. Back up before upgrading; failed transactional migrations roll back. No destructive downgrade command is provided.

`PostgresStore` supports catalogue registration, lookup, updates and discovery, plus atomic observation insertion and ordered retrieval. Observation identity is `(dataset_id, id)`; collisions are rejected. Source identity is `(provider, source_id)`. Measurement contracts cannot change after observations exist; lifecycle and descriptive metadata remain editable. Caller-owned transactions can combine operations atomically. Current catalogue search loads metadata into memory and observation retrieval is intended for the example, not large unbounded production requests.

Database integration tests require **a dedicated disposable test database**: they drop the `matrix_db` schema. Never point `MATRIX_DB_TEST_DATABASE_URL` at production or a database containing retained data.

```sh
MATRIX_DB_TEST_DATABASE_URL='postgresql://matrix:YOUR_LOCAL_PASSWORD@127.0.0.1:5432/matrix_test' \
  python -m unittest discover -s tests -v
```

Create `matrix_test` separately before running. Without that variable the seven database integration tests are explicitly skipped; the PostGIS CI job sets it and runs them, followed by migrate/readiness/seed/inspect smoke commands. Local server restart verification requires Docker; reconnect persistence is covered in integration tests. Step 4 is ingestion jobs, duplicate policies and rejection reporting.

## Step 4: batched ingestion

`db-import` streams CSV, top-level JSON arrays, JSON Lines, and gzip-compressed variants. A registered dataset is required. JSON records follow the observation contract; CSV uses matching headers, numeric `value` conversion according to the variable, and an empty value for missing data. Optional `vertical` CSV cells contain JSON objects. `--mapping mapping.json` accepts a JSON object mapping destination contract fields to source column/property names; omitted fields use contract defaults or fail required-field validation. Units must already match; no implicit unit conversion occurs.

```sh
matrix-db db-migrate
matrix-db db-import examples/weather-records.json --dataset demo-weather --format json --batch-size 500
matrix-db db-job JOB_ID
matrix-db db-import examples/weather-records.json --dataset demo-weather --format json --job-id JOB_ID
```

Each committed batch updates its job checkpoint and counts atomically. Identical observation identities and contents count as duplicates; conflicting contents are rejected rather than overwriting historical data. Invalid records produce numbered rejection reasons. Stored observations reference their first import job. Completed jobs update actual ingested time coverage and last successful ingestion metadata.

A failed job retains committed batches and reports its ID. Resume requires the same file bytes, mapping, format, dataset ID and definition; it rereads the source and skips committed record numbers. This trades extra I/O for bounded-memory replay. Dataset changes during ingestion stop the job. Sources should be immutable snapshots: checksums detect changed files but cannot undo batches already committed before a modification is detected. A malformed JSON-array stream fails the job; malformed JSON Lines records can be rejected individually. The CLI returns success for a completed job even if some records were rejected: inspect the counters and rejection report. Reports include at most 1,000 rejection reasons and identify truncation; full reasons remain in the database.

Batch size is limited to 1–10,000 records. Memory scales with batch size and the largest record, not the whole file; no per-record size limit is enforced yet. Ingestion is synchronous, without a scheduler or worker leasing. Historical corrections, revision policies and background scheduling remain later work.

## Step 5: spatial observation queries

```sh
matrix-db db-migrate
matrix-db db-query examples/spatial-query.json
```

The query file accepts `dataset_id`, `variable_id`, `bbox`, a GeoJSON `polygon`, `distance`, `time`, `vertical`, `order`, `limit` and `cursor`. All supplied filters combine with AND. `PostgresStore.query_observations(ObservationQuery(...))` exposes the same interface programmatically. Unknown datasets or unmatched filters return an empty page.

- Bounding boxes use longitude/latitude degrees and inclusive boundaries. West greater than east crosses the antimeridian; the query splits that region into two indexed envelopes. Numeric longitude signs are preserved at ±180.
- Polygons and MultiPolygons use WGS84 longitude/latitude positions and include points on exterior or hole boundaries. Holes exclude their interiors. Closed rings, bounds and structure are checked before SQL; PostGIS rejects invalid topology. Regions use planar edges in EPSG:4326. Split crossing polygons into a MultiPolygon at the antimeridian rather than using edges spanning more than 180 degrees. Input is capped at 10,000 vertices; empty polygons and extra CRS properties are rejected.
- `distance` requires `longitude`, `latitude` and `radius_metres` (0–20,040,000). Radius filtering and reported distances use WGS84 spheroidal geography calculations in metres and a matching geography GiST index. `order: "nearest"` requires a radius, so nearest searches always have an explicit maximum range. Results are observations, not one result per sensor or dataset.
- `time` has timezone-aware `start`/`end`, both inclusive. `vertical` has `minimum`, `maximum`, `reference`, `positive` and optional `unit: "m"`; missing vertical data and different references/directions do not match. No implicit vertical transformation is performed.
- Pages default to 100 observations, with a strict maximum of 1,000. Each item contains its observation and distance (null without a distance filter). Ordering is `(valid_time, dataset_id, id)`, preceded by distance for nearest searches. The opaque `next_cursor` resumes after the last result and is bound to filters/order; page size may change. Cursors are validated and parameterized, not an authorization mechanism.
- Pagination is consistent for unchanged data. Concurrent inserts before a cursor are not revisited; a frozen cross-page snapshot requires later historical-version work. Query execution time and total sort work are not yet budgeted; the result limit bounds returned records, not all database work.

Migration 4 adds the geography expression index alongside the existing geometry and time indexes. Integration tests cover boundary points, holes, invalid geometry, antimeridian boxes/MultiPolygons, metre distances, zero-radius queries, combined filters, vertical references, tied pagination and index eligibility. Small fixtures disable sequential scans for index-eligibility checks; this does not claim a production performance benchmark. Step 6 adds full temporal querying and historical revisions.

## Step 6: historical revisions, forecasts and time series

Migration 5 adds an append-only revision ledger populated by database triggers for observation inserts and updates. Initial records, corrections, restorations and withdrawals retain full values, source references, import links, reason, actor, revision number and a server recording timestamp. Existing records are backfilled at migration time; earlier knowledge is not inferred from client-supplied ingestion timestamps. The ledger rejects UPDATE/DELETE operations; database owners can still alter schema or disable triggers, so this is not a tamper-proof audit system.

```sh
matrix-db db-migrate
matrix-db db-series examples/time-series.json
matrix-db db-playback examples/playback.json
matrix-db db-revise examples/correction.json
matrix-db db-history examples/history.json
```

The correction example expects revision 1 and changes one synthetic value to 20. Repeating it fails with a revision conflict. `db-withdraw` accepts a request containing `dataset_id`, `observation_id`, `expected_revision`, `reason` and `actor`. `db-revise` uses an `observation` object instead of those identity fields. Both commands require a positive expected revision plus nonblank reason/actor; correction can explicitly restore a withdrawn record. The actor is supplied provenance, not an authenticated identity. Reads expose revision numbers and server recording times. History is newest-first, capped at 1,000 revisions per call, with `before_revision` for further pages.

Add `as_of` to an observation query to select the latest revision with `recorded_at <= as_of`, then apply spatial, variable and valid-time filters. Withdrawals disappear from current results while earlier as-of views retain them. Recording time is assigned inside the write transaction, **not at commit**: as-of semantics describe ledger recording times once transactions are committed, not exact wall-clock visibility during long-running transactions. Transaction rollbacks remove both the current change and its new revision. Raw query time ranges remain inclusive at both ends.

Forecast queries support `forecast_issue_time` for a specific run or `forecast: "latest"` for the latest available issue at each dataset/variable/valid-time/location/vertical coordinate. Latest mode excludes non-forecast records, orders by issue time, then recording time and ID, and respects the as-of recording cutoff and issue-time availability. A withdrawn latest record falls back to an older available issue. Latest mode and a specific issue time cannot be combined. Forecast series/playback require one of these modes to avoid silently mixing runs.

`db-series` takes a dataset, variable, exact point, time interval and integer `bucket_seconds`. Optional `vertical` selects an exact level/reference; omitting it selects only unlevelled records. At most 1,000 buckets are returned. Buckets are anchored to the requested start and use **[start, end)** boundaries, with a possibly shorter final bucket. Output includes units, total record counts, nonmissing sample counts, missing flags, mean/minimum/maximum and a sample sum. Empty/all-null buckets retain null statistics. These are unweighted numeric summaries over matching records and all quality flags; a sample sum is not a time integral or a physical accumulation. `as_of` selects a historical series.

`db-playback` requires an `at` timestamp and `tolerance_seconds` (0–604,800). It selects the closest sample within the inclusive tolerance at the exact point/vertical level. Equal temporal distances prefer the earlier valid time, then stable identity. No match returns null; no interpolation or silent carry-forward occurs. The signed offset in seconds makes stale/future sampling explicit. Historical and forecast selections also apply.

Series and playback are computed in SQL and bound their output, not total execution work. Exact coordinates currently identify a series; stable sensor/entity IDs and interpolation are later extensions. An as-of selection is included in cursor fingerprints. Catalogue metadata is not yet revisioned: the observation ledger preserves measurements while current variable contracts remain protected from changes after observations exist. Correcting valid times/locations can change current data coverage; ingestion coverage envelopes are not automatically recomputed by revision commands yet.

## Step 7: chunked multidimensional fields

Migration 6 adds immutable `array_versions` with dataset references, predecessor links, publication times and checksummed manifests. Chunks live separately in a configured POSIX filesystem; PostgreSQL holds metadata and references, not the numerical payloads. This initial native format uses one losslessly compressed NPZ file per chunk, with numeric data and a separate boolean missing mask. It is not yet a Zarr/NetCDF interface.

```sh
export MATRIX_DB_ARRAY_ROOT='./array-data'
matrix-db db-migrate
matrix-db db-array-demo examples/grid-spec.json
# Copy the returned version UUID into a subset query JSON file.
matrix-db db-array-read subset.json
matrix-db db-array-inventory
```

The demo requires the registered `demo-weather` dataset. It generates synthetic values chunk by chunk; it is not an external-data importer. A subset request looks like:

```json
{
  "version_id": "REPLACE_WITH_PUBLISHED_UUID",
  "variables": ["temperature"],
  "slices": {"time": {"start": 1, "stop": 2}},
  "ranges": {
    "depth": {"minimum": 10, "maximum": 10},
    "lat": {"minimum": 52.5, "maximum": 52.75}
  }
}
```

Index slices use [start,stop). Coordinate windows include cell centres at both endpoints; they do not select cells solely because their bounds overlap a region. Time windows require timezone-aware strings and normalize to UTC. Numeric windows use the axis's declared units. A longitude minimum greater than its maximum selects both sides of the antimeridian, preserving native axis order. Unknown axes, out-of-bounds index slices and ambiguous index/range selections are rejected. Empty selections return empty arrays and read no chunks. Variables without a selected dimension are unaffected by that dimension's selection.

Grid definitions support regular **EPSG:4326 longitude/latitude** and **EPSG:3857 x/y** axes, either ascending or descending. Horizontal spacing is checked; explicit cell bounds or inferred half-step bounds describe each cell. Singleton horizontal axes require explicit bounds. Time coordinates are unique and increasing. Vertical axes require metres, reference and positive direction. Each variable declares its own dimension order, exact numeric dtype, units and chunk shape. Units, integer types, finite nonmissing values and valid ranges must agree with registered variables. Projected grid coordinates remain in their declared CRS: no reprojection is performed, and registration's geographic coverage is not automatically derived from projected bounds.

Programmatic publication uses `ArrayStore.publish(spec, read_chunk)`. The callback receives a variable ID and tuple of index slices and returns `(data, missing_mask)` of exactly the requested shape. Supported data dtypes are float32/float64/int16/int32/int64/uint8; the mask must be boolean and true means missing. Masked payload bits, including NaN, are preserved; nonmissing NaN/infinity is rejected. The publisher never asks for more than one chunk at a time. The provider remains responsible for avoiding full-dataset allocation. `ArrayStore.read(SubsetQuery(...))` returns typed ndarrays, masks, selected axes, dimension order, units and the number of chunks opened; the CLI renders missing values as JSON null alongside masks.

Publication stages files, syncs them, verifies checksums, renames the complete directory and only then commits its immutable PostgreSQL reference. Missing or corrupted published files fail reads rather than silently filling values. Readers verify manifest and selected-chunk checksums and disable NumPy pickle loading. New versions receive UUIDs and may reference a predecessor from the same dataset; published versions cannot be edited through SQL updates/deletes. Dataset measurement definitions are protected after observations or array versions exist.

A failed provider cleans its own staging directory without exposing a version. A crash or outer-transaction rollback after the rename can leave unreferenced files, which `db-array-inventory` lists without deleting anything. Listed directories may belong to in-flight transactions: automatic reclamation, grace periods and crash recovery workers are later work. PostgreSQL backups must be paired with the chunk directory. All readers/writers must use the same durable storage root; changing the root does not relocate data. Publication assumes a single POSIX filesystem with atomic directory rename/fsync; object storage and distributed locking are not implemented.

Uncompressed data plus masks are capped at 64 MiB per chunk and per returned subset. This bounds payload allocations, not total process RSS or serialization overhead. Axis metadata is capped at 100,000 coordinates per axis and manifests at 100,000 chunks per variable. Metadata is currently loaded as a whole; payload I/O visits only intersecting chunks. JSON CLI output can be large; select a small window. Step 8 adds unit conversion and dataset alignment.

## Step 8: unit conversion and explicit dataset alignment

`db-align request.json` aligns pinned source array versions and publishes a new immutable target version. Register the target dataset and variable definitions first; their units/ranges must match the desired output. Python callers can use `align(arrays, request)` for an in-memory result or `align_and_publish(arrays, request)` for persisted chunks. The source versions remain unchanged.

An alignment request contains a complete `target` GridSpec and one `inputs` entry per target variable. Each input specifies `version_id`, `variable_id`, `output_variable`, and its explicit alignment rules. The target defines CRS, grid coordinates, timestamps, levels, dimension order, dtype, units and chunk sizes. `examples/prepare-alignment.py` prepares a runnable synthetic Celsius-to-Kelvin example from the version produced by `db-array-demo`:

```sh
matrix-db db-array-demo examples/grid-spec.json > /tmp/matrix-source-version.json
python examples/prepare-alignment.py /tmp/matrix-source-version.json /tmp/matrix-alignment.json
matrix-db db-align /tmp/matrix-alignment.json
```

Supported conversions include K/degC/degF (with offsets), explicit temperature differences, common length/time/speed/pressure units, percentage/fraction, precipitation rates and density. Conversion between incompatible quantities fails: accumulated rainfall in mm is not interchangeable with a rate in mm/h. Identical labels can pass through without conversion, including category units. The intentionally small registry is in `units.py`; unsupported conversions require an explicit registry extension. Affine offset conversion is rejected for extensive totals.

Spatial methods are `nearest`, `linear` (bilinear across the two horizontal axes), and `conservative`. Nearest/linear can reproject between EPSG:4326 and EPSG:3857 through pyproj with explicit x/y order. Nearest stays within source cell bounds; linear requires bracketing centres and does not extrapolate. `max_horizontal_gap`, when supplied, is expressed in each source horizontal axis's units. Categories require nearest/exact methods and identical units; they are never averaged. Vector-basis rotation, periodic interpolation across the antimeridian, curvilinear grids and arbitrary CRSs are not implemented.

Conservative remapping currently requires the same CRS and explicit/inferred rectangular cell bounds. Intensive values use overlap/target-area weights; `spatial_extensive: true` redistributes cell totals with overlap/source-area weights. Geographic weights use spherical longitude/sine-latitude areas; Web Mercator weights preserve projected-plane integrals, not true Earth surface areas. No cross-CRS conservative clipping is claimed. Conservation applies to compatible complete coverage and does not turn an intensive quantity into an extensive one.

Temporal methods are `exact`, `nearest`, `linear` and `overlap`. Nearest/linear require `max_time_gap_seconds`: nearest limits sample distance and linear limits bracket separation. They do not extend beyond the available timeline. `time_semantics` distinguishes instantaneous values, interval means and accumulations. Means/accumulations require overlap plus explicit `source_time_bounds` and `target_time_bounds` (lists of `{start,end}`), with positive nonoverlapping intervals enclosing the axis coordinates. Means are duration-weighted; accumulations are distributed by source-interval fractions. This assumes uniform values within each declared interval and never infers whether a source is an accumulated quantity.

Vertical methods are exact/nearest/linear; interpolation requires `max_vertical_gap_metres`. Coordinates must use metre units and explicit direction/reference. Positive-up/down directions are accounted for. Different references require `vertical_offset_metres`, defined as a constant added to target positive-up coordinates to express them relative to the source reference. This is a caller-supplied offset, not a geoid/datum transformation. Extra time/vertical target dimensions require explicit `broadcast: ["time"]` or `["vertical"]`; other dimensions must match exactly and source dimensions cannot be silently dropped.

Every output cell has data, a missing mask and a coverage fraction. `minimum_coverage` defaults to 1, so missing contributors or uncovered regions mask the result. A lower threshold explicitly permits partial results. Without renormalization, absent contributions contribute no value; `renormalize_missing: true` divides intensive results by valid covered fraction. Renormalization is forbidden for accumulations/cell totals because it would invent missing totals. When accepting partial coverage, interpret values together with coverage. Publication persists coverage in the compressed chunks and provenance in the manifest; subset reads return both. Chaining a partially covered, nonmissing result into another alignment is rejected until composition semantics are implemented.

Provenance records algorithm/unit-registry/library versions, exact source version IDs and manifest checksums, conversion scales/offsets, all methods/tolerances/interval bounds, and the complete target definition. Repeating the same pinned request reproduces values under the recorded software environment; source updates cannot silently change the input version. Floating target dtypes may round as requested; integer outputs reject fractional/out-of-range values, and source integers beyond exact float64 representation are rejected.

This initial alignment engine reads one bounded source variable at a time rather than streaming arbitrary large source versions. Source working payloads and combined output data/mask/coverage each have a 64 MiB budget. Computation is additionally limited to 100,000 output cells, 2,000,000 contributions and a coordinate-work estimate of 5,000,000 comparisons. Split larger jobs into smaller source/target grids. Limits bound payload/work estimates, not full process RSS or elapsed time. Background distributed alignment is later work. The following milestone is dependency relationships and derived variables.

## Step 9: relationships and derived variables

Migration 7 adds immutable versioned calculation definitions, computational dependency links, separate descriptive associations, and an exact-input result cache referencing source versions and definitions. The runnable example derives wind magnitude from east/north components, then a strict threshold-exceedance flag:

```sh
matrix-db db-migrate
python examples/prepare-derivation.py /tmp/matrix-derived-request.json
matrix-db db-derived-register examples/derived-wind-definitions.json
matrix-db db-derived-inspect examples/derived-reference.json
matrix-db db-derived-run /tmp/matrix-derived-request.json
# Repeat the identical request: cache_hit becomes true and version_id is unchanged.
matrix-db db-derived-run /tmp/matrix-derived-request.json
```

The example publishes synthetic vector components, including suspect quality flags, and prepares a request for `wind-speed@1` and `wind-alert@1`. The flag's default threshold is 4 m/s; it is an arithmetic example, not a calibrated risk model. Components must already use a consistent physical vector basis; the magnitude function does not rotate them between projections.

Definitions have stable `id` and integer `version`, description, computational inputs, named unit-bearing parameters, output unit, and exactly one expression or registered function. Inputs refer to named source bindings or exact derived-definition versions. Batch registration resolves dependencies and rejects cycles, absent references and incompatible edge units atomically. Re-registering identical contents is safe; editing an existing version is rejected. Use a new version to change a calculation. Descriptive `Association` records, registered with `db-derived-associate`, are stored separately and do not trigger calculations or assert causation.

Registered version-1 functions are `wind_speed(east,north)`, `threshold_exceedance(value,threshold)`, `difference(left,right)` and `mean(left,right)`. Expressions support named inputs/parameters, finite numeric literals, +/−/×/÷, bounded integer powers, single comparisons, and sqrt/abs/hypot/minimum/maximum. A restricted AST interpreter performs dimensional checks; no eval, attributes, indexing, imports, comprehensions, arbitrary calls or Python execution are allowed. Expressions are limited to 2,048 characters and 128 AST nodes. Every declared input/parameter must be used.

Arithmetic uses float64 SI values and the explicit unit registry. Output units are checked before execution. A dimensioned threshold must be a parameter with a unit: `speed > limit`, not `speed > 4`. Absolute temperatures and temperature differences remain distinct; subtracting absolute temperatures produces a difference. Use the registered mean function to average absolute temperatures. Unsupported derived unit labels require a registry extension. Extremely large integer inputs and unsafe integer output casts are rejected rather than silently rounded.

A derivation request includes a target GridSpec, output-variable → definition-version mappings, pinned source bindings and optional parameter overrides keyed by `id@version`. Every output must share a common dimension order. Bindings can select index/coordinate subsets; names, units, coordinates, bounds, CRS and vertical references are checked. Equivalent dimension orders can be transposed explicitly by coordinate role. Mismatched grids require a binding's `alignment` object (the Step 8 AlignmentInput contract referencing the same source version/variable); resampling or broadcasting is never inferred. Parameter overrides must name declared parameters and use compatible units.

Missing inputs propagate by default, or `missing: "reject"` can stop the calculation. Coverage is the minimum reported coverage across required operands. Invalid domains, nonfinite results and declared range violations become masked/bad cells unless `invalid: "reject"` is selected. These are explicit numerical/data policies, not scientific validation of a formula. Every declared input is required even if an arithmetic identity could eliminate it.

Chunks now optionally retain uint8 quality codes: good=0, suspect=1, bad=2, unknown=3. Providers may return `(data,mask,coverage,quality)`; older chunks without quality remain readable and are treated as unknown during derivation. Output quality uses worst severity (good < unknown < suspect < bad), with `excluded_quality: [2]` masking bad inputs by default. Suspect/unknown inputs remain flagged. Newly invalid calculations are marked bad. Alignment carries quality from contributing cells, and publications/subset reads preserve masks, coverage and quality together. Coverage is a support indicator, not a probability or uncertainty estimate.

Cache identity includes the complete target/selection request, pinned source version IDs and manifest checksums, reachable definition versions/checksums, parameter overrides, alignment rules and engine/unit/library versions. Reuse returns the same immutable result version. A transaction-level lock serializes identical requests; output publication and cache references commit together. Cache hits verify result-manifest and chunk checksums and fail on corrupted storage. Published provenance contains the full dependency chain, expressions/functions, parameters, source selections and conversion/alignment settings. Descriptive associations do not invalidate computational results.

Current execution is bounded to 128 reachable definitions, 64 dependency levels, 100,000 target cells, and a conservative 64 MiB source/intermediate payload budget. The source-budget estimate currently uses complete variable shapes even when bindings select smaller subsets; stage smaller source versions if needed. Metadata catalogue loading is capped at 10,000 definitions. Limits bound payload estimates rather than total RSS. Calculations run synchronously; distributed evaluation, cache eviction and larger graph execution are later work. Quality is preserved, but catalogue/array provenance is not a tamper-proof audit mechanism against database owners. Step 10 is simulation scenario and result storage.

## Step 10: simulation scenarios, attempts and results

Migration 8 adds immutable scenario versions, pinned input/calculation references, execution attempts with append-only state events, provisional/final output links and stored comparisons. Matrix DB manages these records; **TerraFlux or another external engine performs the physical simulation**. The included demo generates synthetic numbers and does not execute TerraFlux or predict hazards.

```sh
matrix-db db-migrate
# Run once in a fresh development database; use new IDs/versions for new input snapshots.
python examples/prepare-simulation.py /tmp/matrix-simulation-demo.json /tmp/matrix-comparison-request.json
matrix-db db-scenario-resolve examples/scenario-reference.json
matrix-db db-run-compare /tmp/matrix-comparison-request.json
```

A `Scenario` defines stable ID/version, name/description, optional parent ID/version, geographic region, simulation period, output GridSpec, unit-bearing parameters and named inputs. Array pins reference an exact version UUID and variable; observation pins reference an exact dataset/observation/revision. Roles distinguish initial, boundary, forcing and reference inputs. Optional derived-calculation references pin exact definition versions and checksums. Initial registration verifies chunk integrity and source availability; observation revisions marked withdrawn cannot initialize a new scenario. Already-pinned older revisions remain reproducible after later corrections or withdrawals. Branches reference existing parent versions without editing them.

The output dataset must be registered with `origin: "simulation"`; output variables/units/types must agree. Output times must fit the scenario period, and WGS84 cell centres must fit its region. For projected grids, the declared geographic region is not automatically derived or checked against projected coordinates. Registering an output scenario protects its dataset measurement definitions from subsequent edits. No physical consistency, sufficient forcing coverage or solver suitability is inferred merely from valid metadata.

`RunRequest` records a scenario reference, engine name/version/revision, declared execution environment, optional random seed and configuration. Each request creates a separate numbered attempt. States are queued → running → completed/failed/cancelled, with queued cancellation also allowed. Terminal attempts cannot restart: create a new attempt to retry. `RunChange` requires the expected run revision, reason and actor, preventing stale transitions. Server timestamps record creation, start and finish; failures retain details. Engine/environment/seed/configuration remain immutable, while every transition is appended to the event history. Actor/environment fields are supplied metadata, not authenticated or independently attested identities.

CLI interfaces accept JSON request files: `db-scenario-register`, `db-scenario-resolve`, `db-run-create`, `db-run-change`, `db-run-attach`, `db-run-inspect`, and `db-run-compare`. Run inspection takes `{ "run_id": "UUID", "include_provisional": true }`; provisional inclusion defaults to false. The example script demonstrates Python request construction and engine-result callbacks. No scheduler, worker process, heartbeat, solver invocation or distributed lease is implemented.

`publish_run_output(arrays, run_id, spec, provider, expected_revision=..., checkpoint=..., provisional=...)` writes an external engine's chunks, records full scenario/input/engine provenance and attaches output references in the same database transaction. A provider uses the array callback contract. Alternatively, `attach_output` links an already-published array version after validation. Attachments are immutable and only allowed while the attempt is running. Repeating an identical checkpoint attachment is safe; a different output cannot overwrite it. Provisional outputs may contain a subset of declared time samples but must retain the declared spatial/vertical grid. Final outputs must match the complete grid, variable, unit and dtype.

Completing a run requires exactly one final reference for every declared variable and re-verifies final chunk integrity. Run result discovery hides provisional/final-candidate links until successful completion; failed/cancelled attempts never expose them as completed results. Provisional inspection remains possible when explicitly requested. This is lifecycle/result-discovery gating, not access control: a caller already holding an array UUID can use the general array API. Arrays are immutable publications independent of their association with a run.

Comparisons accept a completed run on at least one side; the other side may be another completed run or a pinned array field, including observational data. Each side explicitly selects its variable and optional index/coordinate subset. The target is a single-variable GridSpec. Grids must match or include explicit Step 8 alignment rules; no automatic interpolation is assumed. Results are **right minus left**, with masks, coverage and quality propagated. Units are checked dimensionally: subtracting absolute temperatures requires a difference unit such as `delta_K`, not `degC`. Output versions store run/scenario/engine references, source versions/checksums, selection/alignment rules and software versions. Stored comparisons retain both source-version references. Difference fields can be read as map slices or location time series through the array subset API.

Scenario input resolution is rechecked when starting a run; the snapshot never resolves a mutable 'latest' alias. Input integrity does not guarantee that an engine actually used those inputs or reproduced its declared configuration. Comparisons and direct result reads use the existing bounded array/alignment policies; scenario snapshot verification streams checksums rather than loading entire fields. Provisional histories are bounded by individual API calls only; retention and job scheduling come later. Filesystem publication still precedes database commit: rollback/crash can leave unreferenced directories, which inventory reports without deletion. Back up PostgreSQL and the chunk store together. Step 11 adds the service API, client libraries and GeoSentinel integration.

## Step 11: versioned service API and clients

Install the package, export the database URL, array root and `MATRIX_DB_API_KEYS`, then run `matrix-db-api`. It binds to **127.0.0.1:8000**; production TLS/reverse-proxy configuration belongs to deployment. `matrix-db-worker` runs the durable calculation queue separately (`--once` processes at most one job). The API never runs migrations automatically: initialize/upgrade with `matrix-db db-migrate` first.

`MATRIX_DB_API_KEYS` is a JSON array of `{token,subject,role,datasets}`. Tokens require at least 24 characters; generate random service credentials and keep them out of source control. The service fails startup with missing/empty credentials or missing array-root configuration. Roles are reader, ingestor and admin. Readers access only listed dataset IDs; ingestors additionally register/update/ingest their listed datasets; admins can access all datasets and administrative workflows. Advanced scenarios, revisions, derived-definition registration and calculation jobs require admin credentials. This is service/dataset access control, not project isolation or per-user OAuth. Policy changes require restarting the API; workers reload environment settings between jobs. No browser bundle should contain a Matrix DB service token.

Endpoints are under `/v1`; interactive documentation and the generated OpenAPI contract are at `/docs` and `/openapi.json`. Public `/health/live` checks the process; authenticated `/v1/ready` checks schema version/PostGIS. Core interfaces include:

| Interface | Endpoints |
|---|---|
| Catalogue | GET `/datasets`, POST `/datasets/search`, GET/PUT `/datasets/{id}`, POST `/datasets` |
| Array versions | GET `/datasets/{id}/versions`, GET `/arrays/{uuid}`, POST `/arrays/subset` |
| Observations | POST `/observations`, `/observations/query`, `/observations/stream`; GET `/observations/{dataset}/{id}/history` |
| Time Machine | POST `/series`, `/playback`, `/observations/revise`, `/observations/withdraw` |
| Import status | GET `/imports/{job_id}` |
| Derived definitions | POST `/derived/definitions` |
| Simulation records | POST `/scenarios`, `/scenarios/resolve`, `/runs`, `/runs/transition`, `/runs/outputs`; GET `/runs/{uuid}` |
| Async calculations | POST `/jobs`, GET `/jobs/{uuid}`, POST `/jobs/{uuid}/cancel` |

Request bodies reuse the existing data/query contracts. Observation ingestion accepts `{dataset_id, observations}` and commits the whole batch atomically. Request bodies are capped at 2 MiB. Native array reads are capped at 512 KiB per HTTP request, with a 2 MiB JSON serialization budget; use smaller subsets for larger fields. Non-admin array responses omit provenance because it may contain other datasets' records or operational configuration. Full provenance remains available to admins and through trusted storage APIs. Catalogue filters currently load metadata into memory; this remains a small-catalogue implementation.

Observations retain units, source references, quality, revision and recording time. Normal query responses include a filter-bound next cursor. NDJSON streaming serves one bounded page, with a metadata line carrying the next cursor followed by observation lines; it does not materialize an unlimited result. Version listing uses UUID ordering/cursors, not a mutable latest alias. Most synchronous SQL operations have a 15-second statement timeout. Errors use `{error:{code,message,request_id}}`, and responses include `X-Request-ID`. Validation and server errors avoid echoing raw credentials, connection strings or filesystem paths.

Async job operations are observations, alignment, derivation and comparison. Submission validates the request and current scope before persisting it. Jobs survive API restarts; a separate worker claims queued jobs with row locking. Job inspection/cancellation checks ownership and current permissions; admin credentials can manage all jobs. Inputs are pinned by the existing query/version contracts. `work_done` counts cooperative activity checkpoints; it is **not percent complete**, and total work is unknown. Cancellation of queued jobs is immediate; running cancellation is acknowledged between chunks/calculation checkpoints or after a database operation. Worker SQL has a 30-second statement timeout. A completion/cancellation race is decided transactionally, so cancelled computations cannot commit their result references.

Cancellation can leave unreferenced filesystem directories after a publication rename; inventory handles this as before. Step 12 adds single-worker crash reconciliation: abandoned running jobs become failed (or cancelled) without automatic replay. See the operations guide. API disconnects alone do not cancel persisted jobs: explicitly call the cancellation endpoint. Credentials are rechecked when a worker claims a job and when results are read; revoking a running job's credential requires cancelling it or restarting the worker for policy changes to take effect.

The Python `MatrixDBClient` uses bounded timeouts, structured errors, paginated observation iteration, and retries only declared read-only requests. Mutation calls are never automatically replayed. It exposes catalogue/query/subset/series/playback, scenario/run/output and job operations:

```python
from matrix_db.client import MatrixDBClient
with MatrixDBClient('http://127.0.0.1:8000', service_token) as client:
    for page in client.pages({'dataset_id': 'demo-weather', 'limit': 100}):
        print(len(page['items']))
```

The TypeScript client is in `clients/typescript` (`npm ci && npm test`). It provides typed observation/subset models, cursor iteration, AbortSignal support, timeouts and the same no-mutation-retry rule. Advanced structured payloads remain JSON-shaped and follow OpenAPI schemas. TypeScript build output and node_modules are excluded from the repository. Clients reject URLs with embedded credentials and do not follow redirects carrying authorization headers.

A GeoSentinel integration uses its authenticated backend as a read-only gateway: server-side credentials, dataset discovery, observation GeoJSON, location playback inspection, time-series buckets and array subsets. The explorer workflow uses persisted data; it does not fetch live providers or substitute synthetic observations on error. API and gateway changes require deployment/configuration before users can access a live connected service. Raster tile delivery, OAuth/project permissions, high-throughput catalogue queries and distributed worker leases remain later milestones.


## Step 12 — Performance, recovery and release qualification

Workers now hold a dedicated session advisory lock through execution and publication.
Competing workers skip; the next owner marks abandoned running jobs `worker_interrupted`
without retrying writes. Pending cancellation recovers as cancelled. Live owners are
never timed out or stolen. This is a single-worker recovery model, not distributed leases.

CI records a rollback-only synthetic 2,000-observation query baseline with timing and
EXPLAIN/BUFFERS output, and builds wheel/sdist artifacts with clean wheel-install checks.
The [operations guide](docs/operations.md) describes repeatable benchmarking, worker
recovery, consistent database-plus-array backups and isolated restore qualification.
Production load capacity, an actual backup/restore drill, full browser E2E and deployment
remain unverified operator gates. No release tag, merge or deployment is made here.


## Step 13 — Providers and scheduled ingestion

`matrix-db-ingest` adds fixed adapters for USGS seismic events, FIRMS fire radiative
power, Open-Meteo historical reanalysis, Sentinel Hub daily area-mean NDVI and Overpass
building-presence snapshots. Configurable schedules use per-source locks, overlapping
watermarks, bounded retries/backoff, explicit bounded backfill, atomic corrections and
freshness tracking. Credential values stay in environment variables; source state and
failure reports store no credential values or upstream exception messages.

See [provider setup, semantics and limits](docs/providers.md) and
[example source configurations](examples/providers.json). Protocol fixtures and PostGIS
transaction tests qualify the adapters; credentialed live accounts and deployment are
not tested. Weather forecasts, native vegetation rasters, population estimates and
historical OSM backfill are not supplied by these adapters. Step 14 adds data-quality
monitoring and operational alerts.


## Step 14 — Quality monitoring and operational alerts

Versioned policies assess range, rate-of-change, count completeness, data freshness,
expected-point coverage and exact-pair variable consistency. Evaluations pin observation
revisions and preserve upstream flags. Failures create deduplicated incidents with
acknowledgement, recovery and durable local notification events. Dataset-scoped health
reads distinguish healthy, degraded, unknown and expired evaluations. The operator
`matrix-db-quality` process can assess data independently of provider ingestion.

See [quality semantics, API and operations](docs/quality.md) and the
[example policy](examples/quality-policy.json). GeoSentinel displays health and local
notifications through its authenticated read-only gateway. External delivery, array
quality scanning, deployment and full browser E2E are not provided here. Step 15 covers
retention, archival and storage lifecycle.

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

Step 3 is persistent storage and migrations. Persistent storage, migrations, live ingestion, APIs, array storage, authentication and GeoSentinel integration are planned, not implemented. The broader roadmap described in the planning conversation is not a claim of available functionality.

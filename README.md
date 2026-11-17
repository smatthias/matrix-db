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

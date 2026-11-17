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

## Next milestone

Step 2 is catalogue registration and discovery. Persistent storage, migrations, live ingestion, APIs, array storage, authentication and GeoSentinel integration are planned, not implemented. The broader roadmap described in the planning conversation is not a claim of available functionality.

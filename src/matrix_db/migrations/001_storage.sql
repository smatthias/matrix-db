CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE matrix_db.sources (
    provider text NOT NULL,
    source_id text NOT NULL,
    PRIMARY KEY (provider, source_id)
);
CREATE TABLE matrix_db.datasets (
    id text PRIMARY KEY,
    provider text NOT NULL,
    source_id text NOT NULL,
    entry jsonb NOT NULL CHECK (jsonb_typeof(entry) = 'object'),
    UNIQUE (provider, source_id),
    FOREIGN KEY (provider, source_id) REFERENCES matrix_db.sources(provider, source_id)
);
CREATE TABLE matrix_db.variables (
    dataset_id text NOT NULL REFERENCES matrix_db.datasets(id),
    id text NOT NULL,
    definition jsonb NOT NULL CHECK (jsonb_typeof(definition) = 'object'),
    PRIMARY KEY (dataset_id, id)
);
CREATE TABLE matrix_db.observations (
    dataset_id text NOT NULL,
    id text NOT NULL,
    variable_id text NOT NULL,
    valid_time timestamptz NOT NULL,
    longitude double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    latitude double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    location geometry(Point, 4326) GENERATED ALWAYS AS
        (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED,
    record jsonb NOT NULL CHECK (jsonb_typeof(record) = 'object'),
    PRIMARY KEY (dataset_id, id),
    FOREIGN KEY (dataset_id, variable_id) REFERENCES matrix_db.variables(dataset_id, id)
);

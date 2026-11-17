CREATE TABLE matrix_db.provider_sources (
    id text PRIMARY KEY,
    config_hash text NOT NULL,
    config jsonb NOT NULL,
    dataset_id text NOT NULL UNIQUE REFERENCES matrix_db.datasets(id),
    watermark timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_success_at timestamptz,
    latest_valid_time timestamptz,
    failures integer NOT NULL DEFAULT 0,
    error_code text
);
CREATE TABLE matrix_db.provider_runs (
    id uuid PRIMARY KEY,
    source_id text NOT NULL REFERENCES matrix_db.provider_sources(id),
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    backfill boolean NOT NULL,
    status text NOT NULL CHECK(status IN ('running','completed','failed')),
    inserted integer NOT NULL DEFAULT 0,
    corrected integer NOT NULL DEFAULT 0,
    unchanged integer NOT NULL DEFAULT 0,
    withdrawn integer NOT NULL DEFAULT 0,
    error_code text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz
);
CREATE INDEX provider_due_idx ON matrix_db.provider_sources(next_attempt_at);
CREATE INDEX provider_runs_source_idx ON matrix_db.provider_runs(source_id,started_at DESC);

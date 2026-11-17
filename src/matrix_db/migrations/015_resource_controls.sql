CREATE TABLE matrix_db.api_rate_windows (
    credential_id text PRIMARY KEY,
    window_start timestamptz NOT NULL,
    used integer NOT NULL CHECK(used>0)
);
CREATE INDEX api_jobs_active_credential_idx ON matrix_db.api_jobs(credential_id) WHERE status IN ('queued','running');
CREATE TABLE matrix_db.array_resources (
    version_id uuid PRIMARY KEY REFERENCES matrix_db.array_versions(version_id),
    stored_bytes bigint NOT NULL CHECK(stored_bytes>=0)
);

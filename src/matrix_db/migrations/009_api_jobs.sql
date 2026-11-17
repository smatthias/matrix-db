CREATE TABLE matrix_db.api_jobs (
    id uuid PRIMARY KEY,
    subject text NOT NULL,
    credential_id text NOT NULL,
    operation text NOT NULL CHECK(operation IN ('observations','alignment','derivation','comparison')),
    request jsonb NOT NULL,
    status text NOT NULL CHECK(status IN ('queued','running','completed','failed','cancelled')),
    cancel_requested boolean NOT NULL DEFAULT false,
    work_done bigint NOT NULL DEFAULT 0,
    result jsonb,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX api_jobs_queue_idx ON matrix_db.api_jobs(created_at,id) WHERE status='queued';

CREATE TABLE matrix_db.import_jobs (
    id text PRIMARY KEY,
    dataset_id text NOT NULL REFERENCES matrix_db.datasets(id),
    source_checksum text NOT NULL,
    dataset_checksum text NOT NULL,
    format text NOT NULL CHECK (format IN ('csv','json','jsonl')),
    status text NOT NULL CHECK (status IN ('running','completed','failed')),
    processed bigint NOT NULL DEFAULT 0,
    accepted bigint NOT NULL DEFAULT 0,
    duplicates bigint NOT NULL DEFAULT 0,
    rejected bigint NOT NULL DEFAULT 0,
    error text,
    started_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE matrix_db.import_rejections (
    job_id text NOT NULL REFERENCES matrix_db.import_jobs(id),
    record_number bigint NOT NULL,
    reason text NOT NULL,
    PRIMARY KEY (job_id, record_number)
);
ALTER TABLE matrix_db.observations ADD COLUMN import_job_id text
    REFERENCES matrix_db.import_jobs(id);

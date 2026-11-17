CREATE TABLE matrix_db.array_versions (
    dataset_id text NOT NULL REFERENCES matrix_db.datasets(id),
    version_id uuid PRIMARY KEY,
    previous_version uuid,
    manifest jsonb NOT NULL,
    manifest_checksum text NOT NULL,
    published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(dataset_id,version_id),
    FOREIGN KEY(dataset_id,previous_version) REFERENCES matrix_db.array_versions(dataset_id,version_id)
);
CREATE INDEX array_versions_dataset_idx ON matrix_db.array_versions(dataset_id,published_at);
CREATE TRIGGER immutable_array_versions BEFORE UPDATE OR DELETE ON matrix_db.array_versions
    FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();

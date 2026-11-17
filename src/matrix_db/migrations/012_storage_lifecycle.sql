CREATE TABLE matrix_db.retention_policies (
    dataset_id text NOT NULL REFERENCES matrix_db.datasets(id),
    revision integer NOT NULL CHECK(revision>0),
    definition jsonb NOT NULL,
    PRIMARY KEY(dataset_id,revision)
);
CREATE TABLE matrix_db.retention_active (
    dataset_id text PRIMARY KEY,
    revision integer NOT NULL,
    FOREIGN KEY(dataset_id,revision) REFERENCES matrix_db.retention_policies(dataset_id,revision)
);
CREATE TABLE matrix_db.retention_plans (
    id uuid PRIMARY KEY,
    dataset_id text NOT NULL REFERENCES matrix_db.datasets(id),
    policy_revision integer NOT NULL,
    created_at timestamptz NOT NULL,
    definition jsonb NOT NULL
);
CREATE TABLE matrix_db.array_locations (
    version_id uuid PRIMARY KEY REFERENCES matrix_db.array_versions(version_id),
    tier text NOT NULL CHECK(tier IN ('hot','archive')),
    verified_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE matrix_db.retention_holds (
    version_id uuid PRIMARY KEY REFERENCES matrix_db.array_versions(version_id),
    active boolean NOT NULL,
    reason text NOT NULL,
    actor text NOT NULL
);
CREATE TABLE matrix_db.lifecycle_events (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version_id uuid REFERENCES matrix_db.array_versions(version_id),
    kind text NOT NULL CHECK(kind IN ('archived','restored','hot_evicted','hold_set','hold_released','revisions_exported')),
    detail jsonb NOT NULL,
    actor text NOT NULL,
    at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE matrix_db.revision_archives (
    id uuid PRIMARY KEY,
    dataset_id text NOT NULL REFERENCES matrix_db.datasets(id),
    before_time timestamptz NOT NULL,
    row_count integer NOT NULL,
    checksum text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER immutable_retention_policies BEFORE UPDATE OR DELETE ON matrix_db.retention_policies FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_retention_plans BEFORE UPDATE OR DELETE ON matrix_db.retention_plans FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_lifecycle_events BEFORE UPDATE OR DELETE ON matrix_db.lifecycle_events FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_revision_archives BEFORE UPDATE OR DELETE ON matrix_db.revision_archives FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();

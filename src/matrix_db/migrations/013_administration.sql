CREATE TABLE matrix_db.administration_actions (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id text NOT NULL REFERENCES matrix_db.datasets(id),
    action text NOT NULL,
    actor text NOT NULL,
    detail jsonb NOT NULL,
    at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX administration_actions_dataset_idx ON matrix_db.administration_actions(dataset_id,at DESC,sequence DESC);
CREATE TRIGGER immutable_administration_actions BEFORE UPDATE OR DELETE ON matrix_db.administration_actions FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();

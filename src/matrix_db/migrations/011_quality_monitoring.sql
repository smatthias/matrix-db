CREATE TABLE matrix_db.quality_policies (
    dataset_id text NOT NULL REFERENCES matrix_db.datasets(id),
    revision integer NOT NULL CHECK(revision>0),
    definition jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(dataset_id,revision)
);
CREATE TABLE matrix_db.quality_active (
    dataset_id text PRIMARY KEY,
    revision integer NOT NULL,
    FOREIGN KEY(dataset_id,revision) REFERENCES matrix_db.quality_policies(dataset_id,revision)
);
CREATE TABLE matrix_db.quality_evaluations (
    id uuid PRIMARY KEY,
    dataset_id text NOT NULL,
    policy_revision integer NOT NULL,
    evaluated_at timestamptz NOT NULL,
    window_start timestamptz NOT NULL,
    findings jsonb NOT NULL,
    snapshot jsonb NOT NULL,
    FOREIGN KEY(dataset_id,policy_revision) REFERENCES matrix_db.quality_policies(dataset_id,revision)
);
CREATE INDEX quality_evaluations_latest_idx ON matrix_db.quality_evaluations(dataset_id,evaluated_at DESC,id);
CREATE TABLE matrix_db.quality_incidents (
    id uuid PRIMARY KEY,
    dataset_id text NOT NULL,
    policy_revision integer NOT NULL,
    rule_id text NOT NULL,
    severity text NOT NULL CHECK(severity IN ('warning','critical')),
    status text NOT NULL CHECK(status IN ('open','acknowledged','resolved')),
    opened_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    evaluation_id uuid NOT NULL REFERENCES matrix_db.quality_evaluations(id),
    FOREIGN KEY(dataset_id,policy_revision) REFERENCES matrix_db.quality_policies(dataset_id,revision)
);
CREATE UNIQUE INDEX quality_incident_active_idx ON matrix_db.quality_incidents(dataset_id,policy_revision,rule_id) WHERE status<>'resolved';
CREATE TABLE matrix_db.quality_events (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incident_id uuid NOT NULL REFERENCES matrix_db.quality_incidents(id),
    dataset_id text NOT NULL,
    kind text NOT NULL CHECK(kind IN ('opened','acknowledged','resolved','policy_superseded')),
    actor text NOT NULL,
    at timestamptz NOT NULL,
    evaluation_id uuid REFERENCES matrix_db.quality_evaluations(id)
);
CREATE INDEX quality_events_dataset_idx ON matrix_db.quality_events(dataset_id,sequence);

CREATE TABLE matrix_db.hazard_policies (
 dataset_id text NOT NULL REFERENCES matrix_db.datasets(id), revision integer NOT NULL CHECK(revision>0),
 definition jsonb NOT NULL, PRIMARY KEY(dataset_id,revision)
);
CREATE TABLE matrix_db.hazard_active (
 dataset_id text PRIMARY KEY, revision integer NOT NULL,
 FOREIGN KEY(dataset_id,revision) REFERENCES matrix_db.hazard_policies(dataset_id,revision)
);
CREATE TABLE matrix_db.hazard_assessments (
 id uuid PRIMARY KEY, dataset_id text NOT NULL, policy_revision integer NOT NULL,
 evaluated_at timestamptz NOT NULL, window_start timestamptz NOT NULL, findings jsonb NOT NULL,
 FOREIGN KEY(dataset_id,policy_revision) REFERENCES matrix_db.hazard_policies(dataset_id,revision)
);
CREATE INDEX hazard_assessments_latest_idx ON matrix_db.hazard_assessments(dataset_id,evaluated_at DESC,id);
CREATE TABLE matrix_db.hazard_assessment_inputs (
 assessment_id uuid NOT NULL REFERENCES matrix_db.hazard_assessments(id), dataset_id text NOT NULL,
 observation_id text NOT NULL, revision integer NOT NULL, PRIMARY KEY(assessment_id,observation_id),
 FOREIGN KEY(dataset_id,observation_id,revision) REFERENCES matrix_db.observation_revisions(dataset_id,id,revision)
);
CREATE TABLE matrix_db.hazard_events (
 id uuid PRIMARY KEY, dataset_id text NOT NULL, policy_revision integer NOT NULL, rule_id text NOT NULL,
 hazard_type text NOT NULL, severity text NOT NULL CHECK(severity IN ('warning','critical')),
 status text NOT NULL CHECK(status IN ('active','resolved','retracted','superseded')),
 detection_state text NOT NULL, revision integer NOT NULL DEFAULT 1,
 opened_at timestamptz NOT NULL, evaluated_at timestamptz NOT NULL, last_valid_time timestamptz,
 acknowledged_at timestamptz, reason text NOT NULL, metric double precision, longitude double precision,latitude double precision,
 bbox jsonb, support_hash text NOT NULL, assessment_id uuid NOT NULL REFERENCES matrix_db.hazard_assessments(id),
 FOREIGN KEY(dataset_id,policy_revision) REFERENCES matrix_db.hazard_policies(dataset_id,revision)
);
CREATE UNIQUE INDEX hazard_event_active_idx ON matrix_db.hazard_events(dataset_id,policy_revision,rule_id) WHERE status='active';
CREATE INDEX hazard_events_dataset_idx ON matrix_db.hazard_events(dataset_id,opened_at DESC,id);
CREATE TABLE matrix_db.hazard_feed (
 sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 event_id uuid NOT NULL REFERENCES matrix_db.hazard_events(id), dataset_id text NOT NULL,
 kind text NOT NULL CHECK(kind IN ('opened','updated','severity_changed','resolved','acknowledged','retracted','policy_superseded')),
 severity text NOT NULL CHECK(severity IN ('warning','critical')), rule_id text NOT NULL,
 actor text NOT NULL, reason text NOT NULL, at timestamptz NOT NULL,
 assessment_id uuid NOT NULL REFERENCES matrix_db.hazard_assessments(id)
);
CREATE INDEX hazard_feed_dataset_idx ON matrix_db.hazard_feed(dataset_id,sequence);
ALTER TABLE matrix_db.notification_deliveries ALTER COLUMN event_sequence DROP NOT NULL;
ALTER TABLE matrix_db.notification_deliveries ADD COLUMN hazard_sequence bigint REFERENCES matrix_db.hazard_feed(sequence);
ALTER TABLE matrix_db.notification_deliveries ADD CONSTRAINT notification_one_topic CHECK((event_sequence IS NOT NULL)::integer+(hazard_sequence IS NOT NULL)::integer=1);
CREATE UNIQUE INDEX notification_hazard_event_idx ON matrix_db.notification_deliveries(owner,hazard_sequence);
CREATE TRIGGER immutable_hazard_policies BEFORE UPDATE OR DELETE ON matrix_db.hazard_policies FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_hazard_assessments BEFORE UPDATE OR DELETE ON matrix_db.hazard_assessments FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_hazard_inputs BEFORE UPDATE OR DELETE ON matrix_db.hazard_assessment_inputs FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_hazard_feed BEFORE UPDATE OR DELETE ON matrix_db.hazard_feed FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();

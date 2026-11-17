CREATE TABLE matrix_db.scenarios (
    id text NOT NULL,
    version integer NOT NULL CHECK(version>0),
    parent_id text,
    parent_version integer,
    definition jsonb NOT NULL,
    checksum text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(id,version),
    FOREIGN KEY(parent_id,parent_version) REFERENCES matrix_db.scenarios(id,version)
);
CREATE TABLE matrix_db.scenario_inputs (
    scenario_id text NOT NULL,
    scenario_version integer NOT NULL,
    name text NOT NULL,
    kind text NOT NULL CHECK(kind IN ('array','observation')),
    array_version uuid REFERENCES matrix_db.array_versions(version_id),
    observation_dataset text,
    observation_id text,
    observation_revision integer,
    snapshot jsonb NOT NULL,
    checksum text NOT NULL,
    PRIMARY KEY(scenario_id,scenario_version,name),
    FOREIGN KEY(scenario_id,scenario_version) REFERENCES matrix_db.scenarios(id,version),
    FOREIGN KEY(observation_dataset,observation_id,observation_revision)
        REFERENCES matrix_db.observation_revisions(dataset_id,id,revision),
    CHECK((kind='array' AND array_version IS NOT NULL AND observation_dataset IS NULL AND observation_id IS NULL AND observation_revision IS NULL)
       OR (kind='observation' AND array_version IS NULL AND observation_dataset IS NOT NULL AND observation_id IS NOT NULL AND observation_revision IS NOT NULL))
);
CREATE TABLE matrix_db.scenario_calculations (
    scenario_id text NOT NULL,
    scenario_version integer NOT NULL,
    calculation_id text NOT NULL,
    calculation_version integer NOT NULL,
    checksum text NOT NULL,
    PRIMARY KEY(scenario_id,scenario_version,calculation_id,calculation_version),
    FOREIGN KEY(scenario_id,scenario_version) REFERENCES matrix_db.scenarios(id,version),
    FOREIGN KEY(calculation_id,calculation_version) REFERENCES matrix_db.derived_definitions(id,version)
);
CREATE TABLE matrix_db.simulation_runs (
    id uuid PRIMARY KEY,
    scenario_id text NOT NULL,
    scenario_version integer NOT NULL,
    attempt integer NOT NULL,
    specification jsonb NOT NULL,
    checksum text NOT NULL,
    status text NOT NULL CHECK(status IN ('queued','running','completed','failed','cancelled')),
    revision integer NOT NULL DEFAULT 1,
    reason text NOT NULL,
    actor text NOT NULL,
    error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    UNIQUE(scenario_id,scenario_version,attempt),
    FOREIGN KEY(scenario_id,scenario_version) REFERENCES matrix_db.scenarios(id,version)
);
CREATE TABLE matrix_db.run_outputs (
    run_id uuid NOT NULL REFERENCES matrix_db.simulation_runs(id),
    variable_id text NOT NULL,
    checkpoint text NOT NULL,
    array_version uuid NOT NULL REFERENCES matrix_db.array_versions(version_id),
    provisional boolean NOT NULL,
    attached_at_revision integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(run_id,variable_id,checkpoint)
);
CREATE UNIQUE INDEX run_final_variable_idx ON matrix_db.run_outputs(run_id,variable_id) WHERE NOT provisional;
CREATE TABLE matrix_db.run_events (
    run_id uuid NOT NULL REFERENCES matrix_db.simulation_runs(id),
    revision integer NOT NULL,
    status text NOT NULL,
    reason text NOT NULL,
    actor text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(run_id,revision)
);
CREATE TABLE matrix_db.run_comparisons (
    id uuid PRIMARY KEY,
    left_run uuid REFERENCES matrix_db.simulation_runs(id),
    right_run uuid REFERENCES matrix_db.simulation_runs(id),
    left_version uuid NOT NULL REFERENCES matrix_db.array_versions(version_id),
    right_version uuid NOT NULL REFERENCES matrix_db.array_versions(version_id),
    output_version uuid NOT NULL REFERENCES matrix_db.array_versions(version_id),
    definition jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK(left_run IS NOT NULL OR right_run IS NOT NULL)
);
CREATE FUNCTION matrix_db.guard_run_transition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected integer;
BEGIN
    IF ROW(NEW.id,NEW.scenario_id,NEW.scenario_version,NEW.attempt,NEW.specification,NEW.checksum,NEW.created_at)
        IS DISTINCT FROM ROW(OLD.id,OLD.scenario_id,OLD.scenario_version,OLD.attempt,OLD.specification,OLD.checksum,OLD.created_at) THEN
        RAISE EXCEPTION 'run specification is immutable';
    END IF;
    IF NOT ((OLD.status='queued' AND NEW.status IN ('running','cancelled')) OR
            (OLD.status='running' AND NEW.status IN ('completed','failed','cancelled'))) THEN
        RAISE EXCEPTION 'invalid run transition';
    END IF;
    IF NEW.status='completed' THEN
        SELECT jsonb_array_length(definition->'grid'->'variables') INTO expected
            FROM matrix_db.scenarios WHERE id=NEW.scenario_id AND version=NEW.scenario_version;
        IF (SELECT count(*) FROM matrix_db.run_outputs WHERE run_id=NEW.id AND NOT provisional)<>expected THEN
            RAISE EXCEPTION 'completed runs require all final output variables';
        END IF;
    END IF;
    IF NEW.status='failed' AND (NEW.error IS NULL OR btrim(NEW.error)='') THEN RAISE EXCEPTION 'failed run requires error details'; END IF;
    NEW.revision:=OLD.revision+1;
    NEW.started_at:=CASE WHEN NEW.status='running' THEN clock_timestamp() ELSE OLD.started_at END;
    NEW.finished_at:=CASE WHEN NEW.status IN ('completed','failed','cancelled') THEN clock_timestamp() ELSE NULL END;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_run_transition BEFORE UPDATE ON matrix_db.simulation_runs
    FOR EACH ROW EXECUTE FUNCTION matrix_db.guard_run_transition();
CREATE FUNCTION matrix_db.record_run_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO matrix_db.run_events(run_id,revision,status,reason,actor)
        VALUES(NEW.id,NEW.revision,NEW.status,NEW.reason,NEW.actor);
    RETURN NEW;
END $$;
CREATE TRIGGER record_run_event AFTER INSERT OR UPDATE ON matrix_db.simulation_runs
    FOR EACH ROW EXECUTE FUNCTION matrix_db.record_run_event();
CREATE FUNCTION matrix_db.guard_output_attachment() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE state text; current_revision integer;
BEGIN
    SELECT status,revision INTO state,current_revision FROM matrix_db.simulation_runs WHERE id=NEW.run_id FOR UPDATE;
    IF state IS DISTINCT FROM 'running' THEN RAISE EXCEPTION 'outputs can only attach to running attempts'; END IF;
    NEW.attached_at_revision:=current_revision;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_output_attachment BEFORE INSERT ON matrix_db.run_outputs
    FOR EACH ROW EXECUTE FUNCTION matrix_db.guard_output_attachment();
CREATE TRIGGER immutable_scenarios BEFORE UPDATE OR DELETE ON matrix_db.scenarios FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_scenario_inputs BEFORE UPDATE OR DELETE ON matrix_db.scenario_inputs FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_scenario_calculations BEFORE UPDATE OR DELETE ON matrix_db.scenario_calculations FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_outputs BEFORE UPDATE OR DELETE ON matrix_db.run_outputs FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_run_events BEFORE UPDATE OR DELETE ON matrix_db.run_events FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER retained_runs BEFORE DELETE ON matrix_db.simulation_runs FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_comparisons BEFORE UPDATE OR DELETE ON matrix_db.run_comparisons FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();

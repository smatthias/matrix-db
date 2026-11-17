ALTER TABLE matrix_db.observations
    ADD COLUMN revision integer NOT NULL DEFAULT 1,
    ADD COLUMN recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN is_withdrawn boolean NOT NULL DEFAULT false,
    ADD COLUMN change_reason text NOT NULL DEFAULT 'initial ingestion',
    ADD COLUMN changed_by text NOT NULL DEFAULT 'ingestion';
CREATE TABLE matrix_db.observation_revisions (
    dataset_id text NOT NULL,
    id text NOT NULL,
    revision integer NOT NULL,
    recorded_at timestamptz NOT NULL,
    variable_id text NOT NULL,
    valid_time timestamptz NOT NULL,
    longitude double precision NOT NULL,
    latitude double precision NOT NULL,
    location geometry(Point,4326) NOT NULL,
    record jsonb NOT NULL,
    import_job_id text REFERENCES matrix_db.import_jobs(id),
    is_withdrawn boolean NOT NULL,
    change_reason text NOT NULL,
    changed_by text NOT NULL,
    PRIMARY KEY(dataset_id,id,revision),
    FOREIGN KEY(dataset_id,id) REFERENCES matrix_db.observations(dataset_id,id)
);
-- Pre-existing data becomes historically known at migration time, not at its source timestamp.
INSERT INTO matrix_db.observation_revisions SELECT dataset_id,id,revision,recorded_at,
    variable_id,valid_time,longitude,latitude,location,record,import_job_id,is_withdrawn,change_reason,changed_by
    FROM matrix_db.observations;
CREATE INDEX observation_revisions_asof_idx
    ON matrix_db.observation_revisions(dataset_id,id,recorded_at DESC,revision DESC);
CREATE FUNCTION matrix_db.prepare_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.dataset_id <> OLD.dataset_id OR NEW.id <> OLD.id THEN
            RAISE EXCEPTION 'observation identity cannot change';
        END IF;
        NEW.revision := OLD.revision + 1;
        NEW.recorded_at := greatest(clock_timestamp(), OLD.recorded_at + interval '1 microsecond');
    ELSE
        NEW.revision := 1;
        NEW.recorded_at := clock_timestamp();
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER prepare_revision BEFORE INSERT OR UPDATE ON matrix_db.observations
    FOR EACH ROW EXECUTE FUNCTION matrix_db.prepare_revision();
CREATE FUNCTION matrix_db.capture_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO matrix_db.observation_revisions VALUES (NEW.dataset_id,NEW.id,NEW.revision,
        NEW.recorded_at,NEW.variable_id,NEW.valid_time,NEW.longitude,NEW.latitude,NEW.location,
        NEW.record,NEW.import_job_id,NEW.is_withdrawn,NEW.change_reason,NEW.changed_by);
    RETURN NEW;
END $$;
CREATE TRIGGER capture_revision AFTER INSERT OR UPDATE ON matrix_db.observations
    FOR EACH ROW EXECUTE FUNCTION matrix_db.capture_revision();
CREATE FUNCTION matrix_db.reject_revision_edit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'revision history is append-only';
END $$;
CREATE TRIGGER immutable_revisions BEFORE UPDATE OR DELETE ON matrix_db.observation_revisions
    FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();

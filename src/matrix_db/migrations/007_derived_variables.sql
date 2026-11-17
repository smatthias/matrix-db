CREATE TABLE matrix_db.derived_definitions (
    id text NOT NULL,
    version integer NOT NULL CHECK(version>0),
    definition jsonb NOT NULL,
    checksum text NOT NULL,
    PRIMARY KEY(id,version)
);
CREATE TABLE matrix_db.derived_dependencies (
    id text NOT NULL,
    version integer NOT NULL,
    input_name text NOT NULL,
    dependency_id text NOT NULL,
    dependency_version integer NOT NULL,
    PRIMARY KEY(id,version,input_name),
    FOREIGN KEY(id,version) REFERENCES matrix_db.derived_definitions(id,version),
    FOREIGN KEY(dependency_id,dependency_version) REFERENCES matrix_db.derived_definitions(id,version)
);
CREATE TABLE matrix_db.derived_associations (
    id text PRIMARY KEY,
    subject_id text NOT NULL,
    subject_version integer NOT NULL,
    object_id text NOT NULL,
    object_version integer NOT NULL,
    kind text NOT NULL CHECK(kind='association'),
    description text NOT NULL,
    FOREIGN KEY(subject_id,subject_version) REFERENCES matrix_db.derived_definitions(id,version),
    FOREIGN KEY(object_id,object_version) REFERENCES matrix_db.derived_definitions(id,version)
);
CREATE TABLE matrix_db.derived_cache (
    cache_key text PRIMARY KEY,
    output_version uuid NOT NULL REFERENCES matrix_db.array_versions(version_id),
    provenance jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE matrix_db.derived_cache_sources (
    cache_key text NOT NULL REFERENCES matrix_db.derived_cache(cache_key),
    binding text NOT NULL,
    source_version uuid NOT NULL REFERENCES matrix_db.array_versions(version_id),
    PRIMARY KEY(cache_key,binding)
);
CREATE TABLE matrix_db.derived_cache_definitions (
    cache_key text NOT NULL REFERENCES matrix_db.derived_cache(cache_key),
    id text NOT NULL,
    version integer NOT NULL,
    PRIMARY KEY(cache_key,id,version),
    FOREIGN KEY(id,version) REFERENCES matrix_db.derived_definitions(id,version)
);
CREATE TRIGGER immutable_definitions BEFORE UPDATE OR DELETE ON matrix_db.derived_definitions
    FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_dependencies BEFORE UPDATE OR DELETE ON matrix_db.derived_dependencies
    FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_associations BEFORE UPDATE OR DELETE ON matrix_db.derived_associations
    FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_derived_cache BEFORE UPDATE OR DELETE ON matrix_db.derived_cache
    FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();

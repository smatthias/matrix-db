CREATE INDEX observations_dataset_variable_time_idx
    ON matrix_db.observations(dataset_id, variable_id, valid_time, id);
CREATE INDEX observations_time_idx ON matrix_db.observations(valid_time);
CREATE INDEX observations_location_idx ON matrix_db.observations USING gist(location);

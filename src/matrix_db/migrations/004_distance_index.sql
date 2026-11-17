-- Radius filtering uses geodesic metres and needs a matching geography index.
CREATE INDEX observations_geography_idx ON matrix_db.observations USING gist ((location::geography));

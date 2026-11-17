"""Transactional PostgreSQL/PostGIS persistence. Connections are short lived."""
from hashlib import sha256
from importlib.resources import files
import os
import psycopg
from psycopg.types.json import Jsonb
from .catalogue import Catalogue, CatalogueEntry
from .contracts import Observation

LATEST_VERSION = 8


def connect():
    dsn = os.environ.get('MATRIX_DB_DATABASE_URL')
    if not dsn:
        raise ValueError('MATRIX_DB_DATABASE_URL is required for database commands')
    return psycopg.connect(dsn, connect_timeout=5)


def migrate(connection, target=LATEST_VERSION):
    """Apply append-only, checksummed migrations atomically under a transaction lock."""
    if target not in range(1, LATEST_VERSION + 1):
        raise ValueError('unsupported migration target')
    with connection.transaction():
        connection.execute('SELECT pg_advisory_xact_lock(734192001)')
        connection.execute('CREATE SCHEMA IF NOT EXISTS matrix_db')
        connection.execute('''CREATE TABLE IF NOT EXISTS matrix_db.schema_migrations (
            version integer PRIMARY KEY, checksum text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT now())''')
        applied = dict(connection.execute('SELECT version, checksum FROM matrix_db.schema_migrations').fetchall())
        if any(version > target for version in applied):
            raise ValueError('database is newer than requested target; downgrades are unsupported')
        resources = sorted(files('matrix_db').joinpath('migrations').iterdir(), key=lambda p:p.name)
        known = {}
        for resource in resources:
            if resource.name.endswith('.sql'):
                version = int(resource.name.split('_')[0])
                sql = resource.read_text(encoding='utf-8')
                known[version] = (sha256(sql.encode()).hexdigest(), sql)
        for version, checksum in applied.items():
            if version not in known or checksum != known[version][0]:
                raise ValueError('migration history differs from installed package')
        if set(applied) != set(range(1, len(applied)+1)):
            raise ValueError('migration history is not contiguous')
        for version in range(1, target+1):
            if version not in applied:
                checksum, sql = known[version]
                connection.execute(sql)
                connection.execute('INSERT INTO matrix_db.schema_migrations(version, checksum) VALUES (%s,%s)', (version, checksum))
    return target


class PostgresStore:
    """Caller owns connection lifetime; each operation is atomic (nested transactions use savepoints)."""
    def __init__(self, connection):
        self.connection = connection

    def register(self, entry: CatalogueEntry):
        entry = CatalogueEntry.model_validate_json(entry.model_dump_json())
        with self.connection.transaction():
            self.connection.execute('INSERT INTO matrix_db.sources(provider, source_id) VALUES (%s,%s) ON CONFLICT DO NOTHING',
                                    (entry.dataset.provider, entry.source_id))
            self.connection.execute('INSERT INTO matrix_db.datasets(id,provider,source_id,entry) VALUES (%s,%s,%s,%s)',
                                    (entry.dataset.id, entry.dataset.provider, entry.source_id, Jsonb(entry.model_dump(mode='json'))))
            self._write_variables(entry)
        return entry

    def _write_variables(self, entry):
        for variable in entry.dataset.variables:
            self.connection.execute('''INSERT INTO matrix_db.variables(dataset_id,id,definition) VALUES (%s,%s,%s)
                ON CONFLICT(dataset_id,id) DO UPDATE SET definition=EXCLUDED.definition''',
                (entry.dataset.id, variable.id, Jsonb(variable.model_dump(mode='json'))))

    def get(self, dataset_id):
        row = self.connection.execute('SELECT entry FROM matrix_db.datasets WHERE id=%s', (dataset_id,)).fetchone()
        if row is None:
            raise KeyError(dataset_id)
        return CatalogueEntry.model_validate(row[0])

    def search(self, **filters):
        # Small-catalogue baseline. SQL filter pushdown is a later optimisation.
        entries = [CatalogueEntry.model_validate(row[0]) for row in
                   self.connection.execute('SELECT entry FROM matrix_db.datasets ORDER BY id').fetchall()]
        return Catalogue(entries).search(**filters)

    def update(self, dataset_id, replacement: CatalogueEntry):
        replacement = CatalogueEntry.model_validate_json(replacement.model_dump_json())
        if dataset_id != replacement.dataset.id:
            raise ValueError('updates must preserve dataset ID')
        with self.connection.transaction():
            row = self.connection.execute('SELECT entry FROM matrix_db.datasets WHERE id=%s FOR UPDATE', (dataset_id,)).fetchone()
            if row is None:
                raise KeyError(dataset_id)
            old = CatalogueEntry.model_validate(row[0])
            # Existing measurements must keep their variable definitions and validation context.
            has_records = self.connection.execute('SELECT 1 FROM matrix_db.observations WHERE dataset_id=%s LIMIT 1', (dataset_id,)).fetchone()
            has_arrays = self.connection.execute('SELECT 1 FROM matrix_db.array_versions WHERE dataset_id=%s LIMIT 1', (dataset_id,)).fetchone()
            has_scenarios = self.connection.execute("SELECT 1 FROM matrix_db.scenarios WHERE definition->'grid'->>'dataset_id'=%s LIMIT 1", (dataset_id,)).fetchone()
            protected = ('variables','coverage','coordinate_system','origin')
            if (has_records or has_arrays or has_scenarios) and any(getattr(old.dataset,k) != getattr(replacement.dataset,k) for k in protected):
                raise ValueError('cannot change measurement contracts after data publication')
            self.connection.execute('INSERT INTO matrix_db.sources(provider,source_id) VALUES (%s,%s) ON CONFLICT DO NOTHING',
                                    (replacement.dataset.provider,replacement.source_id))
            self.connection.execute('UPDATE matrix_db.datasets SET provider=%s,source_id=%s,entry=%s WHERE id=%s',
                                    (replacement.dataset.provider,replacement.source_id,Jsonb(replacement.model_dump(mode='json')),dataset_id))
            self.connection.execute('DELETE FROM matrix_db.variables WHERE dataset_id=%s AND NOT(id=ANY(%s))',
                                    (dataset_id,[v.id for v in replacement.dataset.variables]))
            self._write_variables(replacement)
        return replacement

    def add_observations(self, dataset_id, observations):
        with self.connection.transaction():
            row = self.connection.execute('SELECT entry FROM matrix_db.datasets WHERE id=%s FOR SHARE', (dataset_id,)).fetchone()
            if row is None:
                raise KeyError(dataset_id)
            dataset = CatalogueEntry.model_validate(row[0]).dataset
            for record in observations:
                record = Observation.model_validate_json(record.model_dump_json()).validate_against(dataset)
                self.connection.execute('''INSERT INTO matrix_db.observations
                    (dataset_id,id,variable_id,valid_time,longitude,latitude,record) VALUES (%s,%s,%s,%s,%s,%s,%s)''',
                    (dataset_id,record.id,record.variable_id,record.valid_time,record.longitude,record.latitude,Jsonb(record.model_dump(mode='json'))))

    def observations(self, dataset_id):
        # Bounded query API follows in Steps 4–6. This method is for the initial example.
        return [Observation.model_validate(row[0]) for row in self.connection.execute(
            'SELECT record FROM matrix_db.observations WHERE dataset_id=%s AND NOT is_withdrawn ORDER BY valid_time,id', (dataset_id,)).fetchall()]

    def query_observations(self, query):
        from .queries import query_observations
        return query_observations(self.connection, query)

    def revise(self, observation, *, expected_revision, reason, actor):
        from .temporal import revise
        return revise(self.connection, observation, expected_revision=expected_revision, reason=reason, actor=actor)

    def withdraw(self, dataset_id, observation_id, *, expected_revision, reason, actor):
        from .temporal import withdraw
        return withdraw(self.connection, dataset_id, observation_id, expected_revision=expected_revision, reason=reason, actor=actor)

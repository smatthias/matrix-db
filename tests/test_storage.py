"""Integration tests use a dedicated, disposable database (never a production DSN)."""
import json
import os
from pathlib import Path
import unittest
import psycopg
from matrix_db.contracts import Bundle
from matrix_db.catalogue import CatalogueEntry
from matrix_db.storage import migrate, PostgresStore

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'

@unittest.skipUnless(DSN, 'MATRIX_DB_TEST_DATABASE_URL is required for PostgreSQL integration tests')
class StorageTest(unittest.TestCase):
    def setUp(self):
        self.connection=psycopg.connect(DSN,autocommit=True)
        self.addCleanup(self.connection.close)
        self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
        migrate(self.connection)
        self.store=PostgresStore(self.connection)
        self.bundle=Bundle.model_validate_json(FIXTURE.read_text())
        self.entry=CatalogueEntry(dataset=self.bundle.dataset,source_id='synthetic-weather')

    def test_upgrade_and_idempotency(self):
        self.connection.execute('DROP SCHEMA matrix_db CASCADE')
        migrate(self.connection,target=1)
        self.store.register(self.entry)
        self.store.add_observations(self.entry.dataset.id,self.bundle.observations)
        migrate(self.connection)
        migrate(self.connection)
        self.assertEqual(len(self.store.observations(self.entry.dataset.id)),3)
        indexes=self.connection.execute("SELECT indexname FROM pg_indexes WHERE schemaname='matrix_db'").fetchall()
        self.assertIn(('observations_location_idx',),indexes)

    def test_reconnect_persists(self):
        self.store.register(self.entry)
        self.store.add_observations(self.entry.dataset.id,self.bundle.observations)
        with psycopg.connect(DSN) as connection:
            store=PostgresStore(connection)
            self.assertEqual(store.get(self.entry.dataset.id),self.entry)
            self.assertEqual(store.observations(self.entry.dataset.id),self.bundle.observations)
            self.assertEqual(len(store.search(variable='humidity')),1)

    def test_batch_rolls_back_on_duplicate(self):
        self.store.register(self.entry)
        with self.assertRaises(psycopg.errors.UniqueViolation):
            self.store.add_observations(self.entry.dataset.id,[self.bundle.observations[0]]*2)
        self.assertEqual(self.store.observations(self.entry.dataset.id),[])

    def test_outer_transaction_rolls_back_registration(self):
        with self.assertRaises(RuntimeError):
            with self.connection.transaction():
                self.store.register(self.entry)
                raise RuntimeError('interrupted import')
        with self.assertRaises(KeyError):self.store.get(self.entry.dataset.id)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM matrix_db.sources').fetchone()[0],0)

    def test_unique_source_and_foreign_keys(self):
        self.store.register(self.entry)
        duplicate=self.entry.model_copy(deep=True);duplicate.dataset.id='duplicate'
        with self.assertRaises(psycopg.errors.UniqueViolation): self.store.register(duplicate)
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            self.connection.execute("INSERT INTO matrix_db.variables VALUES ('absent','x','{}')")

    def test_update_preserves_measurement_contract(self):
        self.store.register(self.entry)
        self.store.add_observations(self.entry.dataset.id,self.bundle.observations)
        replacement=self.entry.model_copy(deep=True);replacement.status='retired'
        self.store.update(replacement.dataset.id,replacement)
        self.assertEqual(self.store.get(replacement.dataset.id).status,'retired')
        replacement.dataset.variables[0].unit='K'
        with self.assertRaises(ValueError): self.store.update(replacement.dataset.id,replacement)
        self.assertEqual(self.store.get(replacement.dataset.id).dataset.variables[0].unit,'degC')

    def test_invalid_batch_and_checksum(self):
        self.store.register(self.entry)
        invalid=self.bundle.observations[1].model_copy(update={'unit':'invalid'})
        with self.assertRaises(ValueError):self.store.add_observations(self.entry.dataset.id,[self.bundle.observations[0],invalid])
        self.assertEqual(self.store.observations(self.entry.dataset.id),[])
        self.connection.execute("UPDATE matrix_db.schema_migrations SET checksum='wrong' WHERE version=1")
        with self.assertRaises(ValueError):migrate(self.connection)

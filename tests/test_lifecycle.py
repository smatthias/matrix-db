from datetime import datetime,timedelta,timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4
import numpy as np
import psycopg
from matrix_db.arrays import ArrayStore,GridSpec,SubsetQuery,array_lock_id,canonical,digest
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle
from matrix_db.storage import migrate,PostgresStore
from matrix_db.lifecycle import RetentionPolicy,roots,checked_files,register_policy,create_plan,apply_plan,move_tier,evict_hot,set_hold,export_revisions,verify_revision_archive

UTC=timezone.utc
DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'

class LifecycleFileTest(unittest.TestCase):
    def test_roots_and_policy_validation(self):
        for a,b in [('/tmp/a','/tmp/a'),('/tmp/a','/tmp/a/cold'),('/tmp/a/cold','/tmp/a')]:
            with self.assertRaises(ValueError):roots(a,b)
        with self.assertRaises(ValueError):RetentionPolicy(dataset_id='data',keep_latest=0)
    def test_integrity_rejects_missing_extra_symlink_and_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);manifest={'chunks':[]};(root/'manifest.json').write_text(canonical(manifest));checksum=digest(root/'manifest.json')
            checked_files(root,manifest,checksum)
            (root/'extra').write_text('data')
            with self.assertRaises(ValueError):checked_files(root,manifest,checksum)
            (root/'extra').unlink();(root/'link').symlink_to(root/'manifest.json')
            with self.assertRaises(ValueError):checked_files(root,manifest,checksum)
            (root/'link').unlink()
            with self.assertRaises(ValueError):checked_files(root,{'chunks':[{'path':'../bad','checksum':'x'}]},checksum)

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class LifecycleStorageTest(unittest.TestCase):
    def setUp(self):
        self.db=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.db.close)
        self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.db)
        self.factory=lambda:psycopg.connect(DSN)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.hot=Path(self.temp.name)/'hot';self.cold=Path(self.temp.name)/'cold'
        self.bundle=Bundle.model_validate_json(FIXTURE.read_text());self.store=PostgresStore(self.db)
        self.store.register(CatalogueEntry(dataset=self.bundle.dataset,source_id='lifecycle'))
        self.store.add_observations('demo-weather',self.bundle.observations)
        self.arrays=ArrayStore(self.db,self.hot,archive_root=self.cold)
        self.spec=GridSpec(dataset_id='demo-weather',axes=[{'name':'lat','kind':'latitude','unit':'degrees_north','values':[52.25,52.75]},
            {'name':'lon','kind':'longitude','unit':'degrees_east','values':[13.25,13.75]}],variables=[{'id':'temperature','unit':'degC','dtype':'float64','dimensions':['lat','lon'],'chunks':[1,1]}])
        self.first=self.publish(10);self.latest=self.publish(20)
        self.future=datetime.now(UTC)+timedelta(days=40)
        self.policy=RetentionPolicy(dataset_id='demo-weather',hot_days=30,keep_latest=1)
        register_policy(self.db,self.policy)
    def tearDown(self):self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
    def publish(self,value):return self.arrays.publish(self.spec,lambda name,slices:(np.full((1,1),float(value)),np.zeros((1,1),dtype=bool)))
    def plan(self):return create_plan(self.db,'demo-weather',self.future)
    def apply(self,plan):
        with patch('matrix_db.lifecycle.datetime',wraps=datetime) as clock:
            clock.now.return_value=self.future
            return apply_plan(self.factory,plan['id'],self.hot,self.cold,'test')
    def evict(self,version):
        with patch('matrix_db.lifecycle.datetime',wraps=datetime) as clock:
            clock.now.return_value=self.future
            return evict_hot(self.db,version,self.hot,self.cold,'test')
    def test_plan_dry_run_latest_age_hold_and_policy_binding(self):
        young=create_plan(self.db,'demo-weather');self.assertTrue(all(e['action']=='keep' for e in young['entries']))
        planned=self.plan();self.assertEqual([e['action'] for e in planned['entries']],['keep','archive'])
        self.assertFalse((self.cold/'versions'/str(self.first)).exists())
        set_hold(self.db,self.first,True,'investigation','test')
        self.assertEqual(self.apply(planned)['results'][0]['reason'],'legal_hold')
        set_hold(self.db,self.first,False,'released','test')
        register_policy(self.db,self.policy.model_copy(update={'hot_days':60}))
        with self.assertRaises(ValueError):self.apply(planned)
    def test_archive_readthrough_evict_and_restore_preserve_identity(self):
        plan=self.plan();self.apply(plan)
        self.assertTrue((self.hot/'versions'/str(self.first)).exists())
        self.evict(self.first)
        self.assertFalse((self.hot/'versions'/str(self.first)).exists())
        result=self.arrays.read(SubsetQuery(version_id=self.first));np.testing.assert_allclose(result['variables']['temperature']['data'],10)
        move_tier(self.db,self.first,self.hot,self.cold,'test',restore=True)
        np.testing.assert_allclose(self.arrays.read(SubsetQuery(version_id=self.first))['variables']['temperature']['data'],10)
        self.assertTrue((self.cold/'versions'/str(self.first)).exists())
        self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.array_versions').fetchone()[0],2)
    def test_committed_routing_required_before_hot_eviction(self):
        with self.factory() as connection,connection.transaction():
            move_tier(connection,self.first,self.hot,self.cold,'test')
            with patch('matrix_db.lifecycle.datetime',wraps=datetime) as clock:
                clock.now.return_value=self.future
                with self.assertRaises(ValueError):evict_hot(connection,self.first,self.hot,self.cold,'test')
        self.assertTrue((self.hot/'versions'/str(self.first)).exists())
    def test_failed_archival_database_rollback_can_retry_verified_copy(self):
        with self.assertRaises(RuntimeError):
            with self.factory() as connection,connection.transaction():
                move_tier(connection,self.first,self.hot,self.cold,'test');raise RuntimeError('rollback')
        self.assertIsNone(self.db.execute('SELECT tier FROM matrix_db.array_locations WHERE version_id=%s',(self.first,)).fetchone())
        move_tier(self.db,self.first,self.hot,self.cold,'test')
        self.assertEqual(self.db.execute('SELECT tier FROM matrix_db.array_locations WHERE version_id=%s',(self.first,)).fetchone()[0],'archive')
    def test_corrupt_archive_blocks_eviction_without_changing_readable_hot(self):
        self.apply(self.plan())
        cold=self.cold/'versions'/str(self.first);chunk=next(cold.rglob('*.npz'));chunk.write_bytes(b'corrupt')
        with self.assertRaises(ValueError):self.evict(self.first)
        self.assertTrue((self.hot/'versions'/str(self.first)).exists())
        with self.assertRaises(ValueError):self.arrays.read(SubsetQuery(version_id=self.first))
    def test_shared_reader_lock_blocks_archival_and_eviction(self):
        with self.factory() as reader,reader.transaction():
            ArrayStore(reader,self.hot,archive_root=self.cold).read(SubsetQuery(version_id=self.first))
            with self.assertRaises(ValueError):move_tier(self.db,self.first,self.hot,self.cold,'test')
        self.apply(self.plan())
        with self.factory() as reader,reader.transaction():
            ArrayStore(reader,self.hot,archive_root=self.cold).manifest(self.first)
            with self.assertRaises(ValueError):self.evict(self.first)
    def test_autocommit_read_holds_lock_while_chunks_are_loaded(self):
        attempts=[]
        def concurrent_archive():
            if attempts:return
            attempts.append(True)
            with self.factory() as connection:
                with self.assertRaises(ValueError):move_tier(connection,self.first,self.hot,self.cold,'test')
        self.arrays.check_cancel=concurrent_archive
        self.arrays.read(SubsetQuery(version_id=self.first))
        self.assertEqual(attempts,[True])

    def test_crash_during_eviction_is_resumable_and_archive_stays_readable(self):
        self.apply(self.plan())
        with patch('matrix_db.lifecycle.shutil.rmtree',side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):self.evict(self.first)
        self.assertFalse((self.hot/'versions'/str(self.first)).exists())
        np.testing.assert_allclose(self.arrays.read(SubsetQuery(version_id=self.first))['variables']['temperature']['data'],10)
        self.evict(self.first);self.assertFalse((self.hot/'.evicted'/str(self.first)).exists())
    def test_hold_and_new_pins_are_rechecked(self):
        # Derived cache references use the same foreign-key protection as real derived outputs.
        self.db.execute('INSERT INTO matrix_db.derived_cache(cache_key,output_version,provenance) VALUES (%s,%s,%s)',('pin',self.first,psycopg.types.json.Jsonb({})))
        entries=self.plan()['entries'];self.assertEqual(entries[1]['reason'],'pinned')
        permissive=self.policy.model_copy(update={'keep_pinned':False});register_policy(self.db,permissive)
        self.apply(self.plan())
        set_hold(self.db,self.first,True,'legal case','test')
        with self.assertRaises(ValueError):self.evict(self.first)
    def test_revision_export_verification_and_append_only_history(self):
        before=datetime.now(UTC)+timedelta(days=1)
        with self.factory() as connection:archive=export_revisions(connection,'demo-weather',before,self.cold,'test')
        self.assertEqual(archive['row_count'],3)
        self.assertTrue(verify_revision_archive(self.db,archive['id'],self.cold)['verified'])
        self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.observation_revisions').fetchone()[0],3)
        path=self.cold/'revisions'/(archive['id']+'.jsonl.gz');path.write_bytes(b'corrupt')
        with self.assertRaises(ValueError):verify_revision_archive(self.db,archive['id'],self.cold)
    def test_export_failure_rolls_back_manifest_and_audit_in_autocommit_mode(self):
        before=datetime.now(UTC)+timedelta(days=1)
        with patch('matrix_db.lifecycle.audit',side_effect=RuntimeError('interrupted')):
            with self.assertRaises(RuntimeError):export_revisions(self.db,'demo-weather',before,self.cold,'test')
        self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.revision_archives').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.observation_revisions').fetchone()[0],3)

    def test_missing_archive_config_and_archive_byte_budget_fail_closed(self):
        self.apply(self.plan())
        with patch.dict(os.environ,{'MATRIX_DB_ARCHIVE_ROOT':''}):
            with self.assertRaises(OSError):ArrayStore(self.db,self.hot).manifest(self.first)
        with patch('matrix_db.lifecycle.MAX_ARCHIVE_BYTES',1):
            with self.assertRaises(ValueError):move_tier(self.db,self.latest,self.hot,self.cold,'test')
        self.assertTrue((self.hot/'versions'/str(self.latest)).exists())

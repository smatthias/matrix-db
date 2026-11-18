from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from uuid import uuid4
import numpy as np
import psycopg
from fastapi.testclient import TestClient
from matrix_db.api import create_app
from matrix_db.api_auth import Settings
from matrix_db.api_jobs import submit,cancel
from matrix_db.arrays import ArrayStore,GridSpec
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle
from matrix_db.storage import migrate,PostgresStore
from matrix_db.quotas import ResourceLimits,QuotaExceeded,charge_request,reconcile_arrays,report

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
TOKEN='quota-test-admin-token-at-least-24-characters'
FIXED=datetime(2030,1,1,12,0,30,tzinfo=timezone.utc)
class ResourceContractTest(unittest.TestCase):
    def test_client_surfaces_retry_after_without_replaying_capacity_errors(self):
        import httpx
        from matrix_db.client import MatrixDBClient,MatrixDBError
        calls=[]
        def limited(request):
            calls.append(request);return httpx.Response(429,headers={'Retry-After':'12'},json={'error':{'code':'quota_exceeded','message':'capacity reached'}})
        with MatrixDBClient('https://matrix.invalid',TOKEN,read_retries=3,transport=httpx.MockTransport(limited)) as client:
            with self.assertRaises(MatrixDBError) as error:client.query({'dataset_id':'demo-weather'})
        self.assertEqual(error.exception.retry_after,12);self.assertEqual(len(calls),1)

    def test_configuration_rejects_invalid_or_unknown_controls(self):
        for value in [{'requests_per_minute':-1},{'observations_per_batch':10001},{'concurrent_http_requests':0},{'unknown':1}]:
            with self.assertRaises(ValueError):ResourceLimits(**value)
        self.assertEqual(ResourceLimits(requests_per_minute=0).requests_per_minute,0)
        with patch.dict(os.environ,{'MATRIX_DB_RESOURCE_LIMITS':'[]'}):
            with self.assertRaises(ValueError):Settings([{'token':TOKEN,'subject':'admin','role':'admin'}],'/tmp/root')

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class ResourceStorageTest(unittest.TestCase):
    def setUp(self):
        self.db=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.db.close)
        self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.db)
        self.factory=lambda:psycopg.connect(DSN)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.bundle=Bundle.model_validate_json((Path(__file__).resolve().parents[1]/'examples/weather.json').read_text())
        self.store=PostgresStore(self.db);self.store.register(CatalogueEntry(dataset=self.bundle.dataset,source_id='quotas'))
        self.settings=Settings([{'token':TOKEN,'subject':'admin','role':'admin'}],self.temp.name);self.principal=self.settings.authenticate(TOKEN)
        self.spec=GridSpec(dataset_id='demo-weather',axes=[{'name':'lat','kind':'latitude','unit':'degrees_north','values':[52.25,52.75]},{'name':'lon','kind':'longitude','unit':'degrees_east','values':[13.25,13.75]}],variables=[{'id':'temperature','unit':'degC','dtype':'float64','dimensions':['lat','lon'],'chunks':[1,1]}])
    def tearDown(self):self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
    def config(self,**values):return patch.dict(os.environ,{'MATRIX_DB_RESOURCE_LIMITS':json.dumps(values)})
    def publish(self,arrays=None):return (arrays or ArrayStore(self.db,self.temp.name)).publish(self.spec,lambda name,slices:(np.ones((1,1)),np.zeros((1,1),dtype=bool)))
    def test_atomic_rate_admission_and_window_reset(self):
        limits=ResourceLimits(requests_per_minute=3)
        def attempt(_):
            try:charge_request(self.factory,self.principal.credential_id,limits,at=FIXED);return True
            except QuotaExceeded:return False
        with ThreadPoolExecutor(max_workers=8) as executor:results=list(executor.map(attempt,range(12)))
        self.assertEqual(sum(results),3)
        self.assertEqual(self.db.execute('SELECT used FROM matrix_db.api_rate_windows').fetchone()[0],3)
        charge_request(self.factory,self.principal.credential_id,limits,at=FIXED.replace(minute=1))
        self.assertEqual(self.db.execute('SELECT used FROM matrix_db.api_rate_windows').fetchone()[0],1)
        charge_request(self.factory,self.principal.credential_id,limits,at=FIXED)
        self.assertEqual(self.db.execute('SELECT used FROM matrix_db.api_rate_windows').fetchone()[0],2)
    def test_parallel_job_capacity_and_terminal_release(self):
        def attempt(_):
            try:
                with self.factory() as connection:return submit(connection,'observations',{'dataset_id':'demo-weather'},self.principal)['id']
            except QuotaExceeded:return None
        with self.config(active_jobs_per_credential=2,active_jobs_global=3):
            with ThreadPoolExecutor(max_workers=6) as executor:ids=[value for value in executor.map(attempt,range(8)) if value]
            self.assertEqual(len(ids),2)
            with self.factory() as connection:cancel(connection,ids[0],self.principal)
            self.assertIsNotNone(attempt(0))
    def test_parallel_dataset_capacity_and_revision_limits(self):
        def attempt(index):
            record=self.bundle.observations[0].model_copy(update={'id':'quota-'+str(index)})
            try:
                with self.factory() as connection:PostgresStore(connection).add_observations('demo-weather',[record])
                return True
            except QuotaExceeded:return False
        with self.config(observations_per_dataset=3,revisions_per_dataset=4):
            with ThreadPoolExecutor(max_workers=6) as executor:results=list(executor.map(attempt,range(8)))
            self.assertEqual(sum(results),3)
            record=self.store.observations('demo-weather')[0]
            self.store.revise(record,expected_revision=1,reason='test',actor='test')
            with self.assertRaises(QuotaExceeded):self.store.withdraw('demo-weather',record.id,expected_revision=2,reason='test',actor='test')
            self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.observation_revisions').fetchone()[0],4)
    def test_batch_rejection_has_no_partial_records(self):
        with self.config(observations_per_batch=1):
            with self.assertRaises(QuotaExceeded):self.store.add_observations('demo-weather',self.bundle.observations)
        self.assertEqual(self.store.observations('demo-weather'),[])
    def test_array_versions_logical_bytes_and_legacy_reconciliation(self):
        first=self.publish();usage=report(self.db,'demo-weather',ResourceLimits())['usage'];self.assertGreater(usage['logical_array_bytes'],0)
        with self.config(array_versions_per_dataset=1):
            with self.assertRaises(QuotaExceeded):self.publish()
        size=usage['logical_array_bytes']
        with self.config(logical_array_bytes_per_dataset=size):
            with self.assertRaises(QuotaExceeded):self.publish()
        self.db.execute('DELETE FROM matrix_db.array_resources WHERE version_id=%s',(first,))
        with self.config(logical_array_bytes_per_dataset=10**7):
            with self.assertRaises(QuotaExceeded) as error:self.publish()
            self.assertEqual(error.exception.resource,'untracked_array_usage')
            with self.factory() as connection:reconcile_arrays(connection,'demo-weather',self.temp.name)
            self.publish()
        self.assertEqual(report(self.db,'demo-weather',ResourceLimits())['usage']['untracked_array_versions'],0)
    def test_notification_capacity_preserves_backlog_and_resumes(self):
        from matrix_db import notifications,quality
        p=quality.Policy(dataset_id='demo-weather',rules=[{'id':'count','kind':'completeness','variable_id':'temperature','expected_count':10,'min_fraction':1}])
        with self.factory() as connection:quality.register_policy(connection,p,'test')
        with self.factory() as connection:quality.evaluate(connection,'demo-weather')
        incident=self.db.execute('SELECT id FROM matrix_db.quality_incidents').fetchone()[0]
        with self.factory() as connection:quality.acknowledge(connection,'demo-weather',incident,'test')
        with self.config(inbox_items_per_owner=1,subscriptions_per_owner=1):
            settings=Settings([{'token':TOKEN,'subject':'admin','role':'admin'}],self.temp.name)
            request=notifications.Subscription(name='capacity',dataset_id='demo-weather',start='beginning',kinds=['opened','acknowledged'])
            with self.factory() as connection:sub=notifications.create(connection,request,self.principal)
            with self.assertRaises(QuotaExceeded):
                with self.factory() as connection:notifications.create(connection,request,self.principal)
            notifications.run_one(settings,self.factory)
            self.assertEqual(len(notifications.inbox(self.db,self.principal)['items']),1)
            self.assertEqual(self.db.execute('SELECT error_code FROM matrix_db.notification_subscriptions WHERE id=%s',(sub['id'],)).fetchone()[0],'inbox_capacity')
            cursor=self.db.execute('SELECT last_sequence FROM matrix_db.notification_subscriptions WHERE id=%s',(sub['id'],)).fetchone()[0]
            self.assertFalse(notifications.run_one(settings,self.factory))
            self.assertEqual(self.db.execute('SELECT last_sequence FROM matrix_db.notification_subscriptions WHERE id=%s',(sub['id'],)).fetchone()[0],cursor)
        settings.limits=ResourceLimits(inbox_items_per_owner=2)
        self.assertTrue(notifications.run_one(settings,self.factory))
        self.assertEqual(len(notifications.inbox(self.db,self.principal)['items']),2)

    def test_http_limit_survives_failed_work_and_returns_retry_headers(self):
        with self.config(requests_per_minute=2):
            settings=Settings([{'token':TOKEN,'subject':'admin','role':'admin'}],self.temp.name)
        original=charge_request
        def fixed(factory,credential,limits):return original(factory,credential,limits,at=FIXED)
        with patch('matrix_db.quotas.charge_request',side_effect=fixed),TestClient(create_app(settings,self.factory)) as client:
            auth={'Authorization':'Bearer '+TOKEN}
            self.assertEqual(client.get('/v1/arrays/'+str(uuid4()),headers=auth).status_code,404)
            self.assertEqual(client.get('/v1/datasets',headers=auth).status_code,200)
            response=client.get('/v1/datasets',headers={'Authorization':'bearer '+TOKEN})
            self.assertEqual(response.status_code,429);self.assertEqual(response.json()['error']['resource'],'requests_per_minute')
            self.assertIn('Retry-After',response.headers);self.assertIn('X-Request-ID',response.headers)
            self.assertEqual(client.get('/health/live').status_code,200)
    def test_process_execution_backpressure_and_liveness(self):
        with self.config(concurrent_http_requests=1):settings=Settings([{'token':TOKEN,'subject':'admin','role':'admin'}],self.temp.name)
        app=create_app(settings,self.factory);entered=threading.Event();release=threading.Event()
        from matrix_db.quotas import report as real_report
        def blocked(*args):entered.set();release.wait(timeout=10);return real_report(*args)
        with patch('matrix_db.quotas.report',side_effect=blocked),TestClient(app) as first,TestClient(app) as second,ThreadPoolExecutor(max_workers=1) as executor:
            auth={'Authorization':'Bearer '+TOKEN};pending=executor.submit(first.get,'/v1/admin/datasets/demo-weather/resources',headers=auth)
            try:
                self.assertTrue(entered.wait(timeout=5))
                response=second.get('/v1/datasets',headers=auth);self.assertEqual(response.status_code,503);self.assertEqual(response.json()['error']['code'],'server_busy')
                self.assertEqual(second.get('/health/live').status_code,200)
            finally:release.set()
            self.assertEqual(pending.result(timeout=10).status_code,200)

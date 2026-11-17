import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import numpy as np
import psycopg
import httpx
from fastapi.testclient import TestClient
from matrix_db.api import create_app
from matrix_db.api_auth import Settings
from matrix_db.api_jobs import run_one
from matrix_db.arrays import ArrayStore,GridSpec
from matrix_db.catalogue import CatalogueEntry
from matrix_db.client import MatrixDBClient,MatrixDBError
from matrix_db.contracts import Bundle
from matrix_db.storage import migrate,PostgresStore

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'
READER='unit-test-reader-token-over-24-chars'
WRITER='unit-test-writer-token-over-24-chars'
ADMIN='unit-test-admin-token-over-24-chars'
OTHER='unit-test-other-token-over-24-chars'
KEYS=[{'token':READER,'subject':'reader','role':'reader','datasets':['weather']},
      {'token':WRITER,'subject':'writer','role':'ingestor','datasets':['weather']},
      {'token':ADMIN,'subject':'admin','role':'admin'},
      {'token':OTHER,'subject':'other','role':'reader','datasets':['other']}]
def headers(token):return {'Authorization':'Bearer '+token}

class APIContractTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.settings=Settings(KEYS,self.temp.name)
        def forbidden_database():raise AssertionError('unauthorized request touched database')
        self.client=TestClient(create_app(self.settings,forbidden_database));self.addCleanup(self.client.close)
    def test_authentication_fail_closed_and_scopes(self):
        self.assertEqual(self.client.get('/health/live').status_code,200)
        self.assertEqual(self.client.get('/v1/datasets').status_code,401)
        self.assertEqual(self.client.get('/v1/datasets',headers=headers('wrong')).status_code,401)
        result=self.client.post('/v1/observations/query',headers=headers(READER),json={'dataset_id':'other'})
        self.assertEqual(result.status_code,403);self.assertEqual(result.json()['error']['code'],'forbidden')
        self.assertEqual(self.client.post('/v1/observations/query',headers=headers(READER),json={}).status_code,403)
        with self.assertRaises(ValueError):Settings([],self.temp.name)
        with self.assertRaises(ValueError):Settings([{'token':'short','subject':'x','role':'admin'}],self.temp.name)
    def test_openapi_validation_envelopes_and_body_limit(self):
        schema=self.client.get('/openapi.json').json()
        self.assertIn('/v1/jobs',schema['paths']);self.assertIn('/v1/arrays/subset',schema['paths'])
        result=self.client.post('/v1/observations/query',headers=headers(READER),json={'limit':0})
        self.assertEqual(result.status_code,422);self.assertIn('X-Request-ID',result.headers)
        self.assertNotIn('input',result.text)
        result=self.client.post('/v1/jobs',headers=headers(ADMIN),content=b'x'*(2*1024*1024+1))
        self.assertEqual(result.status_code,413)
        self.assertEqual(self.client.get('/not-a-route').json()['error']['code'],'http_error')
    def test_python_client_retries_only_reads_and_structured_errors(self):
        calls=[]
        def handler(request):
            calls.append(request)
            if len(calls)==1:raise httpx.ConnectError('offline')
            return httpx.Response(200,json={'items':[],'next_cursor':None})
        with MatrixDBClient('https://matrix.invalid',READER,transport=httpx.MockTransport(handler)) as client:
            self.assertEqual(list(client.pages({'dataset_id':'weather'}))[0]['items'],[])
        self.assertEqual(len(calls),2)
        calls=[]
        def failed(request):calls.append(request);raise httpx.ReadTimeout('private connection details')
        with MatrixDBClient('https://matrix.invalid',ADMIN,transport=httpx.MockTransport(failed),read_retries=3) as client:
            with self.assertRaises(MatrixDBError):client.create_run({})
        self.assertEqual(len(calls),1)
        with MatrixDBClient('https://matrix.invalid',READER,transport=httpx.MockTransport(lambda request:httpx.Response(403,json={'error':{'code':'forbidden','message':'access denied','request_id':'r1'}}))) as client:
            with self.assertRaises(MatrixDBError) as error:client.query({})
            self.assertEqual((error.exception.status,error.exception.request_id),(403,'r1'))

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class APIStorageTest(unittest.TestCase):
    def setUp(self):
        self.connection=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.connection.close)
        self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.connection)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.settings=Settings(KEYS,self.temp.name)
        self.factory=lambda:psycopg.connect(DSN)
        self.client=TestClient(create_app(self.settings,self.factory));self.addCleanup(self.client.close)
        bundle=Bundle.model_validate_json(FIXTURE.read_text());dataset=bundle.dataset.model_copy(deep=True);dataset.id='weather'
        self.bundle=bundle;self.store=PostgresStore(self.connection)
        self.store.register(CatalogueEntry(dataset=dataset,source_id='weather'))
        other=dataset.model_copy(deep=True);other.id='other';self.store.register(CatalogueEntry(dataset=other,source_id='other'))
        self.observations=[record.model_copy(update={'dataset_id':'weather'}) for record in bundle.observations]
        self.store.add_observations('weather',self.observations)
        self.arrays=ArrayStore(self.connection,self.temp.name)
        self.spec=GridSpec(dataset_id='weather',axes=[{'name':'lat','kind':'latitude','unit':'degrees_north','values':[52.25,52.75]},
            {'name':'lon','kind':'longitude','unit':'degrees_east','values':[13.25,13.75]}],variables=[{'id':'temperature','unit':'degC','dtype':'float64','dimensions':['lat','lon'],'chunks':[1,1]}])
        self.version=self.arrays.publish(self.spec,lambda variable,slices:(np.full((1,1),10.),np.zeros((1,1),dtype=bool)),provenance={'snapshot':{'private_record':'hidden-from-reader'}})
    def tearDown(self):self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
    def test_catalogue_query_stream_series_and_playback_workflow(self):
        def forward(request):
            result=self.client.request(request.method,str(request.url).replace('http://testserver',''),content=request.content,headers=dict(request.headers))
            return httpx.Response(result.status_code,json=result.json())
        with MatrixDBClient('http://testserver',READER,transport=httpx.MockTransport(forward)) as sdk:
            self.assertEqual(len(list(sdk.pages({'dataset_id':'weather','limit':1}))),3)
        result=self.client.get('/v1/datasets',headers=headers(READER))
        self.assertEqual(result.status_code,200);self.assertEqual([entry['dataset']['id'] for entry in result.json()['items']],['weather'])
        query={'dataset_id':'weather','limit':1}
        pages=[]
        while True:
            result=self.client.post('/v1/observations/query',headers=headers(READER),json=query)
            self.assertEqual(result.status_code,200,result.text);page=result.json();pages.extend(page['items'])
            if not page['next_cursor']:break
            query['cursor']=page['next_cursor']
        self.assertEqual(len(pages),3)
        stream=self.client.post('/v1/observations/stream',headers=headers(READER),json={'dataset_id':'weather','limit':2})
        lines=[json.loads(line) for line in stream.text.splitlines()]
        self.assertEqual([line['type'] for line in lines],['metadata','observation','observation'])
        base={'dataset_id':'weather','variable_id':'temperature','point':{'longitude':13.4,'latitude':52.5}}
        result=self.client.post('/v1/series',headers=headers(READER),json={**base,'time':{'start':'2026-11-17T08:00:00Z','end':'2026-11-17T10:00:00Z'},'bucket_seconds':3600})
        self.assertAlmostEqual(result.json()['buckets'][0]['mean'],15.2)
        result=self.client.post('/v1/playback',headers=headers(READER),json={**base,'at':'2026-11-17T08:05:00Z','tolerance_seconds':300})
        self.assertEqual(result.json()['offset_seconds'],-300)
    def test_ingestion_permissions_atomicity_and_provenance_redaction(self):
        record=self.observations[0].model_copy(update={'id':'new'})
        payload={'dataset_id':'weather','observations':[record.model_dump(mode='json')]}
        self.assertEqual(self.client.post('/v1/observations',headers=headers(READER),json=payload).status_code,403)
        self.assertEqual(self.client.post('/v1/observations',headers=headers(WRITER),json=payload).status_code,201)
        self.assertEqual(self.client.post('/v1/observations',headers=headers(WRITER),json=payload).status_code,409)
        result=self.client.post('/v1/arrays/subset',headers=headers(READER),json={'version_id':str(self.version)})
        self.assertEqual(result.status_code,200,result.text);self.assertIsNone(result.json()['provenance'])
        self.assertEqual(self.client.get('/v1/arrays/'+str(self.version),headers=headers(OTHER)).status_code,403)
        result=self.client.get('/v1/arrays/'+str(self.version),headers=headers(READER))
        self.assertNotIn('private_record',result.text)
        versions=self.client.get('/v1/datasets/weather/versions',headers=headers(READER)).json()
        self.assertEqual(versions['items'][0]['version_id'],str(self.version))
    def test_jobs_completion_ownership_revocation_and_queued_cancel(self):
        result=self.client.post('/v1/jobs',headers=headers(READER),json={'operation':'observations','payload':{'dataset_id':'weather','limit':1}})
        self.assertEqual(result.status_code,202,result.text);ident=result.json()['id']
        self.assertEqual(self.client.get('/v1/jobs/'+ident,headers=headers(OTHER)).status_code,403)
        self.assertTrue(run_one(self.settings,self.factory))
        completed=self.client.get('/v1/jobs/'+ident,headers=headers(READER)).json()
        self.assertEqual(completed['status'],'completed');self.assertEqual(len(completed['result']['items']),1)
        restricted=[{**value,'datasets':[]} if value['token']==READER else value for value in KEYS]
        with TestClient(create_app(Settings(restricted,self.temp.name),self.factory)) as client:
            self.assertEqual(client.get('/v1/jobs/'+ident,headers=headers(READER)).status_code,403)
        result=self.client.post('/v1/jobs',headers=headers(READER),json={'operation':'observations','payload':{'dataset_id':'weather'}})
        ident=result.json()['id'];self.client.post('/v1/jobs/'+ident+'/cancel',headers=headers(READER))
        self.assertFalse(run_one(self.settings,self.factory))
        self.assertEqual(self.client.get('/v1/jobs/'+ident,headers=headers(READER)).json()['status'],'cancelled')
    def test_running_cancellation_rolls_back_result(self):
        ident=self.client.post('/v1/jobs',headers=headers(READER),json={'operation':'observations','payload':{'dataset_id':'weather'}}).json()['id']
        from matrix_db.queries import query_observations
        def cancel_then_query(connection,query):
            result=self.client.post('/v1/jobs/'+ident+'/cancel',headers=headers(READER))
            self.assertEqual(result.status_code,200)
            return query_observations(connection,query)
        with patch('matrix_db.queries.query_observations',cancel_then_query):self.assertTrue(run_one(self.settings,self.factory))
        result=self.client.get('/v1/jobs/'+ident,headers=headers(READER)).json()
        self.assertEqual(result['status'],'cancelled');self.assertIsNone(result['result'])
    def test_admin_alignment_job_and_service_readiness(self):
        self.assertEqual(self.client.get('/v1/ready',headers=headers(READER)).status_code,200)
        dataset=self.store.get('weather').dataset;dataset.id='aligned';dataset.origin='simulation';dataset.variables[0].unit='K';dataset.variables[0].minimum=0;dataset.variables[0].maximum=400
        self.store.register(CatalogueEntry(dataset=dataset,source_id='aligned'))
        target=self.spec.model_copy(deep=True);target.dataset_id='aligned';target.variables[0].unit='K'
        payload={'target':target.model_dump(mode='json'),'inputs':[{'version_id':str(self.version),'variable_id':'temperature','output_variable':'temperature'}]}
        self.assertEqual(self.client.post('/v1/jobs',headers=headers(READER),json={'operation':'alignment','payload':payload}).status_code,403)
        result=self.client.post('/v1/jobs',headers=headers(ADMIN),json={'operation':'alignment','payload':payload})
        self.assertEqual(result.status_code,202,result.text);ident=result.json()['id']
        run_one(self.settings,self.factory)
        result=self.client.get('/v1/jobs/'+ident,headers=headers(ADMIN)).json()
        self.assertEqual(result['status'],'completed',result)
        field=self.client.post('/v1/arrays/subset',headers=headers(ADMIN),json={'version_id':result['result']['version_id']}).json()
        np.testing.assert_allclose(field['variables']['temperature']['data'],283.15)

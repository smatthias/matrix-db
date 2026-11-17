import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import psycopg
from fastapi.testclient import TestClient
from matrix_db.api import create_app
from matrix_db.api_auth import Settings
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle
from matrix_db.storage import migrate,PostgresStore
from matrix_db.administration import activity,MetadataChange

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'
ADMIN='administration-test-admin-token-over-24'
READER='administration-test-reader-token-over-24'
def auth(token):return {'Authorization':'Bearer '+token}

class AdministrationContractTest(unittest.TestCase):
    def test_admin_denied_before_database_and_invalid_metadata(self):
        with tempfile.TemporaryDirectory() as root:
            settings=Settings([{'token':READER,'subject':'reader','role':'reader','datasets':['demo-weather']}],root)
            def forbidden():raise AssertionError('unauthorized database access')
            with TestClient(create_app(settings,forbidden)) as client:
                for method,path in [('GET','/v1/admin/datasets/demo-weather'),('GET','/v1/admin/datasets/demo-weather/activity'),('POST','/v1/admin/datasets/demo-weather/retention-plan')]:
                    self.assertEqual(client.request(method,path,headers=auth(READER)).status_code,403)
        with self.assertRaises(ValueError):MetadataChange(expected_checksum='a'*64,name=' ',license='MIT',status='active')

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class AdministrationStorageTest(unittest.TestCase):
    def setUp(self):
        self.db=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.db.close)
        self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.db)
        self.factory=lambda:psycopg.connect(DSN)
        self.bundle=Bundle.model_validate_json(FIXTURE.read_text());self.store=PostgresStore(self.db)
        self.store.register(CatalogueEntry(dataset=self.bundle.dataset,source_id='admin-test'))
        self.store.add_observations('demo-weather',self.bundle.observations)
        other=self.bundle.dataset.model_copy(deep=True);other.id='private';other.name='Invisible data';self.store.register(CatalogueEntry(dataset=other,source_id='private'))
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.settings=Settings([{'token':ADMIN,'subject':'operator','role':'admin'},{'token':READER,'subject':'reader','role':'reader','datasets':['demo-weather']}],self.temp.name)
        self.client=TestClient(create_app(self.settings,self.factory));self.addCleanup(self.client.close)
    def tearDown(self):self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
    def overview(self):return self.client.get('/v1/admin/datasets/demo-weather',headers=auth(ADMIN)).json()
    def test_metadata_optimistic_conflict_audit_and_contract_preservation(self):
        overview=self.overview();entry=overview['entry'];payload={'expected_checksum':overview['checksum'],'name':'Renamed dataset','license':entry['dataset']['license'],'status':'stale','attribution':'USGS','contact':None}
        path='/v1/admin/datasets/demo-weather/metadata'
        result=self.client.post(path,headers=auth(ADMIN),json=payload);self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(self.client.post(path,headers=auth(ADMIN),json=payload).status_code,409)
        changed=self.store.get('demo-weather');self.assertEqual(changed.dataset.variables,self.bundle.dataset.variables)
        self.assertEqual(self.store.observations('demo-weather'),self.bundle.observations)
        log=activity(self.db,'demo-weather','audit')['items'];self.assertEqual(len(log),1);self.assertEqual(log[0]['actor'],'operator')
    def test_audit_failure_rolls_back_metadata(self):
        overview=self.overview();payload={'expected_checksum':overview['checksum'],'name':'No commit','license':'MIT','status':'active'}
        with patch('matrix_db.administration.audit',side_effect=ValueError('interrupted')):
            self.assertEqual(self.client.post('/v1/admin/datasets/demo-weather/metadata',headers=auth(ADMIN),json=payload).status_code,400)
        self.assertEqual(self.store.get('demo-weather').dataset.name,self.bundle.dataset.name)
    def test_policy_conflicts_and_non_destructive_preview(self):
        p={'dataset_id':'demo-weather','hot_days':30,'keep_latest':1,'keep_pinned':True}
        path='/v1/admin/datasets/demo-weather/retention-policy';change={'expected_revision':None,'policy':p}
        result=self.client.post(path,headers=auth(ADMIN),json=change);self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(self.client.post(path,headers=auth(ADMIN),json=change).status_code,409)
        preview=self.client.post('/v1/admin/datasets/demo-weather/retention-plan',headers=auth(ADMIN));self.assertEqual(preview.status_code,200,preview.text)
        self.assertFalse(preview.json()['deletes']);self.assertEqual(preview.json()['entries'],[])
        quality={'dataset_id':'demo-weather','rules':[{'id':'age','kind':'freshness','variable_id':'temperature','max_age_seconds':60}]}
        path='/v1/admin/datasets/demo-weather/quality-policy'
        self.assertEqual(self.client.post(path,headers=auth(ADMIN),json={'expected_revision':None,'policy':quality}).status_code,200)
        self.assertIsNotNone(self.overview()['quality_policy'])
    def test_job_listing_cursor_binding_and_cancel_dataset_checks(self):
        ids=[]
        for _ in range(3):ids.append(self.client.post('/v1/jobs',headers=auth(ADMIN),json={'operation':'observations','payload':{'dataset_id':'demo-weather'}}).json()['id'])
        first=activity(self.db,'demo-weather','jobs',limit=1);self.assertEqual(len(first['items']),1);self.assertIsNotNone(first['next_cursor'])
        second=activity(self.db,'demo-weather','jobs',first['next_cursor'],limit=2);self.assertEqual(len(second['items']),2)
        self.assertNotIn('request',first['items'][0]);self.assertNotIn('result',first['items'][0])
        with self.assertRaises(ValueError):activity(self.db,'demo-weather','imports',first['next_cursor'])
        path='/v1/admin/datasets/demo-weather/jobs/'+ids[0]+'/cancel'
        self.assertEqual(self.client.post(path,headers=auth(ADMIN)).status_code,200)
        self.assertEqual(self.client.post('/v1/admin/datasets/private/jobs/'+ids[1]+'/cancel',headers=auth(ADMIN)).status_code,404)
    def test_catalogue_text_pagination_and_scoped_search(self):
        result=self.client.post('/v1/datasets/search',headers=auth(READER),json={'text':'BERLIN','limit':1})
        self.assertEqual([e['dataset']['id'] for e in result.json()['items']],['demo-weather'])
        result=self.client.post('/v1/datasets/search',headers=auth(READER),json={'text':'Invisible'})
        self.assertEqual(result.json()['items'],[])
    def test_operational_views_omit_source_credentials_and_raw_import_errors(self):
        from psycopg.types.json import Jsonb
        self.db.execute('INSERT INTO matrix_db.provider_sources(id,config_hash,config,dataset_id) VALUES (%s,%s,%s,%s)',('source','hash',Jsonb({'provider':'usgs','credential_env':'PRIVATE_SECRET_NAME'}),'demo-weather'))
        self.db.execute("INSERT INTO matrix_db.import_jobs(id,dataset_id,source_checksum,dataset_checksum,format,status,error) VALUES ('import','demo-weather','x','y','json','failed','private upstream url with credential')")
        overview=self.overview();self.assertNotIn('SECRET',json.dumps(overview));self.assertNotIn('config_hash',json.dumps(overview))
        imports=activity(self.db,'demo-weather','imports');self.assertNotIn('credential',json.dumps(imports,default=str));self.assertNotIn('error',imports['items'][0])
        for kind in ['providers','storage','audit']:self.assertIsInstance(activity(self.db,'demo-weather',kind)['items'],list)

from datetime import datetime,timedelta,timezone
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
from matrix_db import hazards,notifications

UTC=timezone.utc
NOW=datetime(2030,1,1,12,tzinfo=UTC)
DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
BUNDLE=Bundle.model_validate_json((Path(__file__).resolve().parents[1]/'examples/weather.json').read_text())
ADMIN='hazard-admin-token-over-24-characters';READER='hazard-reader-token-over-24-characters';OTHER='hazard-other-token-over-24-characters'
KEYS=[{'token':ADMIN,'subject':'operator','role':'admin'},{'token':READER,'subject':'reader','role':'reader','datasets':['demo-weather']},{'token':OTHER,'subject':'other','role':'reader','datasets':['other']}]
def policy(**updates):
 rule={'id':'heat','name':'Synthetic threshold signal','hazard_type':'heat','variable_id':'temperature','unit':'degC','trigger_threshold':30,'release_threshold':25,'critical_threshold':40,'max_age_seconds':600,**updates}
 return hazards.Policy(dataset_id='demo-weather',lookback_seconds=3600,rules=[rule])
def sample(value=35.,ident='one',offset=0,**updates):
 return BUNDLE.observations[0].model_copy(update={'id':ident,'value':value,'valid_time':NOW+timedelta(seconds=offset),**updates})
def auth(token=ADMIN):return {'Authorization':'Bearer '+token}

class HazardContractTest(unittest.TestCase):
 def test_threshold_units_and_persistence_validation(self):
  for updates in [{'release_threshold':30},{'critical_threshold':29},{'minimum_duration_seconds':4000}]:
   with self.assertRaises(ValueError):policy(**updates)
 def test_detection_persistence_hysteresis_and_unknown(self):
  p=policy(minimum_samples=2,minimum_duration_seconds=60)
  result=hazards.assess(p,[(sample(35,offset=-60),1,NOW),(sample(36,ident='two'),1,NOW)],NOW)[0]
  self.assertEqual(result['state'],'detected');self.assertEqual(result['support_count'],2)
  self.assertEqual(hazards.assess(policy(),[(sample(27),1,NOW)],NOW)[0]['state'],'holding')
  self.assertEqual(hazards.assess(policy(),[(sample(20),1,NOW)],NOW)[0]['state'],'clear')
  self.assertEqual(hazards.assess(policy(),[(sample(50,offset=-1000),1,NOW)],NOW)[0]['state'],'unknown')
  first=hazards.assess(policy(aggregation='mean'),[(sample(35),1,NOW),(sample(29,ident='low'),1,NOW)],NOW)[0]
  changed=hazards.assess(policy(aggregation='mean'),[(sample(35),1,NOW),(sample(28,ident='low'),2,NOW)],NOW)[0]
  self.assertNotEqual(first['support_hash'],changed['support_hash'])
 def test_region_bad_flags_antimeridian_and_below_direction(self):
  p=policy(region={'west':170,'east':-170,'south':-90,'north':90})
  result=hazards.assess(p,[(sample(35,longitude=179),1,NOW),(sample(50,ident='bad',quality='bad'),1,NOW)],NOW)[0]
  self.assertEqual(result['support_count'],1)
  box=hazards.bounds([sample(longitude=179),sample(longitude=-179)]);self.assertGreater(box['west'],box['east'])
  below=policy(direction='below',trigger_threshold=0,release_threshold=2,critical_threshold=-5)
  self.assertEqual(hazards.assess(below,[(sample(-6),1,NOW)],NOW)[0]['severity'],'critical')

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class HazardStorageTest(unittest.TestCase):
 def setUp(self):
  self.db=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.db.close)
  self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.db)
  self.factory=lambda:psycopg.connect(DSN)
  self.store=PostgresStore(self.db);self.store.register(CatalogueEntry(dataset=BUNDLE.dataset,source_id='hazard-test'))
  self.store.add_observations('demo-weather',[sample()])
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  self.settings=Settings(KEYS,self.temp.name)
  self.client=TestClient(create_app(self.settings,self.factory));self.addCleanup(self.client.close)
  with self.factory() as connection:hazards.register(connection,hazards.PolicyChange(policy=policy()),'test')
 def tearDown(self):self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
 def evaluate(self,now=NOW):
  with self.factory() as connection:return hazards.evaluate(connection,'demo-weather',now)
 def events(self):return hazards.listing(self.db,'demo-weather')['items']
 def correct(self,value,revision):self.store.revise(sample(value),expected_revision=revision,reason='fixture correction',actor='test')
 def test_lifecycle_escalation_resolution_and_reopening(self):
  self.evaluate();first=self.events()[0];self.evaluate(NOW+timedelta(seconds=1));self.assertEqual(len(self.events()),1)
  self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.hazard_feed').fetchone()[0],1)
  self.correct(45,1);self.evaluate(NOW+timedelta(seconds=2));event=self.events()[0];self.assertEqual(event['severity'],'critical')
  self.correct(20,2);self.evaluate(NOW+timedelta(seconds=3));self.assertEqual(self.events()[0]['status'],'resolved')
  self.correct(35,3);self.evaluate(NOW+timedelta(seconds=4));self.assertEqual(len(self.events()),2)
  kinds=[row[0] for row in self.db.execute('SELECT kind FROM matrix_db.hazard_feed ORDER BY sequence').fetchall()]
  self.assertEqual(kinds,['opened','severity_changed','resolved','opened'])
 def test_revision_evidence_immutable_and_bad_or_missing_does_not_clear(self):
  first=self.evaluate();self.correct(40,1)
  pinned=self.db.execute('SELECT revision FROM matrix_db.hazard_assessment_inputs WHERE assessment_id=%s',(first['id'],)).fetchone()[0];self.assertEqual(pinned,1)
  self.store.withdraw('demo-weather','one',expected_revision=2,reason='source withdrew',actor='test')
  result=self.evaluate(NOW+timedelta(seconds=2));self.assertEqual(result['findings'][0]['state'],'unknown');self.assertEqual(self.events()[0]['status'],'active')
  count=self.db.execute('SELECT count(*) FROM matrix_db.hazard_feed').fetchone()[0];self.evaluate(NOW+timedelta(seconds=3));self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.hazard_feed').fetchone()[0],count)
  self.assertEqual(self.store.observations('demo-weather'),[])
 def test_acknowledge_retract_and_same_support_suppression(self):
  self.evaluate();event=self.events()[0]
  with self.factory() as connection:hazards.change_event(connection,'demo-weather',event['id'],hazards.EventChange(expected_revision=event['revision'],reason='reviewed'),'operator')
  event=self.events()[0];self.assertIsNotNone(event['acknowledged_at'])
  with self.factory() as connection:hazards.change_event(connection,'demo-weather',event['id'],hazards.EventChange(expected_revision=event['revision'],reason='false positive'),'operator',retract=True)
  result=self.evaluate(NOW+timedelta(seconds=1));self.assertEqual(result['findings'][0]['state'],'suppressed');self.assertEqual(len(self.events()),1);self.assertEqual(self.events()[0]['status'],'retracted')
  self.correct(36,1);self.evaluate(NOW+timedelta(seconds=2));self.assertEqual(len(self.events()),2)
 def test_policy_revision_conflict_units_and_supersession(self):
  self.evaluate()
  with self.assertRaises(hazards.Conflict):
   with self.factory() as connection:hazards.register(connection,hazards.PolicyChange(policy=policy()),'test')
  with self.assertRaises(ValueError):
   with self.factory() as connection:hazards.register(connection,hazards.PolicyChange(expected_revision=1,policy=policy(unit='K')),'test')
  with self.factory() as connection:hazards.register(connection,hazards.PolicyChange(expected_revision=1,policy=policy(trigger_threshold=36)),'test')
  self.assertEqual(self.events()[0]['status'],'superseded')
  self.assertEqual(hazards.detail(self.db,'demo-weather',self.events()[0]['id'])['policy']['rules'][0]['trigger_threshold'],30)
  self.assertEqual(self.db.execute('SELECT kind FROM matrix_db.hazard_feed ORDER BY sequence DESC LIMIT 1').fetchone()[0],'policy_superseded')
 def test_budget_and_write_failure_roll_back_complete_assessment(self):
  with patch('matrix_db.hazards.MAX_SAMPLES',0):
   with self.assertRaises(ValueError):self.evaluate()
  self.assertEqual(self.events(),[])
  with patch('matrix_db.hazards.feed',side_effect=RuntimeError('interrupted')):
   with self.assertRaises(RuntimeError):self.evaluate()
  self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.hazard_assessments').fetchone()[0],0)
  self.assertEqual(self.events(),[])
 def test_hazard_notifications_topic_severity_snapshot_and_dedup(self):
  principal=self.settings.authenticate(READER)
  with self.factory() as connection:notifications.create(connection,notifications.Subscription(name='hazards',dataset_id='demo-weather',topic='hazard',start='beginning',kinds=['opened','severity_changed','resolved'] ),principal)
  self.evaluate();self.correct(45,1);self.evaluate(NOW+timedelta(seconds=1))
  notifications.run_one(self.settings,self.factory);items=notifications.inbox(self.db,principal)['items']
  self.assertEqual(len(items),2);self.assertEqual({item['notification']['topic'] for item in items},{'hazard'})
  self.assertEqual(items[-1]['notification']['severity'],'warning');self.assertEqual(items[0]['notification']['severity'],'critical')
  self.assertFalse(notifications.run_one(self.settings,self.factory));self.assertEqual(len(notifications.inbox(self.db,principal)['items']),2)
 def test_api_scopes_policy_and_paging(self):
  self.assertEqual(self.client.get('/v1/hazards/demo-weather',headers=auth(OTHER)).status_code,403)
  self.assertEqual(self.client.post('/v1/admin/datasets/demo-weather/hazard-evaluate',headers=auth(READER)).status_code,403)
  self.assertEqual(self.client.get('/v1/hazards/demo-weather/policy',headers=auth(READER)).status_code,200)
  self.evaluate();event=self.events()[0]
  result=self.client.get('/v1/hazards/demo-weather/events/'+str(event['id']),headers=auth(READER));self.assertEqual(result.status_code,200,result.text)
  self.assertEqual(result.json()['assessment']['findings'][0]['evidence'][0]['revision'],1)
  self.assertEqual(self.client.get('/v1/hazards/demo-weather?status=active&limit=1',headers=auth(READER)).status_code,200)
  self.assertEqual(self.client.get('/v1/hazards/demo-weather?status=invalid',headers=auth(READER)).status_code,400)
  with self.assertRaises(ValueError):self.evaluate(NOW-timedelta(seconds=1))

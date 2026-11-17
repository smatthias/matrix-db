import json
import os
from datetime import datetime,timedelta,timezone
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
from matrix_db.quality import Policy,Rule,assess,evaluate,register_policy,health,acknowledge,notifications

UTC=timezone.utc
NOW=datetime(2026,11,17,9,tzinfo=UTC)
DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
BUNDLE=Bundle.model_validate_json((Path(__file__).resolve().parents[1]/'examples/weather.json').read_text())

def policy(rules):return Policy(dataset_id='demo-weather',lookback_seconds=7200,evaluation_max_age_seconds=600,rules=rules)
def row(value=15.,variable='temperature',offset=0,longitude=13.4,ident='a',quality='good'):
    sample=BUNDLE.observations[0].model_copy(update={'id':ident,'variable_id':variable,'value':value,'valid_time':NOW+timedelta(seconds=offset),'longitude':longitude,'quality':quality})
    return sample,1,NOW

class AssessmentTest(unittest.TestCase):
    def test_rule_validation(self):
        for data in [dict(id='x',kind='range',variable_id='temperature'),dict(id='x',kind='range',variable_id='temperature',minimum=20,maximum=10),
                     dict(id='x',kind='freshness',variable_id='temperature',max_age_seconds=60,maximum=9),
                     dict(id='x',kind='consistency',variable_id='temperature',other_variable='temperature',tolerance=0)]:
            with self.assertRaises(ValueError):Rule(**data)
    def test_ranges_missing_values_and_upstream_flags_are_preserved(self):
        p=policy([{'id':'range','kind':'range','variable_id':'temperature','minimum':0,'maximum':20}])
        samples=[row(25,ident='hot',quality='suspect'),row(None,ident='missing')]
        before=samples[0][0].model_dump()
        result=assess(p,samples,NOW,{})[0]
        self.assertEqual(result['status'],'fail');self.assertEqual(result['evidence'][0]['observation_id'],'hot')
        self.assertEqual(before,samples[0][0].model_dump());self.assertEqual(assess(p,[],NOW,{})[0]['status'],'unknown')
    def test_rate_never_compares_different_locations_and_flags_zero_time_conflicts(self):
        p=policy([{'id':'rate','kind':'rate','variable_id':'temperature','max_rate_per_second':.1}])
        samples=[row(0,offset=-60),row(20,ident='b'),row(100,longitude=13.5,ident='other')]
        self.assertEqual(assess(p,samples,NOW,{})[0]['failed_count'],1)
        self.assertEqual(assess(p,[row(0),row(1,ident='b')],NOW,{})[0]['status'],'fail')
    def test_completeness_coverage_and_freshness_are_distinct(self):
        p=policy([{'id':'count','kind':'completeness','variable_id':'temperature','expected_count':2,'min_fraction':1},
                  {'id':'area','kind':'coverage','variable_id':'temperature','points':[{'longitude':13.4,'latitude':52.5},{'longitude':13.5,'latitude':52.5}],'min_fraction':1},
                  {'id':'age','kind':'freshness','variable_id':'temperature','max_age_seconds':60}])
        # Same physical/time key cannot inflate completeness, bad flags cannot cover a point.
        findings=assess(p,[row(),row(ident='duplicate'),row(99,longitude=13.5,ident='bad',quality='bad')],NOW,{'temperature':NOW-timedelta(seconds=61)})
        self.assertEqual([f['status'] for f in findings],['fail']*3)
        self.assertEqual(findings[0]['metric'],.5)
    def test_consistency_requires_exact_pair_and_does_not_pick_duplicate_counterpart(self):
        p=policy([{'id':'pair','kind':'consistency','variable_id':'temperature','other_variable':'other','tolerance':1}])
        self.assertEqual(assess(p,[row(10),row(11,variable='other',ident='other')],NOW,{})[0]['status'],'pass')
        self.assertEqual(assess(p,[row(10)],NOW,{})[0]['status'],'fail')
        self.assertEqual(assess(p,[row(10),row(11,variable='other',ident='b'),row(11,variable='other',ident='c')],NOW,{})[0]['status'],'fail')

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class QualityStorageTest(unittest.TestCase):
    def setUp(self):
        self.db=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.db.close)
        self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.db)
        self.factory=lambda:psycopg.connect(DSN)
        self.store=PostgresStore(self.db);self.store.register(CatalogueEntry(dataset=BUNDLE.dataset,source_id='quality-test'))
        self.store.add_observations(BUNDLE.dataset.id,BUNDLE.observations)
        self.policy=policy([{'id':'temp','kind':'range','variable_id':'temperature','maximum':10,'severity':'critical'}])
        with self.factory() as connection:register_policy(connection,self.policy,'operator')
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.admin='quality-admin-token-over-24-characters';self.reader='quality-reader-token-over-24-characters';self.other='quality-other-token-over-24-characters'
        self.settings=Settings([{'token':self.admin,'subject':'operator','role':'admin'},{'token':self.reader,'subject':'reader','role':'reader','datasets':['demo-weather']},{'token':self.other,'subject':'other','role':'reader','datasets':['other']}],self.temp.name)
        self.client=TestClient(create_app(self.settings,self.factory));self.addCleanup(self.client.close)
    def tearDown(self):self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
    def evaluate(self,now=NOW):
        with self.factory() as connection:return evaluate(connection,'demo-weather',now)
    def test_incident_dedup_acknowledgement_recovery_and_reopen(self):
        self.evaluate();self.evaluate(NOW+timedelta(seconds=1))
        incident=self.db.execute("SELECT id FROM matrix_db.quality_incidents WHERE status='open'").fetchone()[0]
        with self.factory() as connection:acknowledge(connection,'demo-weather',incident,'operator');acknowledge(connection,'demo-weather',incident,'operator')
        self.assertEqual(len(notifications(self.db,'demo-weather')['items']),2)
        original=BUNDLE.observations[0]
        self.store.revise(original.model_copy(update={'value':5.}),expected_revision=1,reason='correction',actor='test')
        self.evaluate(NOW+timedelta(seconds=2))
        self.assertEqual(health(self.db,'demo-weather',NOW+timedelta(seconds=3))['status'],'healthy')
        self.store.revise(original,expected_revision=2,reason='new correction',actor='test');self.evaluate(NOW+timedelta(seconds=4))
        self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.quality_incidents').fetchone()[0],2)
        self.assertEqual([e['kind'] for e in notifications(self.db,'demo-weather')['items']],['opened','acknowledged','resolved','opened'])
        events=notifications(self.db,'demo-weather',limit=2);self.assertIsNotNone(events['next_cursor'])
        self.assertEqual(len(notifications(self.db,'demo-weather',after=events['next_cursor'])['items']),2)
    def test_evidence_pins_revisions_and_never_rewrites_quality(self):
        self.evaluate()
        saved=self.db.execute('SELECT snapshot FROM matrix_db.quality_evaluations').fetchone()[0]
        self.assertEqual(len(saved),3);self.assertTrue(all(item['revision']==1 for item in saved))
        self.assertEqual(self.store.observations('demo-weather'),BUNDLE.observations)
        self.store.revise(BUNDLE.observations[0].model_copy(update={'value':5}),expected_revision=1,reason='later',actor='test')
        self.assertEqual(self.db.execute('SELECT snapshot FROM matrix_db.quality_evaluations').fetchone()[0],saved)
    def test_policy_revision_supersedes_without_claiming_recovery_and_unknown_health(self):
        self.assertEqual(health(self.db,'demo-weather',NOW)['status'],'unknown')
        self.evaluate()
        changed=self.policy.model_copy(deep=True);changed.rules[0].maximum=20
        with self.factory() as connection:
            self.assertEqual(register_policy(connection,changed,'operator')['revision'],2)
            self.assertEqual(register_policy(connection,changed,'operator')['revision'],2)
        self.assertEqual(health(self.db,'demo-weather',NOW)['status'],'unknown')
        self.assertEqual(notifications(self.db,'demo-weather')['items'][-1]['kind'],'policy_superseded')
        self.evaluate()
        self.assertEqual(health(self.db,'demo-weather',NOW+timedelta(seconds=601))['status'],'unknown')
    def test_budget_and_failure_rollback_never_resolve_incident(self):
        self.evaluate()
        with patch('matrix_db.quality.MAX_SAMPLES',1):
            with self.assertRaises(ValueError):self.evaluate()
        self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.quality_evaluations').fetchone()[0],1)
        self.assertEqual(health(self.db,'demo-weather',NOW)['status'],'degraded')
        with patch('matrix_db.quality.event',side_effect=RuntimeError('interrupted')):
            changed=self.policy.model_copy(deep=True);changed.rules[0].maximum=20
            with self.assertRaises(RuntimeError):
                with self.factory() as connection:register_policy(connection,changed,'operator')
        self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.quality_policies').fetchone()[0],1)
    def test_outside_window_freshness_input_is_pinned(self):
        p=policy([{'id':'age','kind':'freshness','variable_id':'temperature','max_age_seconds':30}]);p.lookback_seconds=60
        with self.factory() as connection:register_policy(connection,p,'operator')
        result=self.evaluate()
        self.assertEqual(result['findings'][0]['status'],'fail')
        snapshot=self.db.execute('SELECT snapshot FROM matrix_db.quality_evaluations ORDER BY evaluated_at DESC LIMIT 1').fetchone()[0]
        self.assertEqual(snapshot[0]['observation_id'],BUNDLE.observations[0].id)
    def test_scoped_api_and_admin_mutations(self):
        auth=lambda token:{'Authorization':'Bearer '+token}
        path='/v1/quality/demo-weather/health'
        self.assertEqual(self.client.get(path).status_code,401)
        self.assertEqual(self.client.get(path,headers=auth(self.other)).status_code,403)
        self.assertEqual(self.client.post('/v1/quality/demo-weather/evaluate',headers=auth(self.reader)).status_code,403)
        self.assertEqual(self.client.get(path,headers=auth(self.reader)).json()['status'],'unknown')
        self.store.add_observations('demo-weather',[row(25)[0].model_copy(update={'id':'api-current','valid_time':datetime.now(UTC)})])
        self.assertEqual(self.client.post('/v1/quality/demo-weather/evaluate',headers=auth(self.admin)).status_code,200)
        result=self.client.get('/v1/quality/demo-weather/events?limit=1',headers=auth(self.reader))
        self.assertEqual(result.status_code,200);incident=result.json()['items'][0]['incident_id']
        self.assertEqual(self.client.post('/v1/quality/demo-weather/incidents/'+incident+'/acknowledge',headers=auth(self.admin)).status_code,200)
        bad=self.policy.model_copy(deep=True);bad.rules[0].variable_id='absent'
        self.assertEqual(self.client.post('/v1/quality/policies',headers=auth(self.admin),json=bad.model_dump(mode='json')).status_code,400)

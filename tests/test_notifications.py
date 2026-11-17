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
from matrix_db.notifications import Subscription,matches,owner_key,run_one
from matrix_db.quality import Policy,register_policy,evaluate

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
READER='notification-reader-token-over-24-characters'
OTHER='notification-other-token-over-24-characters'
GATEWAY='notification-gateway-token-over-24-characters'
KEYS=[{'token':READER,'subject':'reader','role':'reader','datasets':['demo-weather']},{'token':OTHER,'subject':'other','role':'reader','datasets':['demo-weather']},{'token':GATEWAY,'subject':'gateway','role':'reader','datasets':['demo-weather'],'notification_gateway':True}]
def headers(token=READER,owner=None):return {'Authorization':'Bearer '+token,**({'X-Matrix-Notification-Owner':owner} if owner else {})}
def request(**kwargs):return {'name':'Test subscription','dataset_id':'demo-weather','start':'beginning',**kwargs}

class NotificationContractTest(unittest.TestCase):
    def test_filter_validation_and_scoped_delegation(self):
        settings=Settings(KEYS,'/tmp/test-notification-root')
        principal=settings.authenticate(READER);gateway=settings.authenticate(GATEWAY)
        self.assertNotEqual(owner_key(principal),owner_key(settings.authenticate(OTHER)))
        self.assertNotEqual(owner_key(gateway,'gs-user:1'),owner_key(gateway,'gs-user:2'))
        for p,owner in [(principal,'gs-user:1'),(gateway,None),(gateway,'arbitrary')]:
            with self.assertRaises(PermissionError):owner_key(p,owner)
        rule=Subscription(**request(minimum_severity='critical',rule_ids=['temperature']))
        self.assertTrue(matches(rule,'opened','critical','temperature'));self.assertFalse(matches(rule,'opened','warning','temperature'));self.assertFalse(matches(rule,'opened','critical','other'))
        with self.assertRaises(ValueError):Subscription(**request(kinds=['opened','opened']))
        with self.assertRaises(ValueError):Settings([{'token':READER,'subject':'bad','role':'admin','notification_gateway':True}],'/tmp/root')
    def test_unauthorized_requests_do_not_touch_database(self):
        settings=Settings(KEYS,'/tmp/test-notification-root')
        def forbidden():raise AssertionError('unauthorized database request')
        with TestClient(create_app(settings,forbidden)) as client:
            self.assertEqual(client.get('/v1/subscriptions').status_code,401)
            # Delegation is checked within the database context, but missing authentication is not.
            self.assertEqual(client.get('/v1/notifications').status_code,401)

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class NotificationStorageTest(unittest.TestCase):
    def setUp(self):
        self.db=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.db.close)
        self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.db)
        self.factory=lambda:psycopg.connect(DSN)
        bundle=Bundle.model_validate_json((Path(__file__).resolve().parents[1]/'examples/weather.json').read_text())
        store=PostgresStore(self.db);store.register(CatalogueEntry(dataset=bundle.dataset,source_id='notifications'));store.add_observations('demo-weather',bundle.observations)
        # Absence/count rule always fails independently of wall-clock fixture age.
        p=Policy(dataset_id='demo-weather',lookback_seconds=2592000,rules=[{'id':'count','kind':'completeness','variable_id':'temperature','expected_count':10,'min_fraction':1,'severity':'critical'}])
        with self.factory() as connection:register_policy(connection,p,'test')
        with self.factory() as connection:evaluate(connection,'demo-weather')
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.settings=Settings(KEYS,self.temp.name)
        self.client=TestClient(create_app(self.settings,self.factory));self.addCleanup(self.client.close)
    def tearDown(self):self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
    def subscribe(self,token=READER,owner=None,**kwargs):
        result=self.client.post('/v1/subscriptions',headers=headers(token,owner),json=request(**kwargs));self.assertEqual(result.status_code,201,result.text);return result.json()
    def inbox(self,token=READER,owner=None):return self.client.get('/v1/notifications',headers=headers(token,owner)).json()
    def test_replay_dedup_overlap_read_and_ownership(self):
        first=self.subscribe();self.subscribe(name='overlap')
        for _ in range(6):run_one(self.settings,self.factory)
        items=self.inbox()['items'];self.assertEqual(len(items),1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.notification_matches').fetchone()[0],2)
        self.assertEqual(self.inbox(OTHER)['items'],[])
        ident=items[0]['id']
        self.assertEqual(self.client.post(f'/v1/notifications/{ident}/read',headers=headers(OTHER)).status_code,404)
        a=self.client.post(f'/v1/notifications/{ident}/read',headers=headers()).json();b=self.client.post(f'/v1/notifications/{ident}/read',headers=headers()).json();self.assertEqual(a,b)
        self.assertEqual(self.client.get('/v1/notifications?unread=true',headers=headers()).json()['items'],[])
        self.assertEqual(self.client.delete('/v1/subscriptions/'+first['id']+'?expected_revision=1',headers=headers(OTHER)).status_code,404)
    def test_start_now_pause_resume_conflict_and_soft_delete(self):
        sub=self.subscribe(start='now');self.assertFalse(run_one(self.settings,self.factory));self.assertEqual(self.inbox()['items'],[])
        config=request(start='beginning',active=False)
        path='/v1/subscriptions/'+sub['id'];change={'expected_revision':1,'subscription':config}
        updated=self.client.put(path,headers=headers(),json=change);self.assertEqual(updated.status_code,200,updated.text)
        self.assertEqual(self.client.put(path,headers=headers(),json=change).status_code,409)
        self.assertFalse(run_one(self.settings,self.factory))
        config['active']=True
        self.assertEqual(self.client.put(path,headers=headers(),json={'expected_revision':2,'subscription':config}).status_code,200)
        self.assertTrue(run_one(self.settings,self.factory));self.assertEqual(len(self.inbox()['items']),1)
        self.assertEqual(self.client.delete(path+'?expected_revision=3',headers=headers()).status_code,200)
        self.assertEqual(self.client.get('/v1/subscriptions',headers=headers()).json()['items'],[])
        self.assertEqual(len(self.inbox()['items']),1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM matrix_db.notification_subscription_versions').fetchone()[0],4)
    def test_atomic_fanout_failure_retries_without_checkpoint_loss(self):
        sub=self.subscribe()
        self.db.execute("""CREATE FUNCTION matrix_db.fail_notification_match() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced delivery rollback'; END $$""")
        self.db.execute('CREATE TRIGGER fail_notification_match BEFORE INSERT ON matrix_db.notification_matches FOR EACH ROW EXECUTE FUNCTION matrix_db.fail_notification_match()')
        with self.assertRaises(psycopg.Error):run_one(self.settings,self.factory)
        self.db.execute('DROP TRIGGER fail_notification_match ON matrix_db.notification_matches')
        self.assertEqual(self.db.execute('SELECT last_sequence FROM matrix_db.notification_subscriptions WHERE id=%s',(sub['id'],)).fetchone()[0],0)
        self.assertEqual(self.inbox()['items'],[]);self.assertTrue(run_one(self.settings,self.factory));self.assertFalse(run_one(self.settings,self.factory))
        self.assertEqual(len(self.inbox()['items']),1)
    def test_delegated_users_are_isolated_and_owner_header_is_required(self):
        sub=self.subscribe(GATEWAY,'gs-user:1')
        self.assertTrue(run_one(self.settings,self.factory))
        self.assertEqual(len(self.inbox(GATEWAY,'gs-user:1')['items']),1)
        self.assertEqual(self.inbox(GATEWAY,'gs-user:2')['items'],[])
        self.assertEqual(self.client.get('/v1/subscriptions',headers=headers(GATEWAY)).status_code,403)
        self.assertEqual(self.client.get('/v1/notifications',headers=headers(READER,'gs-user:1')).status_code,403)
        self.assertEqual(self.client.put('/v1/subscriptions/'+sub['id'],headers=headers(GATEWAY,'gs-user:2'),json={'expected_revision':1,'subscription':request()}).status_code,404)
    def test_filters_advance_without_delivery_and_revocation_disables(self):
        sub=self.subscribe(rule_ids=['does-not-match'])
        self.assertTrue(run_one(self.settings,self.factory));self.assertEqual(self.inbox()['items'],[])
        self.assertGreater(self.db.execute('SELECT last_sequence FROM matrix_db.notification_subscriptions WHERE id=%s',(sub['id'],)).fetchone()[0],0)
        revoked=Settings([KEYS[1]],self.temp.name);self.assertTrue(run_one(revoked,self.factory))
        self.assertEqual(self.db.execute('SELECT active,error_code FROM matrix_db.notification_subscriptions WHERE id=%s',(sub['id'],)).fetchone(),(False,'authorization_revoked'))
    def test_scope_recheck_hides_previously_delivered_records(self):
        self.subscribe();run_one(self.settings,self.factory)
        restricted=Settings([{**KEYS[0],'datasets':[]}],self.temp.name)
        with TestClient(create_app(restricted,self.factory)) as client:
            self.assertEqual(client.get('/v1/notifications',headers=headers()).json()['items'],[])
            self.assertEqual(client.get('/v1/subscriptions',headers=headers()).json()['items'],[])
            self.assertEqual(client.post('/v1/subscriptions',headers=headers(),json=request()).status_code,403)
    def test_pagination_and_sanitized_notification_payload(self):
        self.subscribe();run_one(self.settings,self.factory)
        result=self.client.get('/v1/notifications?limit=1',headers=headers()).json();self.assertEqual(len(result['items']),1)
        cursor=result['items'][0]['id']
        self.assertEqual(self.client.get(f'/v1/notifications?before={cursor}',headers=headers()).json()['items'],[])
        self.assertNotIn('actor',result['items'][0]['notification'])
        self.assertEqual(self.client.get('/v1/notifications?limit=101',headers=headers()).status_code,422)

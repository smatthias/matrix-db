from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import unittest
import psycopg
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle
from matrix_db.queries import ObservationQuery, build_query
from matrix_db.storage import migrate, PostgresStore
from matrix_db.temporal import Change, SeriesQuery, PlaybackQuery, history, series, playback

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'
POINT={'longitude':13.4,'latitude':52.5}
BASE={'dataset_id':'demo-weather','variable_id':'temperature','point':POINT}

class TemporalContractTest(unittest.TestCase):
    def test_revision_and_selection_validation(self):
        for kwargs in [dict(expected_revision=0,reason='fix',actor='source'),dict(expected_revision=1,reason=' ',actor='source')]:
            with self.assertRaises(ValueError):Change(**kwargs)
        for field in ('as_of','forecast_issue_time'):
            with self.assertRaises(ValueError):ObservationQuery(**{field:'2026-11-17T08:00:00'})
        with self.assertRaises(ValueError):ObservationQuery(forecast='latest',forecast_issue_time='2026-11-17T08:00:00Z')
        with self.assertRaises(ValueError):PlaybackQuery(**BASE,at='2026-11-17T08:00:00Z',tolerance_seconds=-1)

    def test_bounded_series(self):
        for window,bucket in [({'start':'2026-11-17T08:00:00Z','end':'2026-11-17T08:00:00Z'},60),
                              ({'start':'2026-11-17T00:00:00Z','end':'2026-11-18T00:00:00Z'},1)]:
            with self.assertRaises(ValueError):SeriesQuery(**BASE,time=window,bucket_seconds=bucket)

    def test_history_selection_precedes_fact_filters(self):
        sql,params=build_query(ObservationQuery(as_of='2026-11-17T08:00:00Z',variable_id='temperature'))
        self.assertLess(sql.index('DISTINCT ON'),sql.index('o.variable_id='))
        self.assertIn('NOT o.is_withdrawn',sql)

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class TemporalTest(unittest.TestCase):
    def setUp(self):
        self.connection=psycopg.connect(DSN,autocommit=True)
        self.addCleanup(self.connection.close)
        self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
        migrate(self.connection)
        self.store=PostgresStore(self.connection)
        self.bundle=Bundle.model_validate_json(FIXTURE.read_text())
        self.store.register(CatalogueEntry(dataset=self.bundle.dataset,source_id='temporal'))
        self.record=self.bundle.observations[0]
        self.store.add_observations(self.record.dataset_id,[self.record])
        self.initial=history(self.connection,self.record.dataset_id,self.record.id)[0]

    def tearDown(self):self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')

    def select(self,**kwargs):return self.store.query_observations(ObservationQuery(**kwargs))
    def correct(self,**updates):
        record=self.record.model_copy(update=updates)
        return self.store.revise(record,expected_revision=1,reason='source correction',actor='test-provider')

    def test_correction_asof_and_optimistic_lock(self):
        change=self.correct(value=20)
        self.assertEqual(self.select().items[0].observation.value,20)
        self.assertEqual(self.select(as_of=self.initial['recorded_at']).items[0].observation.value,15.2)
        self.assertEqual(self.select(as_of=change['recorded_at']).items[0].revision,2)
        with self.assertRaisesRegex(ValueError,'revision conflict'):self.correct(value=21)
        self.assertEqual(len(history(self.connection,self.record.dataset_id,self.record.id)),2)
        self.assertEqual(self.select(as_of=self.initial['recorded_at']-timedelta(microseconds=1)).items,[])

    def test_withdrawal_restore_and_history_paging(self):
        withdrawn=self.store.withdraw(self.record.dataset_id,self.record.id,expected_revision=1,reason='bad reading',actor='reviewer')
        self.assertEqual(self.select().items,[])
        self.assertEqual(self.select(as_of=withdrawn['recorded_at']).items,[])
        self.assertEqual(len(self.select(as_of=self.initial['recorded_at']).items),1)
        self.assertEqual(self.store.observations(self.record.dataset_id),[])
        self.store.revise(self.record,expected_revision=2,reason='revalidated',actor='reviewer')
        self.assertEqual(self.select().items[0].revision,3)
        page=history(self.connection,self.record.dataset_id,self.record.id,limit=1,before_revision=3)
        self.assertEqual(page[0]['revision'],2);self.assertTrue(page[0]['is_withdrawn'])
        with self.assertRaises(psycopg.Error):self.connection.execute('UPDATE matrix_db.observation_revisions SET change_reason=\'edited\'')
        with self.assertRaises(psycopg.Error):self.connection.execute('DELETE FROM matrix_db.observation_revisions')

    def test_corrected_location_and_time_filter_after_revision_resolution(self):
        self.correct(longitude=13.8,valid_time=self.record.valid_time+timedelta(hours=1))
        region={'west':13.3,'east':13.5,'south':52.4,'north':52.6}
        self.assertEqual(self.select(bbox=region).items,[])
        self.assertEqual(len(self.select(bbox=region,as_of=self.initial['recorded_at']).items),1)
        early={'start':'2026-11-17T08:00:00Z','end':'2026-11-17T08:00:00Z'}
        self.assertEqual(self.select(time=early).items,[])
        self.assertEqual(len(self.select(time=early,as_of=self.initial['recorded_at']).items),1)

    def test_rollback_preserves_history(self):
        with self.assertRaises(RuntimeError):
            with self.connection.transaction():
                self.correct(value=30)
                raise RuntimeError('interrupted')
        self.assertEqual(self.select().items[0].revision,1)
        self.assertEqual(len(history(self.connection,self.record.dataset_id,self.record.id)),1)

    def test_series_buckets_counts_missing_and_asof(self):
        later=self.record.model_copy(update={'id':'later','valid_time':self.record.valid_time+timedelta(minutes=30),'value':20})
        missing=self.record.model_copy(update={'id':'missing','valid_time':self.record.valid_time+timedelta(hours=1),'value':None})
        endpoint=self.record.model_copy(update={'id':'endpoint','valid_time':self.record.valid_time+timedelta(hours=3),'value':50})
        self.store.add_observations(self.record.dataset_id,[later,missing,endpoint])
        query=SeriesQuery(**BASE,time={'start':'2026-11-17T08:00:00Z','end':'2026-11-17T11:00:00Z'},bucket_seconds=3600)
        result=series(self.connection,query)
        self.assertEqual(result['unit'],'degC')
        buckets=result['buckets']
        self.assertAlmostEqual(buckets[0]['mean'],17.6)
        self.assertEqual((buckets[0]['sample_count'],buckets[1]['record_count'],buckets[1]['sample_count']),(2,1,0))
        self.assertTrue(buckets[2]['missing']);self.assertIsNone(buckets[2]['sum'])
        self.correct(value=25)
        historical=series(self.connection,query.model_copy(update={'as_of':self.initial['recorded_at']}))
        self.assertAlmostEqual(historical['buckets'][0]['mean'],15.2)
        self.assertEqual(historical['buckets'][0]['sample_count'],1)

    def test_playback_tolerance_ties_and_utc(self):
        later=self.record.model_copy(update={'id':'later','valid_time':self.record.valid_time+timedelta(minutes=10),'value':20})
        self.store.add_observations(self.record.dataset_id,[later])
        query=PlaybackQuery(**BASE,at='2026-11-17T09:05:00+01:00',tolerance_seconds=300)
        result=playback(self.connection,query)
        self.assertEqual(result['match']['observation']['id'],self.record.id)
        self.assertEqual(result['offset_seconds'],-300)
        self.assertIsNone(playback(self.connection,query.model_copy(update={'tolerance_seconds':299}))['match'])

    def test_forecast_run_selection(self):
        dataset=self.bundle.dataset.model_copy(deep=True);dataset.id='forecast';dataset.origin='forecast'
        self.store.register(CatalogueEntry(dataset=dataset,source_id='forecast'))
        now=datetime.now(timezone.utc)
        old=self.record.model_copy(update={'dataset_id':'forecast','id':'old','valid_time':now+timedelta(hours=2),
            'forecast_issue_time':now-timedelta(hours=2),'value':10})
        new=old.model_copy(update={'id':'new','forecast_issue_time':now-timedelta(hours=1),'value':20})
        self.store.add_observations('forecast',[old])
        old_time=history(self.connection,'forecast','old')[0]['recorded_at']
        self.store.add_observations('forecast',[new])
        self.assertEqual(self.select(dataset_id='forecast',forecast='latest').items[0].observation.id,'new')
        self.assertEqual(self.select(dataset_id='forecast',forecast='latest',as_of=old_time).items[0].observation.id,'old')
        self.assertEqual(self.select(dataset_id='forecast',forecast_issue_time=old.forecast_issue_time).items[0].observation.id,'old')
        self.store.withdraw('forecast','new',expected_revision=1,reason='withdrawn run',actor='provider')
        self.assertEqual(self.select(dataset_id='forecast',forecast='latest').items[0].observation.id,'old')

    def test_upgrade_backfills_only_at_migration_time(self):
        self.connection.execute('DROP SCHEMA matrix_db CASCADE')
        migrate(self.connection,target=4)
        self.store.register(CatalogueEntry(dataset=self.bundle.dataset,source_id='backfill'))
        self.store.add_observations(self.record.dataset_id,[self.record])
        before=datetime.now(timezone.utc)
        migrate(self.connection)
        self.assertEqual(self.select(as_of=before).items,[])
        self.assertEqual(self.select().items[0].revision,1)

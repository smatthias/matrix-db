"""Provider protocol fixtures and transactional scheduler regression tests; no credentials/live calls."""
from datetime import datetime,timedelta,timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import httpx
import psycopg
from matrix_db.providers import SourceConfig,ProviderError,Transport,fetch_records,identity
from matrix_db.provider_ingestion import poll_source,source_state,source_lock,window
from matrix_db.storage import migrate,PostgresStore

UTC=timezone.utc
NOW=datetime(2035,11,17,12,tzinfo=UTC)
START=NOW-timedelta(hours=1)
DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')

def config(provider='usgs'):
    definitions={'usgs':[('magnitude','earthquake_magnitude','1')],'firms':[('frp','fire_radiative_power','MW')],
        'open_meteo':[('temperature','reanalysis_air_temperature','degC'),('humidity','reanalysis_relative_humidity','%'),('wind','reanalysis_wind_speed','m/s')],
        'sentinel_ndvi':[('ndvi','area_mean_normalized_difference_vegetation_index','1')],'overpass':[('building_presence','osm_building_presence','1')]}
    data={'id':'source-'+provider,'provider':provider,'start_at':START.isoformat(),'interval_seconds':60,'overlap_seconds':600,
        'entry':{'source_id':provider,'dataset':{'id':'data-'+provider,'name':provider,'provider':provider,'license':'fixture-only',
            'source_reference':'test:provider','coverage':{'west':13,'east':14,'south':52,'north':53},'origin':'observation',
            'variables':[{'id':v,'quantity':q,'unit':u} for v,q,u in definitions[provider]]}}}
    if provider in ('firms','sentinel_ndvi'):data['credential_env']='MATRIX_DB_PROVIDER_TEST'
    if provider=='open_meteo':data.update(latitude=52.5,longitude=13.5)
    if provider=='sentinel_ndvi':data.update(start_at=(NOW-timedelta(days=3)).replace(hour=0).isoformat(),window_days=2,overlap_seconds=86400)
    return SourceConfig.model_validate(data)

def seismic(value=3.2,at=NOW-timedelta(minutes=5),ident='event1'):
    return {'type':'FeatureCollection','features':[{'id':ident,'geometry':{'coordinates':[13.5,52.5,10]},'properties':{'time':int(at.timestamp()*1000),'mag':value,'status':'reviewed'}}]}

class ReadersTest(unittest.TestCase):
    def run_reader(self,provider,payload,start=START,end=NOW):
        calls=[]
        def handle(request):
            calls.append(request)
            return httpx.Response(200,content=payload if isinstance(payload,bytes) else json.dumps(payload).encode())
        with httpx.Client(transport=httpx.MockTransport(handle)) as client,patch.dict(os.environ,{'MATRIX_DB_PROVIDER_TEST':'fixture-not-a-real-credential'}):
            result=fetch_records(config(provider),start,end,NOW,Transport(client,sleep=lambda n:None))
        return result,calls
    def test_usgs_stable_identity_correction_and_explicit_deletion(self):
        (first,_),calls=self.run_reader('usgs',seismic())
        (second,_),_=self.run_reader('usgs',seismic(4.1))
        self.assertEqual(first[0].id,second[0].id);self.assertNotEqual(first[0].value,second[0].value)
        payload=seismic();payload['features'][0]['properties']['status']='deleted'
        (records,removed),_=self.run_reader('usgs',payload)
        self.assertEqual(records,[]);self.assertEqual(removed,[first[0].id])
        self.assertEqual(calls[0].url.host,'earthquake.usgs.gov')
    def test_usgs_empty_204_is_success_but_malformed_200_is_not(self):
        with httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(204))) as client:
            self.assertEqual(fetch_records(config(),START,NOW,NOW,Transport(client)),([],[]))
        with httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(200,content=b''))) as client:
            with self.assertRaises(ProviderError):fetch_records(config(),START,NOW,NOW,Transport(client))

    def test_firms_utc_csv_identity_quality_and_secret_not_in_records(self):
        raw=b'latitude,longitude,acq_date,acq_time,frp,confidence\n52.5,13.5,2035-11-17,1130,7.4,l\n'
        (records,_),calls=self.run_reader('firms',raw)
        self.assertEqual(records[0].value,7.4);self.assertEqual(records[0].unit,'MW');self.assertEqual(records[0].quality,'suspect')
        self.assertEqual(records[0].valid_time.hour,11)
        self.assertNotIn('fixture-not-a-real-credential',records[0].model_dump_json())
        self.assertIn('/VIIRS_SNPP_NRT/',str(calls[0].url))
    def test_weather_reanalysis_units_missing_values_and_actual_grid_location(self):
        payload={'longitude':13.4,'latitude':52.4,'hourly_units':{'temperature_2m':'°C','relative_humidity_2m':'%','wind_speed_10m':'m/s'},
            'hourly':{'time':[int(START.timestamp())],'temperature_2m':[12.],'relative_humidity_2m':[None],'wind_speed_10m':[4.]}}
        (records,_),_=self.run_reader('open_meteo',payload)
        self.assertEqual(len(records),3);self.assertEqual(records[0].longitude,13.4);self.assertIsNone(records[1].value)
        payload['hourly_units']['wind_speed_10m']='km/h'
        with self.assertRaises(ProviderError):self.run_reader('open_meteo',payload)
    def test_ndvi_area_mean_and_no_data(self):
        start=NOW.replace(hour=0)-timedelta(days=2);end=start+timedelta(days=1)
        payload={'status':'OK','data':[{'interval':{'from':start.isoformat(),'to':end.isoformat()},'outputs':{'ndvi':{'bands':{'B0':{'stats':{'sampleCount':10,'noDataCount':2,'mean':.6}}}}}}]}
        (records,_),calls=self.run_reader('sentinel_ndvi',payload,start,end)
        self.assertEqual(records[0].value,.6);self.assertEqual(records[0].longitude,13.5)
        self.assertIn('dataMask',json.loads(calls[0].content)['aggregation']['evalscript'])
        payload['data'][0]['outputs']['ndvi']['bands']['B0']['stats']['noDataCount']=10
        (records,_),_=self.run_reader('sentinel_ndvi',payload,start,end)
        self.assertIsNone(records[0].value);self.assertEqual(records[0].quality,'bad')
    def test_exposure_point_and_bbox_center_are_not_population_estimates(self):
        payload={'osm3s':{'timestamp_osm_base':START.isoformat()},'elements':[{'type':'node','id':1,'lon':13.5,'lat':52.5},
            {'type':'way','id':2,'center':{'lon':13.6,'lat':52.6}}]}
        (records,_),calls=self.run_reader('overpass',payload)
        self.assertEqual([r.value for r in records],[1,1]);self.assertEqual(records[0].variable_id,'building_presence')
        self.assertIn(b'building',calls[0].content)
        payload['remark']='runtime timeout'
        with self.assertRaises(ProviderError):self.run_reader('overpass',payload)
    def test_schema_change_duplicate_and_size_fail_closed(self):
        with self.assertRaises(ProviderError):self.run_reader('usgs',{'error':'private details'})
        payload=seismic();payload['features']*=2
        with self.assertRaises(ProviderError):self.run_reader('usgs',payload)
        with patch('matrix_db.providers.MAX_BYTES',10):
            with self.assertRaises(ProviderError):self.run_reader('usgs',seismic())
    def test_http_retry_rate_limit_redirect_and_credential_failure(self):
        calls=[];delays=[]
        def handler(request):
            calls.append(request)
            return httpx.Response(429,headers={'Retry-After':'2'}) if len(calls)<3 else httpx.Response(200,content=b'ok')
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            self.assertEqual(Transport(client,sleep=delays.append).fetch('GET','https://example.test'),b'ok')
        self.assertEqual(delays,[2,2])
        for code,headers in [(302,{'Location':'https://other.test'}),(401,{}),(429,{'Retry-After':'90'})]:
            with httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(code,headers=headers))) as client:
                with self.assertRaises(ProviderError):Transport(client,sleep=lambda n:None).fetch('GET','https://example.test')
        with patch.dict(os.environ,{},clear=True):
            with self.assertRaises(ProviderError) as error:fetch_records(config('firms'),START,NOW,NOW,Transport())
        self.assertEqual(error.exception.code,'credential_missing')
    def test_config_schedules_and_availability(self):
        source=config();source.overlap_seconds=90000
        with self.assertRaises(ValueError):SourceConfig.model_validate(source.model_dump())
        source=config('open_meteo');start,end=window(source,None,NOW)
        self.assertLessEqual(end,NOW-timedelta(days=7))
        source=config();start,end=window(source,START,NOW)
        self.assertGreater(end,START)

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class SchedulerTest(unittest.TestCase):
    def setUp(self):
        self.db=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.db.close)
        self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.db)
        self.factory=lambda:psycopg.connect(DSN)
        self.source=config();self.payload=seismic()
        self.client=httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(200,json=self.payload)));self.addCleanup(self.client.close)
        self.transport=Transport(self.client,sleep=lambda n:None)
    def tearDown(self):self.db.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
    def poll(self,now=NOW,backfill=None):return poll_source(self.source,self.factory,self.transport,now,backfill)
    def test_duplicate_correction_late_data_and_watermark_atomicity(self):
        first=self.poll();self.assertEqual(first['inserted'],1)
        second=self.poll(NOW+timedelta(seconds=61));self.assertEqual(second['unchanged'],1)
        # A correction and late arrival remain inside the configured overlap.
        self.payload=seismic(4.5,at=NOW-timedelta(minutes=5))
        self.payload['features'].append(seismic(2.,at=NOW-timedelta(minutes=4),ident='late')['features'][0])
        third=self.poll(NOW+timedelta(seconds=122));self.assertEqual(third['corrected'],1);self.assertEqual(third['inserted'],1)
        store=PostgresStore(self.db);self.assertEqual(len(store.observations('data-usgs')),2)
        from matrix_db.temporal import history as revisions
        history=revisions(self.db,'data-usgs',identity('event1','magnitude'))
        self.assertEqual(len(history),2)
        self.assertEqual(source_state(self.db,self.source.id)['watermark'],NOW+timedelta(seconds=122))
    def test_failure_does_not_advance_or_partially_publish_and_backoff(self):
        self.assertEqual(self.poll()['status'],'completed')
        previous=source_state(self.db,self.source.id)['watermark']
        self.payload=seismic(9.,at=NOW-timedelta(minutes=5),ident='new')
        with patch('matrix_db.provider_ingestion.apply_records',side_effect=RuntimeError('secret-url-and-token')):
            result=self.poll(NOW+timedelta(seconds=61))
        self.assertEqual(result['status'],'failed');self.assertNotIn('secret',str(result))
        self.assertEqual(source_state(self.db,self.source.id)['watermark'],previous)
        self.assertEqual(self.poll(NOW+timedelta(seconds=62))['status'],'not_due')
        self.assertEqual(len(PostgresStore(self.db).observations('data-usgs')),1)
    def test_mid_write_failure_rolls_back_and_retries_without_duplicates(self):
        from matrix_db.provider_ingestion import apply_records
        def interrupted(*args):apply_records(*args);raise RuntimeError('interrupted')
        with patch('matrix_db.provider_ingestion.apply_records',interrupted):self.assertEqual(self.poll()['status'],'failed')
        self.assertEqual(PostgresStore(self.db).observations('data-usgs'),[])
        self.assertIsNone(source_state(self.db,self.source.id)['watermark'])
        self.assertEqual(self.poll(NOW+timedelta(seconds=61))['inserted'],1)
    def test_lock_excludes_second_poller_and_recovers_abandoned_run(self):
        with self.factory() as owner:
            owner.execute('SELECT pg_advisory_lock(%s)',(source_lock(self.source.id),));owner.commit()
            self.assertEqual(self.poll()['status'],'busy')
        self.poll()
        self.db.execute("UPDATE matrix_db.provider_runs SET status='running'")
        self.poll(NOW+timedelta(seconds=61))
        self.assertEqual(self.db.execute("SELECT count(*) FROM matrix_db.provider_runs WHERE error_code='worker_interrupted'").fetchone()[0],1)
    def test_explicit_withdrawal_backfill_and_configuration_binding(self):
        self.poll()
        self.payload['features'][0]['properties']['status']='deleted'
        result=self.poll(NOW+timedelta(seconds=61),backfill=(START,NOW))
        self.assertEqual(result['withdrawn'],1)
        self.assertEqual(source_state(self.db,self.source.id)['watermark'],NOW)
        self.assertEqual(PostgresStore(self.db).observations('data-usgs'),[])
        self.source.interval_seconds=120
        with self.assertRaises(ValueError):self.poll(NOW+timedelta(seconds=200))

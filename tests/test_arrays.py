import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4
import numpy as np
import psycopg
from matrix_db.arrays import Axis, ArrayStore, GridSpec, SubsetQuery
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle
from matrix_db.storage import migrate, PostgresStore

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'

def specification():
    return GridSpec.model_validate({'dataset_id':'demo-weather','axes':[
        {'name':'time','kind':'time','unit':'UTC','values':['2026-11-17T08:00:00Z','2026-11-17T09:00:00Z','2026-11-17T10:00:00Z']},
        {'name':'depth','kind':'vertical','unit':'m','reference':'sea_surface','positive':'down','values':[0,10]},
        {'name':'lat','kind':'latitude','unit':'degrees_north','values':[52.25,52.5,52.75]},
        {'name':'lon','kind':'longitude','unit':'degrees_east','values':[13.1,13.3,13.5,13.7,13.9]}],
        'variables':[{'id':'temperature','unit':'degC','dtype':'float32','dimensions':['time','depth','lat','lon'],'chunks':[2,1,2,2]}]})

class GridContractTest(unittest.TestCase):
    def test_regular_axis_and_vertical_validation(self):
        for axis in [dict(name='x',kind='longitude',unit='degrees_east',values=[0,1,3]),
                     dict(name='x',kind='longitude',unit='degrees_east',values=[1]),
                     dict(name='time',kind='time',unit='UTC',values=[None]),
                     dict(name='time',kind='time',unit='UTC',values=['2026-11-17T08:00:00']),
                     dict(name='z',kind='vertical',unit='m',values=[1,2])]:
            with self.subTest(axis=axis),self.assertRaises(ValueError):Axis.model_validate(axis)
        descending=Axis(name='y',kind='latitude',unit='degrees_north',values=[53,52])
        self.assertEqual(descending.bounds,[(52.5,53.5),(51.5,52.5)])

    def test_dimensions_crs_and_memory_limits(self):
        spec=specification().model_dump(mode='json')
        for change in [lambda d:d['variables'][0].update(dimensions=['time','unknown']),
                       lambda d:d.update(crs='EPSG:3857'),
                       lambda d:d['variables'][0].update(chunks=[1,0,1,1])]:
            data=copy.deepcopy(spec);change(data)
            with self.assertRaises(ValueError):GridSpec.model_validate(data)
        data=copy.deepcopy(spec)
        for axis in data['axes']:
            if axis['kind']=='time':continue
            axis['values']=list(range(1000));axis['bounds']=None
            if axis['kind'] in ('longitude','latitude'):
                axis.update(kind='other',unit='index')
        data['axes'][-2].update(kind='y',unit='m');data['axes'][-1].update(kind='x',unit='m')
        data['crs']='EPSG:3857';data['variables'][0]['chunks']=[3,1000,1000,1000]
        with self.assertRaisesRegex(ValueError,'64 MiB'):GridSpec.model_validate(data)

    def test_projected_and_selection_contracts(self):
        spec=GridSpec(dataset_id='demo-weather',crs='EPSG:3857',axes=[
            Axis(name='x',kind='x',unit='m',values=[0,100]),Axis(name='y',kind='y',unit='m',values=[100,0])],
            variables=[{'id':'temperature','unit':'degC','dtype':'float64','dimensions':['y','x'],'chunks':[1,1]}])
        self.assertEqual(spec.crs,'EPSG:3857')
        with self.assertRaises(ValueError):SubsetQuery(version_id=uuid4(),slices={'x':{'start':0,'stop':1}},ranges={'x':{'minimum':0,'maximum':1}})
        with self.assertRaises(ValueError):SubsetQuery(version_id='../outside')

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class ArrayStorageTest(unittest.TestCase):
    def setUp(self):
        self.connection=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.connection.close)
        self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.connection)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.arrays=ArrayStore(self.connection,self.temp.name)
        dataset=Bundle.model_validate_json(FIXTURE.read_text()).dataset
        PostgresStore(self.connection).register(CatalogueEntry(dataset=dataset,source_id='array-fixture'))
        self.spec=specification()
        self.data=(np.arange(90,dtype='float32')/10).reshape(3,2,3,5)
        self.mask=np.zeros(self.data.shape,dtype=bool);self.mask[1,1,1,3]=True
        self.data[1,1,1,3]=np.nan

    def tearDown(self):self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
    def provider(self,variable,slices):return self.data[slices],self.mask[slices]

    def test_roundtrip_dtype_masks_edge_chunks_and_subset_io(self):
        version=self.arrays.publish(self.spec,self.provider)
        result=self.arrays.read(SubsetQuery(version_id=version))
        np.testing.assert_equal(result['variables']['temperature']['data'],self.data)
        np.testing.assert_array_equal(result['variables']['temperature']['mask'],self.mask)
        self.assertEqual(result['variables']['temperature']['data'].dtype,np.dtype('float32'))
        selected=self.arrays.read(SubsetQuery(version_id=version,slices={'time':{'start':1,'stop':2},'depth':{'start':1,'stop':2},
            'lat':{'start':1,'stop':2},'lon':{'start':3,'stop':5}}))
        np.testing.assert_equal(selected['variables']['temperature']['data'],self.data[1:2,1:2,1:2,3:5])
        self.assertEqual(selected['chunks_read'],2)
        self.assertLess(selected['chunks_read'],result['chunks_read'])
        last=self.arrays.read(SubsetQuery(version_id=version,slices={'time':{'start':2,'stop':3},'lat':{'start':2,'stop':3},'lon':{'start':4,'stop':5}}))
        self.assertEqual(last['variables']['temperature']['data'].shape,(1,2,1,1))

    def test_coordinate_windows_and_empty_results(self):
        version=self.arrays.publish(self.spec,self.provider)
        result=self.arrays.read(SubsetQuery(version_id=version,ranges={
            'time':{'minimum':'2026-11-17T10:00:00+01:00','maximum':'2026-11-17T10:00:00Z'},
            'depth':{'minimum':10,'maximum':10},'lat':{'minimum':52.5,'maximum':52.75}}))
        np.testing.assert_equal(result['variables']['temperature']['data'],self.data[1:3,1:2,1:3,:])
        empty=self.arrays.read(SubsetQuery(version_id=version,ranges={'lat':{'minimum':0,'maximum':1}}))
        self.assertEqual(empty['chunks_read'],0)
        self.assertEqual(empty['variables']['temperature']['data'].shape,(3,2,0,5))
        with self.assertRaises(ValueError):self.arrays.read(SubsetQuery(version_id=version,slices={'lon':{'start':0,'stop':99}}))
        with self.assertRaises(ValueError):self.arrays.read(SubsetQuery(version_id=version,variables=['absent']))

    def test_failed_publication_has_no_catalogue_entry(self):
        calls=0
        def interrupted(variable,slices):
            nonlocal calls
            calls+=1
            if calls==3:raise RuntimeError('simulated interruption')
            return self.provider(variable,slices)
        with self.assertRaises(RuntimeError):self.arrays.publish(self.spec,interrupted)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM matrix_db.array_versions').fetchone()[0],0)
        self.assertEqual(self.arrays.unreferenced_directories(),{'unreferenced_or_inflight':[],'staging':[]})

    def test_version_isolation_protection_and_transaction_orphan(self):
        old=self.arrays.publish(self.spec,self.provider)
        self.data+=10
        revised=self.spec.model_copy(update={'previous_version':old})
        new=self.arrays.publish(revised,self.provider)
        self.assertEqual(self.arrays.read(SubsetQuery(version_id=old))['variables']['temperature']['data'][0,0,0,0],0)
        self.assertEqual(self.arrays.read(SubsetQuery(version_id=new))['variables']['temperature']['data'][0,0,0,0],10)
        with self.assertRaises(psycopg.Error):self.connection.execute('DELETE FROM matrix_db.array_versions')
        store=PostgresStore(self.connection);replacement=store.get('demo-weather');replacement.dataset.variables[0].unit='K'
        with self.assertRaises(ValueError):store.update('demo-weather',replacement)
        orphan=None
        with self.assertRaises(RuntimeError):
            with self.connection.transaction():
                orphan=self.arrays.publish(self.spec,self.provider)
                raise RuntimeError('rollback after file publication')
        with self.assertRaises(KeyError):self.arrays.manifest(orphan)
        self.assertIn(str(orphan),self.arrays.unreferenced_directories()['unreferenced_or_inflight'])

    def test_corruption_is_detected(self):
        version=self.arrays.publish(self.spec,self.provider)
        manifest=self.arrays.manifest(version)
        path=Path(self.temp.name)/'versions'/str(version)/manifest['chunks'][0]['path']
        path.write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError,'checksum'):self.arrays.read(SubsetQuery(version_id=version))
        (path.parent.parent/'manifest.json').write_text('{}')
        with self.assertRaisesRegex(ValueError,'manifest checksum'):self.arrays.manifest(version)

    def test_provider_contract_rejection(self):
        with self.assertRaisesRegex(ValueError,'dtype'):
            self.arrays.publish(self.spec,lambda variable,slices:(self.data[slices].astype('float64'),self.mask[slices]))
        with self.assertRaisesRegex(ValueError,'shape'):
            self.arrays.publish(self.spec,lambda variable,slices:(np.ones((1,),dtype='float32'),np.zeros((1,),dtype=bool)))
        with self.assertRaisesRegex(ValueError,'finite'):
            self.arrays.publish(self.spec,lambda variable,slices:(np.full(self.data[slices].shape,np.inf,dtype='float32'),self.mask[slices]))

    def test_arbitrary_dimension_order_and_read_budget(self):
        spec=self.spec.model_copy(deep=True)
        spec.variables[0].dimensions=['lon','lat','depth','time']
        spec.variables[0].chunks=[2,2,1,2]
        transposed=self.data.transpose(3,2,1,0);mask=self.mask.transpose(3,2,1,0)
        version=self.arrays.publish(spec,lambda variable,slices:(transposed[slices],mask[slices]))
        result=self.arrays.read(SubsetQuery(version_id=version))
        np.testing.assert_equal(result['variables']['temperature']['data'],transposed)
        with patch('matrix_db.arrays.MAX_BYTES',64):
            with self.assertRaisesRegex(ValueError,'subset exceeds'):
                self.arrays.read(SubsetQuery(version_id=version))

    def test_staged_corruption_prevents_publication(self):
        calls=0
        def corrupt_earlier_chunk(variable,slices):
            nonlocal calls
            calls+=1
            if calls==2:
                first=next((Path(self.temp.name)/'.staging').rglob('*.npz'))
                first.write_bytes(b'corrupted before publication')
            return self.provider(variable,slices)
        with self.assertRaisesRegex(ValueError,'changed during staging'):
            self.arrays.publish(self.spec,corrupt_earlier_chunk)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM matrix_db.array_versions').fetchone()[0],0)

from datetime import datetime, timedelta, timezone
import copy
import os
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4
import numpy as np
import psycopg
from matrix_db.alignment import AlignmentInput, AlignmentRequest, align_field, align, align_and_publish
from matrix_db.arrays import Axis, GridSpec, ArrayStore, SubsetQuery
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle
from matrix_db.storage import migrate, PostgresStore
from matrix_db.units import conversion, convert

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'
EPOCH=datetime(2026,11,17,tzinfo=timezone.utc)

def stamp(seconds):return (EPOCH+timedelta(seconds=seconds)).isoformat()

def grid(x=(0.,1.),y=(0.,1.),*,unit='degC',dtype='float64',time=None,vertical=None,crs='EPSG:4326'):
    xkind,ykind=('longitude','latitude') if crs=='EPSG:4326' else ('x','y')
    def horizontal(name,kind,values):
        unit={'longitude':'degrees_east','latitude':'degrees_north','x':'m','y':'m'}[kind]
        return Axis(name=name,kind=kind,unit=unit,values=list(values),bounds=[(values[0]-.5,values[0]+.5)] if len(values)==1 else None)
    axes=[];dimensions=[]
    if time is not None:
        axes.append(Axis(name='time',kind='time',unit='UTC',values=[stamp(value) for value in time]));dimensions.append('time')
    if vertical is not None:axes.append(vertical);dimensions.append(vertical.name)
    axes += [horizontal('row',ykind,y),horizontal('column',xkind,x)];dimensions+=['row','column']
    return GridSpec(dataset_id='source',crs=crs,axes=axes,variables=[{'id':'v','unit':unit,'dtype':dtype,'dimensions':dimensions,'chunks':[1]*len(dimensions)}])

def options(**kwargs):return AlignmentInput(version_id=uuid4(),variable_id='v',output_variable='v',**kwargs)

def calculate(data,source,target,mask=None,**kwargs):
    data=np.asarray(data,dtype=source.variables[0].dtype)
    if mask is None:mask=np.zeros(data.shape,dtype=bool)
    return align_field(data,mask,source,source.variables[0],target,target.variables[0],options(**kwargs))

class UnitAndAlignmentTest(unittest.TestCase):
    def test_affine_and_incompatible_conversions(self):
        np.testing.assert_allclose(convert([0,100],'degC','K'),[273.15,373.15])
        np.testing.assert_allclose(convert([32,212],'degF','degC'),[0,100],atol=1e-12)
        np.testing.assert_allclose(convert([36],'km/h','m/s'),[10])
        np.testing.assert_allclose(convert([50],'%','1'),[.5])
        np.testing.assert_allclose(convert([1],'delta_degF','delta_K'),[5/9])
        for source,target in [('degC','m/s'),('mm','mm/h'),('mystery','K'),('degC','delta_K')]:
            with self.subTest(source=source,target=target),self.assertRaises(ValueError):conversion(source,target)

    def test_linear_plane_conversion_and_descending_axes(self):
        source=grid();target=grid(x=(.25,.75),y=(.25,.75),unit='K')
        result=calculate([[0,1],[2,3]],source,target,spatial='linear')
        np.testing.assert_allclose(result['data'],np.array([[.75,1.25],[1.75,2.25]])+273.15)
        self.assertFalse(result['mask'].any());np.testing.assert_allclose(result['coverage'],1)
        reverse=grid(x=(1.,0.),y=(1.,0.))
        other=calculate([[3,2],[1,0]],reverse,target,spatial='linear')
        np.testing.assert_allclose(other['data'],result['data'])

    def test_categories_missing_coverage_and_no_extrapolation(self):
        source=grid(unit='1',dtype='int16');target=grid(x=(.6,),y=(.6,),unit='1',dtype='int16')
        result=calculate([[1,5],[5,1]],source,target,categorical=True)
        self.assertEqual(result['data'][0,0],1)
        with self.assertRaises(ValueError):options(categorical=True,spatial='linear')
        source=grid();target=grid(x=(.5,),y=(.5,));mask=np.array([[True,False],[False,False]])
        result=calculate([[0,1],[2,3]],source,target,mask,spatial='linear')
        self.assertTrue(result['mask'][0,0]);self.assertAlmostEqual(result['coverage'][0,0],.75)
        result=calculate([[0,1],[2,3]],source,target,mask,spatial='linear',minimum_coverage=.75,renormalize_missing=True)
        self.assertAlmostEqual(result['data'][0,0],2)
        outside=calculate([[0,1],[2,3]],source,grid(x=(5.,),y=(.5,)))
        self.assertTrue(outside['mask'].all())

    def test_conservative_spherical_means_and_cell_totals(self):
        source=grid(x=(.5,1.5),y=(.5,1.5),unit='mm')
        target=grid(x=(1.,),y=(1.,),unit='mm')
        for axis in target.axes:axis.bounds=[(0,2)]
        data=np.array([[10.,20.],[30.,40.]])
        result=calculate(data,source,target,spatial='conservative')
        rows=np.diff(np.sin(np.radians([0,1,2])))
        source_integral=np.sum(data*rows[:,None])*np.radians(1)
        destination_area=np.radians(2)*(np.sin(np.radians(2))-np.sin(0))
        self.assertAlmostEqual(result['data'][0,0]*destination_area,source_integral)
        extensive=calculate([[1,2],[3,4]],source,target,spatial='conservative',spatial_extensive=True)
        self.assertAlmostEqual(extensive['data'][0,0],10)
        refined=calculate([[10]],target,source,spatial='conservative',spatial_extensive=True)
        self.assertAlmostEqual(refined['data'].sum(),10)
        self.assertFalse(refined['mask'].any())

    def test_time_interpolation_gap_and_interval_semantics(self):
        source=grid(time=(0,3600,7200));target=grid(time=(1800,))
        data=np.broadcast_to(np.array([0.,10.,20.])[:,None,None],(3,2,2))
        result=calculate(data,source,target,temporal='linear',max_time_gap_seconds=3600)
        np.testing.assert_allclose(result['data'],5)
        result=calculate(data,source,target,temporal='linear',max_time_gap_seconds=1800)
        self.assertTrue(result['mask'].all())
        source=grid(time=(1800,5400),unit='mm');target=grid(time=(3600,),unit='mm')
        data=np.broadcast_to(np.array([2.,4.])[:,None,None],(2,2,2))
        bounds=[{'start':stamp(0),'end':stamp(3600)},{'start':stamp(3600),'end':stamp(7200)}]
        target_bounds=[{'start':stamp(0),'end':stamp(7200)}]
        result=calculate(data,source,target,temporal='overlap',time_semantics='interval_mean',source_time_bounds=bounds,target_time_bounds=target_bounds)
        np.testing.assert_allclose(result['data'],3)
        result=calculate(data,source,target,temporal='overlap',time_semantics='accumulation',source_time_bounds=bounds,target_time_bounds=target_bounds)
        np.testing.assert_allclose(result['data'],6)
        refined=grid(time=(900,2700,4500,6300),unit='mm')
        refined_bounds=[{'start':stamp(t),'end':stamp(t+1800)} for t in range(0,7200,1800)]
        result=calculate(data,source,refined,temporal='overlap',time_semantics='accumulation',source_time_bounds=bounds,target_time_bounds=refined_bounds)
        np.testing.assert_allclose(result['data'].sum(axis=0),6)
        with self.assertRaises(ValueError):options(temporal='linear')
        with self.assertRaises(ValueError):options(temporal='linear',max_time_gap_seconds=3600,time_semantics='accumulation')

    def test_vertical_reference_direction_and_explicit_broadcast(self):
        source_axis=Axis(name='z',kind='vertical',unit='m',reference='surface',positive='down',values=[0,10])
        target_axis=Axis(name='level',kind='vertical',unit='m',reference='surface',positive='up',values=[-5])
        source=grid(vertical=source_axis);target=grid(vertical=target_axis)
        data=np.broadcast_to(np.array([0.,10.])[:,None,None],(2,2,2))
        result=calculate(data,source,target,vertical='linear',max_vertical_gap_metres=10)
        np.testing.assert_allclose(result['data'],5)
        target.axes[0].reference='another-datum'
        with self.assertRaises(ValueError):calculate(data,source,target,vertical='linear',max_vertical_gap_metres=10)
        result=calculate(data,source,target,vertical='linear',max_vertical_gap_metres=10,vertical_offset_metres=2)
        np.testing.assert_allclose(result['data'],3)
        with self.assertRaises(ValueError):calculate(np.ones((2,2)),grid(),grid(time=(0,1)))
        result=calculate(np.ones((2,2)),grid(),grid(time=(0,1)),broadcast=['time'])
        np.testing.assert_allclose(result['data'],1)

    def test_category_and_integer_output_precision(self):
        with self.assertRaisesRegex(ValueError,'category codes'):
            calculate([[16777217]*2]*2,grid(unit='1',dtype='int32'),grid(unit='1',dtype='float32'),categorical=True)
        with self.assertRaisesRegex(ValueError,'integer output'):
            calculate([[2**53]*2]*2,grid(unit='km',dtype='int64'),grid(unit='m',dtype='int64'))
        with self.assertRaisesRegex(ValueError,'vertical offsets'):
            calculate([[1,2],[3,4]],grid(),grid(),vertical_offset_metres=10)

    def test_reprojection_and_lossy_integer_rejection(self):
        source=grid();target=grid(x=(55659.74539663678,),y=(55660.45186542152,),crs='EPSG:3857')
        result=calculate([[0,1],[2,3]],source,target,spatial='linear')
        self.assertAlmostEqual(result['data'][0,0],1.5,places=8)
        with self.assertRaises(ValueError):calculate([[0,1],[2,3]],source,target,spatial='conservative')
        with self.assertRaises(ValueError):calculate([[0,1],[2,3]],source,grid(x=(.5,),y=(.5,),dtype='int16'),spatial='linear')
        with self.assertRaises(ValueError):calculate([[2**54,2**54],[2**54,2**54]],grid(unit='1',dtype='int64'),grid(unit='1',dtype='int64'))

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class AlignmentStorageTest(unittest.TestCase):
    def setUp(self):
        self.connection=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.connection.close)
        self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.connection)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.arrays=ArrayStore(self.connection,self.temp.name);self.store=PostgresStore(self.connection)
        dataset=Bundle.model_validate_json(FIXTURE.read_text()).dataset
        dataset.id='source';dataset.variables[0].id='v'
        self.store.register(CatalogueEntry(dataset=dataset,source_id='source'))
        target=dataset.model_copy(deep=True);target.id='target';target.variables[0].unit='K';target.variables[0].minimum=0;target.variables[0].maximum=400
        self.store.register(CatalogueEntry(dataset=target,source_id='target'))

    def tearDown(self):self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')

    def test_publish_conversion_coverage_and_reproducible_provenance(self):
        source=grid();data=np.array([[0.,1.],[2.,3.]])
        version=self.arrays.publish(source,lambda variable,slices:(data[slices],np.zeros(data[slices].shape,dtype=bool)))
        target=grid(x=(.25,.75),y=(.25,.75),unit='K');target.dataset_id='target'
        request=AlignmentRequest(target=target,inputs=[AlignmentInput(version_id=version,variable_id='v',output_variable='v',spatial='linear')])
        result=align(self.arrays,request);again=align(self.arrays,request)
        np.testing.assert_equal(result['variables']['v']['data'],again['variables']['v']['data'])
        self.assertEqual(result['provenance'],again['provenance'])
        published=align_and_publish(self.arrays,request)
        restored=self.arrays.read(SubsetQuery(version_id=published['version_id']))
        np.testing.assert_allclose(restored['variables']['v']['data'],np.array([[.75,1.25],[1.75,2.25]])+273.15)
        np.testing.assert_allclose(restored['variables']['v']['coverage'],1)
        self.assertEqual(restored['provenance']['sources'][0]['version_id'],str(version))
        self.assertEqual(restored['provenance']['sources'][0]['unit_conversion']['offset'],273.15)

    def test_partial_coverage_survives_storage(self):
        source=grid();data=np.array([[0.,1.],[2.,3.]]);mask=np.array([[True,False],[False,False]])
        version=self.arrays.publish(source,lambda variable,slices:(data[slices],mask[slices]))
        target=grid(x=(.5,),y=(.5,),unit='K');target.dataset_id='target'
        request=AlignmentRequest(target=target,inputs=[AlignmentInput(version_id=version,variable_id='v',output_variable='v',spatial='linear',minimum_coverage=.75,renormalize_missing=True)])
        published=align_and_publish(self.arrays,request)
        restored=self.arrays.read(SubsetQuery(version_id=published['version_id']))
        self.assertAlmostEqual(restored['variables']['v']['coverage'][0,0],.75)
        self.assertAlmostEqual(restored['variables']['v']['data'][0,0],275.15)
        chained=request.model_copy(deep=True);chained.inputs[0].version_id=published['version_id']
        with self.assertRaisesRegex(ValueError,'full coverage'):align(self.arrays,chained)

    def test_shared_weather_rainfall_and_categorical_grid(self):
        source=grid(time=(0,3600))
        planes=np.broadcast_to(np.array([0.,10.])[:,None,None],(2,2,2))+np.array([[0.,1.],[2.,3.]])[None,:,:]
        version=self.arrays.publish(source,lambda variable,slices:(planes[slices],np.zeros(planes[slices].shape,dtype=bool)))
        prototype=self.store.get('source').dataset
        rain_dataset=prototype.model_copy(deep=True);rain_dataset.id='rain-source';rain_dataset.variables=rain_dataset.variables[:1]
        rain_dataset.variables[0].id='rain';rain_dataset.variables[0].unit='mm';rain_dataset.variables[0].minimum=0;rain_dataset.variables[0].maximum=1000
        self.store.register(CatalogueEntry(dataset=rain_dataset,source_id='rain-source'))
        cover_dataset=rain_dataset.model_copy(deep=True);cover_dataset.id='cover-source';cover_dataset.variables[0].id='cover'
        cover_dataset.variables[0].unit='1';cover_dataset.variables[0].data_type='integer'
        self.store.register(CatalogueEntry(dataset=cover_dataset,source_id='cover-source'))
        rain=grid(time=(1800,),unit='mm');rain.dataset_id='rain-source';rain.variables[0].id='rain'
        rain_values=np.ones((1,2,2))
        rain_version=self.arrays.publish(rain,lambda variable,slices:(rain_values[slices],np.zeros(rain_values[slices].shape,dtype=bool)))
        cover=grid(unit='1',dtype='int16');cover.dataset_id='cover-source';cover.variables[0].id='cover'
        classes=np.array([[1,2],[3,4]],dtype='int16')
        cover_version=self.arrays.publish(cover,lambda variable,slices:(classes[slices],np.zeros(classes[slices].shape,dtype=bool)))
        target=grid(x=(.25,.75),y=(.25,.75),time=(1800,),unit='K');target.dataset_id='target'
        for name,unit,dtype in [('rain','mm','float64'),('cover','1','int16')]:
            variable=target.variables[0].model_copy(deep=True);variable.id=name;variable.unit=unit;variable.dtype=dtype
            target.variables.append(variable)
        registration=self.store.get('target')
        registration.dataset.variables.extend([rain_dataset.variables[0],cover_dataset.variables[0]])
        self.store.update('target',registration)
        bounds=[{'start':stamp(0),'end':stamp(3600)}]
        request=AlignmentRequest(target=target,inputs=[
            AlignmentInput(version_id=version,variable_id='v',output_variable='v',spatial='linear',temporal='linear',max_time_gap_seconds=3600),
            AlignmentInput(version_id=rain_version,variable_id='rain',output_variable='rain',spatial='conservative',temporal='overlap',time_semantics='accumulation',source_time_bounds=bounds,target_time_bounds=bounds),
            AlignmentInput(version_id=cover_version,variable_id='cover',output_variable='cover',categorical=True,broadcast=['time'])])
        result=align(self.arrays,request)
        np.testing.assert_allclose(result['variables']['v']['data'][0],np.array([[5.75,6.25],[6.75,7.25]])+273.15)
        np.testing.assert_allclose(result['variables']['rain']['data'],1)
        np.testing.assert_array_equal(result['variables']['cover']['data'][0],classes)
        for variable in result['variables'].values():self.assertFalse(variable['mask'].any())

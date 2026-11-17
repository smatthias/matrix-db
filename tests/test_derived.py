import copy
import os
from pathlib import Path
import tempfile
import unittest
from uuid import UUID
import numpy as np
import psycopg
from matrix_db.alignment import AlignmentInput
from matrix_db.arrays import ArrayStore, GridSpec, SubsetQuery
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle, Variable
from matrix_db.derived import Definition, DefinitionRef, DerivationRequest, Association, Parameter, calculate, dependency_order, register_definitions, register_association, derive_and_publish
from matrix_db.storage import migrate, PostgresStore

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'

def definitions():
    speed=Definition(id='wind-speed',version=1,description='Magnitude of aligned east/north components',unit='m/s',function='wind_speed',minimum=0,
        inputs={'east':{'kind':'source','source':'east_source','unit':'m/s'},'north':{'kind':'source','source':'north_source','unit':'m/s'}})
    alert=Definition(id='wind-alert',version=1,description='Strict threshold exceedance, not a calibrated risk estimate',unit='1',expression='speed > limit',minimum=0,maximum=1,
        inputs={'speed':{'kind':'derived','definition':{'id':'wind-speed','version':1},'unit':'m/s'}},parameters={'limit':{'value':4,'unit':'m/s'}})
    return speed,alert

def field(data,unit='m/s',mask=None,quality=None):
    data=np.array(data,dtype='float64')
    return {'data':data,'mask':np.zeros(data.shape,dtype=bool) if mask is None else mask,'quality':quality,'coverage':None,'unit':unit,'dimensions':['row','column']}

def grid(dataset,variables):
    return GridSpec(dataset_id=dataset,axes=[{'name':'row','kind':'latitude','unit':'degrees_north','values':[0.,1.]},
        {'name':'column','kind':'longitude','unit':'degrees_east','values':[0.,1.]}],variables=[
            {'id':name,'unit':unit,'dtype':dtype,'dimensions':['row','column'],'chunks':[1,1]} for name,unit,dtype in variables])

class DerivedCalculationTest(unittest.TestCase):
    def test_registered_and_expression_wind_speed(self):
        speed,alert=definitions()
        inputs={'east':field([[3,0],[-3,0]]),'north':field([[14.4,18],[14.4,0]],unit='km/h')}
        result=calculate(speed,inputs)
        np.testing.assert_allclose(result['data'],[[5,5],[5,0]])
        expression=speed.model_dump();expression.update(function=None,expression='sqrt(east ** 2 + north ** 2)',version=2)
        other=calculate(Definition.model_validate(expression),inputs)
        np.testing.assert_allclose(other['data'],result['data'])
        flag=calculate(alert,{'speed':result});np.testing.assert_array_equal(flag['data'],[[1,1],[1,0]])
        flag=calculate(alert,{'speed':result},{'limit':Parameter(value=18,unit='km/h')})
        np.testing.assert_array_equal(flag['data'],0)

    def test_unsafe_or_dimensionally_invalid_expressions(self):
        speed,_=definitions()
        for expression in ["__import__('os')",'east.real','east[0]','[x for x in east]','east ** 100','east + 1','sqrt(east)','east > 2','east ** '+('9'*200)]:
            payload=speed.model_dump();payload.update(function=None,expression=expression)
            with self.subTest(expression=expression),self.assertRaises(ValueError):Definition.model_validate(payload)
        payload=speed.model_dump();payload['unit']='K'
        with self.assertRaises(ValueError):Definition.model_validate(payload)

    def test_dependency_order_missing_and_cycles(self):
        speed,alert=definitions();registry={d.key():d for d in (alert,speed)}
        self.assertEqual(dependency_order(registry,[alert.key()]),[speed.key(),alert.key()])
        with self.assertRaisesRegex(ValueError,'missing'):dependency_order({alert.key():alert},[alert.key()])
        def dependent(name,other):return Definition(id=name,version=1,description='test',unit='m/s',expression='x',inputs={'x':{'kind':'derived','definition':{'id':other,'version':1},'unit':'m/s'}})
        left,right=dependent('left','right'),dependent('right','left')
        with self.assertRaisesRegex(ValueError,'cyclic'):dependency_order({left.key():left,right.key():right},[left.key()])

    def test_missing_quality_coverage_and_ranges(self):
        speed,_=definitions();quality=np.array([[0,1],[2,3]],dtype='uint8')
        inputs={'east':field([[3,3],[3,3]],quality=quality),'north':field([[4,4],[4,4]],quality=np.zeros((2,2),dtype='uint8'))}
        result=calculate(speed,inputs)
        np.testing.assert_array_equal(result['quality'],quality)
        np.testing.assert_array_equal(result['mask'],[[False,False],[True,False]])
        reject=Definition.model_validate({**speed.model_dump(),'missing':'reject'})
        with self.assertRaisesRegex(ValueError,'missing'):calculate(reject,inputs)
        inputs['east']['mask']=np.ones((2,2),dtype=bool)
        result=calculate(speed,inputs);self.assertTrue(result['mask'].all())
        constrained=Definition.model_validate({**speed.model_dump(),'maximum':4})
        result=calculate(constrained,{'east':field([[3]]),'north':field([[4]])})
        self.assertTrue(result['mask'][0,0]);self.assertEqual(result['quality'][0,0],2)

    def test_invalid_domains_and_parameter_checks(self):
        ratio=Definition(id='ratio',version=1,description='test',unit='1',expression='a / b',inputs={
            'a':{'kind':'source','source':'a','unit':'m/s'},'b':{'kind':'source','source':'b','unit':'m/s'}})
        inputs={'a':field([[1,1]]),'b':field([[0,2]])}
        result=calculate(ratio,inputs)
        np.testing.assert_array_equal(result['mask'],[[True,False]]);self.assertEqual(result['quality'][0,0],2)
        self.assertEqual(result['data'][0,1],.5)
        reject=Definition.model_validate({**ratio.model_dump(),'invalid':'reject'})
        with self.assertRaisesRegex(ValueError,'invalid'):calculate(reject,inputs)
        _,alert=definitions()
        with self.assertRaises(ValueError):calculate(alert,{'speed':field([[1]])},{'unknown':Parameter(value=1)})
        with self.assertRaises(ValueError):calculate(alert,{'speed':field([[1]])},{'limit':Parameter(value=1,unit='K')})

    def test_temperature_difference_and_registered_mean(self):
        inputs={'left':{'kind':'source','source':'left','unit':'degC'},'right':{'kind':'source','source':'right','unit':'K'}}
        difference=Definition(id='difference',version=1,description='temperature difference',unit='delta_K',function='difference',inputs=inputs)
        values={'left':field([[20]],unit='degC'),'right':field([[290]],unit='K')}
        self.assertAlmostEqual(calculate(difference,values)['data'][0,0],3.15)
        mean=Definition(id='mean',version=1,description='temperature mean',unit='degC',function='mean',inputs=inputs)
        self.assertAlmostEqual(calculate(mean,values)['data'][0,0],18.425)

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class DerivedStorageTest(unittest.TestCase):
    def setUp(self):
        self.connection=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.connection.close)
        self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.connection)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.arrays=ArrayStore(self.connection,self.temp.name);self.store=PostgresStore(self.connection)
        dataset=Bundle.model_validate_json(FIXTURE.read_text()).dataset
        dataset.id='wind-source';dataset.variables=[Variable(id='east',quantity='east_wind_component',unit='m/s'),Variable(id='north',quantity='north_wind_component',unit='km/h')]
        self.store.register(CatalogueEntry(dataset=dataset,source_id='vector-wind'))
        target=dataset.model_copy(deep=True);target.id='derived-wind'
        target.variables=[Variable(id='speed',quantity='wind_speed',unit='m/s',minimum=0),Variable(id='alert',quantity='threshold_flag',unit='1',data_type='integer',minimum=0,maximum=1)]
        self.store.register(CatalogueEntry(dataset=target,source_id='derived-wind'))
        self.source=grid('wind-source',[('east','m/s','float64'),('north','km/h','float64')])
        self.data={'east':np.array([[3.,0.],[-3.,0.]]),'north':np.array([[14.4,18.],[14.4,0.]])}
        self.version=self.publish()
        self.speed,self.alert=definitions();register_definitions(self.connection,[self.alert,self.speed])
        self.request=DerivationRequest(target=grid('derived-wind',[('speed','m/s','float64'),('alert','1','uint8')]),outputs={
            'speed':DefinitionRef(id='wind-speed',version=1),'alert':DefinitionRef(id='wind-alert',version=1)},bindings={
            'east_source':{'version_id':self.version,'variable_id':'east'},'north_source':{'version_id':self.version,'variable_id':'north'}})

    def tearDown(self):self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
    def publish(self):
        def provider(variable,slices):
            data=self.data[variable][slices]
            return data,np.zeros(data.shape,dtype=bool),np.ones(data.shape),np.zeros(data.shape,dtype='uint8')
        return self.arrays.publish(self.source,provider)

    def test_graph_publication_cache_and_provenance(self):
        result=derive_and_publish(self.arrays,self.request)
        self.assertFalse(result['cache_hit'])
        restored=self.arrays.read(SubsetQuery(version_id=result['version_id']))
        np.testing.assert_allclose(restored['variables']['speed']['data'],[[5,5],[5,0]])
        np.testing.assert_array_equal(restored['variables']['alert']['data'],[[1,1],[1,0]])
        np.testing.assert_array_equal(restored['variables']['alert']['quality'],0)
        np.testing.assert_array_equal(restored['variables']['speed']['coverage'],1)
        self.assertEqual([d['key'] for d in restored['provenance']['definitions']],['wind-speed@1','wind-alert@1'])
        cached=derive_and_publish(self.arrays,self.request)
        self.assertTrue(cached['cache_hit']);self.assertEqual(cached['version_id'],result['version_id'])
        self.assertEqual(self.connection.execute('SELECT count(*) FROM matrix_db.derived_cache_sources').fetchone()[0],2)
        with self.assertRaises(psycopg.Error):self.connection.execute("UPDATE matrix_db.derived_cache SET cache_key='changed'")

    def test_cache_changes_for_parameters_definition_and_source_versions(self):
        original=derive_and_publish(self.arrays,self.request)
        changed=self.request.model_copy(deep=True);changed.parameters={'wind-alert@1':{'limit':Parameter(value=18,unit='km/h')}}
        other=derive_and_publish(self.arrays,changed)
        self.assertNotEqual(original['cache_key'],other['cache_key'])
        np.testing.assert_array_equal(self.arrays.read(SubsetQuery(version_id=other['version_id']))['variables']['alert']['data'],0)
        speed2=Definition.model_validate({**self.speed.model_dump(),'version':2,'function':None,'expression':'hypot(east,north)'})
        register_definitions(self.connection,[speed2])
        changed=self.request.model_copy(deep=True);changed.outputs['speed']=DefinitionRef(id='wind-speed',version=2)
        newer=derive_and_publish(self.arrays,changed)
        self.assertNotEqual(newer['cache_key'],original['cache_key'])
        self.data['east'][0,0]=6;version=self.publish()
        changed=self.request.model_copy(deep=True)
        for binding in changed.bindings.values():binding.version_id=version
        newer=derive_and_publish(self.arrays,changed)
        self.assertNotEqual(newer['cache_key'],original['cache_key'])
        self.assertAlmostEqual(self.arrays.read(SubsetQuery(version_id=newer['version_id']))['variables']['speed']['data'][0,0],np.hypot(6,4))
        self.assertEqual(derive_and_publish(self.arrays,self.request)['version_id'],original['version_id'])

    def test_scope_selection_and_explicit_alignment(self):
        original=derive_and_publish(self.arrays,self.request)
        selected=self.request.model_copy(deep=True)
        for binding in selected.bindings.values():binding.slices={'column':{'start':0,'stop':1}}
        selected.target.axes[1].values=[0.];selected.target.axes[1].bounds=[(-.5,.5)]
        result=derive_and_publish(self.arrays,selected)
        self.assertNotEqual(result['cache_key'],original['cache_key'])
        self.assertEqual(self.arrays.read(SubsetQuery(version_id=result['version_id']))['variables']['speed']['data'].shape,(2,1))
        shifted=self.request.model_copy(deep=True);shifted.target.axes[1].values=[.25,.75];shifted.target.axes[1].bounds=None
        with self.assertRaisesRegex(ValueError,'alignment'):derive_and_publish(self.arrays,shifted)
        for binding in shifted.bindings.values():binding.alignment=AlignmentInput(version_id=self.version,variable_id=binding.variable_id,output_variable=binding.variable_id,spatial='linear')
        result=derive_and_publish(self.arrays,shifted)
        restored=self.arrays.read(SubsetQuery(version_id=result['version_id']))
        self.assertAlmostEqual(restored['variables']['speed']['data'][0,0],np.hypot(2.25,4.25))
        np.testing.assert_array_equal(restored['variables']['speed']['quality'],0)

    def test_immutable_registration_and_descriptive_associations(self):
        register_definitions(self.connection,[self.speed,self.alert])
        with self.assertRaises(ValueError):register_definitions(self.connection,[Definition.model_validate({**self.speed.model_dump(),'description':'different'})])
        with self.assertRaises(psycopg.Error):self.connection.execute("UPDATE matrix_db.derived_definitions SET checksum='changed'")
        cached=derive_and_publish(self.arrays,self.request)
        register_association(self.connection,Association(id='wind-context',subject=DefinitionRef(id='wind-speed',version=1),object=DefinitionRef(id='wind-alert',version=1),description='Descriptive context only; no causal assertion'))
        self.assertEqual(derive_and_publish(self.arrays,self.request)['version_id'],cached['version_id'])
        cyclic=Definition(id='self-cycle',version=1,description='cycle',unit='m/s',expression='x',inputs={'x':{'kind':'derived','definition':{'id':'self-cycle','version':1},'unit':'m/s'}})
        with self.assertRaisesRegex(ValueError,'cyclic'):register_definitions(self.connection,[cyclic])
        self.assertEqual(self.connection.execute('SELECT count(*) FROM matrix_db.derived_definitions').fetchone()[0],2)

    def test_cached_payload_integrity_and_missing_sources(self):
        result=derive_and_publish(self.arrays,self.request)
        manifest=self.arrays.manifest(result['version_id'])
        path=Path(self.temp.name)/'versions'/result['version_id']/manifest['chunks'][0]['path'];path.write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError,'cached output'):derive_and_publish(self.arrays,self.request)
        broken=self.request.model_copy(deep=True);del broken.bindings['north_source']
        with self.assertRaisesRegex(ValueError,'bindings'):derive_and_publish(self.arrays,broken)

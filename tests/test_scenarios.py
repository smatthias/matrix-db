from datetime import datetime,timezone
import copy
import os
from pathlib import Path
import tempfile
import unittest
from uuid import UUID,uuid4
import numpy as np
import psycopg
from matrix_db.arrays import ArrayStore,GridSpec,SubsetQuery
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle,Variable
from matrix_db.derived import Definition,DefinitionRef,register_definitions
from matrix_db.scenarios import Scenario,ScenarioRef,ObservationPin,ComparisonSide,RunRequest,RunChange,OutputAttachment,ComparisonRequest,register_scenario,resolve_inputs,create_run,transition_run,get_run,attach_output,publish_run_output,compare_and_publish
from matrix_db.storage import migrate,PostgresStore

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'
TIMES=['2026-11-17T08:00:00Z','2026-11-17T09:00:00Z','2026-11-17T10:00:00Z']
ENGINE={'name':'synthetic-test','version':'1','revision':'fixture-v1','environment':{'runtime':'numpy-fixture'}}

def grid(dataset='scenario-results',variable='temperature',unit='degC'):
    return GridSpec(dataset_id=dataset,axes=[{'name':'time','kind':'time','unit':'UTC','values':TIMES},
        {'name':'lat','kind':'latitude','unit':'degrees_north','values':[52.25,52.75]},
        {'name':'lon','kind':'longitude','unit':'degrees_east','values':[13.25,13.75]}],variables=[
            {'id':variable,'unit':unit,'dtype':'float64','dimensions':['time','lat','lon'],'chunks':[1,1,1]}])

def scenario(version=1,pin=None):
    return Scenario(id='warming',version=version,name='Synthetic warming scenario',description='Storage fixture; no physical prediction',
        region={'west':13,'east':14,'south':52,'north':53},period={'start':TIMES[0],'end':'2026-11-17T11:00:00Z'},grid=grid(),
        inputs={'forcing':{'kind':'array','role':'forcing','version_id':uuid4() if pin is None else pin,'variable_id':'temperature'}},
        parameters={'warming':{'value':0,'unit':'delta_K'}})

class ScenarioContractTest(unittest.TestCase):
    def test_time_region_parent_and_pin_validation(self):
        base=scenario().model_dump(mode='json')
        for mutation in [lambda d:d.update(parent={'id':'warming','version':1}),
                         lambda d:d['period'].update(end=TIMES[0]),
                         lambda d:d['period'].update(end='2026-11-17T08:30:00Z'),
                         lambda d:d['region'].update(east=13.3),
                         lambda d:d['inputs']['forcing'].update(kind='observation')]:
            payload=copy.deepcopy(base);mutation(payload)
            with self.assertRaises(ValueError):Scenario.model_validate(payload)

    def test_execution_and_comparison_validation(self):
        payload={'scenario':{'id':'warming','version':1},'engine':ENGINE}
        with self.assertRaises(ValueError):RunRequest.model_validate({**payload,'seed':-1})
        with self.assertRaises(ValueError):RunRequest.model_validate({**payload,'engine':{**ENGINE,'environment':{}}})
        with self.assertRaises(ValueError):RunRequest.model_validate({**payload,'configuration':{'bad':float('nan')}})
        with self.assertRaises(ValueError):RunChange(run_id=uuid4(),expected_revision=1,status='running',actor=' ',reason='start')
        with self.assertRaises(ValueError):ComparisonRequest(left={'kind':'array','version_id':uuid4(),'variable_id':'temperature'},
            right={'kind':'array','version_id':uuid4(),'variable_id':'temperature'},target=grid('scenario-difference','difference','delta_K'))

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class ScenarioStorageTest(unittest.TestCase):
    def setUp(self):
        self.connection=psycopg.connect(DSN,autocommit=True);self.addCleanup(self.connection.close)
        self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE');migrate(self.connection)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.arrays=ArrayStore(self.connection,self.temp.name);self.store=PostgresStore(self.connection)
        bundle=Bundle.model_validate_json(FIXTURE.read_text());source=bundle.dataset.model_copy(deep=True);source.id='scenario-source'
        self.store.register(CatalogueEntry(dataset=source,source_id=source.id))
        output=source.model_copy(deep=True);output.id='scenario-results';output.origin='simulation'
        self.store.register(CatalogueEntry(dataset=output,source_id=output.id))
        difference=output.model_copy(deep=True);difference.id='scenario-difference'
        difference.variables=[Variable(id='difference',quantity='temperature_difference',unit='delta_K')]
        self.store.register(CatalogueEntry(dataset=difference,source_id=difference.id))
        self.observation=bundle.observations[0].model_copy(update={'dataset_id':'scenario-source'})
        self.store.add_observations('scenario-source',[self.observation])
        self.source_data=np.full((3,2,2),10.,dtype='float64')
        self.source_version=self.arrays.publish(grid('scenario-source'),self.provider(self.source_data))
        identity=Definition(id='initial-temperature',version=1,description='Pinned temperature identity',unit='degC',expression='x',
            inputs={'x':{'kind':'source','source':'forcing','unit':'degC'}})
        register_definitions(self.connection,[identity])
        self.scenario=scenario(pin=self.source_version)
        self.scenario.inputs['initial-point']=ObservationPin(role='initial',dataset_id='scenario-source',observation_id=self.observation.id,revision=1)
        self.scenario=Scenario.model_validate(self.scenario.model_dump(mode='json'))
        self.scenario.calculations=[DefinitionRef(id='initial-temperature',version=1)]
        self.scenario=Scenario.model_validate(self.scenario.model_dump(mode='json'))
        register_scenario(self.arrays,self.scenario)

    def tearDown(self):self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
    @staticmethod
    def provider(data):return lambda variable,slices:(data[slices],np.zeros(data[slices].shape,dtype=bool))
    def request(self,version=1):return RunRequest(scenario={'id':'warming','version':version},engine=ENGINE,seed=42,configuration={'fixture':'temperature'})
    def start(self,version=1):
        run=create_run(self.arrays,self.request(version))
        return transition_run(self.arrays,RunChange(run_id=run['id'],expected_revision=1,status='running',reason='worker started',actor='test-worker'))
    def change(self,run,status,reason='fixture transition'):
        return transition_run(self.arrays,RunChange(run_id=run['id'],expected_revision=run['revision'],status=status,reason=reason,actor='test-worker'))
    def complete(self,value,version=1):
        run=self.start(version);data=np.broadcast_to(np.arange(3.)[:,None,None]+value,(3,2,2)).copy()
        published=publish_run_output(self.arrays,run['id'],grid(),self.provider(data),expected_revision=run['revision'],checkpoint='final',provisional=False)
        return self.change(run,'completed'),published

    def test_pinned_inputs_branching_and_metadata_protection(self):
        original=resolve_inputs(self.arrays,ScenarioRef(id='warming',version=1))
        self.source_data+=1;new_version=self.arrays.publish(grid('scenario-source'),self.provider(self.source_data))
        corrected=self.observation.model_copy(update={'value':20})
        self.store.revise(corrected,expected_revision=1,reason='late source correction',actor='provider')
        snapshot=resolve_inputs(self.arrays,ScenarioRef(id='warming',version=1))
        self.assertEqual(snapshot,original)
        self.assertEqual(snapshot['inputs']['initial-point']['record']['value'],15.2)
        self.assertEqual(snapshot['inputs']['forcing']['pin']['version_id'],str(self.source_version))
        branch=self.scenario.model_copy(deep=True);branch.version=2;branch.parent=ScenarioRef(id='warming',version=1)
        branch.inputs['forcing'].version_id=new_version;branch.inputs['initial-point'].revision=2
        register_scenario(self.arrays,branch)
        self.assertEqual(resolve_inputs(self.arrays,ScenarioRef(id='warming',version=2))['inputs']['initial-point']['record']['value'],20)
        with self.assertRaises(ValueError):register_scenario(self.arrays,self.scenario.model_copy(update={'description':'changed'}))
        replacement=self.store.get('scenario-results');replacement.dataset.variables[0].unit='K'
        with self.assertRaises(ValueError):self.store.update('scenario-results',replacement)
        with self.assertRaises(psycopg.Error):self.connection.execute("UPDATE matrix_db.scenarios SET checksum='changed'")

    def test_lifecycle_retries_failures_and_cancellation(self):
        queued=create_run(self.arrays,self.request())
        self.assertEqual((queued['status'],queued['attempt'],queued['revision']),('queued',1,1))
        self.assertIsNone(queued['started_at'])
        with self.assertRaisesRegex(ValueError,'transition'):self.change(queued,'completed')
        running=self.change(queued,'running');self.assertIsNotNone(running['started_at'])
        with self.assertRaisesRegex(ValueError,'revision conflict'):self.change(queued,'cancelled')
        failed=self.change(running,'failed','solver failed fixture')
        self.assertEqual(failed['error'],'solver failed fixture')
        self.assertEqual([event['status'] for event in failed['events']],['queued','running','failed'])
        self.assertEqual(failed['specification']['seed'],42)
        with self.assertRaises(ValueError):self.change(failed,'running')
        retry=create_run(self.arrays,self.request());self.assertEqual(retry['attempt'],2)
        cancelled=self.change(retry,'cancelled');self.assertIsNotNone(cancelled['finished_at'])
        self.assertEqual(cancelled['outputs'],[])

    def test_provisional_outputs_and_completed_publication(self):
        run=self.start();partial=grid();partial.axes[0].values=partial.axes[0].values[:1]
        data=np.full((1,2,2),10.)
        provisional=publish_run_output(self.arrays,run['id'],partial,self.provider(data),expected_revision=2,checkpoint='hour-0',provisional=True)
        self.assertEqual(get_run(self.connection,run['id'])['outputs'],[])
        self.assertTrue(get_run(self.connection,run['id'],include_provisional=True)['outputs'][0]['provisional'])
        with self.assertRaisesRegex(ValueError,'every final'):self.change(run,'completed')
        with self.assertRaises(ValueError):publish_run_output(self.arrays,run['id'],partial,self.provider(data),expected_revision=2,checkpoint='bad-final',provisional=False)
        final=publish_run_output(self.arrays,run['id'],grid(),self.provider(np.full((3,2,2),12.)),expected_revision=2,checkpoint='final',provisional=False)
        attachment=OutputAttachment(run_id=run['id'],expected_revision=2,version_id=final['version_id'],variable_id='temperature',checkpoint='final',provisional=False)
        attach_output(self.arrays,attachment)
        completed=self.change(run,'completed')
        self.assertEqual(len(completed['outputs']),1);self.assertFalse(completed['outputs'][0]['provisional'])
        self.assertEqual(self.arrays.manifest(final['version_id'])['provenance']['run_id'],str(run['id']))
        with self.assertRaises(ValueError):attach_output(self.arrays,attachment)
        self.assertNotEqual(provisional['version_id'],final['version_id'])

    def test_output_contract_and_integrity_checks_block_completion(self):
        run=self.start();wrong=grid();wrong.variables[0].unit='K'
        with self.assertRaisesRegex(ValueError,'contract'):publish_run_output(self.arrays,run['id'],wrong,self.provider(np.full((3,2,2),280.)),expected_revision=2,checkpoint='wrong',provisional=False)
        final=publish_run_output(self.arrays,run['id'],grid(),self.provider(np.full((3,2,2),10.)),expected_revision=2,checkpoint='final',provisional=False)
        manifest=self.arrays.manifest(final['version_id']);chunk=manifest['chunks'][0]
        (Path(self.temp.name)/'versions'/final['version_id']/chunk['path']).write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError,'checksum'):self.change(run,'completed')
        self.assertEqual(get_run(self.connection,run['id'])['status'],'running')
        self.assertEqual(get_run(self.connection,run['id'])['outputs'],[])

    def test_run_and_observation_comparisons_maps_and_time_series(self):
        left,_=self.complete(10)
        branch=self.scenario.model_copy(deep=True);branch.version=2;branch.parent=ScenarioRef(id='warming',version=1)
        branch.parameters['warming'].value=2;register_scenario(self.arrays,branch)
        right,_=self.complete(12,version=2)
        request=ComparisonRequest(left={'kind':'run','run_id':left['id'],'variable_id':'temperature'},
            right={'kind':'run','run_id':right['id'],'variable_id':'temperature'},target=grid('scenario-difference','difference','delta_K'))
        comparison=compare_and_publish(self.arrays,request)
        restored=self.arrays.read(SubsetQuery(version_id=comparison['version_id']))
        np.testing.assert_allclose(restored['variables']['difference']['data'],2)
        map_slice=self.arrays.read(SubsetQuery(version_id=comparison['version_id'],slices={'time':{'start':1,'stop':2}}))
        series=self.arrays.read(SubsetQuery(version_id=comparison['version_id'],slices={'lat':{'start':0,'stop':1},'lon':{'start':0,'stop':1}}))
        self.assertEqual(map_slice['variables']['difference']['data'].shape,(1,2,2))
        self.assertEqual(series['variables']['difference']['data'].shape,(3,1,1))
        self.assertEqual(restored['provenance']['direction'],'right_minus_left')
        observed=request.model_copy(deep=True)
        observed.left=ComparisonSide(kind='array',version_id=self.source_version,variable_id='temperature')
        observed=ComparisonRequest.model_validate(observed.model_dump(mode='json'))
        comparison=compare_and_publish(self.arrays,observed)
        data=self.arrays.read(SubsetQuery(version_id=comparison['version_id']))['variables']['difference']['data']
        np.testing.assert_allclose(data[:,0,0],[2,3,4])
        wrong=request.model_copy(deep=True);wrong.target.variables[0].unit='degC'
        with self.assertRaises(ValueError):compare_and_publish(self.arrays,wrong)
        incomplete=create_run(self.arrays,self.request());wrong=request.model_copy(deep=True);wrong.left.run_id=incomplete['id']
        with self.assertRaisesRegex(ValueError,'completed'):compare_and_publish(self.arrays,wrong)

    def test_comparison_selection_and_explicit_alignment(self):
        left,left_output=self.complete(10);right,right_output=self.complete(12)
        request=ComparisonRequest(left={'kind':'run','run_id':left['id'],'variable_id':'temperature','slices':{'time':{'start':1,'stop':2}}},
            right={'kind':'run','run_id':right['id'],'variable_id':'temperature','slices':{'time':{'start':1,'stop':2}}},target=grid('scenario-difference','difference','delta_K'))
        request.target.axes[0].values=request.target.axes[0].values[1:2]
        result=compare_and_publish(self.arrays,request)
        self.assertEqual(self.arrays.read(SubsetQuery(version_id=result['version_id']))['variables']['difference']['data'].shape,(1,2,2))
        request=ComparisonRequest(left={'kind':'run','run_id':left['id'],'variable_id':'temperature'},right={'kind':'run','run_id':right['id'],'variable_id':'temperature'},target=grid('scenario-difference','difference','delta_K'))
        request.target.axes[1].values=[52.375,52.625];request.target.axes[1].bounds=None
        with self.assertRaisesRegex(ValueError,'alignment'):compare_and_publish(self.arrays,request)
        from matrix_db.alignment import AlignmentInput
        for side,output in [(request.left,left_output),(request.right,right_output)]:
            side.alignment=AlignmentInput(version_id=output['version_id'],variable_id='temperature',output_variable='temperature',spatial='linear')
        result=compare_and_publish(self.arrays,request)
        np.testing.assert_allclose(self.arrays.read(SubsetQuery(version_id=result['version_id']))['variables']['difference']['data'],2)

    def test_transaction_rollback_and_missing_snapshot_inputs(self):
        published=None;run=None
        with self.assertRaises(RuntimeError):
            with self.connection.transaction():
                run=self.start()
                published=publish_run_output(self.arrays,run['id'],grid(),self.provider(np.full((3,2,2),10.)),expected_revision=2,checkpoint='final',provisional=False)
                raise RuntimeError('interrupted external transaction')
        with self.assertRaises(KeyError):get_run(self.connection,run['id'])
        self.assertIn(published['version_id'],self.arrays.unreferenced_directories()['unreferenced_or_inflight'])
        broken=self.scenario.model_copy(deep=True);broken.id='missing-input';broken.inputs['forcing'].version_id=uuid4()
        with self.assertRaises(KeyError):register_scenario(self.arrays,broken)
        self.store.withdraw('scenario-source',self.observation.id,expected_revision=1,reason='withdrawn source',actor='provider')
        broken=self.scenario.model_copy(deep=True);broken.id='withdrawn-pin';broken.inputs['initial-point'].revision=2
        with self.assertRaisesRegex(ValueError,'withdrawn'):register_scenario(self.arrays,broken)

    def test_input_corruption_prevents_run_start(self):
        queued=create_run(self.arrays,self.request())
        manifest=self.arrays.manifest(self.source_version);chunk=manifest['chunks'][0]
        (Path(self.temp.name)/'versions'/str(self.source_version)/chunk['path']).write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError,'checksum'):self.change(queued,'running')
        self.assertEqual(get_run(self.connection,queued['id'])['status'],'queued')

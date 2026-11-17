"""Synthetic storage/lifecycle demo, not a TerraFlux simulation or hazard prediction."""
import json
import os
from pathlib import Path
import sys
import numpy as np
from matrix_db.arrays import ArrayStore,GridSpec
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Dataset,Variable
from matrix_db.scenarios import Scenario,ScenarioRef,RunRequest,RunChange,register_scenario,create_run,transition_run,publish_run_output
from matrix_db.storage import connect,PostgresStore

source=Dataset(id='simulation-demo-forcing',name='Synthetic forcing',provider='Matrix DB examples',license='CC0-1.0',
    source_reference='synthetic:simulation-demo',origin='simulation',coverage={'west':13,'east':14,'south':52,'north':53},
    variables=[Variable(id='temperature',quantity='air_temperature',unit='degC',minimum=-90,maximum=60)])
output=source.model_copy(deep=True);output.id='simulation-demo-results';output.name='Synthetic execution outputs'
difference=output.model_copy(deep=True);difference.id='simulation-demo-difference';difference.name='Synthetic scenario difference'
difference.variables=[Variable(id='difference',quantity='temperature_difference',unit='delta_K')]
axes=[{'name':'time','kind':'time','unit':'UTC','values':['2026-11-17T08:00:00Z','2026-11-17T09:00:00Z','2026-11-17T10:00:00Z']},
    {'name':'lat','kind':'latitude','unit':'degrees_north','values':[52.25,52.75]},
    {'name':'lon','kind':'longitude','unit':'degrees_east','values':[13.25,13.75]}]
def spec(dataset):
    return GridSpec(dataset_id=dataset.id,axes=axes,variables=[{'id':v.id,'unit':v.unit,'dtype':'float64','dimensions':['time','lat','lon'],'chunks':[1,1,1]} for v in dataset.variables])
def provider(data):return lambda variable,slices:(data[slices],np.zeros(data[slices].shape,dtype=bool))
engine={'name':'synthetic-example','version':'1','revision':'fixture-v1','environment':{'numpy':np.__version__,'fixture':'constant-grid-plus-time'}}
with connect() as connection:
    store=PostgresStore(connection);arrays=ArrayStore(connection,os.environ['MATRIX_DB_ARRAY_ROOT'])
    for dataset in (source,output,difference):
        try:store.get(dataset.id)
        except KeyError:store.register(CatalogueEntry(dataset=dataset,source_id=dataset.id))
    forcing=arrays.publish(spec(source),provider(np.full((3,2,2),10.)))
    base=Scenario(id='synthetic-warming',version=1,name='Synthetic warming',description='Demonstrates immutable inputs and external execution results only',
        region=source.coverage,period={'start':axes[0]['values'][0],'end':'2026-11-17T11:00:00Z'},grid=spec(output),
        inputs={'forcing':{'kind':'array','role':'forcing','version_id':forcing,'variable_id':'temperature'}},parameters={'warming':{'value':0,'unit':'delta_K'}})
    branch=base.model_copy(deep=True);branch.version=2;branch.parent=ScenarioRef(id=base.id,version=1);branch.parameters['warming'].value=2
    runs=[]
    for scenario in (base,branch):
        register_scenario(arrays,scenario)
        request=RunRequest(scenario={'id':scenario.id,'version':scenario.version},engine=engine,seed=7,
                           configuration={'fixture_offset':scenario.parameters['warming'].value})
        run=create_run(arrays,request)
        run=transition_run(arrays,RunChange(run_id=run['id'],expected_revision=1,status='running',reason='synthetic worker started',actor='example'))
        data=np.broadcast_to(10+scenario.parameters['warming'].value+np.arange(3.)[:,None,None],(3,2,2)).copy()
        published=publish_run_output(arrays,run['id'],scenario.grid,provider(data),expected_revision=run['revision'],checkpoint='final',provisional=False)
        run=transition_run(arrays,RunChange(run_id=run['id'],expected_revision=run['revision'],status='completed',reason='synthetic output verified',actor='example'))
        runs.append({'run_id':str(run['id']),'scenario_version':scenario.version,'version_id':published['version_id']})
    Path(sys.argv[1]).write_text(json.dumps({'runs':runs},indent=2)+'\n')
    comparison={'left':{'kind':'run','run_id':runs[0]['run_id'],'variable_id':'temperature'},
        'right':{'kind':'run','run_id':runs[1]['run_id'],'variable_id':'temperature'},'target':spec(difference).model_dump(mode='json')}
    Path(sys.argv[2]).write_text(json.dumps(comparison,indent=2)+'\n')

"""Prepare a small synthetic Celsius-to-Kelvin alignment for db-align."""
import json
import os
from pathlib import Path
import sys
from matrix_db.arrays import ArrayStore, GridSpec
from matrix_db.catalogue import CatalogueEntry
from matrix_db.storage import connect, PostgresStore

source_version=json.loads(Path(sys.argv[1]).read_text())['version_id']
with connect() as connection:
    arrays=ArrayStore(connection,os.environ['MATRIX_DB_ARRAY_ROOT'])
    manifest=arrays.manifest(source_version)
    store=PostgresStore(connection)
    dataset=store.get(manifest['spec']['dataset_id']).dataset.model_copy(deep=True)
    dataset.id='aligned-weather';dataset.name='Aligned synthetic Kelvin weather'
    dataset.variables=[v for v in dataset.variables if v.id=='temperature']
    dataset.variables[0].unit='K';dataset.variables[0].minimum=0;dataset.variables[0].maximum=400
    try:store.get(dataset.id)
    except KeyError:store.register(CatalogueEntry(dataset=dataset,source_id='synthetic:aligned-weather'))
    target=manifest['spec']
    target['dataset_id']=dataset.id;target['previous_version']=None
    target['variables'][0]['unit']='K';target['variables'][0]['dtype']='float64'
    for axis in target['axes']:
        if axis['kind']=='time':axis['values']=['2026-11-17T08:30:00Z','2026-11-17T09:30:00Z']
        elif axis['kind']=='latitude':axis['values']=[52.375,52.625];axis['bounds']=None
        elif axis['kind']=='longitude':axis['values']=[13.2,13.6];axis['bounds']=None
    target=GridSpec.model_validate(target).model_dump(mode='json')
    request={'target':target,'inputs':[{'version_id':source_version,'variable_id':'temperature','output_variable':'temperature',
        'spatial':'linear','temporal':'linear','max_time_gap_seconds':3600}]}
    Path(sys.argv[2]).write_text(json.dumps(request,indent=2)+'\n')

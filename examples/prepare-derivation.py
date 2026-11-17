"""Register and publish a tiny synthetic vector field and prepare a derived request."""
import json
import os
from pathlib import Path
import sys
import numpy as np
from matrix_db.arrays import ArrayStore, GridSpec
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Dataset, Variable
from matrix_db.storage import connect, PostgresStore

source=Dataset(id='derived-demo-source',name='Synthetic vector wind',provider='Matrix DB examples',license='CC0-1.0',
    source_reference='synthetic:vector-wind',coverage={'west':-1,'east':2,'south':-1,'north':2},origin='simulation',variables=[
        Variable(id='east',quantity='east_wind_component',unit='m/s'),Variable(id='north',quantity='north_wind_component',unit='km/h')])
target=source.model_copy(deep=True);target.id='derived-demo-result';target.name='Derived synthetic wind'
target.variables=[Variable(id='wind-speed',quantity='wind_speed',unit='m/s',minimum=0),
    Variable(id='wind-alert',quantity='threshold_flag',unit='1',data_type='integer',minimum=0,maximum=1)]
axes=[{'name':'row','kind':'latitude','unit':'degrees_north','values':[0.,1.]},
    {'name':'column','kind':'longitude','unit':'degrees_east','values':[0.,1.]}]
def specification(dataset):
    return GridSpec(dataset_id=dataset.id,axes=axes,variables=[{'id':variable.id,'unit':variable.unit,
        'dtype':'uint8' if variable.data_type=='integer' else 'float64','dimensions':['row','column'],'chunks':[1,1]} for variable in dataset.variables])
with connect() as connection:
    store=PostgresStore(connection)
    for dataset in (source,target):
        try:store.get(dataset.id)
        except KeyError:store.register(CatalogueEntry(dataset=dataset,source_id=dataset.id))
    arrays=ArrayStore(connection,os.environ['MATRIX_DB_ARRAY_ROOT'])
    values={'east':np.array([[3.,0.],[-3.,0.]]),'north':np.array([[14.4,18.],[14.4,0.]])}
    quality=np.array([[0,0],[1,0]],dtype='uint8')
    def provider(variable,slices):
        data=values[variable][slices]
        return data,np.zeros(data.shape,dtype=bool),np.ones(data.shape),quality[slices]
    version=arrays.publish(specification(source),provider)
    request={'target':specification(target).model_dump(mode='json'),'outputs':{
        'wind-speed':{'id':'wind-speed','version':1},'wind-alert':{'id':'wind-alert','version':1}},
        'bindings':{'east_source':{'version_id':str(version),'variable_id':'east'},
                    'north_source':{'version_id':str(version),'variable_id':'north'}}}
    Path(sys.argv[1]).write_text(json.dumps(request,indent=2)+'\n')

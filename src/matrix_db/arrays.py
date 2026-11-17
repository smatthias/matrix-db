"""Regular grids, lossless per-chunk NPZ storage and immutable Postgres manifests."""
from datetime import datetime
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import shutil
from typing import Annotated, Literal
from uuid import UUID, uuid4
import numpy as np
from pydantic import Field, StrictInt, model_validator
from psycopg.types.json import Jsonb
from .catalogue import CatalogueEntry
from .contracts import Contract, Identifier, Observation

MAX_BYTES=64*1024*1024
DTYPES=Literal['float32','float64','int16','int32','int64','uint8']

def digest(path):
    result=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):result.update(block)
    return result.hexdigest()

def canonical(value):
    # JSONB normalizes signed numeric zero; hash the same representation on disk and in SQL.
    def normalize(item):
        if isinstance(item,float) and item==0:return 0.0
        if isinstance(item,list):return [normalize(v) for v in item]
        if isinstance(item,dict):return {k:normalize(v) for k,v in item.items()}
        return item
    return json.dumps(normalize(value),sort_keys=True,separators=(',',':'),allow_nan=False)

def sync_dir(path):
    fd=os.open(path,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)

class Axis(Contract):
    name: Identifier
    kind: Literal['longitude','latitude','x','y','time','vertical','other']
    unit: str = Field(min_length=1)
    values: list = Field(min_length=1,max_length=100000)
    bounds: list[tuple[float,float]] | None = None
    reference: str | None = None
    positive: Literal['up','down'] | None = None

    @model_validator(mode='after')
    def validate_axis(self):
        if self.kind=='time':
            if self.unit!='UTC' or self.bounds is not None:raise ValueError('time axes require UTC and no numeric bounds')
            normalized=[]
            for value in self.values:
                if value is None:raise ValueError('time coordinates cannot be null')
                raw=Observation.explicit_timestamp(value)
                if isinstance(raw,str):raw=datetime.fromisoformat(raw.replace('Z','+00:00'))
                normalized.append(Observation.utc_time(raw))
            if any(a>=b for a,b in zip(normalized,normalized[1:])):raise ValueError('times must be strictly increasing')
            self.values=[v.isoformat() for v in normalized]
            return self
        if any(type(v) not in (float,int) or not math.isfinite(v) for v in self.values):
            raise ValueError('numeric axes require finite numbers')
        values=np.array(self.values,dtype=np.float64)
        steps=np.diff(values)
        if len(steps) and not (np.all(steps>0) or np.all(steps<0)):
            raise ValueError('axis coordinates must be strictly monotonic')
        horizontal=self.kind in ('longitude','latitude','x','y')
        if horizontal and len(steps) and not np.allclose(steps,steps[0],rtol=1e-8,atol=1e-10):
            raise ValueError('horizontal grids must have regular spacing')
        if self.kind=='vertical' and (not self.reference or self.positive is None or self.unit!='m'):
            raise ValueError('vertical axes require metre units, reference and positive direction')
        expected={'longitude':'degrees_east','latitude':'degrees_north','x':'m','y':'m'}.get(self.kind)
        if expected and self.unit!=expected:raise ValueError(f'{self.kind} unit must be {expected}')
        if horizontal and self.bounds is None:
            if not len(steps):raise ValueError('singleton horizontal axes require explicit bounds')
            half=abs(steps[0])/2
            self.bounds=[(float(v-half),float(v+half)) for v in values]
        if self.bounds is not None:
            if len(self.bounds)!=len(values):raise ValueError('bounds must match coordinate count')
            for value,(low,high) in zip(values,self.bounds):
                if not low<high or not low<=value<=high:raise ValueError('bounds must enclose each cell centre')
            if len(values)>1:
                ordered=self.bounds if values[0]<values[-1] else list(reversed(self.bounds))
                if any(left[1]>right[0]+1e-10 for left,right in zip(ordered,ordered[1:])):
                    raise ValueError('cell bounds must not overlap')
        limit={'longitude':180,'latitude':90}.get(self.kind)
        if limit and (np.any(np.abs(values)>limit) or any(abs(v)>limit for pair in self.bounds or [] for v in pair)):
            raise ValueError('geographic coordinates and bounds exceed allowed range')
        return self

class ArrayVariable(Contract):
    id: Identifier
    unit: str = Field(min_length=1)
    dtype: DTYPES
    dimensions: list[Identifier] = Field(min_length=2,max_length=8)
    chunks: list[Annotated[StrictInt,Field(ge=1)]]

class GridSpec(Contract):
    schema_version: Literal[1]=1
    dataset_id: Identifier
    previous_version: UUID | None = None
    crs: Literal['EPSG:4326','EPSG:3857']='EPSG:4326'
    axes: list[Axis] = Field(min_length=2,max_length=8)
    variables: list[ArrayVariable] = Field(min_length=1)

    @model_validator(mode='after')
    def consistent_dimensions(self):
        axes={a.name:a for a in self.axes}
        if len(axes)!=len(self.axes):raise ValueError('axis names must be unique')
        roles=[a.kind for a in self.axes if a.kind!='other']
        if len(roles)!=len(set(roles)):raise ValueError('axis roles must be unique')
        required={'longitude','latitude'} if self.crs=='EPSG:4326' else {'x','y'}
        if not required.issubset(roles) or (set(roles)&{'longitude','latitude','x','y'})!=required:
            raise ValueError('horizontal axes must match CRS')
        ids=[v.id for v in self.variables]
        if len(ids)!=len(set(ids)):raise ValueError('variable IDs must be unique')
        for variable in self.variables:
            if len(variable.dimensions)!=len(variable.chunks) or len(set(variable.dimensions))!=len(variable.dimensions):
                raise ValueError('dimensions and chunk sizes must match without duplicates')
            if not set(variable.dimensions).issubset(axes):raise ValueError('unknown variable dimension')
            if not required.issubset({axes[name].kind for name in variable.dimensions}):
                raise ValueError('each variable requires horizontal dimensions')
            actual=[min(size,len(axes[name].values)) for name,size in zip(variable.dimensions,variable.chunks)]
            if math.prod(actual)*(np.dtype(variable.dtype).itemsize+1)>MAX_BYTES:raise ValueError('chunk exceeds 64 MiB')
            count=math.prod(math.ceil(len(axes[name].values)/size) for name,size in zip(variable.dimensions,variable.chunks))
            if count>100000:raise ValueError('variable exceeds 100000 chunks')
        return self

class IndexWindow(Contract):
    start: Annotated[StrictInt,Field(ge=0)]
    stop: Annotated[StrictInt,Field(ge=0)]
    @model_validator(mode='after')
    def ordered(self):
        if self.start>self.stop:raise ValueError('slice start must not follow stop')
        return self

class CoordinateWindow(Contract):
    minimum: float | str
    maximum: float | str

class SubsetQuery(Contract):
    version_id: UUID
    variables: list[Identifier] | None = None
    slices: dict[str,IndexWindow] = Field(default_factory=dict)
    ranges: dict[str,CoordinateWindow] = Field(default_factory=dict)
    @model_validator(mode='after')
    def unambiguous(self):
        if set(self.slices)&set(self.ranges):raise ValueError('choose index or coordinate selection per axis')
        if self.variables is not None and (not self.variables or len(set(self.variables))!=len(self.variables)):
            raise ValueError('variable selection must be nonempty and unique')
        return self

class ArrayStore:
    def __init__(self,connection,root):
        self.connection=connection
        self.root=Path(root).resolve()
        self.root.mkdir(parents=True,exist_ok=True)
        for name in ('.staging','versions'):(self.root/name).mkdir(exist_ok=True)

    def publish(self,spec:GridSpec,read_chunk,*,provenance=None):
        """Callback returns (numeric ndarray, boolean missing mask) for variable and slices."""
        spec=GridSpec.model_validate_json(spec.model_dump_json())
        with self.connection.transaction():
            row=self.connection.execute('SELECT entry FROM matrix_db.datasets WHERE id=%s',(spec.dataset_id,)).fetchone()
            if row is None:raise KeyError(spec.dataset_id)
            registered=CatalogueEntry.model_validate(row[0]).dataset
        definitions={v.id:v for v in registered.variables}
        for variable in spec.variables:
            definition=definitions.get(variable.id)
            if definition is None or definition.unit!=variable.unit:raise ValueError('array variable/unit must match registered dataset')
            if definition.data_type=='integer' and np.dtype(variable.dtype).kind not in ('i','u'):
                raise ValueError('integer variables require integer array storage')
        version=uuid4()
        staging=self.root/'.staging'/str(version)
        final=self.root/'versions'/str(version)
        staging.mkdir()
        manifest={'spec':spec.model_dump(mode='json'),'version_id':str(version),'chunks':[]}
        if provenance is not None:manifest['provenance']=provenance
        try:
            axes={a.name:a for a in spec.axes}
            for variable in spec.variables:
                shape=[len(axes[name].values) for name in variable.dimensions]
                starts=[range(0,size,chunk) for size,chunk in zip(shape,variable.chunks)]
                directory=staging/variable.id;directory.mkdir()
                variable_coverage=None
                for offset in itertools.product(*starts):
                    slices=tuple(slice(start,min(start+chunk,size)) for start,chunk,size in zip(offset,variable.chunks,shape))
                    expected=tuple(s.stop-s.start for s in slices)
                    supplied=read_chunk(variable.id,slices)
                    if len(supplied) not in (2,3):raise ValueError('provider must return data, mask and optional coverage')
                    data,mask=supplied[:2]
                    data=np.asarray(data);mask=np.asarray(mask)
                    coverage=None if len(supplied)==2 else np.asarray(supplied[2],dtype=np.float64)
                    has_coverage=coverage is not None
                    if variable_coverage is None:variable_coverage=has_coverage
                    if variable_coverage!=has_coverage:raise ValueError('all variable chunks must agree on coverage presence')
                    if has_coverage and (coverage.shape!=expected or not np.isfinite(coverage).all() or np.any((coverage<0)|(coverage>1))):
                        raise ValueError('coverage must match shape and contain fractions in [0,1]')
                    if data.nbytes+mask.nbytes+(0 if coverage is None else coverage.nbytes)>MAX_BYTES:
                        raise ValueError('chunk data/mask/coverage exceeds 64 MiB')
                    if data.shape!=expected or mask.shape!=expected:raise ValueError('provider returned incorrect chunk shape')
                    if data.dtype!=np.dtype(variable.dtype) or mask.dtype!=np.dtype('bool'):
                        raise ValueError('provider dtype/mask differs from declared contract')
                    valid=data[~mask]
                    if not np.isfinite(valid).all():raise ValueError('nonmissing values must be finite')
                    definition=definitions[variable.id]
                    if definition.minimum is not None and np.any(valid<definition.minimum):raise ValueError('array value below variable minimum')
                    if definition.maximum is not None and np.any(valid>definition.maximum):raise ValueError('array value above variable maximum')
                    relative=f'{variable.id}/'+'.'.join(str(v) for v in offset)+'.npz'
                    path=staging/relative
                    with path.open('wb') as stream:
                        extras={} if coverage is None else {'coverage':coverage}
                        np.savez_compressed(stream,data=data,mask=mask,**extras)
                        stream.flush();os.fsync(stream.fileno())
                    manifest['chunks'].append({'variable':variable.id,'offset':list(offset),'shape':list(expected),
                        'path':relative,'checksum':digest(path),'coverage':has_coverage})
                sync_dir(directory)
            for chunk in manifest['chunks']:
                if digest(staging/chunk['path'])!=chunk['checksum']:
                    raise ValueError('chunk changed during staging')
            payload=canonical(manifest)
            with (staging/'manifest.json').open('w',encoding='utf-8') as stream:
                stream.write(payload);stream.flush();os.fsync(stream.fileno())
            sync_dir(staging)
            with self.connection.transaction():
                row=self.connection.execute('SELECT entry FROM matrix_db.datasets WHERE id=%s FOR SHARE',(spec.dataset_id,)).fetchone()
                if row is None:raise KeyError(spec.dataset_id)
                dataset=CatalogueEntry.model_validate(row[0]).dataset
                if dataset.variables!=registered.variables:
                    raise ValueError('dataset variables changed while staging; restart publication')
                # Files precede the commit: unreferenced directories are recoverable, missing published files are not.
                os.replace(staging,final);sync_dir(self.root/'versions')
                self.connection.execute('''INSERT INTO matrix_db.array_versions
                    (dataset_id,version_id,previous_version,manifest,manifest_checksum) VALUES (%s,%s,%s,%s,%s)''',
                    (spec.dataset_id,version,spec.previous_version,Jsonb(manifest),hashlib.sha256(payload.encode()).hexdigest()))
            return version
        finally:
            if staging.exists():shutil.rmtree(staging)

    def manifest(self,version_id):
        version=UUID(str(version_id))
        row=self.connection.execute('SELECT manifest,manifest_checksum FROM matrix_db.array_versions WHERE version_id=%s',(version,)).fetchone()
        if row is None:raise KeyError(str(version))
        payload=canonical(row[0])
        if hashlib.sha256(payload.encode()).hexdigest()!=row[1]:raise ValueError('catalogue manifest checksum mismatch')
        path=self.root/'versions'/str(version)/'manifest.json'
        if digest(path)!=row[1]:raise ValueError('stored manifest checksum mismatch')
        return row[0]

    def read(self,query:SubsetQuery):
        query=SubsetQuery.model_validate_json(query.model_dump_json())
        manifest=self.manifest(query.version_id)
        spec=GridSpec.model_validate(manifest['spec'])
        axes={a.name:a for a in spec.axes}
        if not (set(query.slices)|set(query.ranges)).issubset(axes):raise ValueError('unknown selection axis')
        variables={v.id:v for v in spec.variables}
        selected=query.variables if query.variables is not None else list(variables)
        if not set(selected).issubset(variables):raise ValueError('unknown selected variable')
        indices={}
        for name,axis in axes.items():
            values=np.array(axis.values)
            if name in query.slices:
                window=query.slices[name]
                if window.stop>len(values):raise ValueError('slice exceeds axis size')
                indices[name]=np.arange(window.start,window.stop)
            elif name in query.ranges:
                window=query.ranges[name]
                if axis.kind=='time':
                    def timestamp(value):
                        value=Observation.explicit_timestamp(value)
                        if not isinstance(value,str):raise ValueError('time windows require timezone-aware strings')
                        return Observation.utc_time(datetime.fromisoformat(value.replace('Z','+00:00'))).timestamp()
                    values=np.array([datetime.fromisoformat(v).timestamp() for v in axis.values])
                    low,high=timestamp(window.minimum),timestamp(window.maximum)
                else:
                    if type(window.minimum) not in (int,float) or type(window.maximum) not in (int,float):
                        raise ValueError('numeric axes require numeric coordinate windows')
                    low,high=window.minimum,window.maximum
                if low>high and axis.kind!='longitude':raise ValueError('coordinate range must be ordered')
                condition=(values>=low)&(values<=high) if low<=high else (values>=low)|(values<=high)
                indices[name]=np.flatnonzero(condition)
            else:indices[name]=np.arange(len(values))
        coverage_variables={chunk['variable'] for chunk in manifest['chunks'] if chunk.get('coverage',False)}
        size=sum(math.prod(len(indices[name]) for name in variables[v].dimensions)*(np.dtype(variables[v].dtype).itemsize+1+(8 if v in coverage_variables else 0)) for v in selected)
        if size>MAX_BYTES:raise ValueError('subset exceeds 64 MiB; select a smaller window')
        result={};read_count=0
        for name in selected:
            variable=variables[name]
            shape=tuple(len(indices[dimension]) for dimension in variable.dimensions)
            output=np.zeros(shape,dtype=variable.dtype);missing=np.ones(shape,dtype=bool)
            coverage_output=np.zeros(shape,dtype=np.float64) if name in coverage_variables else None
            relevant=[chunk for chunk in manifest['chunks'] if chunk['variable']==name and
                all(np.any((indices[dimension]>=offset)&(indices[dimension]<offset+length))
                    for dimension,offset,length in zip(variable.dimensions,chunk['offset'],chunk['shape']))]
            for chunk in relevant:
                path=self.root/'versions'/str(query.version_id)/chunk['path']
                if digest(path)!=chunk['checksum']:raise ValueError('chunk checksum mismatch')
                with np.load(path,allow_pickle=False) as archive:
                    data=archive['data'];mask=archive['mask']
                    chunk_coverage=archive['coverage'] if chunk.get('coverage',False) else None
                if list(data.shape)!=chunk['shape'] or mask.shape!=data.shape or data.dtype!=np.dtype(variable.dtype) or mask.dtype!=bool:
                    raise ValueError('stored chunk does not match manifest')
                destinations=[];sources=[]
                for dimension,offset,length in zip(variable.dimensions,chunk['offset'],chunk['shape']):
                    index=indices[dimension]
                    positions=np.flatnonzero((index>=offset)&(index<offset+length))
                    destinations.append(positions);sources.append(index[positions]-offset)
                output[np.ix_(*destinations)]=data[np.ix_(*sources)]
                missing[np.ix_(*destinations)]=mask[np.ix_(*sources)]
                if coverage_output is not None:
                    if chunk_coverage is None or chunk_coverage.shape!=data.shape or not np.isfinite(chunk_coverage).all() or np.any((chunk_coverage<0)|(chunk_coverage>1)):
                        raise ValueError('stored coverage does not match manifest')
                    coverage_output[np.ix_(*destinations)]=chunk_coverage[np.ix_(*sources)]
                read_count+=1
            result[name]={'data':output,'mask':missing,'dimensions':variable.dimensions,'unit':variable.unit,'coverage':coverage_output}
        return {'version_id':str(query.version_id),'crs':spec.crs,
            'axes':{name:{**axis.model_dump(mode='json'),'values':[axis.values[int(i)] for i in indices[name]],
                'bounds':None if axis.bounds is None else [axis.bounds[int(i)] for i in indices[name]]} for name,axis in axes.items()},
            'variables':result,'chunks_read':read_count,'provenance':manifest.get('provenance')}

    def unreferenced_directories(self):
        # Read-only inventory; a directory can belong to a transaction not yet committed.
        referenced={str(row[0]) for row in self.connection.execute('SELECT version_id FROM matrix_db.array_versions')}
        return {'unreferenced_or_inflight':sorted(p.name for p in (self.root/'versions').iterdir() if p.is_dir() and p.name not in referenced),
                'staging':sorted(p.name for p in (self.root/'.staging').iterdir() if p.is_dir())}

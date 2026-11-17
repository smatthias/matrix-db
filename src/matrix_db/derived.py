"""Immutable calculation definitions, dependency evaluation and exact-input caches."""
import hashlib
import math
from typing import Annotated, Literal
from uuid import UUID
import numpy as np
import pyproj
from pydantic import Field, StrictInt, model_validator, field_validator
from psycopg.types.json import Jsonb
from .alignment import AlignmentInput, align_field, cast_output, role
from .arrays import ArrayStore, ArrayVariable, GridSpec, SubsetQuery, IndexWindow, CoordinateWindow, MAX_BYTES, canonical, digest
from .contracts import Contract, Identifier, Number
from .expressions import unit_info,parse,analyze,evaluate,registered_signature,registered_evaluate
from .units import conversion

ENGINE_VERSION='matrix-db-derived-1'
Alias=Annotated[str,Field(pattern=r'^[A-Za-z][A-Za-z0-9_]*$',max_length=80)]

class DefinitionRef(Contract):
    id: Identifier
    version: Annotated[StrictInt,Field(ge=1,le=1000000000)]
    def key(self):return f'{self.id}@{self.version}'

class InputRef(Contract):
    kind: Literal['source','derived']
    unit: str=Field(min_length=1)
    source: Identifier | None=None
    definition: DefinitionRef | None=None
    @model_validator(mode='after')
    def exactly_one(self):
        if self.kind=='source' and (self.source is None or self.definition is not None):raise ValueError('source input requires only a source binding name')
        if self.kind=='derived' and (self.definition is None or self.source is not None):raise ValueError('derived input requires only a definition reference')
        unit_info(self.unit)
        return self

class Parameter(Contract):
    value: Number
    unit: str='1'
    @model_validator(mode='after')
    def known_unit(self):unit_info(self.unit);return self

class Definition(DefinitionRef):
    description: str=Field(min_length=1,max_length=2000)
    relationship: Literal['computational']='computational'
    inputs: dict[Alias,InputRef]=Field(min_length=1,max_length=16)
    parameters: dict[Alias,Parameter]=Field(default_factory=dict,max_length=16)
    unit: str=Field(min_length=1)
    expression: str | None=None
    function: Literal['wind_speed','threshold_exceedance','difference','mean'] | None=None
    function_version: Literal['1']='1'
    missing: Literal['propagate','reject']='propagate'
    invalid: Literal['mask','reject']='mask'
    excluded_quality: list[Literal[0,1,2,3]]=Field(default_factory=lambda:[2])
    minimum: float | None=None
    maximum: float | None=None

    @model_validator(mode='after')
    def valid_calculation(self):
        if (self.expression is None)==(self.function is None):raise ValueError('choose exactly one expression or registered function')
        if set(self.inputs)&set(self.parameters):raise ValueError('input and parameter names must be distinct')
        if self.minimum is not None and self.maximum is not None and self.minimum>self.maximum:raise ValueError('output range must be ordered')
        signatures={name:unit_info(value.unit)[0] for name,value in {**self.inputs,**self.parameters}.items()}
        if self.expression is not None:
            signature,used=analyze(parse(self.expression),signatures)
            if used!=set(signatures):raise ValueError('every declared input/parameter must be used')
        else:signature=registered_signature(self.function,signatures)
        if signature!=unit_info(self.unit)[0]:raise ValueError('declared output unit does not match calculation dimensions')
        return self

class Association(Contract):
    id: Identifier
    subject: DefinitionRef
    object: DefinitionRef
    kind: Literal['association']='association'
    description: str=Field(min_length=1,max_length=2000)

class SourceBinding(Contract):
    version_id: UUID
    variable_id: Identifier
    slices: dict[str,IndexWindow]=Field(default_factory=dict)
    ranges: dict[str,CoordinateWindow]=Field(default_factory=dict)
    alignment: AlignmentInput | None=None
    @model_validator(mode='after')
    def consistent_selection(self):
        SubsetQuery(version_id=self.version_id,slices=self.slices,ranges=self.ranges)
        if self.alignment is not None and (self.alignment.version_id!=self.version_id or self.alignment.variable_id!=self.variable_id):
            raise ValueError('alignment must reference the same pinned source variable')
        return self

class DerivationRequest(Contract):
    target: GridSpec
    outputs: dict[Identifier,DefinitionRef]=Field(min_length=1,max_length=16)
    bindings: dict[Identifier,SourceBinding]=Field(min_length=1,max_length=32)
    parameters: dict[str,dict[Alias,Parameter]]=Field(default_factory=dict)
    @model_validator(mode='after')
    def same_grid(self):
        variables=self.target.variables
        if set(self.outputs)!={v.id for v in variables}:raise ValueError('each target variable requires a derived output reference')
        if any(v.dimensions!=variables[0].dimensions for v in variables):raise ValueError('derived outputs must share one explicit dimension order')
        return self


def checksum(definition):return hashlib.sha256(canonical(definition.model_dump(mode='json')).encode()).hexdigest()


def dependency_order(definitions,roots):
    order=[];state={}
    def visit(key,path):
        if len(path)>64:raise ValueError('dependency graph exceeds 64 levels')
        if key not in definitions:raise ValueError(f'missing calculation dependency: {key}')
        if state.get(key)==1:raise ValueError('cyclic calculation dependencies: '+' -> '.join(path+[key]))
        if state.get(key)==2:return
        state[key]=1
        for reference in definitions[key].inputs.values():
            if reference.kind=='derived':visit(reference.definition.key(),path+[key])
        state[key]=2;order.append(key)
        if len(order)>128:raise ValueError('dependency graph exceeds 128 definitions')
    for root in roots:visit(root,[])
    return order


def load_definitions(connection):
    rows=connection.execute('SELECT definition,checksum FROM matrix_db.derived_definitions ORDER BY id,version LIMIT 10001').fetchall()
    if len(rows)>10000:raise ValueError('definition catalogue exceeds current execution limit')
    definitions={}
    for payload,expected in rows:
        definition=Definition.model_validate(payload)
        if checksum(definition)!=expected:raise ValueError('calculation definition checksum mismatch')
        definitions[definition.key()]=definition
    return definitions


def register_definitions(connection,definitions):
    definitions=[Definition.model_validate_json(d.model_dump_json()) for d in definitions]
    if not definitions or len(definitions)>128:raise ValueError('register 1–128 definitions per batch')
    keys=[d.key() for d in definitions]
    if len(keys)!=len(set(keys)):raise ValueError('duplicate definition in registration batch')
    with connection.transaction():
        connection.execute('SELECT pg_advisory_xact_lock(734192009)')
        existing=load_definitions(connection);combined={**existing,**{d.key():d for d in definitions}}
        dependency_order(combined,keys)
        for definition in definitions:
            if definition.key() in existing and checksum(existing[definition.key()])!=checksum(definition):
                raise ValueError('definition version already exists with different contents; create a new version')
            for reference in definition.inputs.values():
                if reference.kind=='derived':conversion(combined[reference.definition.key()].unit,reference.unit)
        for definition in definitions:
            connection.execute('INSERT INTO matrix_db.derived_definitions(id,version,definition,checksum) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                (definition.id,definition.version,Jsonb(definition.model_dump(mode='json')),checksum(definition)))
        for definition in definitions:
            for name,reference in definition.inputs.items():
                if reference.kind=='derived':
                    connection.execute('''INSERT INTO matrix_db.derived_dependencies
                        (id,version,input_name,dependency_id,dependency_version) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                        (definition.id,definition.version,name,reference.definition.id,reference.definition.version))
    return keys


def register_association(connection,association):
    association=Association.model_validate_json(association.model_dump_json())
    with connection.transaction():
        connection.execute('''INSERT INTO matrix_db.derived_associations
            (id,subject_id,subject_version,object_id,object_version,kind,description) VALUES (%s,%s,%s,%s,%s,%s,%s)''',
            (association.id,association.subject.id,association.subject.version,association.object.id,association.object.version,association.kind,association.description))
    return association.id


def combine_quality(qualities,shape):
    # Codes: good=0, suspect=1, bad=2, unknown=3; severity order is good < unknown < suspect < bad.
    ranks=np.array([0,2,3,1],dtype='uint8');codes=np.array([0,3,1,2],dtype='uint8')
    worst=np.zeros(shape,dtype='uint8')
    for quality in qualities:worst=np.maximum(worst,ranks[quality])
    return codes[worst]


def calculate(definition,fields,overrides=None):
    overrides={} if overrides is None else overrides
    if set(overrides)-set(definition.parameters):raise ValueError('unknown calculation parameter override')
    if set(fields)!=set(definition.inputs):raise ValueError('calculation inputs do not match definition')
    first=next(iter(fields.values()));shape=first['data'].shape
    values={};masks=[];qualities=[];coverages=[]
    for name,reference in definition.inputs.items():
        field=fields[name];data=np.asarray(field['data']);mask=np.asarray(field['mask'])
        if data.shape!=shape or mask.shape!=shape or mask.dtype!=bool:raise ValueError('calculation input shapes/masks differ')
        if data.dtype.kind not in ('i','u','f'):raise ValueError('calculations require numeric inputs')
        if data.dtype.kind in ('i','u') and np.any(np.abs(data[~mask].astype(object))>2**53):raise ValueError('source integers exceed exact float64 arithmetic')
        conversion(field['unit'],reference.unit)
        _,scale,offset=unit_info(field['unit'])
        values[name]=data.astype('float64')*scale+offset
        quality=field.get('quality')
        quality=np.full(shape,3,dtype='uint8') if quality is None else np.asarray(quality)
        if quality.shape!=shape or quality.dtype!=np.dtype('uint8') or np.any(quality>3):raise ValueError('invalid input quality codes')
        masks.append(mask|np.isin(quality,definition.excluded_quality));qualities.append(quality)
        coverage=field.get('coverage')
        coverage=np.where(mask,0.,1.) if coverage is None else np.asarray(coverage,dtype='float64')
        if coverage.shape!=shape or not np.isfinite(coverage).all() or np.any((coverage<0)|(coverage>1)):raise ValueError('invalid input coverage')
        coverages.append(coverage)
    for name,default in definition.parameters.items():
        parameter=overrides.get(name,default)
        conversion(parameter.unit,default.unit)
        _,scale,offset=unit_info(parameter.unit)
        values[name]=parameter.value*scale+offset
    mask=np.logical_or.reduce(masks)
    if definition.missing=='reject' and mask.any():raise ValueError('calculation rejects missing or excluded-quality inputs')
    quality=combine_quality(qualities,shape);coverage=np.minimum.reduce(coverages)
    with np.errstate(all='ignore'):
        raw=evaluate(parse(definition.expression),values) if definition.expression is not None else registered_evaluate(definition.function,values)
        _,scale,offset=unit_info(definition.unit)
        data=np.broadcast_to((np.asarray(raw,dtype='float64')-offset)/scale,shape).copy()
    invalid=~np.isfinite(data)
    if definition.minimum is not None:invalid|=data<definition.minimum
    if definition.maximum is not None:invalid|=data>definition.maximum
    if definition.invalid=='reject' and np.any(invalid&~mask):raise ValueError('calculation produced invalid or out-of-range values')
    quality=np.where(invalid&~mask,2,quality).astype('uint8');mask|=invalid
    data=np.where(mask,0.,data)
    return {'data':data,'mask':mask,'coverage':coverage,'quality':quality,'unit':definition.unit,'dimensions':first['dimensions']}


def source_field(arrays,binding,target):
    manifest=arrays.manifest(binding.version_id);spec=GridSpec.model_validate(manifest['spec'])
    variable=next((v for v in spec.variables if v.id==binding.variable_id),None)
    if variable is None:raise ValueError('unknown bound source variable')
    subset=arrays.read(SubsetQuery(version_id=binding.version_id,variables=[binding.variable_id],slices=binding.slices,ranges=binding.ranges))
    field=subset['variables'][binding.variable_id]
    selected_spec=spec.model_copy(deep=True)
    selected_spec.axes=[type(axis).model_validate(subset['axes'][axis.name]) for axis in spec.axes]
    target_variable=target.variables[0]
    if binding.alignment is not None:
        if field.get('coverage') is not None and np.any((field['coverage']<1-1e-10)&~field['mask']):
            raise ValueError('explicit realignment requires full-coverage source cells')
        temporary=target.model_copy(deep=True)
        mapped=ArrayVariable(id=binding.variable_id,unit=variable.unit,dtype='float64',dimensions=target_variable.dimensions,chunks=target_variable.chunks)
        temporary.variables=[mapped]
        return align_field(field['data'],field['mask'],selected_spec,variable,temporary,mapped,binding.alignment,quality=field.get('quality'),check_cancel=arrays.check_cancel)
    source_axes={axis.name:axis for axis in selected_spec.axes};target_axes={axis.name:axis for axis in target.axes}
    source_roles=[role(source_axes[name]) for name in variable.dimensions]
    target_roles=[role(target_axes[name]) for name in target_variable.dimensions]
    if spec.crs!=target.crs or set(source_roles)!=set(target_roles):raise ValueError('source grid differs; supply explicit alignment')
    permutation=[]
    for name,key in zip(target_variable.dimensions,target_roles):
        original=source_axes[variable.dimensions[source_roles.index(key)]];desired=target_axes[name]
        if original.model_dump(exclude={'name'})!=desired.model_dump(exclude={'name'}):raise ValueError('source coordinates differ; supply explicit alignment')
        permutation.append(source_roles.index(key))
    result={**field,'dimensions':target_variable.dimensions}
    for name in ('data','mask','coverage','quality'):
        if result.get(name) is not None:result[name]=np.transpose(result[name],permutation)
    return result


def derive_and_publish(arrays:ArrayStore,request:DerivationRequest):
    request=DerivationRequest.model_validate_json(request.model_dump_json())
    connection=arrays.connection;definitions=load_definitions(connection)
    order=dependency_order(definitions,[reference.key() for reference in request.outputs.values()])
    required={reference.source for key in order for reference in definitions[key].inputs.values() if reference.kind=='source'}
    if required!=set(request.bindings):raise ValueError('source bindings must exactly match reachable dependencies')
    if set(request.parameters)-set(order):raise ValueError('parameter overrides reference an unreachable definition')
    for key,parameters in request.parameters.items():
        definition=definitions[key]
        if set(parameters)-set(definition.parameters):raise ValueError('unknown parameter override')
        for name,parameter in parameters.items():conversion(parameter.unit,definition.parameters[name].unit)
    axes={axis.name:axis for axis in request.target.axes}
    cells=math.prod(len(axes[name].values) for name in request.target.variables[0].dimensions)
    if cells>100000 or cells*(len(order)+len(request.bindings)+len(request.outputs))*18>MAX_BYTES:
        raise ValueError('derived graph exceeds bounded cell/working-memory budget')
    sources=[];source_work=0
    for name,binding in sorted(request.bindings.items()):
        manifest=arrays.manifest(binding.version_id)
        source_spec=GridSpec.model_validate(manifest['spec'])
        source_variable=next((v for v in source_spec.variables if v.id==binding.variable_id),None)
        if source_variable is None:raise ValueError('unknown bound source variable')
        source_axes={a.name:a for a in source_spec.axes}
        source_work+=math.prod(len(source_axes[axis].values) for axis in source_variable.dimensions)*18
        if source_work+cells*(len(order)+len(request.outputs))*18>MAX_BYTES:
            raise ValueError('derived source/graph payloads exceed 64 MiB working budget')
        sources.append({'binding':name,'version_id':str(binding.version_id),'manifest_checksum':hashlib.sha256(canonical(manifest).encode()).hexdigest()})
    environment={'engine':ENGINE_VERSION,'numpy':np.__version__,'pyproj':pyproj.__version__,'proj':pyproj.proj_version_str,'unit_registry':1}
    provenance={**environment,'request':request.model_dump(mode='json'),'definitions':[
        {'key':key,'checksum':checksum(definitions[key]),'definition':definitions[key].model_dump(mode='json')} for key in order],'sources':sources}
    cache_key=hashlib.sha256(canonical(provenance).encode()).hexdigest()
    with connection.transaction():
        lock=int.from_bytes(bytes.fromhex(cache_key[:16]),byteorder='big',signed=True)
        connection.execute('SELECT pg_advisory_xact_lock(%s)',(lock,))
        cached=connection.execute('SELECT output_version FROM matrix_db.derived_cache WHERE cache_key=%s',(cache_key,)).fetchone()
        if cached:
            manifest=arrays.manifest(cached[0])
            for chunk in manifest['chunks']:
                if digest(arrays.root/'versions'/str(cached[0])/chunk['path'])!=chunk['checksum']:raise ValueError('cached output chunk checksum mismatch')
            return {'version_id':str(cached[0]),'cache_key':cache_key,'cache_hit':True}
        bound={name:source_field(arrays,binding,request.target) for name,binding in request.bindings.items()}
        computed={}
        for key in order:
            arrays.check_cancel()
            definition=definitions[key]
            inputs={name:bound[reference.source] if reference.kind=='source' else computed[reference.definition.key()]
                    for name,reference in definition.inputs.items()}
            computed[key]=calculate(definition,inputs,request.parameters.get(key))
        output={}
        for variable in request.target.variables:
            field=computed[request.outputs[variable.id].key()]
            scale,offset=conversion(field['unit'],variable.unit)
            output[variable.id]={**field,'data':cast_output(field['data']*scale+offset,field['mask'],variable.dtype),'unit':variable.unit}
        provenance['cache_key']=cache_key
        def provider(variable,slices):
            field=output[variable]
            return field['data'][slices],field['mask'][slices],field['coverage'][slices],field['quality'][slices]
        version=arrays.publish(request.target,provider,provenance=provenance)
        connection.execute('INSERT INTO matrix_db.derived_cache(cache_key,output_version,provenance) VALUES (%s,%s,%s)',
                           (cache_key,version,Jsonb(provenance)))
        for name,binding in request.bindings.items():
            connection.execute('INSERT INTO matrix_db.derived_cache_sources VALUES (%s,%s,%s)',(cache_key,name,binding.version_id))
        for key in order:
            arrays.check_cancel()
            definition=definitions[key]
            connection.execute('INSERT INTO matrix_db.derived_cache_definitions VALUES (%s,%s,%s)',(cache_key,definition.id,definition.version))
    return {'version_id':str(version),'cache_key':cache_key,'cache_hit':False}

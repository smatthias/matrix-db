"""Versioned scenario snapshots and external simulation execution/result records."""
import hashlib
import math
from typing import Annotated, Literal
from uuid import UUID,uuid4
import numpy as np
import pyproj
from pydantic import Field,JsonValue,StrictInt,model_validator,field_validator
from psycopg.types.json import Jsonb
from .alignment import AlignmentInput,cast_output
from .arrays import ArrayStore,GridSpec,IndexWindow,CoordinateWindow,MAX_BYTES,canonical,digest
from .catalogue import TimeCoverage
from .contracts import Contract,Coverage,Identifier,Number
from .derived import Definition,DefinitionRef,SourceBinding,calculate,source_field
from .storage import PostgresStore

PositiveRevision=Annotated[StrictInt,Field(ge=1)]

class ScenarioRef(Contract):
    id: Identifier
    version: PositiveRevision

class ArrayPin(Contract):
    kind: Literal['array']='array'
    role: Literal['initial','boundary','forcing','reference']
    version_id: UUID
    variable_id: Identifier

class ObservationPin(Contract):
    kind: Literal['observation']='observation'
    role: Literal['initial','boundary','forcing','reference']
    dataset_id: Identifier
    observation_id: Identifier
    revision: PositiveRevision

Pin=Annotated[ArrayPin|ObservationPin,Field(discriminator='kind')]

class ScenarioParameter(Contract):
    value: Number
    unit: str=Field(min_length=1)

class Scenario(ScenarioRef):
    name: str=Field(min_length=1,max_length=200)
    description: str=Field(min_length=1,max_length=2000)
    parent: ScenarioRef | None=None
    region: Coverage
    period: TimeCoverage
    grid: GridSpec
    inputs: dict[Identifier,Pin]=Field(min_length=1,max_length=32)
    calculations: list[DefinitionRef]=Field(default_factory=list,max_length=128)
    parameters: dict[Identifier,ScenarioParameter]=Field(default_factory=dict,max_length=128)

    @model_validator(mode='after')
    def explicit_extent(self):
        if self.period.start>=self.period.end:raise ValueError('scenario period requires positive duration')
        if self.parent is not None and (self.parent.id,self.parent.version)==(self.id,self.version):raise ValueError('scenario cannot branch from itself')
        calculation_keys=[reference.key() for reference in self.calculations]
        if len(calculation_keys)!=len(set(calculation_keys)):raise ValueError('duplicate calculation pin')
        for axis in self.grid.axes:
            if axis.kind=='time':
                from datetime import datetime
                if any(not self.period.start<=datetime.fromisoformat(value)<=self.period.end for value in axis.values):
                    raise ValueError('output grid times exceed scenario period')
        if self.grid.crs=='EPSG:4326':
            longitude=next(a for a in self.grid.axes if a.kind=='longitude')
            latitude=next(a for a in self.grid.axes if a.kind=='latitude')
            if any(not self.region.contains(x,y) for x in longitude.values for y in (min(latitude.values),max(latitude.values))):
                raise ValueError('geographic output centres exceed scenario region')
        if len(canonical(self.model_dump(mode='json')))>131072:raise ValueError('scenario metadata exceeds 128 KiB')
        return self

class EngineBuild(Contract):
    name: str=Field(min_length=1,max_length=200)
    version: str=Field(min_length=1,max_length=200)
    revision: str=Field(min_length=1,max_length=200)
    environment: dict[str,JsonValue]=Field(min_length=1,max_length=64)

class RunRequest(Contract):
    scenario: ScenarioRef
    engine: EngineBuild
    seed: Annotated[StrictInt,Field(ge=0,le=9223372036854775807)] | None=None
    configuration: dict[str,JsonValue]=Field(default_factory=dict,max_length=128)
    @model_validator(mode='after')
    def bounded(self):
        if len(canonical(self.model_dump(mode='json')))>65536:raise ValueError('run specification exceeds 64 KiB')
        return self

class RunChange(Contract):
    run_id: UUID
    expected_revision: PositiveRevision
    status: Literal['running','completed','failed','cancelled']
    reason: str=Field(min_length=1,max_length=2000)
    actor: str=Field(min_length=1,max_length=200)
    @field_validator('reason','actor')
    @classmethod
    def nonblank(cls,value):
        if not value.strip():raise ValueError('reason and actor must not be blank')
        return value

class OutputAttachment(Contract):
    run_id: UUID
    expected_revision: PositiveRevision
    version_id: UUID
    variable_id: Identifier
    checkpoint: Identifier
    provisional: bool=True

class ComparisonSide(Contract):
    kind: Literal['run','array']
    variable_id: Identifier
    run_id: UUID | None=None
    version_id: UUID | None=None
    slices: dict[str,IndexWindow]=Field(default_factory=dict)
    ranges: dict[str,CoordinateWindow]=Field(default_factory=dict)
    alignment: AlignmentInput | None=None
    @model_validator(mode='after')
    def one_reference(self):
        if self.kind=='run' and (self.run_id is None or self.version_id is not None):raise ValueError('run comparison requires only run_id')
        if self.kind=='array' and (self.version_id is None or self.run_id is not None):raise ValueError('array comparison requires only version_id')
        return self

class ComparisonRequest(Contract):
    left: ComparisonSide
    right: ComparisonSide
    target: GridSpec
    @model_validator(mode='after')
    def single_difference(self):
        if len(self.target.variables)!=1:raise ValueError('comparison target requires exactly one difference variable')
        if self.left.kind!='run' and self.right.kind!='run':raise ValueError('at least one comparison side must be a completed run')
        return self


def checksum(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def verify_variable(arrays,version,variable_id):
    manifest=arrays.manifest(version)
    spec=GridSpec.model_validate(manifest['spec'])
    variable=next((v for v in spec.variables if v.id==variable_id),None)
    if variable is None:raise ValueError('pinned array variable is missing')
    for chunk in manifest['chunks']:
        if chunk['variable']==variable_id and digest(arrays.root/'versions'/str(version)/chunk['path'])!=chunk['checksum']:
            raise ValueError('simulation input/output chunk checksum mismatch')
    return manifest,spec,variable


def register_scenario(arrays:ArrayStore,scenario:Scenario):
    scenario=Scenario.model_validate_json(scenario.model_dump_json());connection=arrays.connection
    snapshots=[];calculations=[]
    for name,pin in scenario.inputs.items():
        if pin.kind=='array':
            manifest,spec,variable=verify_variable(arrays,pin.version_id,pin.variable_id)
            snapshot={'pin':pin.model_dump(mode='json'),'manifest_checksum':checksum(manifest),
                      'dataset_id':spec.dataset_id,'variable':variable.model_dump(mode='json')}
        else:
            row=connection.execute('''SELECT record,recorded_at,is_withdrawn FROM matrix_db.observation_revisions
                WHERE dataset_id=%s AND id=%s AND revision=%s''',(pin.dataset_id,pin.observation_id,pin.revision)).fetchone()
            if row is None:raise ValueError('pinned observation revision is missing')
            if row[2]:raise ValueError('cannot initialize a scenario with a withdrawn observation revision')
            snapshot={'pin':pin.model_dump(mode='json'),'record':row[0],'recorded_at':row[1].isoformat()}
        snapshots.append((name,pin,snapshot,checksum(snapshot)))
    for reference in scenario.calculations:
        row=connection.execute('SELECT checksum FROM matrix_db.derived_definitions WHERE id=%s AND version=%s',(reference.id,reference.version)).fetchone()
        if row is None:raise ValueError('pinned calculation version is missing')
        calculations.append((reference,row[0]))
    payload=scenario.model_dump(mode='json')
    with connection.transaction():
        row=connection.execute('SELECT entry FROM matrix_db.datasets WHERE id=%s FOR SHARE',(scenario.grid.dataset_id,)).fetchone()
        if row is None:raise KeyError(scenario.grid.dataset_id)
        from .catalogue import CatalogueEntry
        output=CatalogueEntry.model_validate(row[0]).dataset
        if output.origin!='simulation':raise ValueError('scenario outputs require a simulation dataset')
        definitions={v.id:v for v in output.variables}
        for variable in scenario.grid.variables:
            if variable.id not in definitions or definitions[variable.id].unit!=variable.unit:
                raise ValueError('scenario output variables/units must match the registered dataset')
            if definitions[variable.id].data_type=='integer' and np.dtype(variable.dtype).kind not in ('i','u'):
                raise ValueError('integer scenario variables require integer storage')
        connection.execute('SELECT pg_advisory_xact_lock(734192010)')
        existing=connection.execute('SELECT checksum FROM matrix_db.scenarios WHERE id=%s AND version=%s',(scenario.id,scenario.version)).fetchone()
        if existing:
            if existing[0]!=checksum(payload):raise ValueError('scenario version already exists; create a new version')
            return {'id':scenario.id,'version':scenario.version}
        if scenario.parent:
            parent=connection.execute('SELECT 1 FROM matrix_db.scenarios WHERE id=%s AND version=%s',(scenario.parent.id,scenario.parent.version)).fetchone()
            if parent is None:raise ValueError('parent scenario version is missing')
        connection.execute('''INSERT INTO matrix_db.scenarios(id,version,parent_id,parent_version,definition,checksum)
            VALUES (%s,%s,%s,%s,%s,%s)''',(scenario.id,scenario.version,None if scenario.parent is None else scenario.parent.id,
            None if scenario.parent is None else scenario.parent.version,Jsonb(payload),checksum(payload)))
        for name,pin,snapshot,expected in snapshots:
            connection.execute('''INSERT INTO matrix_db.scenario_inputs
                (scenario_id,scenario_version,name,kind,array_version,observation_dataset,observation_id,observation_revision,snapshot,checksum)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (scenario.id,scenario.version,name,pin.kind,pin.version_id if pin.kind=='array' else None,
                 pin.dataset_id if pin.kind=='observation' else None,pin.observation_id if pin.kind=='observation' else None,
                 pin.revision if pin.kind=='observation' else None,Jsonb(snapshot),expected))
        for reference,expected in calculations:
            connection.execute('INSERT INTO matrix_db.scenario_calculations VALUES (%s,%s,%s,%s,%s)',
                (scenario.id,scenario.version,reference.id,reference.version,expected))
    return {'id':scenario.id,'version':scenario.version}


def get_scenario(connection,reference:ScenarioRef):
    row=connection.execute('SELECT definition,checksum FROM matrix_db.scenarios WHERE id=%s AND version=%s',(reference.id,reference.version)).fetchone()
    if row is None:raise KeyError(f'{reference.id}@{reference.version}')
    if checksum(row[0])!=row[1]:raise ValueError('scenario checksum mismatch')
    return Scenario.model_validate(row[0]),row[1]


def resolve_inputs(arrays,reference):
    scenario,expected=get_scenario(arrays.connection,reference);result={}
    for name,snapshot,stored in arrays.connection.execute('SELECT name,snapshot,checksum FROM matrix_db.scenario_inputs WHERE scenario_id=%s AND scenario_version=%s ORDER BY name',(reference.id,reference.version)):
        if checksum(snapshot)!=stored:raise ValueError('scenario input snapshot checksum mismatch')
        pin=scenario.inputs[name]
        if pin.kind=='array':
            manifest,_,_=verify_variable(arrays,pin.version_id,pin.variable_id)
            if checksum(manifest)!=snapshot['manifest_checksum']:raise ValueError('pinned input manifest changed')
        else:
            row=arrays.connection.execute('SELECT record FROM matrix_db.observation_revisions WHERE dataset_id=%s AND id=%s AND revision=%s',
                (pin.dataset_id,pin.observation_id,pin.revision)).fetchone()
            if row is None or row[0]!=snapshot['record']:raise ValueError('pinned observation revision changed')
        result[name]=snapshot
    calculation_pins=[]
    for ident,version,stored,actual in arrays.connection.execute('''SELECT p.calculation_id,p.calculation_version,p.checksum,d.checksum
        FROM matrix_db.scenario_calculations p JOIN matrix_db.derived_definitions d ON d.id=p.calculation_id AND d.version=p.calculation_version
        WHERE p.scenario_id=%s AND p.scenario_version=%s ORDER BY p.calculation_id,p.calculation_version''',(reference.id,reference.version)):
        if stored!=actual:raise ValueError('pinned calculation checksum changed')
        calculation_pins.append({'id':ident,'version':version,'checksum':stored})
    if set(result)!=set(scenario.inputs) or len(calculation_pins)!=len(scenario.calculations):raise ValueError('scenario snapshot references are incomplete')
    return {'scenario':scenario.model_dump(mode='json'),'scenario_checksum':expected,'inputs':result,'calculations':calculation_pins}


def create_run(arrays,request:RunRequest):
    request=RunRequest.model_validate_json(request.model_dump_json());connection=arrays.connection
    snapshot=resolve_inputs(arrays,request.scenario)
    specification={**request.model_dump(mode='json'),'scenario_checksum':snapshot['scenario_checksum']}
    with connection.transaction():
        connection.execute('SELECT id FROM matrix_db.scenarios WHERE id=%s AND version=%s FOR UPDATE',(request.scenario.id,request.scenario.version))
        attempt=connection.execute('SELECT coalesce(max(attempt),0)+1 FROM matrix_db.simulation_runs WHERE scenario_id=%s AND scenario_version=%s',
                                   (request.scenario.id,request.scenario.version)).fetchone()[0]
        run=uuid4()
        connection.execute('''INSERT INTO matrix_db.simulation_runs
            (id,scenario_id,scenario_version,attempt,specification,checksum,status,reason,actor)
            VALUES (%s,%s,%s,%s,%s,%s,'queued','execution attempt registered','matrix-db')''',
            (run,request.scenario.id,request.scenario.version,attempt,Jsonb(specification),checksum(specification)))
    return get_run(connection,run)


def get_run(connection,run_id,*,include_provisional=False):
    cursor=connection.execute('SELECT * FROM matrix_db.simulation_runs WHERE id=%s',(UUID(str(run_id)),));row=cursor.fetchone()
    if row is None:raise KeyError(str(run_id))
    result=dict(zip([c.name for c in cursor.description],row))
    if checksum(result['specification'])!=result['checksum']:raise ValueError('run specification checksum mismatch')
    outputs=[]
    for variable,checkpoint,version,provisional in connection.execute('SELECT variable_id,checkpoint,array_version,provisional FROM matrix_db.run_outputs WHERE run_id=%s ORDER BY variable_id,checkpoint',(UUID(str(run_id)),)):
        published=result['status']=='completed' and not provisional
        if published or include_provisional:
            outputs.append({'variable_id':variable,'checkpoint':checkpoint,'version_id':str(version),'provisional':not published,'final_candidate':not provisional})
    result['outputs']=outputs
    result['events']=[dict(zip(('revision','status','reason','actor','recorded_at'),event)) for event in connection.execute(
        'SELECT revision,status,reason,actor,recorded_at FROM matrix_db.run_events WHERE run_id=%s ORDER BY revision',(UUID(str(run_id)),))]
    return result


def validate_output(arrays,scenario,version,variable_id,provisional):
    manifest,spec,variable=verify_variable(arrays,version,variable_id)
    validate_output_spec(scenario,spec,variable_id,provisional)
    return manifest


def validate_output_spec(scenario,spec,variable_id,provisional):
    variable=next((v for v in spec.variables if v.id==variable_id),None)
    expected=next((v for v in scenario.grid.variables if v.id==variable_id),None)
    if expected is None or variable is None:raise ValueError('unexpected simulation output variable')
    if spec.dataset_id!=scenario.grid.dataset_id or spec.crs!=scenario.grid.crs or variable.unit!=expected.unit or variable.dtype!=expected.dtype or variable.dimensions!=expected.dimensions:
        raise ValueError('simulation output contract differs from scenario grid')
    actual_axes={a.name:a for a in spec.axes};expected_axes={a.name:a for a in scenario.grid.axes}
    for name in variable.dimensions:
        actual=actual_axes[name];required=expected_axes[name]
        if provisional and actual.kind=='time':
            if not set(actual.values).issubset(required.values):raise ValueError('provisional output times exceed declared grid')
        elif actual.model_dump()!=required.model_dump():raise ValueError('simulation output coordinates differ from scenario grid')


def attach_output(arrays,attachment:OutputAttachment):
    attachment=OutputAttachment.model_validate_json(attachment.model_dump_json());connection=arrays.connection
    with connection.transaction():
        row=connection.execute('SELECT scenario_id,scenario_version,status,revision FROM matrix_db.simulation_runs WHERE id=%s FOR UPDATE',(attachment.run_id,)).fetchone()
        if row is None:raise KeyError(str(attachment.run_id))
        if row[2]!='running':raise ValueError('outputs require a running execution attempt')
        if row[3]!=attachment.expected_revision:raise ValueError('run revision conflict')
        scenario,_=get_scenario(connection,ScenarioRef(id=row[0],version=row[1]))
        validate_output(arrays,scenario,attachment.version_id,attachment.variable_id,attachment.provisional)
        old=connection.execute('SELECT array_version,provisional FROM matrix_db.run_outputs WHERE run_id=%s AND variable_id=%s AND checkpoint=%s',
            (attachment.run_id,attachment.variable_id,attachment.checkpoint)).fetchone()
        if old:
            if old!=(attachment.version_id,attachment.provisional):raise ValueError('checkpoint already references different output')
        else:
            connection.execute('INSERT INTO matrix_db.run_outputs(run_id,variable_id,checkpoint,array_version,provisional,attached_at_revision) VALUES (%s,%s,%s,%s,%s,%s)',
                (attachment.run_id,attachment.variable_id,attachment.checkpoint,attachment.version_id,attachment.provisional,attachment.expected_revision))
    return get_run(connection,attachment.run_id,include_provisional=True)


def transition_run(arrays,change:RunChange):
    change=RunChange.model_validate_json(change.model_dump_json());connection=arrays.connection
    with connection.transaction():
        row=connection.execute('SELECT scenario_id,scenario_version,status,revision FROM matrix_db.simulation_runs WHERE id=%s FOR UPDATE',(change.run_id,)).fetchone()
        if row is None:raise KeyError(str(change.run_id))
        if row[3]!=change.expected_revision:raise ValueError('run revision conflict')
        allowed={'queued':{'running','cancelled'},'running':{'completed','failed','cancelled'}}
        if change.status not in allowed.get(row[2],set()):raise ValueError('invalid run transition')
        if change.status=='running':resolve_inputs(arrays,ScenarioRef(id=row[0],version=row[1]))
        if change.status=='completed':
            scenario,_=get_scenario(connection,ScenarioRef(id=row[0],version=row[1]))
            outputs=connection.execute('SELECT variable_id,array_version FROM matrix_db.run_outputs WHERE run_id=%s AND NOT provisional',(change.run_id,)).fetchall()
            if {variable for variable,_ in outputs}!={v.id for v in scenario.grid.variables}:raise ValueError('completion requires every final output variable')
            for variable,version in outputs:validate_output(arrays,scenario,version,variable,False)
        connection.execute('UPDATE matrix_db.simulation_runs SET status=%s,reason=%s,actor=%s,error=%s WHERE id=%s',
            (change.status,change.reason,change.actor,change.reason if change.status=='failed' else None,change.run_id))
    return get_run(connection,change.run_id)


def completed_version(arrays,run_id,variable_id):
    run=get_run(arrays.connection,run_id)
    if run['status']!='completed':raise ValueError('comparison requires completed execution results')
    output=next((item for item in run['outputs'] if item['variable_id']==variable_id),None)
    if output is None:raise ValueError('completed run does not contain requested variable')
    return UUID(output['version_id']),run


def compare_and_publish(arrays,request:ComparisonRequest):
    request=ComparisonRequest.model_validate_json(request.model_dump_json())
    target=request.target;axes={a.name:a for a in target.axes};variable=target.variables[0]
    cells=math.prod(len(axes[name].values) for name in variable.dimensions)
    if cells>100000 or cells*18*3>MAX_BYTES:raise ValueError('comparison exceeds bounded working budget')
    fields=[];sources=[];versions=[]
    for side in (request.left,request.right):
        version,run=completed_version(arrays,side.run_id,side.variable_id) if side.kind=='run' else (side.version_id,None)
        manifest,source_spec,source_variable=verify_variable(arrays,version,side.variable_id)
        source_axes={axis.name:axis for axis in source_spec.axes}
        if math.prod(len(source_axes[name].values) for name in source_variable.dimensions)*18>MAX_BYTES/2:raise ValueError('comparison source exceeds working budget')
        binding=SourceBinding(version_id=version,variable_id=side.variable_id,slices=side.slices,ranges=side.ranges,alignment=side.alignment)
        fields.append(source_field(arrays,binding,target));versions.append(version)
        sources.append({'selection':side.model_dump(mode='json'),'version_id':str(version),'manifest_checksum':checksum(manifest),
            'dataset_id':source_spec.dataset_id,'unit':source_variable.unit,'origin':PostgresStore(arrays.connection).get(source_spec.dataset_id).dataset.origin,
            'scenario':None if run is None else {'id':run['scenario_id'],'version':run['scenario_version']},'execution':None if run is None else run['specification']})
    definition=Definition(id='simulation-difference',version=1,description='Right minus left under explicit selection/alignment rules',function='difference',unit=variable.unit,
        inputs={'left':{'kind':'source','source':'right','unit':fields[1]['unit']},'right':{'kind':'source','source':'left','unit':fields[0]['unit']}})
    result=calculate(definition,{'left':fields[1],'right':fields[0]})
    data=cast_output(result['data'],result['mask'],variable.dtype)
    provenance={'operation':'simulation-difference-1','numpy':np.__version__,'pyproj':pyproj.__version__,'proj':pyproj.proj_version_str,'unit_registry':1,'direction':'right_minus_left','request':request.model_dump(mode='json'),'sources':sources}
    comparison=uuid4()
    with arrays.connection.transaction():
        version=arrays.publish(target,lambda name,slices:(data[slices],result['mask'][slices],result['coverage'][slices],result['quality'][slices]),provenance=provenance)
        arrays.connection.execute('INSERT INTO matrix_db.run_comparisons(id,left_run,right_run,left_version,right_version,output_version,definition) VALUES (%s,%s,%s,%s,%s,%s,%s)',
            (comparison,request.left.run_id,request.right.run_id,versions[0],versions[1],version,Jsonb(provenance)))
    return {'comparison_id':str(comparison),'version_id':str(version),'direction':'right_minus_left'}


def publish_run_output(arrays,run_id,spec,provider,*,expected_revision,checkpoint,provisional=True):
    """External engine callback publishes chunks and attaches their run provenance atomically."""
    spec=GridSpec.model_validate_json(spec.model_dump_json())
    with arrays.connection.transaction():
        row=arrays.connection.execute('SELECT status,revision FROM matrix_db.simulation_runs WHERE id=%s FOR UPDATE',(UUID(str(run_id)),)).fetchone()
        if row is None:raise KeyError(str(run_id))
        if row!=('running',expected_revision):raise ValueError('run must be running at the expected revision')
        run=get_run(arrays.connection,run_id)
        reference=ScenarioRef(id=run['scenario_id'],version=run['scenario_version'])
        snapshot=resolve_inputs(arrays,reference);scenario=Scenario.model_validate(snapshot['scenario'])
        for variable in spec.variables:validate_output_spec(scenario,spec,variable.id,provisional)
        provenance={'operation':'external-simulation-output-1','run_id':str(run_id),'attempt':run['attempt'],
                    'execution':run['specification'],'snapshot':snapshot,'checkpoint':checkpoint,'provisional_at_attachment':provisional}
        version=arrays.publish(spec,provider,provenance=provenance)
        for variable in spec.variables:
            attach_output(arrays,OutputAttachment(run_id=run_id,expected_revision=expected_revision,version_id=version,
                variable_id=variable.id,checkpoint=checkpoint,provisional=provisional))
    return {'version_id':str(version),'run_id':str(run_id),'provisional':provisional}

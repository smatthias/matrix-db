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
    alignment: AlignmentInput …7087 tokens truncated…None:
            job_id=str(uuid4())
            connection.execute('''INSERT INTO matrix_db.import_jobs
                (id,dataset_id,source_checksum,dataset_checksum,format,status) VALUES (%s,%s,%s,%s,%s,'running')''',
                (job_id,dataset_id,source_hash,definition_hash,format))
        else:
            row=connection.execute('''SELECT dataset_id,source_checksum,dataset_checksum,format,status
                FROM matrix_db.import_jobs WHERE id=%s FOR UPDATE''',(job_id,)).fetchone()
            if row is None: raise KeyError(job_id)
            if row[:4]!=(dataset_id,source_hash,definition_hash,format):
                raise ValueError('resume requires unchanged source, mapping and dataset definition')
            if row[4]=='completed': return job_id
            connection.execute("UPDATE matrix_db.import_jobs SET status='running',error=NULL WHERE id=%s",(job_id,))
    try:
        iterator=enumerate(records(path,format),start=1)
        while True:
            batch=list(itertools.islice(iterator,batch_size))
            if not batch: break
            with connection.transaction():
                state=connection.execute('SELECT processed,status FROM matrix_db.import_jobs WHERE id=%s FOR UPDATE',(job_id,)).fetchone()
                if state[1]=='completed': return job_id
                # Lock definition against concurrent metadata/contract updates.
                current=connection.execute('SELECT entry FROM matrix_db.datasets WHERE id=%s FOR SHARE',(dataset_id,)).fetchone()
                from .catalogue import CatalogueEntry
                current_dataset=CatalogueEntry.model_validate(current[0]).dataset
                if current_dataset!=dataset: raise ValueError('dataset changed during import; restart against the new definition')
                accepted=duplicates=rejected=0
                last=state[0]
                for number,raw in batch:
                    if number<=state[0]: continue
                    last=number
                    try:
                        record=normalize(raw,dataset,format,mapping)
                        inserted=connection.execute('''INSERT INTO matrix_db.observations
                            (dataset_id,id,variable_id,valid_time,longitude,latitude,record,import_job_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT(dataset_id,id) DO NOTHING RETURNING id''',
                            (dataset_id,record.id,record.variable_id,record.valid_time,record.longitude,record.latitude,
                             Jsonb(record.model_dump(mode='json')),job_id)).fetchone()
                        if inserted: accepted+=1
                        else:
                            old=connection.execute('SELECT record FROM matrix_db.observations WHERE dataset_id=%s AND id=%s',
                                                   (dataset_id,record.id)).fetchone()[0]
                            if old!=record.model_dump(mode='json'):
                                raise ValueError('identity conflict: existing record differs; corrections require explicit revision support')
                            duplicates+=1
                    except (ValueError,TypeError,OverflowError) as error:
                        rejected+=1
                        # Keep record position and reason; do not copy raw source records into job logs.
                        reason=str(error).split('\n')[0][:1000]
                        connection.execute('INSERT INTO matrix_db.import_rejections(job_id,record_number,reason) VALUES (%s,%s,%s)',
                                           (job_id,number,reason))
                connection.execute('''UPDATE matrix_db.import_jobs SET processed=%s,accepted=accepted+%s,
                    duplicates=duplicates+%s,rejected=rejected+%s,updated_at=now() WHERE id=%s''',
                    (last,accepted,duplicates,rejected,job_id))
        if checksum(path)!=source_hash: raise ValueError('source changed during import')
        with connection.transaction():
            row=connection.execute('SELECT entry FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset_id,)).fetchone()
            from .catalogue import CatalogueEntry, TimeCoverage
            entry=CatalogueEntry.model_validate(row[0])
            bounds=connection.execute('SELECT min(valid_time),max(valid_time) FROM matrix_db.observations WHERE dataset_id=%s',(dataset_id,)).fetchone()
            if bounds[0] is not None:
                entry.ingested_time=TimeCoverage(start=bounds[0],end=bounds[1])
            from datetime import datetime, timezone
            entry.last_successful_ingestion=datetime.now(timezone.utc)
            connection.execute('UPDATE matrix_db.datasets SET entry=%s WHERE id=%s',(Jsonb(entry.model_dump(mode='json')),dataset_id))
            connection.execute("UPDATE matrix_db.import_jobs SET status='completed',updated_at=now() WHERE id=%s",(job_id,))
    except Exception:
        with connection.transaction():
            connection.execute("UPDATE matrix_db.import_jobs SET status='failed',error='Import failed; inspect source and retry the same job ID',updated_at=now() WHERE id=%s AND status<>'completed'",(job_id,))
        raise ValueError(f'Import job {job_id} failed; committed batches are retained. Resume with the same source and job ID.') from None
    return job_id


def job_report(connection, job_id):
    cursor=connection.execute('SELECT * FROM matrix_db.import_jobs WHERE id=%s',(job_id,))
    row=cursor.fetchone()
    if row is None: raise KeyError(job_id)
    result=dict(zip([column.name for column in cursor.description],row))
    result['rejections']=[{'record_number':n,'reason':reason} for n,reason in connection.execute(
        'SELECT record_number,reason FROM matrix_db.import_rejections WHERE job_id=%s ORDER BY record_number LIMIT 1000',(job_id,))]
    result['rejections_truncated']=result['rejected']>len(result['rejections'])
    return result

"""Bounded CSV, JSON-array and JSON-lines imports with committed batch checkpoints."""
import csv
import gzip
import hashlib
import itertools
import json
from pathlib import Path
from uuid import uuid4
import ijson
from psycopg.types.json import Jsonb
from .contracts import Observation
from .storage import PostgresStore


def checksum(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):
            digest.update(chunk)
    return digest.hexdigest()


def records(path, format):
    opener=gzip.open if str(path).endswith('.gz') else open
    if format=='json':
        with opener(path,'rb') as stream:
            # Only top-level arrays are accepted; ijson streams one object at a time.
            first=b''
            while not first or first.isspace():
                first=stream.read(1)
                if not first: raise ValueError('JSON input must be an array')
            if first!=b'[': raise ValueError('JSON input must be an array')
        with opener(path,'rb') as stream:
            yield from ijson.items(stream,'item',use_float=True)
    else:
        with opener(path,'rt',encoding='utf-8',newline='') as stream:
            if format=='csv':
                yield from csv.DictReader(stream)
            elif format=='jsonl':
                for line in stream:
                    try: yield json.loads(line)
                    except ValueError: yield {'__parse_error__':'invalid JSON line'}
            else: raise ValueError('unsupported import format')


def normalize(raw, dataset, format, mapping=None):
    if not isinstance(raw,dict): raise ValueError('record must be an object')
    if '__parse_error__' in raw: raise ValueError(raw['__parse_error__'])
    row={key:raw[source] for key,source in mapping.items() if source in raw} if mapping else dict(raw)
    if format=='csv':
        if 'schema_version' in row: row['schema_version']=int(row['schema_version'])
        if 'vertical' in row: row['vertical']=json.loads(row['vertical']) if row['vertical'] else None
        variable=next((v for v in dataset.variables if v.id==row.get('variable_id')),None)
        if row.get('value')=='': row['value']=None
        elif variable and 'value' in row:
            row['value']=int(row['value']) if variable.data_type=='integer' else float(row['value'])
        if row.get('forecast_issue_time')=='': row['forecast_issue_time']=None
    return Observation.model_validate(row).validate_against(dataset)


def import_file(connection, dataset_id, path, *, format, batch_size=500, job_id=None, mapping=None):
    if format not in ('csv','json','jsonl'): raise ValueError('unsupported import format')
    if type(batch_size) is not int or not 1<=batch_size<=10000: raise ValueError('batch_size must be between 1 and 10000')
    if mapping is not None and (not isinstance(mapping,dict) or any(not isinstance(k,str) or not isinstance(v,str) for k,v in mapping.items())):
        raise ValueError('mapping must map contract field names to source field names')
    source_hash=checksum(path)
    store=PostgresStore(connection)
    with connection.transaction():
        dataset=store.get(dataset_id).dataset
    definition_hash=hashlib.sha256((dataset.model_dump_json()+json.dumps(mapping,sort_keys=True)).encode()).hexdigest()
    with connection.transaction():
        if job_id is None:
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

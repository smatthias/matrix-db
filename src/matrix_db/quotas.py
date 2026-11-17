"""Shared logical capacity controls; request admission commits separately from work."""
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import json
import os
from pydantic import Field
from .contracts import Contract

class ResourceLimits(Contract):
    requests_per_minute: int=Field(default=600,ge=0,le=1000000)
    concurrent_http_requests: int=Field(default=64,ge=1,le=1024)
    active_jobs_per_credential: int=Field(default=50,ge=1,le=100000)
    active_jobs_global: int=Field(default=500,ge=1,le=1000000)
    observations_per_batch: int=Field(default=10000,ge=1,le=10000)
    observations_per_dataset: int=Field(default=1000000,ge=1,le=1000000000)
    revisions_per_dataset: int=Field(default=5000000,ge=1,le=10000000000)
    array_versions_per_dataset: int=Field(default=10000,ge=1,le=1000000)
    logical_array_bytes_per_dataset: int=Field(default=0,ge=0,le=10**15)
    subscriptions_per_owner: int=Field(default=50,ge=1,le=50)
    inbox_items_per_owner: int=Field(default=10000,ge=1,le=1000000)
    @classmethod
    def from_environment(cls):
        raw=json.loads(os.environ.get('MATRIX_DB_RESOURCE_LIMITS','{}'))
        if not isinstance(raw,dict):raise ValueError('resource limits must be a JSON object')
        return cls.model_validate(raw)

class QuotaExceeded(Exception):
    def __init__(self,resource,limit,retry_after=None):
        self.code='quota_exceeded';self.resource=resource;self.limit=limit;self.retry_after=retry_after
        super().__init__('configured resource capacity reached: '+resource)

@dataclass
class Admission:
    credential_id: str
    charged: bool=False
current_admission=ContextVar('matrix_request_admission',default=None)

def lock_key(namespace,value=''):
    return int.from_bytes(hashlib.sha256(('matrix-quota:'+namespace+':'+value).encode()).digest()[:8],'big',signed=True)

def charge_request(connection_factory,credential,limits,*,at=None):
    if limits.requests_per_minute==0:return
    with connection_factory() as connection:
        connection.execute("SET LOCAL statement_timeout='5s'")
        used,retry=connection.execute('''INSERT INTO matrix_db.api_rate_windows VALUES (%s,date_trunc('minute',coalesce(%s::timestamptz,statement_timestamp())),1)
            ON CONFLICT(credential_id) DO UPDATE SET window_start=greatest(api_rate_windows.window_start,EXCLUDED.window_start),
            used=CASE WHEN api_rate_windows.window_start>=EXCLUDED.window_start THEN api_rate_windows.used+1 ELSE 1 END
            RETURNING used,greatest(1,ceil(extract(epoch FROM window_start+interval '1 minute'-coalesce(%s::timestamptz,statement_timestamp()))))''',(credential,at,at)).fetchone()
        if used>limits.requests_per_minute:raise QuotaExceeded('requests_per_minute',limits.requests_per_minute,int(retry))

def admit(connection_factory,limits):
    state=current_admission.get()
    if state is not None and not state.charged:
        charge_request(connection_factory,state.credential_id,limits);state.charged=True

def check_jobs(connection,credential,limits):
    connection.execute('SELECT pg_advisory_xact_lock(%s)',(lock_key('job-admission'),))
    global_count=connection.execute("SELECT count(*) FROM matrix_db.api_jobs WHERE status IN ('queued','running')").fetchone()[0]
    owned=connection.execute("SELECT count(*) FROM matrix_db.api_jobs WHERE credential_id=%s AND status IN ('queued','running')",(credential,)).fetchone()[0]
    if global_count>=limits.active_jobs_global:raise QuotaExceeded('active_jobs_global',limits.active_jobs_global,5)
    if owned>=limits.active_jobs_per_credential:raise QuotaExceeded('active_jobs_per_credential',limits.active_jobs_per_credential,5)

def check_records(connection,dataset,addition,limits):
    count=connection.execute('SELECT count(*) FROM matrix_db.observations WHERE dataset_id=%s',(dataset,)).fetchone()[0]
    if count+addition>limits.observations_per_dataset:raise QuotaExceeded('observations_per_dataset',limits.observations_per_dataset)
    check_revisions(connection,dataset,addition,limits)

def check_revisions(connection,dataset,addition=1,limits=None):
    limits=limits or ResourceLimits.from_environment()
    historical=connection.execute("SELECT to_regclass('matrix_db.observation_revisions')").fetchone()[0]
    table='observation_revisions' if historical else 'observations'
    count=connection.execute(f'SELECT count(*) FROM matrix_db.{table} WHERE dataset_id=%s',(dataset,)).fetchone()[0]
    if count+addition>limits.revisions_per_dataset:raise QuotaExceeded('revisions_per_dataset',limits.revisions_per_dataset)

def check_arrays(connection,dataset,addition,limits):
    count,tracked,used=connection.execute('''SELECT count(*),count(r.version_id),coalesce(sum(r.stored_bytes),0)
        FROM matrix_db.array_versions v LEFT JOIN matrix_db.array_resources r USING(version_id) WHERE v.dataset_id=%s''',(dataset,)).fetchone()
    if count>=limits.array_versions_per_dataset:raise QuotaExceeded('array_versions_per_dataset',limits.array_versions_per_dataset)
    if limits.logical_array_bytes_per_dataset:
        if tracked<count:raise QuotaExceeded('untracked_array_usage',limits.logical_array_bytes_per_dataset)
        if used+addition>limits.logical_array_bytes_per_dataset:raise QuotaExceeded('logical_array_bytes_per_dataset',limits.logical_array_bytes_per_dataset)

def inbox_capacity(connection,owner,limits):
    connection.execute('SELECT pg_advisory_xact_lock(%s)',(lock_key('inbox',owner),))
    return connection.execute('SELECT count(*) FROM matrix_db.notification_deliveries WHERE owner=%s',(owner,)).fetchone()[0]<limits.inbox_items_per_owner

def report(connection,dataset,limits):
    from .storage import PostgresStore
    PostgresStore(connection).get(dataset)
    observations=connection.execute('SELECT count(*) FROM matrix_db.observations WHERE dataset_id=%s',(dataset,)).fetchone()[0]
    revisions=connection.execute('SELECT count(*) FROM matrix_db.observation_revisions WHERE dataset_id=%s',(dataset,)).fetchone()[0]
    versions,tracked,stored=connection.execute('''SELECT count(*),count(r.version_id),coalesce(sum(r.stored_bytes),0)
        FROM matrix_db.array_versions v LEFT JOIN matrix_db.array_resources r USING(version_id) WHERE dataset_id=%s''',(dataset,)).fetchone()
    active=connection.execute("SELECT count(*) FROM matrix_db.api_jobs WHERE status IN ('queued','running')").fetchone()[0]
    return {'dataset_id':dataset,'limits':limits.model_dump(),'usage':{'observations':observations,'revisions':revisions,'array_versions':versions,
        'logical_array_bytes':int(stored),'untracked_array_versions':versions-tracked,'active_jobs_global':active},'request_scope':'per service credential; shared keys aggregate users'}

def reconcile_arrays(connection,dataset,root,archive_root=None):
    from .arrays import ArrayStore
    from .lifecycle import checked_files
    from .storage import PostgresStore
    connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset,));PostgresStore(connection).get(dataset)
    rows=connection.execute('SELECT version_id,manifest,manifest_checksum FROM matrix_db.array_versions WHERE dataset_id=%s LIMIT 1001',(dataset,)).fetchall()
    if len(rows)>1000:raise ValueError('reconciliation exceeds 1000-version budget')
    arrays=ArrayStore(connection,root,archive_root=archive_root)
    for version,manifest,checksum in rows:
        # Shared reader lock excludes tier mutation during checksum verification/accounting.
        directory=arrays.directory(version);arrays.manifest(version)
        size=checked_files(directory,manifest,checksum)
        connection.execute('INSERT INTO matrix_db.array_resources VALUES (%s,%s) ON CONFLICT(version_id) DO UPDATE SET stored_bytes=EXCLUDED.stored_bytes',(version,size))
    return {'dataset_id':dataset,'verified_versions':len(rows)}

def main():
    import argparse
    from .storage import connect
    parser=argparse.ArgumentParser();parser.add_argument('--dataset',required=True);parser.add_argument('--reconcile-arrays',action='store_true');args=parser.parse_args()
    try:
        with connect() as connection:
            connection.execute("SET LOCAL statement_timeout='30s'")
            if args.reconcile_arrays:
                root=os.environ.get('MATRIX_DB_ARRAY_ROOT')
                if not root:raise ValueError('array root required')
                result=reconcile_arrays(connection,args.dataset,root,os.environ.get('MATRIX_DB_ARCHIVE_ROOT'))
            else:result=report(connection,args.dataset,ResourceLimits.from_environment())
        print(json.dumps(result,default=str))
    except Exception:parser.exit(1,'resource inspection unavailable; check configuration, integrity and database\n')

if __name__=='__main__':main()

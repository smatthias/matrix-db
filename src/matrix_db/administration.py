"""Bounded operational views and optimistic metadata edits; no filesystem mutations."""
import base64
from datetime import datetime
import hashlib
import json
from typing import Literal
from uuid import UUID
from pydantic import Field,field_validator
from psycopg.types.json import Jsonb
from .contracts import Contract,Observation
from .arrays import canonical
from .storage import PostgresStore
from . import quality,lifecycle,api_jobs,hazards

class MetadataChange(Contract):
    expected_checksum: str=Field(pattern=r'^[a-f0-9]{64}$')
    name: str=Field(min_length=1,max_length=300)
    license: str=Field(min_length=1,max_length=1000)
    attribution: str | None=Field(default=None,max_length=2000)
    contact: str | None=Field(default=None,max_length=500)
    status: Literal['active','stale','retired']
    @field_validator('name','license')
    @classmethod
    def nonblank(cls,value):
        if not value.strip():raise ValueError('nonblank value required')
        return value.strip()

class PolicyChange(Contract):
    expected_revision: int | None=Field(default=None,ge=1)
    policy: quality.Policy
class RetentionChange(Contract):
    expected_revision: int | None=Field(default=None,ge=1)
    policy: lifecycle.RetentionPolicy
class StaleEdit(Exception):pass

def checksum(entry):return hashlib.sha256(canonical(entry.model_dump(mode='json')).encode()).hexdigest()
def audit(connection,dataset,action,principal,**detail):
    connection.execute('INSERT INTO matrix_db.administration_actions(dataset_id,action,actor,detail) VALUES (%s,%s,%s,%s)',(dataset,action,principal.subject,Jsonb(detail)))
def dict_rows(connection,sql,params):
    cursor=connection.execute(sql,params);names=[c.name for c in cursor.description]
    return [dict(zip(names,row)) for row in cursor.fetchall()]

def overview(connection,dataset):
    entry=PostgresStore(connection).get(dataset)
    provider=dict_rows(connection,'''SELECT id,config->>'provider' AS provider,watermark,next_attempt_at,last_success_at,latest_valid_time,failures,error_code
        FROM matrix_db.provider_sources WHERE dataset_id=%s''',(dataset,))
    observation=connection.execute('SELECT count(*),count(*) FILTER(WHERE is_withdrawn),max(valid_time) FILTER(WHERE NOT is_withdrawn) FROM matrix_db.observations WHERE dataset_id=%s',(dataset,)).fetchone()
    tiers=dict(connection.execute("SELECT coalesce(l.tier,'hot'),count(*) FROM matrix_db.array_versions v LEFT JOIN matrix_db.array_locations l USING(version_id) WHERE dataset_id=%s GROUP BY 1",(dataset,)).fetchall())
    try:revision,p=quality.get_policy(connection,dataset);quality_policy={'revision':revision,'policy':p.model_dump(mode='json')}
    except KeyError:quality_policy=None
    try:revision,p=lifecycle.policy_for(connection,dataset);retention_policy={'revision':revision,'policy':p.model_dump(mode='json')}
    except KeyError:retention_policy=None
    try:revision,p=hazards.get_policy(connection,dataset);hazard_policy={'revision':revision,'policy':p.model_dump(mode='json')}
    except KeyError:hazard_policy=None
    hazard_events=hazards.rows(connection,"SELECT * FROM matrix_db.hazard_events WHERE dataset_id=%s AND status='active' ORDER BY opened_at,id LIMIT 20",(dataset,))
    return {'entry':entry.model_dump(mode='json'),'checksum':checksum(entry),'observations':{'total':observation[0],'withdrawn':observation[1],'latest_valid_time':observation[2]},
        'array_tiers':tiers,'provider':provider[0] if provider else None,'quality_policy':quality_policy,'retention_policy':retention_policy,'hazard_policy':hazard_policy,'hazard_events':hazard_events}

def edit_metadata(connection,dataset,change,principal):
    connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset,))
    store=PostgresStore(connection);entry=store.get(dataset)
    if checksum(entry)!=change.expected_checksum:raise StaleEdit()
    previous={'name':entry.dataset.name,'license':entry.dataset.license,'status':entry.status,'attribution':entry.attribution,'contact':entry.contact}
    entry.dataset.name=change.name;entry.dataset.license=change.license
    entry.status=change.status;entry.attribution=change.attribution;entry.contact=change.contact
    store.update(dataset,entry)
    audit(connection,dataset,'metadata_updated',principal,previous=previous,current=change.model_dump(exclude={'expected_checksum'}))
    return {'entry':entry.model_dump(mode='json'),'checksum':checksum(entry)}

def save_policy(connection,dataset,change,principal,*,retention=False):
    if dataset!=change.policy.dataset_id:raise ValueError('policy dataset mismatch')
    connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset,))
    PostgresStore(connection).get(dataset)
    getter=lifecycle.policy_for if retention else quality.get_policy
    try:revision,_=getter(connection,dataset)
    except KeyError:revision=None
    if revision!=change.expected_revision:raise StaleEdit()
    result=lifecycle.register_policy(connection,change.policy) if retention else quality.register_policy(connection,change.policy,principal.subject)['revision']
    audit(connection,dataset,'retention_policy_saved' if retention else 'quality_policy_saved',principal,revision=result)
    return {'revision':result}

def cancel_job(connection,dataset,ident,principal):
    row=connection.execute("SELECT coalesce(request->>'dataset_id',request#>>'{target,dataset_id}') FROM matrix_db.api_jobs WHERE id=%s",(ident,)).fetchone()
    if row is None or row[0]!=dataset:raise KeyError(str(ident))
    result=api_jobs.cancel(connection,ident,principal);audit(connection,dataset,'job_cancel_requested',principal,job_id=str(ident))
    return result

# Queries expose operational summaries only, never source config, request/result bodies,
# credentials, raw import error text or rejection payloads.
ACTIVITIES={
 'providers':('uuid',"SELECT r.id AS activity_id,r.started_at AS activity_at,r.source_id,r.status,r.window_start,r.window_end,r.inserted,r.corrected,r.unchanged,r.withdrawn,r.error_code FROM matrix_db.provider_runs r JOIN matrix_db.provider_sources s ON s.id=r.source_id WHERE s.dataset_id=%s"),
 'imports':('text',"SELECT id AS activity_id,started_at AS activity_at,status,processed,accepted,duplicates,rejected FROM matrix_db.import_jobs WHERE dataset_id=%s"),
 'jobs':('uuid',"SELECT id AS activity_id,created_at AS activity_at,operation,status,work_done,cancel_requested,error_code FROM matrix_db.api_jobs WHERE coalesce(request->>'dataset_id',request#>>'{target,dataset_id}')=%s"),
 'storage':('bigint',"SELECT sequence AS activity_id,at AS activity_at,kind,version_id,actor,detail FROM matrix_db.lifecycle_events WHERE version_id IN (SELECT version_id FROM matrix_db.array_versions WHERE dataset_id=%s) OR detail->>'dataset_id'=%s"),
 'audit':('bigint',"SELECT sequence AS activity_id,at AS activity_at,action,actor,detail FROM matrix_db.administration_actions WHERE dataset_id=%s")}

def activity(connection,dataset,kind,after=None,limit=50):
    if kind not in ACTIVITIES or type(limit) is not int or not 1<=limit<=100:raise ValueError('unsupported activity request')
    PostgresStore(connection).get(dataset);cast,sql=ACTIVITIES[kind];timestamp=None;ident=None
    if after:
        try:
            raw=json.loads(base64.b64decode(after,altchars=b'-_',validate=True))
            if set(raw)!={'dataset','kind','time','id'} or raw['dataset']!=dataset or raw['kind']!=kind:raise ValueError()
            timestamp=Observation.utc_time(datetime.fromisoformat(raw['time']))
            ident=UUID(raw['id']) if cast=='uuid' else int(raw['id']) if cast=='bigint' else raw['id']
            if cast=='text' and (not isinstance(ident,str) or len(ident)>500):raise ValueError()
        except (ValueError,TypeError,KeyError,AttributeError):raise ValueError('invalid activity cursor') from None
    params=[dataset,dataset] if kind=='storage' else [dataset]
    # Only predefined query/cast fragments are interpolated; all client values are parameters.
    items=dict_rows(connection,f'''WITH activity AS ({sql}) SELECT * FROM activity
        WHERE (%s::timestamptz IS NULL OR (activity_at,activity_id)<(%s,%s::{cast}))
        ORDER BY activity_at DESC,activity_id DESC LIMIT %s''',(*params,timestamp,timestamp,ident,limit+1))
    selected=items[:limit];cursor=None
    if len(items)>limit:
        last=selected[-1];cursor=base64.urlsafe_b64encode(json.dumps({'dataset':dataset,'kind':kind,'time':last['activity_at'].isoformat(),'id':str(last['activity_id'])}).encode()).decode()
    return {'items':selected,'next_cursor':cursor}

"""Durable single-worker jobs with cooperative cancellation and atomic publication."""
from uuid import UUID,uuid4
from psycopg.types.json import Jsonb
from .queries import ObservationQuery
from .alignment import AlignmentRequest,align_and_publish
from .derived import DerivationRequest,derive_and_publish
from .scenarios import ComparisonRequest,compare_and_publish
from .arrays import ArrayStore

MODELS={'observations':ObservationQuery,'alignment':AlignmentRequest,'derivation':DerivationRequest,'comparison':ComparisonRequest}
class JobCancelled(Exception):pass

def validate_request(operation,payload,principal):
    if operation not in MODELS:raise ValueError('unknown job operation')
    request=MODELS[operation].model_validate(payload)
    if operation=='observations':
        if request.dataset_id is None and principal.role!='admin':raise PermissionError('an explicit authorized dataset is required')
        if request.dataset_id is not None and not principal.permits(request.dataset_id):raise PermissionError('dataset access denied')
    elif principal.role!='admin':raise PermissionError('administrative credential required')
    return request

def submit(connection,operation,payload,principal):
    request=validate_request(operation,payload,principal);ident=uuid4()
    connection.execute('INSERT INTO matrix_db.api_jobs(id,subject,credential_id,operation,request,status) VALUES (%s,%s,%s,%s,%s,\'queued\')',
        (ident,principal.subject,principal.credential_id,operation,Jsonb(request.model_dump(mode='json'))))
    return {'id':str(ident),'status':'queued'}

def inspect(connection,ident,principal):
    cursor=connection.execute('SELECT * FROM matrix_db.api_jobs WHERE id=%s',(UUID(str(ident)),));row=cursor.fetchone()
    if row is None:raise KeyError(str(ident))
    result=dict(zip([c.name for c in cursor.description],row))
    if principal.role!='admin' and result['credential_id']!=principal.credential_id:raise PermissionError('job access denied')
    validate_request(result['operation'],result['request'],principal)
    result.pop('credential_id');result.pop('request')
    return result

def cancel(connection,ident,principal):
    inspect(connection,ident,principal)
    row=connection.execute('''UPDATE matrix_db.api_jobs SET cancel_requested=true,
        status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END,updated_at=clock_timestamp()
        WHERE id=%s AND status IN ('queued','running') RETURNING status''',(UUID(str(ident)),)).fetchone()
    if row is None:raise ValueError('job is already terminal')
    return {'id':str(ident),'status':row[0],'cancel_requested':True}

def run_one(settings,connection_factory):
    with connection_factory() as control:
        row=control.execute('''WITH candidate AS (SELECT id FROM matrix_db.api_jobs WHERE status='queued'
            ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1)
            UPDATE matrix_db.api_jobs SET status='running',updated_at=clock_timestamp() WHERE id=(SELECT id FROM candidate)
            RETURNING id,credential_id,operation,request''').fetchone()
    if row is None:return False
    ident,credential,operation,payload=row
    import time
    last_check=0.
    def check():
        nonlocal last_check
        now=time.monotonic()
        if now-last_check<.1:return
        last_check=now
        with connection_factory() as control:
            state=control.execute('UPDATE matrix_db.api_jobs SET work_done=work_done+1,updated_at=clock_timestamp() WHERE id=%s RETURNING cancel_requested',(ident,)).fetchone()
        if state[0]:raise JobCancelled()
        if credential not in settings.keys:raise PermissionError('authorization revoked')
    try:
        principal=settings.keys.get(credential)
        if principal is None:raise PermissionError('authorization revoked')
        request=validate_request(operation,payload,principal)
        with connection_factory() as connection:
            connection.execute("SET LOCAL statement_timeout='30s'")
            arrays=ArrayStore(connection,settings.array_root,check_cancel=check)
            check()
            if operation=='observations':
                from .queries import query_observations
                result=query_observations(connection,request).model_dump(mode='json')
            elif operation=='alignment':result=align_and_publish(arrays,request)
            elif operation=='derivation':result=derive_and_publish(arrays,request)
            else:result=compare_and_publish(arrays,request)
            check()
            updated=connection.execute('''UPDATE matrix_db.api_jobs SET status='completed',result=%s,updated_at=clock_timestamp()
                WHERE id=%s AND status='running' AND NOT cancel_requested RETURNING id''',(Jsonb(result),ident)).fetchone()
            if updated is None:raise JobCancelled()
    except Exception as error:
        status='cancelled' if isinstance(error,JobCancelled) else 'failed'
        code=None if status=='cancelled' else 'authorization_revoked' if isinstance(error,PermissionError) else 'job_failed'
        with connection_factory() as control:
            control.execute('UPDATE matrix_db.api_jobs SET status=%s,error_code=%s,updated_at=clock_timestamp() WHERE id=%s',(status,code,ident))
    return True

def main():
    import argparse,time
    from .api_auth import Settings
    from .storage import connect
    parser=argparse.ArgumentParser();parser.add_argument('--once',action='store_true');args=parser.parse_args()
    while True:
        worked=run_one(Settings.from_environment(),connect)
        if args.once:return
        if not worked:time.sleep(1)

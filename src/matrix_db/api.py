"""Version-one HTTP API. Advanced mutations require an administrative credential."""
import json
import threading
from contextlib import contextmanager
from uuid import UUID,uuid4
import numpy as np
import psycopg
from fastapi import FastAPI,Depends,HTTPException,Query,Header
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse,StreamingResponse
from fastapi.security import HTTPBearer,HTTPAuthorizationCredentials
from starlette.exceptions import HTTPException as StarletteHTTPException
from pydantic import Field
from .api_auth import Settings
from .contracts import Contract,Identifier,Observation
from .catalogue import CatalogueEntry,TimeCoverage
from .queries import ObservationQuery,Page,query_observations
from .temporal import SeriesQuery,PlaybackQuery,RevisionRequest,WithdrawalRequest,series,playback,history
from .arrays import ArrayStore,SubsetQuery
from .alignment import AlignmentRequest,align_and_publish
from .derived import Definition,DerivationRequest,register_definitions,derive_and_publish
from .scenarios import Scenario,ScenarioRef,RunRequest,RunChange,OutputAttachment,ComparisonRequest,register_scenario,resolve_inputs,create_run,transition_run,attach_output,get_run,compare_and_publish
from .storage import connect,PostgresStore,LATEST_VERSION
from . import api_jobs,quality,administration,lifecycle,notifications,quotas,hazards

class ObservationBatch(Contract):
    dataset_id: Identifier
    observations: list[Observation]=Field(min_length=1,max_length=10000)

class CatalogueQuery(Contract):
    text: str | None=Field(default=None,max_length=200)
    variable: str | None=None
    provider: str | None=None
    origin: str | None=None
    status: str | None=None
    region: dict | None=None
    time: TimeCoverage | None=None
    availability: str='declared'
    after: str | None=None
    limit: int=Field(default=100,ge=1,le=1000)

class JobRequest(Contract):
    operation: str
    payload: dict

def serialize_subset(result,admin=False):
    for field in result['variables'].values():
        data=field['data'].astype(object);data[field['mask']]=None
        field['data']=data.tolist();field['mask']=field['mask'].tolist()
        for name in ('coverage','quality'):
            if field.get(name) is not None:field[name]=field[name].tolist()
    if not admin:result['provenance']=None
    if len(json.dumps(result,allow_nan=False))>2*1024*1024:raise ValueError('serialized subset exceeds API budget; request a smaller subset')
    return result

def create_app(settings=None,connection_factory=connect):
    settings=Settings.from_environment() if settings is None else settings
    app=FastAPI(title='Matrix DB',version='1.0.0')
    bearer=HTTPBearer(auto_error=False)
    slots=threading.BoundedSemaphore(settings.limits.concurrent_http_requests)
    @contextmanager
    def db():
        quotas.admit(connection_factory,settings.limits)
        with connection_factory() as connection:
            connection.execute("SET LOCAL statement_timeout='15s'")
            yield connection
    def principal(credentials:HTTPAuthorizationCredentials|None=Depends(bearer)):
        value=None if credentials is None else settings.authenticate(credentials.credentials)
        if value is None:raise HTTPException(401,'authentication required',headers={'WWW-Authenticate':'Bearer'})
        return value
    def admin(value=Depends(principal)):
        if value.role!='admin':raise PermissionError('administrative credential required')
        return value
    def permit(value,dataset,write=False):
        if not value.permits(dataset,write):raise PermissionError('dataset access denied')
    def arrays(connection):return ArrayStore(connection,settings.array_root)
    def version_access(connection,version,value):
        row=connection.execute('SELECT dataset_id FROM matrix_db.array_versions WHERE version_id=%s',(version,)).fetchone()
        if row is None:raise KeyError(str(version))
        permit(value,row[0])
    def response_error(code,message,status,request_id=None):
        return JSONResponse({'error':{'code':code,'message':message,'request_id':request_id}},status_code=status)
    @app.middleware('http')
    async def envelope(request,call_next):
        request_id=str(uuid4());request.state.request_id=request_id
        if request.method=='GET' and request.url.path=='/health/live':
            response=JSONResponse({'status':'live'});response.headers['X-Request-ID']=request_id;return response
        if not slots.acquire(blocking=False):
            response=response_error('server_busy','request execution capacity reached',503,request_id)
            response.headers.update({'Retry-After':'1','X-Request-ID':request_id});return response
        token=None
        try:
            size=0;parts=[]
            async for chunk in request.stream():
                size+=len(chunk)
                if size>2*1024*1024:return response_error('request_too_large','request exceeds 2 MiB',413,request_id)
                parts.append(chunk)
            request._body=b''.join(parts)
            auth=request.headers.get('Authorization','')
            scheme,_,credential=auth.partition(' ')
            value=settings.authenticate(credential) if scheme.lower()=='bearer' else None
            token=quotas.current_admission.set(quotas.Admission(value.credential_id) if value else None)
            response=await call_next(request);response.headers['X-Request-ID']=request_id
            return response
        finally:
            if token is not None:quotas.current_admission.reset(token)
            slots.release()
    @app.exception_handler(StarletteHTTPException)
    async def http_error(request,error):
        response=response_error('http_error',str(error.detail),error.status_code,request.state.request_id)
        if error.headers:response.headers.update(error.headers)
        return response
    @app.exception_handler(RequestValidationError)
    async def validation_error(request,error):return response_error('validation_error','request does not match the API schema',422,request.state.request_id)
    for exception,code,message,status in [(PermissionError,'forbidden','access denied',403),(KeyError,'not_found','resource not found',404),
        (ValueError,'invalid_request','request conflicts with data or exceeds supported limits',400),(psycopg.IntegrityError,'conflict','resource identity or references conflict',409),(psycopg.Error,'database_error','database operation failed',503),
        (OSError,'storage_error','storage is unavailable',503)]:
        async def handler(request,error,code=code,message=message,status=status):return response_error(code,message,status,request.state.request_id)
        app.add_exception_handler(exception,handler)
    @app.exception_handler(Exception)
    async def internal_error(request,error):return response_error('internal_error','request could not be completed',500,getattr(request.state,'request_id',None))
    @app.exception_handler(administration.StaleEdit)
    async def stale_edit(request,error):return response_error('stale_edit','data changed; reload before saving',409,request.state.request_id)
    @app.exception_handler(quotas.QuotaExceeded)
    async def quota_error(request,error):
        response=JSONResponse({'error':{'code':'quota_exceeded','message':'configured resource capacity reached','resource':error.resource,'limit':error.limit,'request_id':request.state.request_id}},status_code=429)
        if error.retry_after is not None:response.headers['Retry-After']=str(error.retry_after)
        return response
    @app.exception_handler(notifications.Conflict)
    async def subscription_conflict(request,error):return response_error('stale_subscription','subscription changed; reload before saving',409,request.state.request_id)
    @app.get('/v1/subscriptions')
    def notification_subscriptions(value=Depends(principal),owner:str|None=Header(None,alias='X-Matrix-Notification-Owner')):
        with db() as connection:return notifications.subscriptions(connection,value,owner)
    @app.post('/v1/subscriptions',status_code=201)
    def notification_subscribe(request:notifications.Subscription,value=Depends(principal),owner:str|None=Header(None,alias='X-Matrix-Notification-Owner')):
        with db() as connection:return notifications.create(connection,request,value,owner)
    @app.put('/v1/subscriptions/{ident}')
    def notification_update(ident:UUID,request:notifications.SubscriptionChange,value=Depends(principal),owner:str|None=Header(None,alias='X-Matrix-Notification-Owner')):
        with db() as connection:return notifications.update(connection,ident,request,value,owner)
    @app.delete('/v1/subscriptions/{ident}')
    def notification_delete(ident:UUID,expected_revision:int=Query(...,ge=1),value=Depends(principal),owner:str|None=Header(None,alias='X-Matrix-Notification-Owner')):
        with db() as connection:return notifications.remove(connection,ident,expected_revision,value,owner)
    @app.get('/v1/notifications')
    def notification_inbox(before:int|None=Query(None,ge=1,le=9223372036854775807),limit:int=Query(50,ge=1,le=100),unread:bool=False,value=Depends(principal),owner:str|None=Header(None,alias='X-Matrix-Notification-Owner')):
        with db() as connection:return jsonable_encoder(notifications.inbox(connection,value,owner,before=before,limit=limit,unread=unread))
    @app.post('/v1/notifications/{ident}/read')
    def notification_read(ident:int,value=Depends(principal),owner:str|None=Header(None,alias='X-Matrix-Notification-Owner')):
        with db() as connection:return jsonable_encoder(notifications.mark_read(connection,ident,value,owner))
    @app.get('/health/live')
    def live():return {'status':'live'}
    @app.get('/v1/ready')
    def ready(value=Depends(principal)):
        with db() as connection:
            actual=connection.execute('SELECT max(version) FROM matrix_db.schema_migrations').fetchone()[0]
            if actual!=LATEST_VERSION:raise HTTPException(503,'schema migration required')
            connection.execute('SELECT PostGIS_Version()')
        return {'status':'ready','schema_version':actual}
    @app.get('/v1/datasets')
    def datasets(limit:int=Query(100,ge=1,le=1000),after:str|None=None,value=Depends(principal)):
        return catalogue(CatalogueQuery(limit=limit,after=after),value)
    @app.post('/v1/datasets/search')
    def catalogue(query:CatalogueQuery,value=Depends(principal)):
        filters=query.model_dump(exclude={'after','limit','text'})
        if query.region is not None:
            from .contracts import Coverage
            filters['region']=Coverage.model_validate(query.region)
        if query.time is not None:filters['time']=query.time
        with db() as connection:entries=PostgresStore(connection).search(**filters)
        entries=[e for e in entries if value.permits(e.dataset.id) and (query.after is None or e.dataset.id>query.after)]
        if query.text:
            term=query.text.casefold()
            entries=[e for e in entries if term in ' '.join([e.dataset.id,e.dataset.name,e.dataset.provider,*[v.id for v in e.dataset.variables]]).casefold()]
        selected=entries[:query.limit]
        return {'items':[e.model_dump(mode='json') for e in selected],'next_cursor':selected[-1].dataset.id if len(entries)>query.limit else None}
    @app.post('/v1/quality/policies')
    def quality_policy(policy:quality.Policy,value=Depends(admin)):
        with db() as connection:return quality.register_policy(connection,policy,value.subject)
    @app.post('/v1/quality/{dataset_id}/evaluate')
    def quality_evaluate(dataset_id:str,value=Depends(admin)):
        # Rate admission commits on a separate connection before the snapshot transaction.
        quotas.admit(connection_factory,settings.limits)
        with connection_factory() as connection:return jsonable_encoder(quality.evaluate(connection,dataset_id))
    @app.get('/v1/quality/{dataset_id}/health')
    def quality_health(dataset_id:str,value=Depends(principal)):
        permit(value,dataset_id)
        with db() as connection:return jsonable_encoder(quality.health(connection,dataset_id))
    @app.get('/v1/quality/{dataset_id}/events')
    def quality_events(dataset_id:str,after:int=Query(0,ge=0),limit:int=Query(100,ge=1,le=1000),value=Depends(principal)):
        permit(value,dataset_id)
        with db() as connection:return jsonable_encoder(quality.notifications(connection,dataset_id,after,limit))
    @app.post('/v1/quality/{dataset_id}/incidents/{incident_id}/acknowledge')
    def quality_acknowledge(dataset_id:str,incident_id:UUID,value=Depends(admin)):
        with db() as connection:return quality.acknowledge(connection,dataset_id,incident_id,value.subject)
    @app.exception_handler(hazards.Conflict)
    async def hazard_conflict(request,error):return response_error('stale_hazard','policy or event changed; reload before saving',409,request.state.request_id)
    @app.get('/v1/hazards/{dataset_id}')
    def hazard_list(dataset_id:str,after:UUID|None=None,limit:int=Query(50,ge=1,le=100),status:str='all',value=Depends(principal)):
        permit(value,dataset_id)
        if status not in ('all','active','resolved','retracted','superseded'):raise ValueError('unknown hazard state')
        with db() as connection:return jsonable_encoder(hazards.listing(connection,dataset_id,after=after,limit=limit,status=status))
    @app.get('/v1/hazards/{dataset_id}/policy')
    def hazard_policy(dataset_id:str,value=Depends(principal)):
        permit(value,dataset_id)
        with db() as connection:
            revision,policy=hazards.get_policy(connection,dataset_id)
            latest=hazards.rows(connection,'SELECT id,evaluated_at,findings FROM matrix_db.hazard_assessments WHERE dataset_id=%s AND policy_revision=%s ORDER BY evaluated_at DESC,id DESC LIMIT 1',(dataset_id,revision))
            return jsonable_encoder({'revision':revision,'policy':policy.model_dump(mode='json'),'latest_assessment':latest[0] if latest else None})
    @app.get('/v1/hazards/{dataset_id}/events/{ident}')
    def hazard_detail(dataset_id:str,ident:UUID,value=Depends(principal)):
        permit(value,dataset_id)
        with db() as connection:return jsonable_encoder(hazards.detail(connection,dataset_id,ident))
    @app.post('/v1/admin/datasets/{dataset_id}/hazard-policy')
    def hazard_save_policy(dataset_id:str,change:hazards.PolicyChange,value=Depends(admin)):
        if dataset_id!=change.policy.dataset_id:raise ValueError('policy dataset mismatch')
        with db() as connection:
            result=hazards.register(connection,change,value.subject)
            administration.audit(connection,dataset_id,'hazard_policy_saved',value,revision=result['revision'])
            return result
    @app.post('/v1/admin/datasets/{dataset_id}/hazard-evaluate')
    def hazard_evaluate(dataset_id:str,value=Depends(admin)):
        quotas.admit(connection_factory,settings.limits)
        with connection_factory() as connection:return jsonable_encoder(hazards.evaluate(connection,dataset_id))
    @app.post('/v1/admin/datasets/{dataset_id}/hazard-events/{ident}/{action}')
    def hazard_change(dataset_id:str,ident:UUID,action:str,change:hazards.EventChange,value=Depends(admin)):
        if action not in ('acknowledge','retract'):raise ValueError('unsupported hazard action')
        with db() as connection:return hazards.change_event(connection,dataset_id,ident,change,value.subject,retract=action=='retract')
    @app.get('/v1/admin/datasets/{dataset_id}/resources')
    def admin_resources(dataset_id:str,value=Depends(admin)):
        with db() as connection:return quotas.report(connection,dataset_id,settings.limits)
    @app.get('/v1/admin/datasets/{dataset_id}')
    def admin_overview(dataset_id:str,value=Depends(admin)):
        with db() as connection:return jsonable_encoder(administration.overview(connection,dataset_id))
    @app.get('/v1/admin/datasets/{dataset_id}/activity')
    def admin_activity(dataset_id:str,kind:str='providers',after:str|None=Query(None,max_length=4096),limit:int=Query(50,ge=1,le=100),value=Depends(admin)):
        with db() as connection:return jsonable_encoder(administration.activity(connection,dataset_id,kind,after,limit))
    @app.post('/v1/admin/datasets/{dataset_id}/metadata')
    def admin_metadata(dataset_id:str,change:administration.MetadataChange,value=Depends(admin)):
        with db() as connection:return administration.edit_metadata(connection,dataset_id,change,value)
    @app.post('/v1/admin/datasets/{dataset_id}/quality-policy')
    def admin_quality_policy(dataset_id:str,change:administration.PolicyChange,value=Depends(admin)):
        with db() as connection:return administration.save_policy(connection,dataset_id,change,value)
    @app.post('/v1/admin/datasets/{dataset_id}/retention-policy')
    def admin_retention_policy(dataset_id:str,change:administration.RetentionChange,value=Depends(admin)):
        with db() as connection:return administration.save_policy(connection,dataset_id,change,value,retention=True)
    @app.post('/v1/admin/datasets/{dataset_id}/retention-plan')
    def admin_retention_plan(dataset_id:str,value=Depends(admin)):
        with db() as connection:
            result=lifecycle.create_plan(connection,dataset_id)
            administration.audit(connection,dataset_id,'retention_plan_created',value,plan_id=result['id'])
            return result
    @app.post('/v1/admin/datasets/{dataset_id}/jobs/{job_id}/cancel')
    def admin_cancel_job(dataset_id:str,job_id:UUID,value=Depends(admin)):
        with db() as connection:return administration.cancel_job(connection,dataset_id,job_id,value)
    @app.get('/v1/datasets/{dataset_id}')
    def dataset(dataset_id:str,value=Depends(principal)):
        permit(value,dataset_id)
        with db() as connection:return PostgresStore(connection).get(dataset_id).model_dump(mode='json')
    @app.get('/v1/datasets/{dataset_id}/versions')
    def versions(dataset_id:str,limit:int=Query(100,ge=1,le=1000),after:UUID|None=None,value=Depends(principal)):
        permit(value,dataset_id)
        with db() as connection:
            rows=connection.execute("SELECT v.version_id,v.published_at,v.previous_version,coalesce(l.tier,'hot') FROM matrix_db.array_versions v LEFT JOIN matrix_db.array_locations l USING(version_id) WHERE v.dataset_id=%s AND (%s::uuid IS NULL OR version_id>%s) ORDER BY version_id LIMIT %s",
                (dataset_id,after,after,limit+1)).fetchall()
        return jsonable_encoder({'items':[{'version_id':r[0],'published_at':r[1],'previous_version':r[2],'tier':r[3]} for r in rows[:limit]],'next_cursor':str(rows[limit-1][0]) if len(rows)>limit else None})
    @app.get('/v1/imports/{job_id}')
    def import_status(job_id:str,value=Depends(principal)):
        from .ingestion import job_report
        with db() as connection:
            row=connection.execute('SELECT dataset_id FROM matrix_db.import_jobs WHERE id=%s',(job_id,)).fetchone()
            if row is None:raise KeyError(job_id)
            permit(value,row[0])
            return jsonable_encoder(job_report(connection,job_id))
    @app.post('/v1/datasets',status_code=201)
    def register(entry:CatalogueEntry,value=Depends(principal)):
        permit(value,entry.dataset.id,True)
        with db() as connection:return PostgresStore(connection).register(entry).model_dump(mode='json')
    @app.put('/v1/datasets/{dataset_id}')
    def update(dataset_id:str,entry:CatalogueEntry,value=Depends(principal)):
        permit(value,dataset_id,True)
        with db() as connection:return PostgresStore(connection).update(dataset_id,entry).model_dump(mode='json')
    @app.post('/v1/observations',status_code=201)
    def ingest(batch:ObservationBatch,value=Depends(principal)):
        permit(value,batch.dataset_id,True)
        with db() as connection:
            from datetime import datetime,timezone
            connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(batch.dataset_id,))
            store=PostgresStore(connection)
            store.add_observations(batch.dataset_id,batch.observations)
            bounds=connection.execute('SELECT min(valid_time),max(valid_time) FROM matrix_db.observations WHERE dataset_id=%s AND NOT is_withdrawn',(batch.dataset_id,)).fetchone()
            entry=store.get(batch.dataset_id)
            if bounds[0] is not None:entry.ingested_time=TimeCoverage(start=bounds[0],end=bounds[1])
            entry.last_successful_ingestion=datetime.now(timezone.utc)
            store.update(batch.dataset_id,entry)
        return {'accepted':len(batch.observations)}
    @app.post('/v1/observations/query',response_model=Page)
    def observations(query:ObservationQuery,value=Depends(principal)):
        if query.dataset_id is None:
            if value.role!='admin':raise PermissionError('explicit dataset required')
        else:permit(value,query.dataset_id)
        with db() as connection:return query_observations(connection,query).model_dump(mode='json')
    @app.post('/v1/observations/stream')
    def stream(query:ObservationQuery,value=Depends(principal)):
        page=observations(query,value)
        def lines():
            yield json.dumps({'type':'metadata','next_cursor':page['next_cursor']})+'\n'
            for item in page['items']:yield json.dumps({'type':'observation',**item})+'\n'
        return StreamingResponse(lines(),media_type='application/x-ndjson')
    @app.post('/v1/series')
    def time_series(query:SeriesQuery,value=Depends(principal)):
        permit(value,query.dataset_id)
        with db() as connection:return jsonable_encoder(series(connection,query))
    @app.post('/v1/playback')
    def sample(query:PlaybackQuery,value=Depends(principal)):
        permit(value,query.dataset_id)
        with db() as connection:return playback(connection,query)
    @app.get('/v1/observations/{dataset_id}/{observation_id}/history')
    def revisions(dataset_id:str,observation_id:str,limit:int=Query(100,ge=1,le=1000),before_revision:int|None=Query(None,ge=1),value=Depends(principal)):
        permit(value,dataset_id)
        with db() as connection:return jsonable_encoder(history(connection,dataset_id,observation_id,limit=limit,before_revision=before_revision))
    @app.post('/v1/observations/revise')
    def revise(request:RevisionRequest,value=Depends(admin)):
        with db() as connection:return jsonable_encoder(PostgresStore(connection).revise(request.observation,expected_revision=request.expected_revision,reason=request.reason,actor=request.actor))
    @app.post('/v1/observations/withdraw')
    def withdraw(request:WithdrawalRequest,value=Depends(admin)):
        with db() as connection:return jsonable_encoder(PostgresStore(connection).withdraw(request.dataset_id,request.observation_id,expected_revision=request.expected_revision,reason=request.reason,actor=request.actor))
    @app.post('/v1/arrays/subset')
    def subset(query:SubsetQuery,value=Depends(principal)):
        with db() as connection:
            version_access(connection,query.version_id,value)
            return serialize_subset(arrays(connection).read(query,max_bytes=512*1024),value.role=='admin')
    @app.get('/v1/arrays/{version_id}')
    def manifest(version_id:UUID,value=Depends(principal)):
        with db() as connection:
            version_access(connection,version_id,value);result=arrays(connection).manifest(version_id)
            if value.role!='admin':result.pop('provenance',None)
            return result
    @app.post('/v1/derived/definitions')
    def definitions(request:list[Definition],value=Depends(admin)):
        with db() as connection:return {'registered':register_definitions(connection,request)}
    @app.post('/v1/scenarios',status_code=201)
    def scenario(request:Scenario,value=Depends(admin)):
        with db() as connection:return register_scenario(arrays(connection),request)
    @app.post('/v1/scenarios/resolve')
    def resolve(request:ScenarioRef,value=Depends(admin)):
        with db() as connection:return resolve_inputs(arrays(connection),request)
    @app.post('/v1/runs',status_code=201)
    def run(request:RunRequest,value=Depends(admin)):
        with db() as connection:return jsonable_encoder(create_run(arrays(connection),request))
    @app.get('/v1/runs/{run_id}')
    def inspect_run(run_id:UUID,include_provisional:bool=False,value=Depends(admin)):
        with db() as connection:return jsonable_encoder(get_run(connection,run_id,include_provisional=include_provisional))
    @app.post('/v1/runs/transition')
    def transition(request:RunChange,value=Depends(admin)):
        with db() as connection:return jsonable_encoder(transition_run(arrays(connection),request))
    @app.post('/v1/runs/outputs')
    def output(request:OutputAttachment,value=Depends(admin)):
        with db() as connection:return jsonable_encoder(attach_output(arrays(connection),request))
    @app.post('/v1/jobs',status_code=202)
    def submit_job(request:JobRequest,value=Depends(principal)):
        with db() as connection:return api_jobs.submit(connection,request.operation,request.payload,value)
    @app.get('/v1/jobs/{job_id}')
    def inspect_job(job_id:UUID,value=Depends(principal)):
        with db() as connection:return jsonable_encoder(api_jobs.inspect(connection,job_id,value))
    @app.post('/v1/jobs/{job_id}/cancel')
    def cancel_job(job_id:UUID,value=Depends(principal)):
        with db() as connection:return api_jobs.cancel(connection,job_id,value)
    return app

def main():
    import uvicorn
    uvicorn.run(create_app(),host='127.0.0.1',port=8000)

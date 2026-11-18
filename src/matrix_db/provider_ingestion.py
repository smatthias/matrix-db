"""Operator-run provider scheduler with atomic checkpoints and explicit backfill."""
from datetime import datetime,timedelta,timezone
from hashlib import sha256
import json
from pathlib import Path
from uuid import uuid4
from psycopg.types.json import Jsonb
from .catalogue import TimeCoverage
from .contracts import Observation
from .providers import SourceConfig,ProviderError,Transport,fetch_records,instant
from .storage import PostgresStore,connect

UTC=timezone.utc

def source_lock(ident):return int.from_bytes(sha256(('matrix-provider:'+ident).encode()).digest()[:8],'big',signed=True)

def register_source(connection,source):
    content=source.model_dump(mode='json')
    digest=sha256(json.dumps(content,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    row=connection.execute('SELECT config_hash FROM matrix_db.provider_sources WHERE id=%s',(source.id,)).fetchone()
    if row:
        if row[0]!=digest:raise ValueError('source configuration changed; use a new source and dataset identity')
        return
    store=PostgresStore(connection)
    try:existing=store.get(source.entry.dataset.id)
    except KeyError:store.register(source.entry.model_copy(update={'update_interval_seconds':source.interval_seconds}))
    else:
        if existing.dataset!=source.entry.dataset or existing.source_id!=source.entry.source_id:
            raise ValueError('provider dataset does not match registered contract')
        # Don't take ownership of manually ingested records.
        if connection.execute('SELECT 1 FROM matrix_db.observations WHERE dataset_id=%s LIMIT 1',(existing.dataset.id,)).fetchone():
            raise ValueError('provider requires an empty dedicated dataset')
    connection.execute('INSERT INTO matrix_db.provider_sources(id,config_hash,config,dataset_id) VALUES (%s,%s,%s,%s)',
        (source.id,digest,Jsonb(content),source.entry.dataset.id))

def source_state(connection,ident):
    cursor=connection.execute('''SELECT id,dataset_id,watermark,next_attempt_at,last_success_at,
        latest_valid_time,failures,error_code FROM matrix_db.provider_sources WHERE id=%s''',(ident,))
    row=cursor.fetchone()
    if row is None:raise KeyError(ident)
    return dict(zip([c.name for c in cursor.description],row))

def window(source,watermark,now):
    available=now
    if source.provider=='open_meteo':available=(now-timedelta(days=7)).replace(hour=0,minute=0,second=0,microsecond=0)
    if source.provider=='sentinel_ndvi':available=now.replace(hour=0,minute=0,second=0,microsecond=0)
    if source.provider=='overpass':return source.start_at,available
    cursor=watermark or source.start_at
    start=max(source.start_at,cursor-timedelta(seconds=source.overlap_seconds)) if watermark else cursor
    end=min(start+timedelta(days=source.window_days),available)
    return start,end

def apply_records(connection,source,records,removed,now):
    store=PostgresStore(connection);dataset_id=source.entry.dataset.id
    # Same lock order as API ingestion/corrections.
    connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset_id,))
    counts={'inserted':0,'corrected':0,'unchanged':0,'withdrawn':0}
    for record in records:
        record=Observation.model_validate_json(record.model_dump_json()).validate_against(source.entry.dataset)
        row=connection.execute('SELECT record,revision,is_withdrawn FROM matrix_db.observations WHERE dataset_id=%s AND id=%s FOR UPDATE',(dataset_id,record.id)).fetchone()
        if row is None:store.add_observations(dataset_id,[record]);counts['inserted']+=1
        elif not row[2] and Observation.model_validate(row[0]).model_dump(exclude={'ingested_at'})==record.model_dump(exclude={'ingested_at'}):counts['unchanged']+=1
        else:
            store.revise(record,expected_revision=row[1],reason='provider correction',actor='provider:'+source.id);counts['corrected']+=1
    for ident in removed:
        row=connection.execute('SELECT revision,is_withdrawn FROM matrix_db.observations WHERE dataset_id=%s AND id=%s FOR UPDATE',(dataset_id,ident)).fetchone()
        if row and not row[1]:
            store.withdraw(dataset_id,ident,expected_revision=row[0],reason='explicit upstream deletion',actor='provider:'+source.id);counts['withdrawn']+=1
    coverage=connection.execute('SELECT min(valid_time),max(valid_time) FROM matrix_db.observations WHERE dataset_id=%s AND NOT is_withdrawn',(dataset_id,)).fetchone()
    entry=store.get(dataset_id);entry.ingested_time=TimeCoverage(start=coverage[0],end=coverage[1]) if coverage[0] else None
    entry.last_successful_ingestion=now;store.update(dataset_id,entry)
    return counts,coverage[1]

def poll_source(source,connection_factory=connect,transport=None,now=None,backfill=None):
    now=now or datetime.now(UTC)
    if now.tzinfo is None:raise ValueError('now must include timezone')
    owned_transport=transport is None;transport=transport or Transport()
    try:
        with connection_factory() as owner:
            if not owner.execute('SELECT pg_try_advisory_lock(%s)',(source_lock(source.id),)).fetchone()[0]:return {'status':'busy'}
            try:
                register_source(owner,source)
                # No data/checkpoint was committed if an earlier process died mid-poll.
                owner.execute("UPDATE matrix_db.provider_runs SET status='failed',error_code='worker_interrupted',finished_at=%s WHERE source_id=%s AND status='running'",(now,source.id))
                state=source_state(owner,source.id);owner.commit()
                if backfill is None and state['next_attempt_at']>now:return {'status':'not_due'}
                start,end=backfill if backfill else window(source,state['watermark'],now)
                if backfill:
                    if source.provider=='overpass':raise ValueError('exposure supports current snapshots only')
                    if source.provider=='sentinel_ndvi' and any(t.hour or t.minute or t.second or t.microsecond for t in (start,end)):raise ValueError('vegetation backfill must use UTC day boundaries')
                    if start.tzinfo is None or end.tzinfo is None:raise ValueError('backfill timestamps must include timezone')
                    if source.provider=='open_meteo' and end>(now-timedelta(days=7)).replace(hour=0,minute=0,second=0,microsecond=0):raise ValueError('weather archive availability lag')
                    if start>=end or end>now or end-start>timedelta(days=source.window_days):raise ValueError('invalid or oversized backfill window')
                if source.provider=='sentinel_ndvi':
                    start=start.replace(hour=0,minute=0,second=0,microsecond=0);end=end.replace(hour=0,minute=0,second=0,microsecond=0)
                if start>=end or (not backfill and state['watermark'] and end<=state['watermark']):
                    owner.execute('UPDATE matrix_db.provider_sources SET next_attempt_at=%s WHERE id=%s',(now+timedelta(seconds=source.interval_seconds),source.id));owner.commit()
                    return {'status':'not_available'}
                ident=uuid4()
                owner.execute("INSERT INTO matrix_db.provider_runs(id,source_id,window_start,window_end,backfill,status) VALUES (%s,%s,%s,%s,%s,'running')",(ident,source.id,start,end,bool(backfill)));owner.commit()
                try:
                    records,removed=fetch_records(source,start,end,now,transport)
                    with connection_factory() as data:
                        data.execute("SET LOCAL statement_timeout='30s'")
                        counts,latest=apply_records(data,source,records,removed,now)
                        # Check the ownership connection before publishing checkpoint/results.
                        owner.execute('SELECT 1')
                        changed=data.execute("""UPDATE matrix_db.provider_runs SET status='completed',inserted=%s,corrected=%s,unchanged=%s,withdrawn=%s,finished_at=%s
                            WHERE id=%s AND status='running' RETURNING id""",(counts['inserted'],counts['corrected'],counts['unchanged'],counts['withdrawn'],now,ident)).fetchone()
                        if changed is None:raise ProviderError('ownership_lost')
                        data.execute('''UPDATE matrix_db.provider_sources SET watermark=CASE WHEN %s THEN watermark ELSE %s END,
                            next_attempt_at=%s,last_success_at=%s,latest_valid_time=%s,failures=0,error_code=NULL WHERE id=%s''',
                            (bool(backfill),end,now+timedelta(seconds=source.interval_seconds),now,latest,source.id))
                    return {'status':'completed','run_id':str(ident),**counts}
                except Exception as error:
                    # Never store exception text: request URLs can contain FIRMS credentials.
                    from .quotas import QuotaExceeded
                    code=error.code if isinstance(error,(ProviderError,QuotaExceeded)) else 'ingestion_failed'
                    delay=max(source.interval_seconds,min(86400,60*2**min(state['failures'],10)),(getattr(error,'retry_after',0) or 0))
                    with connection_factory() as control:
                        changed=control.execute("UPDATE matrix_db.provider_runs SET status='failed',error_code=%s,finished_at=%s WHERE id=%s AND status='running' RETURNING id",(code,now,ident)).fetchone()
                        if changed:control.execute('UPDATE matrix_db.provider_sources SET failures=failures+1,error_code=%s,next_attempt_at=%s WHERE id=%s',(code,now+timedelta(seconds=delay),source.id))
                    return {'status':'failed','run_id':str(ident),'error_code':code}
            finally:
                if not owner.closed:owner.execute('SELECT pg_advisory_unlock(%s)',(source_lock(source.id),))
    finally:
        if owned_transport:transport.close()

def load_sources(path):
    raw=json.loads(Path(path).read_text())
    if not isinstance(raw,list) or len(raw)>100:raise ValueError('configuration must contain up to 100 sources')
    sources=[SourceConfig.model_validate(item) for item in raw]
    if len({s.id for s in sources})!=len(sources) or len({s.entry.dataset.id for s in sources})!=len(sources):raise ValueError('duplicate source or dataset')
    return sources

def main():
    import argparse,time
    parser=argparse.ArgumentParser(description='Operator-only provider ingestion; configure credentials in environment')
    parser.add_argument('config');parser.add_argument('--once',action='store_true');parser.add_argument('--status',action='store_true')
    parser.add_argument('--source');parser.add_argument('--from',dest='start');parser.add_argument('--to',dest='end');args=parser.parse_args()
    try:
        sources=load_sources(args.config)
        if args.source:
            sources=[s for s in sources if s.id==args.source]
            if not sources:raise ValueError('unknown source')
        if bool(args.start)!=bool(args.end):raise ValueError('backfill requires --from and --to')
        if args.start and (not args.once or not args.source or args.status):raise ValueError('backfill requires --once and --source')
        backfill=(instant(args.start),instant(args.end)) if args.start else None
        while True:
            failed=False
            for source in sources:
                if args.status:
                    with connect() as connection:result=source_state(connection,source.id)
                else:result=poll_source(source,backfill=backfill)
                print(json.dumps({'source':source.id,**result},default=str),flush=True)
                failed=failed or result.get('status')=='failed'
            if args.once or args.status:
                if failed:raise SystemExit(1)
                return
            time.sleep(10)
    except (ValueError,KeyError):
        parser.exit(2,'invalid source configuration or backfill request\n')
    except Exception:
        parser.exit(1,'provider ingestion unavailable; check database connectivity and migration level\n')

if __name__=='__main__':main()

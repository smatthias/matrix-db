"""Retention plans, reversible POSIX tiering and non-destructive revision archives."""
from datetime import datetime,timedelta,timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import psycopg
from uuid import UUID,uuid4
from pydantic import Field
from psycopg.types.json import Jsonb
from .contracts import Contract,Identifier,Observation
from .arrays import array_lock_id,canonical,digest,sync_dir
from .storage import PostgresStore

UTC=timezone.utc
MAX_VERSIONS=1000
MAX_ARCHIVE_BYTES=1024*1024*1024

class RetentionPolicy(Contract):
    dataset_id: Identifier
    hot_days: int=Field(default=30,ge=1,le=36500)
    keep_latest: int=Field(default=1,ge=1,le=1000)
    keep_pinned: bool=True

def audit(connection,version,kind,actor,**detail):
    if not actor.strip():raise ValueError('actor is required')
    connection.execute('INSERT INTO matrix_db.lifecycle_events(version_id,kind,detail,actor) VALUES (%s,%s,%s,%s)',(version,kind,Jsonb(detail),actor))

def lock_version(connection,version):
    if not connection.execute('SELECT pg_try_advisory_xact_lock(%s)',(array_lock_id(version),)).fetchone()[0]:raise ValueError('version has an active reader or lifecycle operation')

def register_policy(connection,policy):
    with connection.transaction():
        connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(policy.dataset_id,))
        PostgresStore(connection).get(policy.dataset_id)
        active=connection.execute('SELECT p.revision,p.definition FROM matrix_db.retention_policies p JOIN matrix_db.retention_active a USING(dataset_id,revision) WHERE dataset_id=%s',(policy.dataset_id,)).fetchone()
        definition=policy.model_dump(mode='json')
        if active and active[1]==definition:return active[0]
        revision=connection.execute('SELECT coalesce(max(revision),0)+1 FROM matrix_db.retention_policies WHERE dataset_id=%s',(policy.dataset_id,)).fetchone()[0]
        connection.execute('INSERT INTO matrix_db.retention_policies VALUES (%s,%s,%s)',(policy.dataset_id,revision,Jsonb(definition)))
        connection.execute('INSERT INTO matrix_db.retention_active VALUES (%s,%s) ON CONFLICT(dataset_id) DO UPDATE SET revision=EXCLUDED.revision',(policy.dataset_id,revision))
        return revision

def policy_for(connection,dataset):
    row=connection.execute('SELECT p.revision,p.definition FROM matrix_db.retention_policies p JOIN matrix_db.retention_active a USING(dataset_id,revision) WHERE dataset_id=%s',(dataset,)).fetchone()
    if row is None:raise KeyError(dataset)
    return row[0],RetentionPolicy.model_validate(row[1])

def pins(connection,version):
    queries={
        'scenario_inputs':'SELECT count(*) FROM matrix_db.scenario_inputs WHERE array_version=%s',
        'run_outputs':'SELECT count(*) FROM matrix_db.run_outputs WHERE array_version=%s',
        'derived_inputs':'SELECT count(*) FROM matrix_db.derived_cache_sources WHERE source_version=%s',
        'derived_outputs':'SELECT count(*) FROM matrix_db.derived_cache WHERE output_version=%s',
        'comparisons':'SELECT count(*) FROM matrix_db.run_comparisons WHERE %s IN (left_version,right_version,output_version)'}
    return {kind:count for kind,sql in queries.items() if (count:=connection.execute(sql,(version,)).fetchone()[0])}

def plan_entries(connection,dataset,now):
    revision,policy=policy_for(connection,dataset)
    rows=connection.execute('''SELECT v.version_id,v.published_at,v.manifest_checksum,coalesce(l.tier,'hot'),coalesce(h.active,false)
        FROM matrix_db.array_versions v LEFT JOIN matrix_db.array_locations l USING(version_id)
        LEFT JOIN matrix_db.retention_holds h USING(version_id) WHERE v.dataset_id=%s
        ORDER BY v.published_at DESC,v.version_id DESC LIMIT %s''',(dataset,MAX_VERSIONS+1)).fetchall()
    if len(rows)>MAX_VERSIONS:raise ValueError('retention plan exceeds 1000-version budget')
    entries=[]
    for index,(version,published,checksum,tier,held) in enumerate(rows):
        references=pins(connection,version)
        reason='eligible'
        if held:reason='legal_hold'
        elif index<policy.keep_latest:reason='latest'
        elif published>now-timedelta(days=policy.hot_days):reason='young'
        elif policy.keep_pinned and references:reason='pinned'
        action='archive' if reason=='eligible' and tier=='hot' else 'keep'
        entries.append({'version_id':str(version),'manifest_checksum':checksum,'tier':tier,'action':action,'reason':reason,'pins':references})
    return revision,entries

def create_plan(connection,dataset,now=None):
    now=now or datetime.now(UTC)
    if now.tzinfo is None:raise ValueError('plan time requires timezone')
    with connection.transaction():
        connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset,))
        revision,entries=plan_entries(connection,dataset,now);ident=uuid4()
        definition={'entries':entries,'deletes':False,'observation_history':'retained'}
        connection.execute('INSERT INTO matrix_db.retention_plans VALUES (%s,%s,%s,%s,%s)',(ident,dataset,revision,now,Jsonb(definition)))
    return {'id':str(ident),'dataset_id':dataset,'policy_revision':revision,**definition}

def roots(hot,cold):
    hot=Path(hot).resolve();cold=Path(cold).resolve()
    if hot==cold or hot in cold.parents or cold in hot.parents:raise ValueError('hot and archive roots must be disjoint')
    for root in (hot,cold):
        if any((root/name).is_symlink() for name in ('versions','.evicted','revisions')):raise ValueError('storage subdirectories cannot be symlinks')
    return hot,cold

def checked_files(directory,manifest,checksum,*,allow_partial=False):
    directory=Path(directory)
    if directory.is_symlink() or not directory.is_dir():raise ValueError('version directory must be a real directory')
    expected={'manifest.json':checksum}
    for chunk in manifest['chunks']:
        path=Path(chunk['path'])
        if path.is_absolute() or '..' in path.parts or str(path) in expected:raise ValueError('unsafe or duplicate archive path')
        expected[str(path)]=chunk['checksum']
    if len(expected)>100000:raise ValueError('archive file budget exceeded')
    total=0;actual=set()
    for path in directory.rglob('*'):
        if path.is_symlink():raise ValueError('archive symlinks are forbidden')
        if path.is_file():
            name=str(path.relative_to(directory));actual.add(name);total+=path.stat().st_size
            if total>MAX_ARCHIVE_BYTES:raise ValueError('archive exceeds one GiB budget')
            if name not in expected or digest(path)!=expected[name]:raise ValueError('archive file checksum mismatch')
        elif not path.is_dir():raise ValueError('archive contains a special file')
    if not allow_partial and actual!=set(expected):raise ValueError('archive is incomplete')
    return total

def copy_verified(source,destination,manifest,checksum):
    checked_files(source,manifest,checksum)
    destination.parent.mkdir(parents=True,exist_ok=True)
    if destination.exists():checked_files(destination,manifest,checksum);return
    staging=destination.parent/('.copy-'+str(uuid4()))
    try:
        shutil.copytree(source,staging,symlinks=True)
        checked_files(staging,manifest,checksum)
        for path in staging.rglob('*'):
            if path.is_file():
                with path.open('rb') as stream:os.fsync(stream.fileno())
        for directory in sorted([p for p in staging.rglob('*') if p.is_dir()],key=lambda p:len(p.parts),reverse=True):sync_dir(directory)
        sync_dir(staging);os.rename(staging,destination);sync_dir(destination.parent)
    finally:
        if staging.exists():shutil.rmtree(staging)

def manifest_row(connection,version):
    row=connection.execute('SELECT dataset_id,manifest,manifest_checksum FROM matrix_db.array_versions WHERE version_id=%s',(version,)).fetchone()
    if row is None:raise KeyError(str(version))
    if hashlib.sha256(canonical(row[1]).encode()).hexdigest()!=row[2]:raise ValueError('catalogue manifest checksum mismatch')
    return row

def set_hold(connection,version,active,reason,actor):
    version=UUID(str(version))
    if not reason.strip():raise ValueError('hold reason is required')
    with connection.transaction():
        lock_version(connection,version);manifest_row(connection,version)
        connection.execute('INSERT INTO matrix_db.retention_holds VALUES (%s,%s,%s,%s) ON CONFLICT(version_id) DO UPDATE SET active=EXCLUDED.active,reason=EXCLUDED.reason,actor=EXCLUDED.actor',(version,active,reason,actor))
        audit(connection,version,'hold_set' if active else 'hold_released',actor,reason=reason)

def move_tier(connection,version,hot,cold,actor,*,restore=False,expected_policy_revision=None):
    version=UUID(str(version));hot,cold=roots(hot,cold)
    with connection.transaction():
        lock_version(connection,version);dataset,manifest,checksum=manifest_row(connection,version)
        if expected_policy_revision is not None:
            revision,policy=policy_for(connection,dataset)
            if revision!=expected_policy_revision:raise ValueError('retention policy changed')
            if policy.keep_pinned and pins(connection,version):raise ValueError('version became pinned; create a new plan')
        if not restore and connection.execute('SELECT 1 FROM matrix_db.retention_holds WHERE version_id=%s AND active',(version,)).fetchone():raise ValueError('version is on legal hold')
        tier=connection.execute('SELECT tier FROM matrix_db.array_locations WHERE version_id=%s',(version,)).fetchone()
        current=tier[0] if tier else 'hot';wanted='hot' if restore else 'archive'
        source=(cold if restore else hot)/'versions'/str(version)
        destination=(hot if restore else cold)/'versions'/str(version)
        if current==wanted:
            checked_files(destination,manifest,checksum)
            return {'version_id':str(version),'tier':wanted,'already_current':True}
        copy_verified(source,destination,manifest,checksum)
        connection.execute('INSERT INTO matrix_db.array_locations VALUES (%s,%s,clock_timestamp()) ON CONFLICT(version_id) DO UPDATE SET tier=EXCLUDED.tier,verified_at=EXCLUDED.verified_at',(version,wanted))
        audit(connection,version,'restored' if restore else 'archived',actor,checksum=checksum)
    return {'version_id':str(version),'tier':wanted,'already_current':False}

def apply_plan(connection_factory,plan_id,hot,cold,actor):
    # One commit per version. Interruption is resumable; versions are never deleted.
    with connection_factory() as connection:
        row=connection.execute('SELECT dataset_id,policy_revision,definition FROM matrix_db.retention_plans WHERE id=%s',(UUID(str(plan_id)),)).fetchone()
        if row is None:raise KeyError(str(plan_id))
    dataset,revision,definition=row;result=[]
    for entry in definition['entries']:
        if entry['action']!='archive':continue
        with connection_factory() as connection:
            # This lock excludes policy edits during eligibility revalidation/publication.
            connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset,))
            active,entries=plan_entries(connection,dataset,datetime.now(UTC))
            if active!=revision:raise ValueError('retention policy changed; create a new plan')
            current=next((item for item in entries if item['version_id']==entry['version_id']),None)
            if current is None or current['manifest_checksum']!=entry['manifest_checksum']:raise ValueError('plan no longer matches versions')
            if current['action']!='archive':result.append({'version_id':entry['version_id'],'status':'skipped','reason':current['reason']});continue
            result.append(move_tier(connection,entry['version_id'],hot,cold,actor,expected_policy_revision=revision))
    return {'plan_id':str(plan_id),'results':result}

def evict_hot(connection,version,hot,cold,actor):
    version=UUID(str(version));hot,cold=roots(hot,cold)
    with connection.transaction():
        dataset,manifest,checksum=manifest_row(connection,version)
        connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset,))
        lock_version(connection,version)
        _,entries=plan_entries(connection,dataset,datetime.now(UTC))
        entry=next(item for item in entries if item['version_id']==str(version))
        if entry['reason']!='eligible':raise ValueError('version is protected by current retention policy')
        tier=connection.execute('SELECT tier FROM matrix_db.array_locations WHERE version_id=%s',(version,)).fetchone()
        if tier is None or tier[0]!='archive':raise ValueError('commit archival before hot eviction')
        if connection.execute('SELECT 1 FROM matrix_db.retention_holds WHERE version_id=%s AND active',(version,)).fetchone():raise ValueError('version is on legal hold')
        # A separate MVCC reader proves the archive routing record was committed
        # before deleting hot bytes; an outer transaction rollback must remain readable.
        with psycopg.connect(connection.info.dsn,connect_timeout=5) as committed:
            routed=committed.execute('SELECT tier FROM matrix_db.array_locations WHERE version_id=%s',(version,)).fetchone()
            if routed is None or routed[0]!='archive':raise ValueError('commit archival before hot eviction')
        checked_files(cold/'versions'/str(version),manifest,checksum)
        directory=hot/'versions'/str(version)
        retired=hot/'.evicted'/str(version)
        retired.parent.mkdir(parents=True,exist_ok=True)
        if retired.exists():
            checked_files(retired,manifest,checksum,allow_partial=True);shutil.rmtree(retired);sync_dir(retired.parent)
        if directory.exists():
            checked_files(directory,manifest,checksum)
            os.rename(directory,retired);sync_dir(directory.parent);sync_dir(retired.parent)
            shutil.rmtree(retired);sync_dir(retired.parent)
        audit(connection,version,'hot_evicted',actor,checksum=checksum)
    return {'version_id':str(version),'hot_evicted':True}


def export_revisions(connection,dataset,before,cold,actor):
    before=Observation.utc_time(before);PostgresStore(connection).get(dataset)
    connection.execute("SET LOCAL statement_timeout='30s'")
    count,size=connection.execute('''SELECT count(*),coalesce(sum(octet_length(record::text)),0) FROM
        (SELECT record FROM matrix_db.observation_revisions WHERE dataset_id=%s AND recorded_at<%s LIMIT 10001) r''',(dataset,before)).fetchone()
    if count>10000 or size>64*1024*1024:raise ValueError('revision export exceeds row or byte budget')
    rows=connection.execute('''SELECT id,revision,recorded_at,record,is_withdrawn,change_reason,changed_by,import_job_id
        FROM matrix_db.observation_revisions WHERE dataset_id=%s AND recorded_at<%s
        ORDER BY id,revision LIMIT 10001''',(dataset,before)).fetchall()
    if len(rows)>10000:raise ValueError('revision export exceeds 10000-row budget')
    ident=uuid4();root=Path(cold).resolve()/'revisions';root.mkdir(parents=True,exist_ok=True)
    final=root/(str(ident)+'.jsonl.gz');staging=root/('.copy-'+str(ident))
    try:
        with staging.open('wb') as raw:
            with gzip.GzipFile(fileobj=raw,mode='wb',mtime=0) as output:
                for row in rows:
                    value=dict(zip(('id','revision','recorded_at','record','is_withdrawn','change_reason','changed_by','import_job_id'),row));value['dataset_id']=dataset
                    output.write((json.dumps(value,sort_keys=True,default=str,allow_nan=False)+'\n').encode())
                    if raw.tell()>MAX_ARCHIVE_BYTES:raise ValueError('revision archive exceeds budget')
            raw.flush();os.fsync(raw.fileno())
        checksum=digest(staging);os.rename(staging,final);sync_dir(root)
        connection.execute('INSERT INTO matrix_db.revision_archives(id,dataset_id,before_time,row_count,checksum) VALUES (%s,%s,%s,%s,%s)',(ident,dataset,before,len(rows),checksum))
        audit(connection,None,'revisions_exported',actor,archive_id=str(ident),dataset_id=dataset,row_count=len(rows),checksum=checksum)
    finally:
        if staging.exists():staging.unlink()
    return {'id':str(ident),'row_count':len(rows),'checksum':checksum}

def verify_revision_archive(connection,ident,cold):
    ident=UUID(str(ident));row=connection.execute('SELECT row_count,checksum FROM matrix_db.revision_archives WHERE id=%s',(ident,)).fetchone()
    if row is None:raise KeyError(str(ident))
    path=Path(cold).resolve()/'revisions'/(str(ident)+'.jsonl.gz')
    if path.is_symlink() or digest(path)!=row[1]:raise ValueError('revision archive checksum mismatch')
    with gzip.open(path,'rt',encoding='utf-8') as stream:count=sum(1 for line in stream if json.loads(line))
    if count!=row[0]:raise ValueError('revision archive row count mismatch')
    return {'id':str(ident),'verified':True,'row_count':count}


def main():
    import argparse
    from .storage import connect
    parser=argparse.ArgumentParser(description='Operator-only storage lifecycle; no automatic deletion')
    actions=parser.add_mutually_exclusive_group(required=True)
    for name in ('policy','plan','apply','restore','evict-hot','hold','release-hold','export-revisions','verify-export'):actions.add_argument('--'+name)
    parser.add_argument('--before');parser.add_argument('--reason');parser.add_argument('--actor',default='operator-cli');args=parser.parse_args()
    hot=os.environ.get('MATRIX_DB_ARRAY_ROOT');cold=os.environ.get('MATRIX_DB_ARCHIVE_ROOT')
    try:
        with connect() as connection:
            if args.policy:result={'revision':register_policy(connection,RetentionPolicy.model_validate_json(Path(args.policy).read_text()))}
            elif args.plan:result=create_plan(connection,args.plan)
            elif args.apply:
                if not hot or not cold:raise ValueError('storage roots required')
                result=apply_plan(connect,args.apply,hot,cold,args.actor)
            elif args.restore or args.evict_hot:
                if not hot or not cold:raise ValueError('storage roots required')
                if args.restore:result=move_tier(connection,args.restore,hot,cold,args.actor,restore=True)
                else:result=evict_hot(connection,args.evict_hot,hot,cold,args.actor)
            elif args.hold or args.release_hold:
                if not args.reason:raise ValueError('reason required')
                set_hold(connection,args.hold or args.release_hold,bool(args.hold),args.reason,args.actor);result={'hold_active':bool(args.hold)}
            elif args.export_revisions:
                if not cold or not args.before:raise ValueError('archive root and timestamp required')
                from .providers import instant
                result=export_revisions(connection,args.export_revisions,instant(args.before),cold,args.actor)
            else:
                if not cold:raise ValueError('archive root required')
                result=verify_revision_archive(connection,args.verify_export,cold)
        print(json.dumps(result,default=str))
    except (ValueError,KeyError):parser.exit(2,'invalid lifecycle request, changed plan, integrity failure or protected version\n')
    except Exception:parser.exit(1,'lifecycle unavailable; check database, roots and migration level\n')

if __name__=='__main__':main()

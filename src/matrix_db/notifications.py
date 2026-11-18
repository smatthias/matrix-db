"""Credential-owned incident subscriptions and atomic, deduplicated in-app delivery."""
from datetime import datetime,timezone
import hashlib
from uuid import UUID,uuid4
from typing import Literal
from pydantic import Field,model_validator
from psycopg.types.json import Jsonb
from .contracts import Contract,Identifier
from .storage import PostgresStore

class Subscription(Contract):
    topic: Literal['quality','hazard']='quality'
    name: str=Field(min_length=1,max_length=150)
    dataset_id: Identifier
    kinds: list[Literal['opened','acknowledged','resolved','policy_superseded','updated','severity_changed','retracted']]=Field(default_factory=lambda:['opened','resolved'],min_length=1,max_length=7)
    minimum_severity: Literal['warning','critical']='warning'
    rule_ids: list[Identifier]=Field(default_factory=list,max_length=50)
    active: bool=True
    start: Literal['now','beginning']='now'
    @model_validator(mode='before')
    @classmethod
    def topic_defaults(cls,value):
        if isinstance(value,dict) and value.get('topic')=='hazard' and 'kinds' not in value:
            return {**value,'kinds':['opened','severity_changed','resolved','retracted','policy_superseded']}
        return value
    @model_validator(mode='after')
    def unique(self):
        if not self.name.strip() or len(set(self.kinds))!=len(self.kinds) or len(set(self.rule_ids))!=len(self.rule_ids):raise ValueError('subscription name and filters must be unambiguous')
        if self.topic=='quality' and any(k in ('updated','severity_changed','retracted') for k in self.kinds):raise ValueError('hazard transitions require hazard topic')
        return self
class SubscriptionChange(Contract):
    expected_revision: int=Field(ge=1)
    subscription: Subscription

class Conflict(Exception):pass

def owner_key(principal,delegated=None):
    if principal.notification_gateway:
        if not delegated or len(delegated)>100 or not delegated.startswith('gs-user:') or not delegated[8:].isdigit():raise PermissionError('gateway notification owner required')
    elif delegated is not None:raise PermissionError('notification delegation is not permitted')
    return hashlib.sha256((principal.credential_id+'\0'+(delegated or '')).encode()).hexdigest()

def permitted(principal,dataset):
    if not principal.permits(dataset):raise PermissionError('dataset access denied')

def version(connection,ident,revision,definition,action):
    connection.execute('INSERT INTO matrix_db.notification_subscription_versions(subscription_id,revision,definition,action) VALUES (%s,%s,%s,%s)',(ident,revision,Jsonb(definition),action))

def boundary(connection,dataset,topic):
    table='hazard_feed' if topic=='hazard' else 'quality_events'
    return connection.execute(f'SELECT coalesce(max(sequence),0) FROM matrix_db.{table} WHERE dataset_id=%s',(dataset,)).fetchone()[0]

def create(connection,request,principal,delegated=None):
    owner=owner_key(principal,delegated);permitted(principal,request.dataset_id)
    # Serialize owner creation across datasets to enforce the small per-owner cap.
    lock=int.from_bytes(bytes.fromhex(owner)[:8],'big',signed=True)
    connection.execute('SELECT pg_advisory_xact_lock(%s)',(lock,))
    count=connection.execute('SELECT count(*) FROM matrix_db.notification_subscriptions WHERE owner=%s AND NOT deleted',(owner,)).fetchone()[0]
    from .quotas import ResourceLimits,QuotaExceeded
    cap=ResourceLimits.from_environment().subscriptions_per_owner
    if count>=cap:raise QuotaExceeded('subscriptions_per_owner',cap)
    connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(request.dataset_id,));PostgresStore(connection).get(request.dataset_id)
    cursor=0 if request.start=='beginning' else boundary(connection,request.dataset_id,request.topic)
    ident=uuid4();definition=request.model_dump(mode='json')
    connection.execute('''INSERT INTO matrix_db.notification_subscriptions(id,owner,credential_id,delegated_owner,dataset_id,definition,active,last_sequence)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',(ident,owner,principal.credential_id,delegated,request.dataset_id,Jsonb(definition),request.active,cursor))
    version(connection,ident,1,definition,'created')
    return {'id':str(ident),'revision':1,'subscription':definition}

def get_owned(connection,ident,principal,delegated=None,*,lock=False):
    owner=owner_key(principal,delegated)
    cursor=connection.execute('SELECT id,dataset_id,revision,definition,active,deleted,last_sequence,error_code FROM matrix_db.notification_subscriptions WHERE id=%s AND owner=%s'+(' FOR UPDATE' if lock else ''),(UUID(str(ident)),owner))
    row=cursor.fetchone()
    if row is None or row[5]:raise KeyError(str(ident))
    permitted(principal,row[1])
    return row

def update(connection,ident,change,principal,delegated=None):
    # Lock order matches creation/event writers: dataset first, subscription second.
    original=get_owned(connection,ident,principal,delegated)
    if original[1]!=change.subscription.dataset_id:raise ValueError('subscription dataset is immutable')
    connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(original[1],))
    row=get_owned(connection,ident,principal,delegated,lock=True)
    if row[2]!=change.expected_revision:raise Conflict()
    definition=change.subscription.model_dump(mode='json');revision=row[2]+1
    # Updates/resume start at the current boundary unless replay was explicitly selected.
    cursor=0 if change.subscription.start=='beginning' else boundary(connection,row[1],change.subscription.topic)
    connection.execute('UPDATE matrix_db.notification_subscriptions SET definition=%s,active=%s,revision=%s,last_sequence=%s,error_code=NULL WHERE id=%s',(Jsonb(definition),change.subscription.active,revision,cursor,row[0]))
    version(connection,row[0],revision,definition,'updated')
    return {'id':str(row[0]),'revision':revision,'subscription':definition}

def remove(connection,ident,expected_revision,principal,delegated=None):
    row=get_owned(connection,ident,principal,delegated,lock=True)
    if row[2]!=expected_revision:raise Conflict()
    revision=row[2]+1
    connection.execute('UPDATE matrix_db.notification_subscriptions SET deleted=true,active=false,revision=%s WHERE id=%s',(revision,row[0]))
    version(connection,row[0],revision,row[3],'deleted')
    return {'id':str(row[0]),'deleted':True}

def subscriptions(connection,principal,delegated=None):
    owner=owner_key(principal,delegated)
    rows=connection.execute('SELECT id,revision,definition,active,error_code FROM matrix_db.notification_subscriptions WHERE owner=%s AND NOT deleted ORDER BY created_at,id',(owner,)).fetchall()
    return {'items':[{'id':str(row[0]),'revision':row[1],'subscription':row[2],'active':row[3],'error_code':row[4]} for row in rows if principal.permits(row[2]['dataset_id'])]}

def matches(request,kind,severity,rule):
    return kind in request.kinds and (request.minimum_severity=='warning' or severity=='critical') and (not request.rule_ids or rule in request.rule_ids)

def run_one(settings,connection_factory,batch_size=100):
    if type(batch_size) is not int or not 1<=batch_size<=1000:raise ValueError('invalid fanout batch size')
    with connection_factory() as connection:
        connection.execute("SET LOCAL statement_timeout='15s'")
        # SKIP LOCKED allows independent subscriptions to progress; one checkpoint transaction
        # contains both delivery and match insertion, so crashes cannot lose/duplicate events.
        row=connection.execute('''SELECT id,owner,credential_id,delegated_owner,dataset_id,revision,definition,last_sequence
            FROM matrix_db.notification_subscriptions WHERE active AND NOT deleted ORDER BY last_polled_at,id
            FOR UPDATE SKIP LOCKED LIMIT 1''').fetchone()
        if row is None:return False
        ident,owner,credential,delegated,dataset,revision,definition,cursor=row
        principal=settings.keys.get(credential)
        allowed=principal is not None and principal.permits(dataset)
        if allowed:
            try:allowed=owner_key(principal,delegated)==owner
            except PermissionError:allowed=False
        if not allowed:
            connection.execute("UPDATE matrix_db.notification_subscriptions SET active=false,error_code='authorization_revoked',revision=revision+1 WHERE id=%s",(ident,))
            version(connection,ident,revision+1,definition,'authorization_revoked');return True
        request=Subscription.model_validate(definition)
        if request.topic=='hazard':
            rows=connection.execute('SELECT sequence,kind,at,event_id,assessment_id,rule_id,severity FROM matrix_db.hazard_feed WHERE dataset_id=%s AND sequence>%s ORDER BY sequence LIMIT %s',(dataset,cursor,batch_size)).fetchall()
            sequence_column='hazard_sequence'
        else:
            rows=connection.execute('''SELECT e.sequence,e.kind,e.at,e.incident_id,e.evaluation_id,i.rule_id,i.severity
                FROM matrix_db.quality_events e JOIN matrix_db.quality_incidents i ON i.id=e.incident_id
                WHERE e.dataset_id=%s AND e.sequence>%s ORDER BY e.sequence LIMIT %s''',(dataset,cursor,batch_size)).fetchall()
            sequence_column='event_sequence'
        from .quotas import inbox_capacity
        processed=False
        for sequence,kind,at,incident,evaluation,rule,severity in rows:
            if not matches(request,kind,severity,rule):cursor=sequence;processed=True;continue
            existing=connection.execute(f'SELECT id FROM matrix_db.notification_deliveries WHERE owner=%s AND {sequence_column}=%s',(owner,sequence)).fetchone()
            if existing is None and not inbox_capacity(connection,owner,settings.limits):
                connection.execute("UPDATE matrix_db.notification_subscriptions SET last_sequence=%s,last_polled_at=clock_timestamp(),error_code='inbox_capacity' WHERE id=%s",(cursor,ident))
                return processed
            payload={'topic':request.topic,'event_sequence':sequence,'dataset_id':dataset,'kind':kind,'at':at.isoformat(),('event_id' if request.topic=='hazard' else 'incident_id'):str(incident),
                'evaluation_id':str(evaluation) if evaluation else None,'rule_id':rule,'severity':severity}
            delivery=connection.execute(f'''INSERT INTO matrix_db.notification_deliveries(owner,dataset_id,{sequence_column},payload)
                VALUES (%s,%s,%s,%s) ON CONFLICT(owner,{sequence_column}) DO UPDATE SET owner=EXCLUDED.owner RETURNING id''',(owner,dataset,sequence,Jsonb(payload))).fetchone()[0]
            connection.execute('INSERT INTO matrix_db.notification_matches VALUES (%s,%s) ON CONFLICT DO NOTHING',(delivery,ident))
            cursor=sequence;processed=True
        connection.execute('UPDATE matrix_db.notification_subscriptions SET last_sequence=%s,last_polled_at=clock_timestamp(),error_code=NULL WHERE id=%s',(cursor,ident))
        return processed

def inbox(connection,principal,delegated=None,*,before=None,limit=50,unread=False):
    owner=owner_key(principal,delegated)
    if type(limit) is not int or not 1<=limit<=100:raise ValueError('invalid inbox page size')
    if before is not None and (type(before) is not int or not 1<=before<=9223372036854775807):raise ValueError('invalid inbox cursor')
    rows=connection.execute('''SELECT id,payload,read_at,created_at FROM matrix_db.notification_deliveries
        WHERE owner=%s AND (%s OR dataset_id=ANY(%s)) AND (%s::bigint IS NULL OR id<%s)
        AND (NOT %s OR read_at IS NULL) ORDER BY id DESC LIMIT %s''',
        (owner,principal.role=='admin',principal.datasets,before,before,unread,limit+1)).fetchall()
    selected=rows[:limit]
    return {'items':[{'id':row[0],'notification':row[1],'read_at':row[2],'created_at':row[3]} for row in selected],
        'next_before':selected[-1][0] if len(rows)>limit else None}

def mark_read(connection,ident,principal,delegated=None):
    owner=owner_key(principal,delegated)
    row=connection.execute('SELECT dataset_id FROM matrix_db.notification_deliveries WHERE id=%s AND owner=%s FOR UPDATE',(ident,owner)).fetchone()
    if row is None:raise KeyError(str(ident))
    permitted(principal,row[0])
    at=connection.execute('UPDATE matrix_db.notification_deliveries SET read_at=coalesce(read_at,clock_timestamp()) WHERE id=%s RETURNING read_at',(ident,)).fetchone()[0]
    return {'id':ident,'read_at':at}

def main():
    import argparse,time
    from .storage import connect
    from .api_auth import Settings
    parser=argparse.ArgumentParser(description='Deliver persisted quality events to in-app inboxes; no external messages')
    parser.add_argument('--once',action='store_true');parser.add_argument('--batch-size',type=int,default=100);args=parser.parse_args()
    try:
        import psycopg
        attempts=0
        while True:
            settings=Settings.from_environment()
            try:worked=run_one(settings,connect,args.batch_size)
            except psycopg.Error:
                if args.once or attempts>=5:raise
                time.sleep(min(2**attempts,16));attempts+=1;continue
            attempts=0
            if args.once:return
            if not worked:time.sleep(.5)
    except Exception:parser.exit(1,'notification worker unavailable; check configuration and database\n')

if __name__=='__main__':main()

"""Versioned, bounded quality assessment; measurements/upstream flags are never rewritten."""
from collections import defaultdict
from datetime import datetime,timedelta,timezone
from uuid import UUID,uuid4
from typing import Literal
from pydantic import Field,model_validator
from psycopg.types.json import Jsonb
from .contracts import Contract,Identifier,Observation
from .queries import Point
from .storage import PostgresStore

UTC=timezone.utc
MAX_SAMPLES=10000

class Rule(Contract):
    id: Identifier
    kind: Literal['range','rate','completeness','freshness','coverage','consistency']
    severity: Literal['warning','critical']='warning'
    variable_id: Identifier
    minimum: float | None=None
    maximum: float | None=None
    max_rate_per_second: float | None=Field(default=None,gt=0)
    expected_count: int | None=Field(default=None,ge=1,le=10000000)
    min_fraction: float | None=Field(default=None,ge=0,le=1)
    max_age_seconds: int | None=Field(default=None,ge=1,le=31536000)
    points: list[Point] | None=Field(default=None,min_length=1,max_length=1000)
    other_variable: Identifier | None=None
    tolerance: float | None=Field(default=None,ge=0)

    @model_validator(mode='after')
    def parameters(self):
        fields={'range':{'minimum','maximum'},'rate':{'max_rate_per_second'},'completeness':{'expected_count','min_fraction'},
            'freshness':{'max_age_seconds'},'coverage':{'points','min_fraction'},'consistency':{'other_variable','tolerance'}}
        provided={key for key in set().union(*fields.values()) if getattr(self,key) is not None}
        allowed=fields[self.kind]
        if provided-allowed or (self.kind!='range' and allowed-provided) or not provided:raise ValueError('parameters must match rule kind')
        if self.minimum is not None and self.maximum is not None and self.minimum>self.maximum:raise ValueError('range bounds must be ordered')
        if self.points and len({(p.longitude,p.latitude) for p in self.points})!=len(self.points):raise ValueError('coverage points must be unique')
        if self.other_variable==self.variable_id:raise ValueError('consistency requires distinct variables')
        return self

class Policy(Contract):
    dataset_id: Identifier
    lookback_seconds: int=Field(default=86400,ge=1,le=2592000)
    evaluation_max_age_seconds: int=Field(default=3600,ge=1,le=2592000)
    rules: list[Rule]=Field(min_length=1,max_length=50)
    @model_validator(mode='after')
    def unique(self):
        if len({r.id for r in self.rules})!=len(self.rules):raise ValueError('rule IDs must be unique')
        return self

def event(connection,incident,dataset,kind,actor,now,evaluation=None):
    connection.execute('INSERT INTO matrix_db.quality_events(incident_id,dataset_id,kind,actor,at,evaluation_id) VALUES (%s,%s,%s,%s,%s,%s)',
        (incident,dataset,kind,actor,now,evaluation))

def register_policy(connection,policy,actor):
    # Caller owns transaction; serialize registration, assessment and acknowledgement per dataset.
    connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(policy.dataset_id,))
    dataset=PostgresStore(connection).get(policy.dataset_id).dataset
    variables={v.id:v for v in dataset.variables}
    for rule in policy.rules:
        if rule.variable_id not in variables or (rule.other_variable and rule.other_variable not in variables):raise ValueError('unknown quality variable')
        if rule.other_variable and variables[rule.other_variable].unit!=variables[rule.variable_id].unit:raise ValueError('consistency requires matching units')
        if rule.points and any(not dataset.coverage.contains(p.longitude,p.latitude) for p in rule.points):raise ValueError('expected point outside dataset')
    definition=policy.model_dump(mode='json')
    active=connection.execute('''SELECT p.revision,p.definition FROM matrix_db.quality_policies p JOIN matrix_db.quality_active a USING(dataset_id,revision) WHERE dataset_id=%s''',(policy.dataset_id,)).fetchone()
    if active and active[1]==definition:return {'dataset_id':policy.dataset_id,'revision':active[0]}
    revision=connection.execute('SELECT coalesce(max(revision),0)+1 FROM matrix_db.quality_policies WHERE dataset_id=%s',(policy.dataset_id,)).fetchone()[0]
    connection.execute('INSERT INTO matrix_db.quality_policies(dataset_id,revision,definition) VALUES (%s,%s,%s)',(policy.dataset_id,revision,Jsonb(definition)))
    connection.execute('INSERT INTO matrix_db.quality_active(dataset_id,revision) VALUES (%s,%s) ON CONFLICT(dataset_id) DO UPDATE SET revision=EXCLUDED.revision',(policy.dataset_id,revision))
    now=datetime.now(UTC)
    rows=connection.execute("UPDATE matrix_db.quality_incidents SET status='resolved',updated_at=%s WHERE dataset_id=%s AND status<>'resolved' RETURNING id",(now,policy.dataset_id)).fetchall()
    for row in rows:event(connection,row[0],policy.dataset_id,'policy_superseded',actor,now)
    return {'dataset_id':policy.dataset_id,'revision':revision}

def get_policy(connection,dataset_id):
    row=connection.execute('''SELECT p.revision,p.definition FROM matrix_db.quality_policies p JOIN matrix_db.quality_active a USING(dataset_id,revision) WHERE dataset_id=%s''',(dataset_id,)).fetchone()
    if row is None:raise KeyError(dataset_id)
    return row[0],Policy.model_validate(row[1])

def usable(sample):return sample.value is not None and sample.quality!='bad'
def position(sample):return (sample.longitude,sample.latitude,sample.vertical.model_dump_json() if sample.vertical else None,sample.forecast_issue_time)
def sample_key(sample):return (sample.valid_time,*position(sample))
def evidence(row):
    sample,revision,recorded_at=row
    return {'observation_id':sample.id,'revision':revision,'recorded_at':recorded_at.isoformat()}

def assess(policy,rows,now,latest):
    """Pure assessment over revision-pinned rows in the selected valid-time window."""
    findings=[]
    for rule in policy.rules:
        selected=[row for row in rows if row[0].variable_id==rule.variable_id]
        good=[row for row in selected if usable(row[0])]
        failed=[];checked=0;metric=None;status='pass'
        if rule.kind=='range':
            checked=sum(row[0].value is not None for row in selected)
            failed=[row for row in selected if row[0].value is not None and ((rule.minimum is not None and row[0].value<rule.minimum) or (rule.maximum is not None and row[0].value>rule.maximum))]
            if not checked:status='unknown'
        elif rule.kind=='rate':
            groups=defaultdict(list)
            for row in good:groups[position(row[0])].append(row)
            for group in groups.values():
                group.sort(key=lambda row:(row[0].valid_time,row[0].id))
                for before,after in zip(group,group[1:]):
                    dt=(after[0].valid_time-before[0].valid_time).total_seconds();checked+=1
                    delta=abs(after[0].value-before[0].value)
                    if (dt==0 and delta!=0) or (dt>0 and delta/dt>rule.max_rate_per_second):failed.append(after)
            if not checked:status='unknown'
        elif rule.kind=='completeness':
            checked=len({sample_key(row[0]) for row in good});metric=min(1,checked/rule.expected_count)
            status='fail' if metric<rule.min_fraction else 'pass'
        elif rule.kind=='freshness':
            at=latest.get(rule.variable_id);metric=(now-at).total_seconds() if at else None
            checked=int(at is not None);status='fail' if at is None or metric>rule.max_age_seconds else 'pass'
        elif rule.kind=='coverage':
            present={(row[0].longitude,row[0].latitude) for row in good}
            checked=sum((p.longitude,p.latitude) in present for p in rule.points);metric=checked/len(rule.points)
            status='fail' if metric<rule.min_fraction else 'pass'
        else:
            # Ambiguous counterpart keys are failures, never an arbitrary last value.
            others=defaultdict(list)
            for row in rows:
                if row[0].variable_id==rule.other_variable and usable(row[0]):others[sample_key(row[0])].append(row[0].value)
            checked=len(good)
            for row in good:
                values=others[sample_key(row[0])]
                if len(values)!=1 or row[0].value>values[0]+rule.tolerance:failed.append(row)
            if not checked:status='unknown'
        if failed:status='fail'
        findings.append({'rule_id':rule.id,'kind':rule.kind,'variable_id':rule.variable_id,'severity':rule.severity,'status':status,'checked':checked,
            'failed_count':len(failed),'metric':metric,'evidence':[evidence(row) for row in failed[:20]]})
    return findings

def evaluate(connection,dataset_id,now=None):
    now=now or datetime.now(UTC)
    if now.tzinfo is None:raise ValueError('evaluation time requires timezone')
    # The dataset row lock serializes all quality state changes. Measurements are
    # selected in one statement; out-of-window freshness inputs use the same snapshot.
    connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    connection.execute("SET LOCAL statement_timeout='30s'")
    connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset_id,))
    revision,policy=get_policy(connection,dataset_id);start=now-timedelta(seconds=policy.lookback_seconds)
    raw=connection.execute('''SELECT record,revision,recorded_at FROM matrix_db.observations WHERE dataset_id=%s AND NOT is_withdrawn
        AND valid_time>=%s AND valid_time<=%s ORDER BY valid_time,id LIMIT %s''',(dataset_id,start,now,MAX_SAMPLES+1)).fetchall()
    if len(raw)>MAX_SAMPLES:raise ValueError('quality window exceeds sample budget; shorten lookback or partition dataset')
    rows=[(Observation.model_validate(record),rev,at) for record,rev,at in raw]
    latest_raw=connection.execute("""SELECT DISTINCT ON(variable_id) record,revision,recorded_at FROM matrix_db.observations
        WHERE dataset_id=%s AND NOT is_withdrawn AND valid_time<=%s AND record->'value'<>'null'::jsonb
        AND record->>'quality'<>'bad' AND variable_id=ANY(%s) ORDER BY variable_id,valid_time DESC,id""",
        (dataset_id,now,list({r.variable_id for r in policy.rules if r.kind=='freshness'}))).fetchall()
    latest_rows=[(Observation.model_validate(record),rev,at) for record,rev,at in latest_raw]
    latest={row[0].variable_id:row[0].valid_time for row in latest_rows}
    findings=assess(policy,rows,now,latest);ident=uuid4()
    # Pin window samples and out-of-window freshness inputs without changing source flags.
    pinned={(row[0].id,row[1]):evidence(row) for row in rows+latest_rows}
    snapshot=list(pinned.values())
    connection.execute('INSERT INTO matrix_db.quality_evaluations(id,dataset_id,policy_revision,evaluated_at,window_start,findings,snapshot) VALUES (%s,%s,%s,%s,%s,%s,%s)',
        (ident,dataset_id,revision,now,start,Jsonb(findings),Jsonb(snapshot)))
    for finding in findings:
        current=connection.execute("SELECT id FROM matrix_db.quality_incidents WHERE dataset_id=%s AND policy_revision=%s AND rule_id=%s AND status<>'resolved' FOR UPDATE",(dataset_id,revision,finding['rule_id'])).fetchone()
        if finding['status']=='fail':
            if current:
                connection.execute('UPDATE matrix_db.quality_incidents SET updated_at=%s,evaluation_id=%s WHERE id=%s',(now,ident,current[0]))
            else:
                incident=uuid4()
                connection.execute("INSERT INTO matrix_db.quality_incidents(id,dataset_id,policy_revision,rule_id,severity,status,opened_at,updated_at,evaluation_id) VALUES (%s,%s,%s,%s,%s,'open',%s,%s,%s)",
                    (incident,dataset_id,revision,finding['rule_id'],finding['severity'],now,now,ident))
                event(connection,incident,dataset_id,'opened','quality-monitor',now,ident)
        elif finding['status']=='pass' and current:
            connection.execute("UPDATE matrix_db.quality_incidents SET status='resolved',updated_at=%s,evaluation_id=%s WHERE id=%s",(now,ident,current[0]))
            event(connection,current[0],dataset_id,'resolved','quality-monitor',now,ident)
    return {'id':str(ident),'dataset_id':dataset_id,'policy_revision':revision,'evaluated_at':now,'findings':findings}

def acknowledge(connection,dataset_id,incident_id,actor):
    connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset_id,))
    row=connection.execute('SELECT status FROM matrix_db.quality_incidents WHERE id=%s AND dataset_id=%s FOR UPDATE',(UUID(str(incident_id)),dataset_id)).fetchone()
    if row is None:raise KeyError(str(incident_id))
    if row[0]=='resolved':raise ValueError('incident is resolved')
    if row[0]=='open':
        now=datetime.now(UTC)
        connection.execute("UPDATE matrix_db.quality_incidents SET status='acknowledged',updated_at=%s WHERE id=%s",(now,incident_id))
        event(connection,incident_id,dataset_id,'acknowledged',actor,now)
    return {'id':str(incident_id),'status':'acknowledged'}

def records(connection,sql,params):
    cursor=connection.execute(sql,params);names=[c.name for c in cursor.description]
    return [dict(zip(names,row)) for row in cursor.fetchall()]

def health(connection,dataset_id,now=None):
    now=now or datetime.now(UTC);PostgresStore(connection).get(dataset_id)
    try:revision,policy=get_policy(connection,dataset_id)
    except KeyError:revision=None;policy=None
    evaluations=records(connection,'''SELECT id,policy_revision,evaluated_at,findings FROM matrix_db.quality_evaluations
        WHERE dataset_id=%s AND policy_revision=%s ORDER BY evaluated_at DESC,id DESC LIMIT 1''',(dataset_id,revision))
    incidents=records(connection,"SELECT id,rule_id,severity,status,opened_at,updated_at FROM matrix_db.quality_incidents WHERE dataset_id=%s AND status<>'resolved' ORDER BY opened_at,id LIMIT 101",(dataset_id,))
    provider=records(connection,'SELECT last_success_at,latest_valid_time,failures,error_code FROM matrix_db.provider_sources WHERE dataset_id=%s',(dataset_id,))
    evaluation=evaluations[0] if evaluations else None
    expired=evaluation is None or (now-evaluation['evaluated_at']).total_seconds()>policy.evaluation_max_age_seconds or evaluation['evaluated_at']>now
    status='unknown' if expired or any(f['status']=='unknown' for f in evaluation['findings']) else 'healthy'
    if incidents or (provider and provider[0]['failures']):status='degraded'
    return {'dataset_id':dataset_id,'status':status,'evaluation_expired':expired,'policy_revision':revision,'evaluation':evaluation,
        'incidents':incidents[:100],'incidents_truncated':len(incidents)>100,'provider':provider[0] if provider else None}

def notifications(connection,dataset_id,after=0,limit=100):
    items=records(connection,'SELECT sequence,incident_id,kind,actor,at,evaluation_id FROM matrix_db.quality_events WHERE dataset_id=%s AND sequence>%s ORDER BY sequence LIMIT %s',(dataset_id,after,limit+1))
    page=items[:limit]
    return {'items':page,'next_cursor':page[-1]['sequence'] if len(items)>limit else None}

def main():
    import argparse,json,time
    from pathlib import Path
    from .storage import connect
    parser=argparse.ArgumentParser();parser.add_argument('--policy');parser.add_argument('--dataset');parser.add_argument('--once',action='store_true')
    parser.add_argument('--interval',type=int,default=300);args=parser.parse_args()
    try:
        if args.interval<60:raise ValueError('interval too short')
        if args.policy:
            policy=Policy.model_validate_json(Path(args.policy).read_text())
            if args.dataset and args.dataset!=policy.dataset_id:raise ValueError('dataset mismatch')
            args.dataset=policy.dataset_id
            with connect() as connection:register_policy(connection,policy,'operator-cli')
        if not args.dataset:raise ValueError('dataset required')
        while True:
            with connect() as connection:result=evaluate(connection,args.dataset)
            print(json.dumps(result,default=str),flush=True)
            if args.once:return
            time.sleep(args.interval)
    except (ValueError,KeyError):parser.exit(2,'invalid quality policy, dataset or evaluation budget\n')
    except Exception:parser.exit(1,'quality monitor unavailable; check database and migration level\n')

if __name__=='__main__':main()

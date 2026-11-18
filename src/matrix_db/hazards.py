"""Configured rolling-window signals with revision evidence, not validated hazard forecasts."""
from datetime import datetime,timedelta,timezone
import hashlib
import math
from typing import Literal
from uuid import UUID,uuid4
from pydantic import Field,model_validator
from psycopg.types.json import Jsonb
from .contracts import Contract,Identifier,Observation,Coverage
from .arrays import canonical
from .storage import PostgresStore

UTC=timezone.utc
MAX_SAMPLES=2000
class Rule(Contract):
 id: Identifier
 name: str=Field(min_length=1,max_length=200)
 hazard_type: Literal['wildfire','seismic','flood','heat','air_quality','custom']
 variable_id: Identifier
 unit: str=Field(min_length=1,max_length=100)
 direction: Literal['above','below']='above'
 aggregation: Literal['maximum','minimum','mean']='maximum'
 trigger_threshold: float
 release_threshold: float
 critical_threshold: float | None=None
 minimum_samples: int=Field(default=1,ge=1,le=MAX_SAMPLES)
 minimum_duration_seconds: int=Field(default=0,ge=0,le=2592000)
 max_age_seconds: int=Field(default=3600,ge=1,le=2592000)
 region: Coverage | None=None
 @model_validator(mode='after')
 def ordered(self):
  if self.direction=='above':
   if self.release_threshold>=self.trigger_threshold or (self.critical_threshold is not None and self.critical_threshold<self.trigger_threshold):raise ValueError('above thresholds must be release < trigger <= critical')
  elif self.release_threshold<=self.trigger_threshold or (self.critical_threshold is not None and self.critical_threshold>self.trigger_threshold):raise ValueError('below thresholds must be critical <= trigger < release')
  return self
class Policy(Contract):
 dataset_id: Identifier
 lookback_seconds: int=Field(default=3600,ge=1,le=2592000)
 rules: list[Rule]=Field(min_length=1,max_length=20)
 @model_validator(mode='after')
 def unique(self):
  if len({r.id for r in self.rules})!=len(self.rules):raise ValueError('duplicate detection rule')
  if any(r.minimum_duration_seconds>self.lookback_seconds for r in self.rules):raise ValueError('persistence exceeds window')
  return self
class PolicyChange(Contract):
 expected_revision: int | None=Field(default=None,ge=1)
 policy: Policy
class EventChange(Contract):
 expected_revision: int=Field(ge=1)
 reason: str=Field(min_length=1,max_length=1000)
 @model_validator(mode='after')
 def nonblank(self):
  if not self.reason.strip():raise ValueError('reason required')
  return self
class Conflict(Exception):pass

def get_policy(connection,dataset):
 row=connection.execute('SELECT p.revision,p.definition FROM matrix_db.hazard_policies p JOIN matrix_db.hazard_active a USING(dataset_id,revision) WHERE dataset_id=%s',(dataset,)).fetchone()
 if row is None:raise KeyError(dataset)
 return row[0],Policy.model_validate(row[1])

def rows(connection,sql,params):
 cursor=connection.execute(sql,params);names=[c.name for c in cursor.description]
 return [dict(zip(names,row)) for row in cursor.fetchall()]

def feed(connection,event,kind,actor,reason,at):
 connection.execute('INSERT INTO matrix_db.hazard_feed(event_id,dataset_id,kind,severity,rule_id,actor,reason,at,assessment_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
  (event['id'],event['dataset_id'],kind,event['severity'],event['rule_id'],actor,reason,at,event['assessment_id']))

def register(connection,change,actor):
 policy=change.policy;connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(policy.dataset_id,))
 dataset=PostgresStore(connection).get(policy.dataset_id).dataset
 if dataset.origin!='observation':raise ValueError('hazard detection requires observation/reanalysis data, not forecasts or simulations')
 definitions={v.id:v for v in dataset.variables}
 for rule in policy.rules:
  if rule.variable_id not in definitions or definitions[rule.variable_id].unit!=rule.unit:raise ValueError('detection variable/unit mismatch')
 try:revision,active=get_policy(connection,policy.dataset_id)
 except KeyError:revision=None;active=None
 if revision!=change.expected_revision:raise Conflict()
 if active and active==policy:return {'revision':revision}
 revision=(revision or 0)+1
 connection.execute('INSERT INTO matrix_db.hazard_policies VALUES (%s,%s,%s)',(policy.dataset_id,revision,Jsonb(policy.model_dump(mode='json'))))
 connection.execute('INSERT INTO matrix_db.hazard_active VALUES (%s,%s) ON CONFLICT(dataset_id) DO UPDATE SET revision=EXCLUDED.revision',(policy.dataset_id,revision))
 current=rows(connection,"SELECT * FROM matrix_db.hazard_events WHERE dataset_id=%s AND status='active' FOR UPDATE",(policy.dataset_id,))
 now=datetime.now(UTC)
 for event in current:
  connection.execute("UPDATE matrix_db.hazard_events SET status='superseded',revision=revision+1,reason='policy superseded' WHERE id=%s",(event['id'],))
  feed(connection,event,'policy_superseded',actor,'policy superseded',now)
 return {'revision':revision}

def exceeds(rule,value,threshold):return value>=threshold if rule.direction=='above' else value<=threshold

def bounds(samples):
 if not samples:return None
 lons=sorted(set((s.longitude+180)%360-180 for s in samples))
 if len(lons)==1:west=east=lons[0]
 else:
  gaps=[((lons[(i+1)%len(lons)]-lon)%360,i) for i,lon in enumerate(lons)]
  _,index=max(gaps);west=lons[(index+1)%len(lons)];east=lons[index]
 return {'west':west,'east':east,'south':min(s.latitude for s in samples),'north':max(s.latitude for s in samples)}

def assess(policy,inputs,now):
 findings=[]
 for rule in policy.rules:
  selected=[(sample,revision,at) for sample,revision,at in inputs if sample.variable_id==rule.variable_id and sample.value is not None and sample.quality!='bad' and (rule.region is None or rule.region.contains(sample.longitude,sample.latitude))]
  samples=[r[0] for r in selected];metric=None;state='unknown';severity='warning';matched=[]
  if samples:
   values=[sample.value for sample in samples]
   metric=max(values) if rule.aggregation=='maximum' else min(values) if rule.aggregation=='minimum' else math.fsum(v/len(values) for v in values)
   matched=[r for r in selected if exceeds(rule,r[0].value,rule.trigger_threshold)]
   fresh=(now-max(s.valid_time for s in samples)).total_seconds()<=rule.max_age_seconds
   span=(max(r[0].valid_time for r in matched)-min(r[0].valid_time for r in matched)).total_seconds() if matched else 0
   if fresh and len(samples)>=rule.minimum_samples:
    if exceeds(rule,metric,rule.trigger_threshold) and len(matched)>=rule.minimum_samples and span>=rule.minimum_duration_seconds:state='detected'
    elif (metric<=rule.release_threshold if rule.direction=='above' else metric>=rule.release_threshold):state='clear'
    else:state='holding'
   if rule.critical_threshold is not None and exceeds(rule,metric,rule.critical_threshold):severity='critical'
  support=[{'observation_id':r[0].id,'revision':r[1],'recorded_at':r[2].isoformat()} for r in matched]
  # All aggregate inputs matter (including below-threshold mean inputs); identity
  # depends on immutable revisions, not database timestamp display/timezone.
  fingerprint=hashlib.sha256(canonical([{'observation_id':r[0].id,'revision':r[1]} for r in sorted(selected,key=lambda row:row[0].id)]).encode()).hexdigest()
  representative=(max(samples,key=lambda s:s.value) if rule.direction=='above' else min(samples,key=lambda s:s.value)) if samples else None
  findings.append({'rule_id':rule.id,'name':rule.name,'hazard_type':rule.hazard_type,'state':state,'severity':severity,'metric':metric,'unit':rule.unit,
   'sample_count':len(samples),'support_count':len(matched),'support_hash':fingerprint,'evidence':support[:20],
   'last_valid_time':max(s.valid_time for s in samples).isoformat() if samples else None,'bbox':bounds(samples),
   'longitude':representative.longitude if representative else None,'latitude':representative.latitude if representative else None})
 return findings

def evaluate(connection,dataset,now=None):
 now=now or datetime.now(UTC)
 if now.tzinfo is None:raise ValueError('evaluation requires UTC-aware time')
 connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');connection.execute("SET LOCAL statement_timeout='30s'")
 connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset,))
 revision,policy=get_policy(connection,dataset)
 prior=connection.execute('SELECT max(evaluated_at) FROM matrix_db.hazard_assessments WHERE dataset_id=%s AND policy_revision=%s',(dataset,revision)).fetchone()[0]
 if prior and now<prior:raise ValueError('backdated live assessment is not allowed')
 start=now-timedelta(seconds=policy.lookback_seconds)
 raw=connection.execute('''SELECT record,revision,recorded_at FROM matrix_db.observations WHERE dataset_id=%s AND NOT is_withdrawn
  AND valid_time>=%s AND valid_time<=%s ORDER BY valid_time,id LIMIT %s''',(dataset,start,now,MAX_SAMPLES+1)).fetchall()
 if len(raw)>MAX_SAMPLES:raise ValueError('hazard sample budget exceeded')
 inputs=[(Observation.model_validate(r),rev,at) for r,rev,at in raw];findings=assess(policy,inputs,now);ident=uuid4()
 for finding in findings:
  if finding['state']=='detected':
   suppressed=connection.execute("SELECT 1 FROM matrix_db.hazard_events WHERE dataset_id=%s AND policy_revision=%s AND rule_id=%s AND status='retracted' AND support_hash=%s AND NOT EXISTS(SELECT 1 FROM matrix_db.hazard_events WHERE dataset_id=%s AND policy_revision=%s AND rule_id=%s AND status='active') LIMIT 1",(dataset,revision,finding['rule_id'],finding['support_hash'],dataset,revision,finding['rule_id'])).fetchone()
   if suppressed:finding['state']='suppressed'
 connection.execute('INSERT INTO matrix_db.hazard_assessments VALUES (%s,%s,%s,%s,%s,%s)',(ident,dataset,revision,now,start,Jsonb(findings)))
 for sample,rev,_ in inputs:connection.execute('INSERT INTO matrix_db.hazard_assessment_inputs VALUES (%s,%s,%s,%s)',(ident,dataset,sample.id,rev))
 for finding in findings:
  active=rows(connection,"SELECT * FROM matrix_db.hazard_events WHERE dataset_id=%s AND policy_revision=%s AND rule_id=%s AND status='active' FOR UPDATE",(dataset,revision,finding['rule_id']))
  event=active[0] if active else None;state=finding['state']
  if event is None and state=='detected':
   event_id=uuid4()
   connection.execute("""INSERT INTO matrix_db.hazard_events(id,dataset_id,policy_revision,rule_id,hazard_type,severity,status,detection_state,opened_at,evaluated_at,last_valid_time,reason,metric,longitude,latitude,bbox,support_hash,assessment_id)
    VALUES (%s,%s,%s,%s,%s,%s,'active',%s,%s,%s,%s,'threshold detected',%s,%s,%s,%s,%s,%s)""",
    (event_id,dataset,revision,finding['rule_id'],finding['hazard_type'],finding['severity'],state,now,now,finding['last_valid_time'],finding['metric'],finding['longitude'],finding['latitude'],Jsonb(finding['bbox']),finding['support_hash'],ident))
   event=rows(connection,'SELECT * FROM matrix_db.hazard_events WHERE id=%s',(event_id,))[0];feed(connection,event,'opened','hazard-detector','threshold detected',now)
  elif event:
   status='resolved' if state=='clear' else 'active';severity=finding['severity'] if state=='detected' else event['severity']
   changed=state!=event['detection_state'] or finding['support_hash']!=event['support_hash'] or (finding['metric'] is not None and finding['metric']!=event['metric'])
   kind='resolved' if status=='resolved' else 'severity_changed' if severity!=event['severity'] else 'updated' if changed else None
   reason='threshold cleared' if status=='resolved' else 'insufficient or stale input' if state=='unknown' else 'assessment updated'
   connection.execute('''UPDATE matrix_db.hazard_events SET status=%s,detection_state=%s,severity=%s,revision=revision+1,evaluated_at=%s,
    last_valid_time=coalesce(%s,last_valid_time),metric=coalesce(%s,metric),longitude=coalesce(%s,longitude),latitude=coalesce(%s,latitude),
    bbox=coalesce(%s,bbox),support_hash=%s,assessment_id=%s,reason=%s WHERE id=%s''',
    (status,state,severity,now,finding['last_valid_time'],finding['metric'],finding['longitude'],finding['latitude'],Jsonb(finding['bbox']) if finding['bbox'] else None,finding['support_hash'],ident,reason,event['id']))
   if kind:
    event['severity']=severity;event['assessment_id']=ident;feed(connection,event,kind,'hazard-detector',reason,now)
 return {'id':str(ident),'dataset_id':dataset,'policy_revision':revision,'evaluated_at':now,'findings':findings}

def change_event(connection,dataset,ident,change,actor,*,retract=False):
 connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR UPDATE',(dataset,))
 items=rows(connection,'SELECT * FROM matrix_db.hazard_events WHERE id=%s AND dataset_id=%s FOR UPDATE',(UUID(str(ident)),dataset))
 if not items:raise KeyError(str(ident))
 event=items[0]
 if event['revision']!=change.expected_revision:raise Conflict()
 if event['status']!='active':raise ValueError('event is terminal')
 if not retract and event['acknowledged_at'] is not None:return {'id':str(ident),'revision':event['revision']}
 now=datetime.now(UTC)
 connection.execute("UPDATE matrix_db.hazard_events SET status=%s,acknowledged_at=CASE WHEN %s THEN acknowledged_at ELSE %s END,revision=revision+1,reason=%s WHERE id=%s",
  ('retracted' if retract else 'active',retract,now,change.reason,event['id']))
 feed(connection,event,'retracted' if retract else 'acknowledged',actor,change.reason,now)
 return {'id':str(ident),'revision':event['revision']+1}

def listing(connection,dataset,*,after=None,limit=50,status='all'):
 PostgresStore(connection).get(dataset)
 items=rows(connection,"SELECT * FROM matrix_db.hazard_events WHERE dataset_id=%s AND (%s='all' OR status=%s) AND (%s::uuid IS NULL OR id>%s) ORDER BY id LIMIT %s",(dataset,status,status,after,after,limit+1))
 return {'items':items[:limit],'next_cursor':str(items[limit-1]['id']) if len(items)>limit else None}

def detail(connection,dataset,ident):
 items=rows(connection,'SELECT * FROM matrix_db.hazard_events WHERE dataset_id=%s AND id=%s',(dataset,ident))
 if not items:raise KeyError(str(ident))
 event=items[0]
 assessment=rows(connection,'SELECT id,policy_revision,evaluated_at,window_start,findings FROM matrix_db.hazard_assessments WHERE id=%s',(event['assessment_id'],))[0]
 assessment['findings']=[f for f in assessment['findings'] if f['rule_id']==event['rule_id']]
 history=rows(connection,'SELECT sequence,kind,severity,actor,reason,at,assessment_id FROM matrix_db.hazard_feed WHERE event_id=%s ORDER BY sequence DESC LIMIT 100',(ident,))
 policy=connection.execute('SELECT definition FROM matrix_db.hazard_policies WHERE dataset_id=%s AND revision=%s',(dataset,event['policy_revision'])).fetchone()[0]
 return {'event':event,'policy':policy,'assessment':assessment,'history':history,'history_limit':100}

def main():
 import argparse,json,time
 from pathlib import Path
 from .storage import connect
 parser=argparse.ArgumentParser(description='Run configured analytical signal detection')
 parser.add_argument('--policy');parser.add_argument('--dataset');parser.add_argument('--once',action='store_true');parser.add_argument('--interval',type=int,default=300);args=parser.parse_args()
 try:
  if args.policy:
   policy=Policy.model_validate_json(Path(args.policy).read_text());args.dataset=policy.dataset_id
   with connect() as connection:
    try:revision,_=get_policy(connection,args.dataset)
    except KeyError:revision=None
    register(connection,PolicyChange(policy=policy,expected_revision=revision),'operator-cli')
  if not args.dataset or args.interval<60:raise ValueError('dataset and valid interval required')
  while True:
   with connect() as connection:result=evaluate(connection,args.dataset)
   print(json.dumps(result,default=str),flush=True)
   if args.once:return
   time.sleep(args.interval)
 except Exception:parser.exit(1,'hazard detection unavailable; check policy, data budget and database\n')

if __name__=='__main__':main()

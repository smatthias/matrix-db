"""Explicit revision writes and bounded point time-series/playback operations."""
from datetime import datetime, timedelta
import math
from typing import Annotated, Literal
from pydantic import Field, StrictInt, field_validator, model_validator
from psycopg.types.json import Jsonb
from .catalogue import CatalogueEntry, TimeCoverage
from .contracts import Contract, Identifier, Observation, VerticalCoordinate
from .queries import ObservationQuery, Point, VerticalRange, Match, build_query

class Change(Contract):
    expected_revision: Annotated[StrictInt, Field(ge=1)]
    reason: Annotated[str, Field(min_length=1,max_length=1000)]
    actor: Annotated[str, Field(min_length=1,max_length=200)]

    @field_validator('reason','actor')
    @classmethod
    def not_blank(cls,value):
        if not value.strip(): raise ValueError('reason and actor must not be blank')
        return value

class RevisionRequest(Change):
    observation: Observation

class WithdrawalRequest(Change):
    dataset_id: Identifier
    observation_id: Identifier


def revise(connection, observation, *, expected_revision, reason, actor):
    change=Change(expected_revision=expected_revision,reason=reason,actor=actor)
    observation=Observation.model_validate_json(observation.model_dump_json())
    with connection.transaction():
        row=connection.execute('SELECT entry FROM matrix_db.datasets WHERE id=%s FOR SHARE',(observation.dataset_id,)).fetchone()
        if row is None: raise KeyError(observation.dataset_id)
        observation.validate_against(CatalogueEntry.model_validate(row[0]).dataset)
        current=connection.execute('SELECT revision FROM matrix_db.observations WHERE dataset_id=%s AND id=%s FOR UPDATE',
                                   (observation.dataset_id,observation.id)).fetchone()
        if current is None: raise KeyError(observation.id)
        if current[0]!=change.expected_revision: raise ValueError('revision conflict; reload current revision before retrying')
        row=connection.execute('''UPDATE matrix_db.observations SET variable_id=%s,valid_time=%s,
            longitude=%s,latitude=%s,record=%s,is_withdrawn=false,change_reason=%s,changed_by=%s
            WHERE dataset_id=%s AND id=%s RETURNING revision,recorded_at''',
            (observation.variable_id,observation.valid_time,observation.longitude,observation.latitude,
             Jsonb(observation.model_dump(mode='json')),change.reason,change.actor,observation.dataset_id,observation.id)).fetchone()
    return {'revision':row[0],'recorded_at':row[1]}


def withdraw(connection, dataset_id, observation_id, *, expected_revision, reason, actor):
    change=Change(expected_revision=expected_revision,reason=reason,actor=actor)
    with connection.transaction():
        # Lock ordering matches correction/ingestion: dataset first, observation second.
        row=connection.execute('SELECT id FROM matrix_db.datasets WHERE id=%s FOR SHARE',(dataset_id,)).fetchone()
        if row is None: raise KeyError(dataset_id)
        current=connection.execute('SELECT revision,is_withdrawn FROM matrix_db.observations WHERE dataset_id=%s AND id=%s FOR UPDATE',
                                   (dataset_id,observation_id)).fetchone()
        if current is None: raise KeyError(observation_id)
        if current[0]!=change.expected_revision: raise ValueError('revision conflict; reload current revision before retrying')
        if current[1]: raise ValueError('observation is already withdrawn')
        row=connection.execute('''UPDATE matrix_db.observations SET is_withdrawn=true,change_reason=%s,changed_by=%s
            WHERE dataset_id=%s AND id=%s RETURNING revision,recorded_at''',
            (change.reason,change.actor,dataset_id,observation_id)).fetchone()
    return {'revision':row[0],'recorded_at':row[1]}


def history(connection,dataset_id,observation_id,*,limit=100,before_revision=None):
    if type(limit) is not int or not 1<=limit<=1000: raise ValueError('history limit must be 1–1000')
    if before_revision is not None and (type(before_revision) is not int or before_revision<1):
        raise ValueError('before_revision must be a positive integer')
    cursor=connection.execute('''SELECT revision,recorded_at,record,is_withdrawn,change_reason,changed_by,import_job_id
        FROM matrix_db.observation_revisions WHERE dataset_id=%s AND id=%s
        AND (%s::integer IS NULL OR revision<%s) ORDER BY revision DESC LIMIT %s''',
        (dataset_id,observation_id,before_revision,before_revision,limit))
    names=[column.name for column in cursor.description]
    return [dict(zip(names,row)) for row in cursor.fetchall()]

class PointSelection(Contract):
    dataset_id: Identifier
    variable_id: Identifier
    point: Point
    vertical: VerticalCoordinate | None = None
    as_of: datetime | None = None
    forecast_issue_time: datetime | None = None
    forecast: Literal['all','latest']='all'
    _explicit = field_validator('as_of','forecast_issue_time',mode='before')(Observation.explicit_timestamp.__func__)
    _utc = field_validator('as_of','forecast_issue_time')(Observation.utc_time.__func__)

    @model_validator(mode='after')
    def forecast_mode(self):
        if self.forecast=='latest' and self.forecast_issue_time is not None:
            raise ValueError('select an issue time or latest forecast, not both')
        return self

class SeriesQuery(PointSelection):
    time: TimeCoverage
    bucket_seconds: Annotated[StrictInt, Field(ge=1,le=31536000)]

    @model_validator(mode='after')
    def bounded_buckets(self):
        duration=(self.time.end-self.time.start).total_seconds()
        if duration<=0: raise ValueError('series interval must have positive duration')
        if math.ceil(duration/self.bucket_seconds)>1000: raise ValueError('series exceeds 1000 buckets')
        return self

class PlaybackQuery(PointSelection):
    at: datetime
    tolerance_seconds: float = Field(ge=0,le=604800)
    _explicit_at = field_validator('at',mode='before')(Observation.explicit_timestamp.__func__)
    _utc_at = field_validator('at')(Observation.utc_time.__func__)


def selection_sql(query,time):
    vertical=None
    if query.vertical is not None:
        v=query.vertical
        vertical=VerticalRange(minimum=v.value,maximum=v.value,reference=v.reference,positive=v.positive,unit=v.unit)
    selection=ObservationQuery(dataset_id=query.dataset_id,variable_id=query.variable_id,
        bbox={'west':query.point.longitude,'east':query.point.longitude,'south':query.point.latitude,'north':query.point.latitude},
        time=time,as_of=query.as_of,forecast_issue_time=query.forecast_issue_time,forecast=query.forecast,vertical=vertical)
    sql,params=build_query(selection,paginate=False)
    # Missing vertical selection explicitly means unlevelled data, never mixed altitude/depth series.
    if query.vertical is None:
        sql=f"SELECT * FROM ({sql}) unlevelled WHERE record->'vertical' IS NULL OR record->'vertical'='null'::jsonb"
    return sql,params


def validate_selection(connection,query):
    from .storage import PostgresStore
    dataset=PostgresStore(connection).get(query.dataset_id).dataset
    variable=next((v for v in dataset.variables if v.id==query.variable_id),None)
    if variable is None: raise ValueError('unknown series variable')
    if dataset.origin=='forecast' and query.forecast=='all' and query.forecast_issue_time is None:
        raise ValueError('forecast series/playback requires latest mode or a specific issue time')
    return variable


def series(connection,query: SeriesQuery):
    query=SeriesQuery.model_validate_json(query.model_dump_json())
    variable=validate_selection(connection,query)
    sql,params=selection_sql(query,query.time)
    params.update(bucket=query.bucket_seconds,anchor=query.time.start,series_end=query.time.end,
                  bucket_count=math.ceil((query.time.end-query.time.start).total_seconds()/query.bucket_seconds))
    result=connection.execute(f'''WITH selected AS ({sql}), stats AS (
        SELECT floor(extract(epoch FROM (valid_time-%(anchor)s::timestamptz))/%(bucket)s)::integer AS bucket_id,
            count(*) AS record_count,count((record->>'value')::double precision) AS sample_count,
            avg((record->>'value')::double precision) AS mean,min((record->>'value')::double precision) AS minimum,
            max((record->>'value')::double precision) AS maximum,sum((record->>'value')::double precision) AS total
        FROM selected WHERE valid_time<%(series_end)s GROUP BY bucket_id)
        SELECT %(anchor)s::timestamptz + bucket_id * %(bucket)s * interval '1 second' AS start,
            least(%(anchor)s::timestamptz+(bucket_id+1)*%(bucket)s*interval '1 second',%(series_end)s) AS end,
            coalesce(record_count,0),coalesce(sample_count,0),mean,minimum,maximum,total
        FROM generate_series(0,%(bucket_count)s-1) AS buckets(bucket_id)
        LEFT JOIN stats USING(bucket_id) ORDER BY bucket_id''',params).fetchall()
    buckets=[{'start':r[0],'end':r[1],'record_count':r[2],'sample_count':r[3],
             'missing':r[3]==0,'mean':r[4],'minimum':r[5],'maximum':r[6],'sum':r[7]} for r in result]
    return {'dataset_id':query.dataset_id,'variable_id':query.variable_id,'unit':variable.unit,
            'interval':'[start,end)','bucket_seconds':query.bucket_seconds,'buckets':buckets}


def playback(connection,query: PlaybackQuery):
    query=PlaybackQuery.model_validate_json(query.model_dump_json())
    validate_selection(connection,query)
    tolerance=timedelta(seconds=query.tolerance_seconds)
    window=TimeCoverage(start=query.at-tolerance,end=query.at+tolerance)
    sql,params=selection_sql(query,window)
    params['at']=query.at
    row=connection.execute(f'''SELECT record,distance_metres,revision,recorded_at,
        extract(epoch FROM (valid_time-%(at)s::timestamptz))::double precision AS offset_seconds
        FROM ({sql}) samples ORDER BY abs(extract(epoch FROM (valid_time-%(at)s::timestamptz))),valid_time,dataset_id,id LIMIT 1''',params).fetchone()
    if row is None:return {'match':None,'offset_seconds':None}
    return {'match':Match(observation=Observation.model_validate(row[0]),distance_metres=row[1],revision=row[2],recorded_at=row[3]).model_dump(mode='json'),
            'offset_seconds':row[4]}

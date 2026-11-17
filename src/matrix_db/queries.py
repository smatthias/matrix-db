"""Bounded WGS84 spatial observation queries with filter-bound keyset cursors."""
import base64
import hashlib
import json
import math
from typing import Annotated, Literal
from pydantic import Field, StrictInt, model_validator
from .catalogue import TimeCoverage
from .contracts import Contract, Coverage, Identifier, Observation

class Point(Contract):
    longitude: float = Field(ge=-180, le=180)
    latitude: float = Field(ge=-90, le=90)

class DistanceFilter(Point):
    radius_metres: float = Field(ge=0, le=20040000)

class VerticalRange(Contract):
    minimum: float
    maximum: float
    reference: Annotated[str, Field(min_length=1)]
    positive: Literal['up','down']
    unit: Literal['m'] = 'm'

    @model_validator(mode='after')
    def ordered(self):
        if self.minimum>self.maximum: raise ValueError('vertical bounds must be ordered')
        return self

class Polygon(Contract):
    type: Literal['Polygon','MultiPolygon']
    coordinates: list

    @model_validator(mode='after')
    def coordinates_valid(self):
        polygons=[self.coordinates] if self.type=='Polygon' else self.coordinates
        vertices=0
        if not polygons: raise ValueError('polygon must not be empty')
        for polygon in polygons:
            if not isinstance(polygon,list) or not polygon: raise ValueError('polygon requires rings')
            for ring in polygon:
                if not isinstance(ring,list) or len(ring)<4: raise ValueError('rings require at least four positions')
                if ring[0]!=ring[-1]: raise ValueError('rings must be closed')
                for position in ring:
                    if not isinstance(position,list) or len(position)!=2:
                        raise ValueError('positions require longitude and latitude only')
                    if any(type(n) not in (int,float) or not math.isfinite(n) for n in position):
                        raise ValueError('positions must contain finite numbers')
                    Point(longitude=position[0],latitude=position[1])
                    vertices+=1
                if any(abs(a[0]-b[0])>180 for a,b in zip(ring,ring[1:])):
                    raise ValueError('split antimeridian-crossing polygons into a MultiPolygon')
        if vertices>10000: raise ValueError('polygon exceeds 10000 vertices')
        return self

class ObservationQuery(Contract):
    dataset_id: Identifier | None = None
    variable_id: Identifier | None = None
    time: TimeCoverage | None = None
    vertical: VerticalRange | None = None
    bbox: Coverage | None = None
    polygon: Polygon | None = None
    distance: DistanceFilter | None = None
    order: Literal['time','nearest'] = 'time'
    limit: Annotated[StrictInt, Field(ge=1,le=1000)] = 100
    cursor: Annotated[str, Field(max_length=4096)] | None = None

    @model_validator(mode='after')
    def nearest_requires_radius(self):
        if self.order=='nearest' and self.distance is None:
            raise ValueError('nearest ordering requires a distance filter with maximum radius')
        return self

class Match(Contract):
    observation: Observation
    distance_metres: float | None = None

class Page(Contract):
    items: list[Match]
    next_cursor: str | None = None


def fingerprint(query):
    definition=query.model_dump(mode='json',exclude={'limit','cursor'})
    return hashlib.sha256(json.dumps(definition,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def decode_cursor(query):
    if query.cursor is None: return None
    try:
        payload=json.loads(base64.b64decode(query.cursor,altchars=b'-_',validate=True))
        if not isinstance(payload,dict) or payload.get('query')!=fingerprint(query):
            raise ValueError('cursor belongs to different filters')
        if set(payload)!= {'query','time','dataset','id','distance'}:
            raise ValueError('unexpected cursor fields')
        # Reuse strict timestamp and ID validation; cursor values never become SQL text.
        time=TimeCoverage(start=payload['time'],end=payload['time']).start
        for key in ('dataset','id'):
            if not isinstance(payload[key],str) or not payload[key]: raise ValueError('invalid cursor identity')
        distance=payload['distance']
        if query.order=='nearest' and (type(distance) not in (int,float) or not math.isfinite(distance) or distance<0):
            raise ValueError('invalid cursor distance')
        return (distance,time,payload['dataset'],payload['id'])
    except (ValueError,TypeError,KeyError,UnicodeError) as error:
        raise ValueError('invalid cursor or cursor does not match query') from error


def build_query(query):
    """Return parameterized SQL; only fixed fragments are composed."""
    params={}
    clauses=[]
    for field,column in [('dataset_id','dataset_id'),('variable_id','variable_id')]:
        value=getattr(query,field)
        if value is not None:
            clauses.append(f'o.{column}=%({field})s');params[field]=value
    if query.time:
        clauses.append('o.valid_time BETWEEN %(start)s AND %(end)s')
        params.update(start=query.time.start,end=query.time.end)
    if query.vertical:
        clauses.extend(["o.record->'vertical'->>'reference'=%(vertical_reference)s",
                        "o.record->'vertical'->>'positive'=%(vertical_positive)s",
                        "o.record->'vertical'->>'unit'='m'",
                        "(o.record->'vertical'->>'value')::double precision BETWEEN %(vertical_min)s AND %(vertical_max)s"])
        params.update(vertical_reference=query.vertical.reference,vertical_positive=query.vertical.positive,
                      vertical_min=query.vertical.minimum,vertical_max=query.vertical.maximum)
    if query.bbox:
        box=query.bbox
        params.update(west=box.west,east=box.east,south=box.south,north=box.north)
        if box.west<=box.east:
            clauses.append('(o.location && ST_MakeEnvelope(%(west)s,%(south)s,%(east)s,%(north)s,4326))')
        else:
            clauses.append('''((o.location && ST_MakeEnvelope(%(west)s,%(south)s,180,%(north)s,4326))
                OR (o.location && ST_MakeEnvelope(-180,%(south)s,%(east)s,%(north)s,4326)))''')
    if query.polygon:
        params['polygon']=query.polygon.model_dump_json()
        clauses.append('ST_Covers(ST_SetSRID(ST_GeomFromGeoJSON(%(polygon)s),4326),o.location)')
    distance_sql='NULL::double precision'
    if query.distance:
        params.update(longitude=query.distance.longitude,latitude=query.distance.latitude,radius=query.distance.radius_metres)
        point='ST_SetSRID(ST_MakePoint(%(longitude)s,%(latitude)s),4326)::geography'
        clauses.append(f'ST_DWithin(o.location::geography,{point},%(radius)s)')
        distance_sql=f'ST_Distance(o.location::geography,{point})'
    cursor=decode_cursor(query)
    where=' AND '.join(clauses) if clauses else 'TRUE'
    order='valid_time,dataset_id,id'
    after='TRUE'
    if query.order=='nearest': order='distance_metres,'+order
    if cursor:
        params.update(last_distance=cursor[0],last_time=cursor[1],last_dataset=cursor[2],last_id=cursor[3])
        if query.order=='nearest':
            after='(distance_metres,valid_time,dataset_id,id)>(%(last_distance)s,%(last_time)s,%(last_dataset)s,%(last_id)s)'
        else: after='(valid_time,dataset_id,id)>(%(last_time)s,%(last_dataset)s,%(last_id)s)'
    params['limit']=query.limit+1
    sql=f'''SELECT record,distance_metres,valid_time,dataset_id,id FROM (
        SELECT o.record,{distance_sql} AS distance_metres,o.valid_time,o.dataset_id,o.id
        FROM matrix_db.observations o WHERE {where}
    ) matches WHERE {after} ORDER BY {order} LIMIT %(limit)s'''
    return sql,params


def query_observations(connection, query: ObservationQuery):
    query=ObservationQuery.model_validate_json(query.model_dump_json())
    sql,params=build_query(query)
    if query.polygon:
        valid,reason=connection.execute('''SELECT ST_IsValid(g),ST_IsValidReason(g)
            FROM (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),4326) AS g) shape''',
            (query.polygon.model_dump_json(),)).fetchone()
        if not valid: raise ValueError(f'invalid polygon: {reason}')
    rows=connection.execute(sql,params).fetchall()
    selected=rows[:query.limit]
    next_cursor=None
    if len(rows)>query.limit:
        last=selected[-1]
        payload={'query':fingerprint(query),'distance':last[1],'time':last[2].isoformat(),'dataset':last[3],'id':last[4]}
        next_cursor=base64.urlsafe_b64encode(json.dumps(payload,separators=(',',':')).encode()).decode()
    return Page(items=[Match(observation=Observation.model_validate(row[0]),distance_metres=row[1]) for row in selected],
                next_cursor=next_cursor)

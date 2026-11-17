"""Bounded provider readers. Endpoints and query templates are fixed, never user URLs."""
import csv
from datetime import datetime,timedelta,timezone
from hashlib import sha256
import io
import json
import os
import time
from email.utils import parsedate_to_datetime
from typing import Literal
import httpx
from pydantic import Field,model_validator,field_validator
from .catalogue import CatalogueEntry
from .contracts import Contract,Identifier,Observation

UTC=timezone.utc
MAX_BYTES=8*1024*1024
MAX_RECORDS=10000
class ProviderError(Exception):
    def __init__(self,code,retry_after=0):
        self.code=code;self.retry_after=retry_after;super().__init__(code)

class SourceConfig(Contract):
    id: Identifier
    provider: Literal['usgs','firms','open_meteo','sentinel_ndvi','overpass']
    entry: CatalogueEntry
    start_at: datetime
    interval_seconds: int = Field(default=3600,ge=60,le=2592000)
    overlap_seconds: int = Field(default=3600,ge=0,le=2592000)
    window_days: int = Field(default=1,ge=1,le=10)
    credential_env: str | None = Field(default=None,pattern=r'^MATRIX_DB_PROVIDER_[A-Z0-9_]+$')
    latitude: float | None = Field(default=None,ge=-90,le=90)
    longitude: float | None = Field(default=None,ge=-180,le=180)
    _explicit=field_validator('start_at',mode='before')(Observation.explicit_timestamp.__func__)
    _utc=field_validator('start_at')(Observation.utc_time.__func__)

    @model_validator(mode='after')
    def compatible(self):
        if self.entry.dataset.origin!='observation':raise ValueError('provider datasets must use observation origin')
        box=self.entry.dataset.coverage
        if box.west>=box.east or box.south>=box.north:raise ValueError('provider bounds must have positive area and not cross antimeridian')
        if self.provider in ('firms','sentinel_ndvi') and not self.credential_env:raise ValueError('credential environment name required')
        if self.overlap_seconds>=self.window_days*86400:raise ValueError('overlap must be shorter than fetch window')
        if self.provider=='sentinel_ndvi' and (self.start_at.hour or self.start_at.minute or self.start_at.second or self.start_at.microsecond or self.overlap_seconds%86400):raise ValueError('vegetation requires UTC day-aligned start and overlap')
        if self.provider=='firms' and self.window_days>9:raise ValueError('FIRMS windows must be at most nine days')
        if self.provider not in ('firms','sentinel_ndvi') and self.credential_env:raise ValueError('this provider does not use a credential')
        if self.provider=='open_meteo' and (self.latitude is None or self.longitude is None):raise ValueError('weather requires an explicit point')
        if self.latitude is not None and self.longitude is not None and not box.contains(self.longitude,self.latitude):raise ValueError('point outside coverage')
        return self

def instant(value):
    return Observation.utc_time(Observation.explicit_timestamp(datetime.fromisoformat(value.replace('Z','+00:00'))))

def identity(*values):return sha256(json.dumps(values,separators=(',',':')).encode()).hexdigest()

def credential(config):
    value=os.environ.get(config.credential_env or '')
    if not value:raise ProviderError('credential_missing')
    if any(c in value for c in '\r\n/\\?#') and config.provider=='firms':raise ProviderError('credential_invalid')
    return value

class Transport:
    """Retry read requests only; no redirects; bounded decoded body and backoff."""
    def __init__(self,client=None,sleep=time.sleep):
        self.client=client or httpx.Client(timeout=30,follow_redirects=False,trust_env=False)
        self.owned=client is None;self.sleep=sleep
    def close(self):
        if self.owned:self.client.close()
    def fetch(self,method,url,*,allow_empty=False,**kwargs):
        for attempt in range(3):
            try:
                with self.client.stream(method,url,follow_redirects=False,**kwargs) as response:
                    if response.status_code in (429,500,502,503,504):
                        if attempt==2:raise ProviderError('upstream_retry_exhausted')
                        retry=response.headers.get('Retry-After','')
                        delay=2**attempt
                        if retry.isdigit():delay=float(retry)
                        elif retry:
                            try:delay=max(0,(parsedate_to_datetime(retry)-datetime.now(UTC)).total_seconds())
                            except (ValueError,TypeError):pass
                        # Never retry earlier than a long upstream Retry-After: defer the poll.
                        if delay>30:raise ProviderError('upstream_rate_limited',retry_after=min(delay,2592000))
                        self.sleep(max(1,delay));continue
                    if response.status_code==204 and allow_empty:return None
                    if response.status_code in (401,403):raise ProviderError('upstream_unauthorized')
                    if response.status_code!=200:raise ProviderError('upstream_http_error')
                    data=bytearray()
                    for chunk in response.iter_bytes(chunk_size=65536):
                        data.extend(chunk)
                        if len(data)>MAX_BYTES:raise ProviderError('response_too_large')
                    return bytes(data)
            except httpx.TransportError:
                if attempt==2:raise ProviderError('upstream_unavailable') from None
                self.sleep(2**attempt)
        raise ProviderError('upstream_unavailable')

def record(config,now,key,variable,lon,lat,at,value,unit,reference,quality='unknown'):
    return Observation(id=identity(key,variable),dataset_id=config.entry.dataset.id,variable_id=variable,
        longitude=lon,latitude=lat,valid_time=at,ingested_at=now,value=value,unit=unit,
        source_reference=reference,quality=quality).validate_against(config.entry.dataset)

# Cloud/no-data classes are excluded; NDVI is an area mean, not a point sample.
NDVI_SCRIPT='''//VERSION=3
function setup(){return {input:[{bands:["B04","B08","SCL","dataMask"]}],output:[{id:"ndvi",bands:1},{id:"dataMask",bands:1}]};}
function evaluatePixel(s){let d=s.B08+s.B04;let valid=s.dataMask && d>0 && ![0,1,3,8,9,10,11].includes(s.SCL);return {ndvi:[d>0?(s.B08-s.B04)/d:0],dataMask:[valid?1:0]};}
'''

def fetch_records(config,start,end,now,transport):
    """Return validated records plus explicit upstream withdrawals; fail whole response."""
    box=config.entry.dataset.coverage
    bounds=[box.west,box.south,box.east,box.north]
    removed=[];records=[]
    def add(*args,**kwargs):
        records.append(record(config,now,*args,**kwargs))
        if len(records)>MAX_RECORDS:raise ProviderError('record_limit')
    try:
        if config.provider=='usgs':
            raw=transport.fetch('GET','https://earthquake.usgs.gov/fdsnws/event/1/query',allow_empty=True,params={
                'format':'geojson','starttime':start.isoformat(),'endtime':end.isoformat(),'orderby':'time-asc','limit':MAX_RECORDS,
                'minlongitude':box.west,'maxlongitude':box.east,'minlatitude':box.south,'maxlatitude':box.north,'includedeleted':'true','nodata':204})
            payload=json.loads(raw) if raw is not None else {'type':'FeatureCollection','features':[]}
            if payload['type']!='FeatureCollection' or len(payload['features'])>=MAX_RECORDS:raise ProviderError('incomplete_response')
            for feature in payload['features']:
                key=feature['id'];props=feature['properties']
                if props.get('status')=='deleted':removed.append(identity(key,'magnitude'));continue
                lon,lat,*_=feature['geometry']['coordinates']
                at=datetime.fromtimestamp(props['time']/1000,UTC)
                if start<=at<=end:add(key,'magnitude',lon,lat,at,props['mag'],'1',f'https://earthquake.usgs.gov/earthquakes/eventpage/{key}')
        elif config.provider=='firms':
            # Area API dates select whole UTC days, so round outwards then filter.
            days=(end.date()-start.date()).days+1
            if days>10:raise ProviderError('window_too_large')
            url='https://firms.modaps.eosdis.nasa.gov/api/area/csv/'+credential(config)+'/VIIRS_SNPP_NRT/'+','.join(map(str,bounds))+f'/{days}/{start.date()}'
            reader=csv.DictReader(io.StringIO(transport.fetch('GET',url).decode('utf-8-sig')))
            required={'latitude','longitude','acq_date','acq_time','frp','confidence'}
            if not required.issubset(reader.fieldnames or []):raise ProviderError('upstream_schema')
            for row in reader:
                at=datetime.strptime(row['acq_date']+' '+row['acq_time'].zfill(4),'%Y-%m-%d %H%M').replace(tzinfo=UTC)
                if start<=at<=end:
                    lon=float(row['longitude']);lat=float(row['latitude'])
                    key=('VIIRS_SNPP_NRT',row['acq_date'],row['acq_time'].zfill(4),lon,lat)
                    add(key,'frp',lon,lat,at,float(row['frp']),'MW','https://firms.modaps.eosdis.nasa.gov/',quality='suspect' if row['confidence']=='l' else 'unknown')
        elif config.provider=='open_meteo':
            payload=json.loads(transport.fetch('GET','https://archive-api.open-meteo.com/v1/archive',params={
                'latitude':config.latitude,'longitude':config.longitude,'start_date':str(start.date()),'end_date':str(end.date()),
                'hourly':'temperature_2m,relative_humidity_2m,wind_speed_10m','timezone':'UTC','timeformat':'unixtime','wind_speed_unit':'ms'}))
            hourly=payload['hourly'];times=hourly['time']
            mapping=[('temperature_2m','temperature','°C','degC'),('relative_humidity_2m','humidity','%','%'),('wind_speed_10m','wind','m/s','m/s')]
            for key,variable,upstream,unit in mapping:
                if payload['hourly_units'][key]!=upstream or len(hourly[key])!=len(times):raise ProviderError('upstream_schema')
                for at,value in zip(times,hourly[key]):
                    at=datetime.fromtimestamp(at,UTC)
                    if start<=at<=end:add(('reanalysis',at.isoformat()),variable,payload['longitude'],payload['latitude'],at,value,unit,'https://open-meteo.com/en/docs/historical-weather-api')
        elif config.provider=='sentinel_ndvi':
            payload=json.loads(transport.fetch('POST','https://services.sentinel-hub.com/api/v1/statistics',
                headers={'Authorization':'Bearer '+credential(config)},json={
                    'input':{'bounds':{'bbox':bounds,'properties':{'crs':'http://www.opengis.net/def/crs/EPSG/0/4326'}},'data':[{'type':'sentinel-2-l2a'}]},
                    'aggregation':{'timeRange':{'from':start.isoformat(),'to':end.isoformat()},'aggregationInterval':{'of':'P1D'},'width':256,'height':256,'evalscript':NDVI_SCRIPT}}))
            if payload['status']!='OK':raise ProviderError('incomplete_response')
            for item in payload['data']:
                at=instant(item['interval']['from']);until=instant(item['interval']['to'])
                if not start<=at<until<=end:raise ProviderError('upstream_schema')
                stats=item['outputs']['ndvi']['bands']['B0']['stats']
                count=stats['sampleCount'];missing=stats['noDataCount']
                if not 0<=missing<=count:raise ProviderError('upstream_schema')
                value=stats['mean'] if count>missing else None
                add(('area-mean',at.isoformat(),until.isoformat()),'ndvi',(box.west+box.east)/2,(box.south+box.north)/2,at,value,'1',
                    'https://services.sentinel-hub.com/api/v1/statistics#'+until.isoformat(),quality='unknown' if value is not None else 'bad')
        else:
            query=f'[out:json][timeout:20][maxsize:8388608];nwr["building"]({box.south},{box.west},{box.north},{box.east});out center;'
            payload=json.loads(transport.fetch('POST','https://overpass-api.de/api/interpreter',data={'data':query}))
            if payload.get('remark'):raise ProviderError('incomplete_response')
            at=instant(payload['osm3s']['timestamp_osm_base'])
            for element in payload['elements']:
                kind=element['type'];ident=element['id'];point=element if kind=='node' else element['center']
                if kind not in ('node','way','relation') or type(ident) is not int:raise ProviderError('upstream_schema')
                if box.contains(point['lon'],point['lat']):
                    add((kind,ident,at.isoformat()),'building_presence',point['lon'],point['lat'],at,1,'1',f'https://www.openstreetmap.org/{kind}/{ident}')
        # Duplicate upstream identities in a response are ambiguous: don't silently pick one.
        if len({r.id for r in records})!=len(records):raise ProviderError('duplicate_upstream_identity')
        return records,removed
    except ProviderError:raise
    except (ValueError,KeyError,TypeError,OverflowError,AttributeError):raise ProviderError('upstream_schema') from None

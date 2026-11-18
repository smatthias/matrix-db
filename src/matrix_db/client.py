"""Synchronous Python client; retries are limited to explicitly read-only operations."""
import time
from urllib.parse import urlparse,quote
import httpx

class MatrixDBError(Exception):
    def __init__(self,code,message,status=0,request_id=None,retry_after=None):
        super().__init__(message);self.code=code;self.status=status;self.request_id=request_id;self.retry_after=retry_after

class MatrixDBClient:
    def __init__(self,base_url,token,*,timeout=15,read_retries=1,transport=None):
        parsed=urlparse(base_url)
        if parsed.scheme not in ('http','https') or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError('base_url must be an HTTP(S) service URL without credentials or query parameters')
        if timeout<=0 or type(read_retries) is not int or not 0<=read_retries<=3:raise ValueError('invalid client timeout/retry settings')
        self.read_retries=read_retries
        self.http=httpx.Client(base_url=base_url.rstrip('/')+'/',headers={'Authorization':'Bearer '+token},timeout=timeout,transport=transport,follow_redirects=False)
    def close(self):self.http.close()
    def __enter__(self):return self
    def __exit__(self,*args):self.close()
    def request(self,method,path,payload=None,*,read_only=False,params=None):
        location=urlparse(path)
        if location.scheme or location.netloc:raise ValueError('relative API paths are required')
        for attempt in range(self.read_retries+1 if read_only else 1):
            try:
                response=self.http.request(method,path.lstrip('/'),json=payload,params=params)
                break
            except httpx.TransportError:
                if not read_only or attempt==self.read_retries:raise MatrixDBError('transport_error','Matrix DB request failed') from None
                time.sleep(.05*2**attempt)
        try:body=response.json()
        except ValueError:raise MatrixDBError('invalid_response','Matrix DB returned an invalid response',response.status_code) from None
        if not response.is_success:
            error=body.get('error',{}) if isinstance(body,dict) else {}
            retry=response.headers.get('Retry-After','')
            raise MatrixDBError(error.get('code','http_error'),error.get('message','Matrix DB request failed'),response.status_code,error.get('request_id'),int(retry) if retry.isdigit() and len(retry)<=5 else None)
        return body
    def datasets(self,*,limit=100,after=None):return self.request('GET','v1/datasets',params={'limit':limit,**({} if after is None else {'after':after})},read_only=True)
    def dataset(self,dataset_id):return self.request('GET','v1/datasets/'+quote(dataset_id,safe=''),read_only=True)
    def search(self,query):return self.request('POST','v1/datasets/search',query,read_only=True)
    def query(self,query):return self.request('POST','v1/observations/query',query,read_only=True)
    def pages(self,query):
        query=dict(query)
        while True:
            page=self.query(query);yield page
            if not page['next_cursor']:return
            query['cursor']=page['next_cursor']
    def subset(self,query):return self.request('POST','v1/arrays/subset',query,read_only=True)
    def series(self,query):return self.request('POST','v1/series',query,read_only=True)
    def playback(self,query):return self.request('POST','v1/playback',query,read_only=True)
    def ingest(self,batch):return self.request('POST','v1/observations',batch)
    def create_scenario(self,scenario):return self.request('POST','v1/scenarios',scenario)
    def create_run(self,run):return self.request('POST','v1/runs',run)
    def transition_run(self,change):return self.request('POST','v1/runs/transition',change)
    def attach_output(self,output):return self.request('POST','v1/runs/outputs',output)
    def submit_job(self,operation,payload):return self.request('POST','v1/jobs',{'operation':operation,'payload':payload})
    def job(self,ident):return self.request('GET','v1/jobs/'+quote(str(ident),safe=''),read_only=True)
    def cancel_job(self,ident):return self.request('POST','v1/jobs/'+quote(str(ident),safe='')+'/cancel')

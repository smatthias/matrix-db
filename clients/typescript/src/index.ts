export interface TimeRange { start: string; end: string }
export interface Bounds { west: number; south: number; east: number; north: number }
export interface ObservationQuery {
  dataset_id?: string; variable_id?: string; time?: TimeRange; as_of?: string;
  forecast?: 'all' | 'latest'; forecast_issue_time?: string; bbox?: Bounds;
  distance?: { longitude: number; latitude: number; radius_metres: number };
  polygon?: { type: 'Polygon' | 'MultiPolygon'; coordinates: number[][][] | number[][][][] };
  vertical?: {minimum: number; maximum: number; reference: string; positive: 'up'|'down'; unit?: 'm'};
  order?: 'time' | 'nearest'; limit?: number; cursor?: string;
}
export interface Observation {
  id: string; dataset_id: string; variable_id: string; longitude: number; latitude: number;
  valid_time: string; ingested_at: string; value: number | null; unit: string;
  quality: 'good' | 'suspect' | 'bad' | 'unknown'; source_reference: string;
}
export interface ObservationPage { items: { observation: Observation; distance_metres: number | null; revision: number; recorded_at: string }[]; next_cursor: string | null }
export interface SubsetQuery { version_id: string; variables?: string[]; slices?: Record<string,{start:number;stop:number}>; ranges?: Record<string,{minimum:number|string;maximum:number|string}> }
export type Json = string | number | boolean | null | Json[] | {[key:string]:Json};
export interface Job { id: string; status: 'queued'|'running'|'completed'|'failed'|'cancelled'; cancel_requested?: boolean; work_done?: number; result?: Json; error_code?: string|null }
export class MatrixDBError extends Error {
  constructor(public code:string, message:string, public status=0, public requestId?:string) { super(message); this.name='MatrixDBError'; }
}
export class MatrixDBClient {
  private base:string;
  constructor(base:string,private token:string,private options:{timeoutMs?:number;readRetries?:number;fetch?:typeof fetch}={}) {
    const url=new URL(base);
    if (!['http:','https:'].includes(url.protocol)||url.username||url.password||url.search||url.hash) throw new Error('Invalid service URL');
    const retries=options.readRetries??1;
    if (!Number.isInteger(retries)||retries<0||retries>3||(options.timeoutMs??15000)<=0) throw new Error('Invalid timeout/retry settings');
    this.base=base.replace(/\/$/,'');
  }
  async request<T>(method:string,path:string,payload?:unknown,signal?:AbortSignal,readOnly=false):Promise<T> {
    const attempts=readOnly?(this.options.readRetries??1)+1:1;
    for(let attempt=0;attempt<attempts;attempt++) {
      if(signal?.aborted) throw new MatrixDBError('aborted','Request cancelled');
      const controller=new AbortController();let timedOut=false;
      const abort=()=>controller.abort();signal?.addEventListener('abort',abort,{once:true});
      const timer=setTimeout(()=>{timedOut=true;controller.abort();},this.options.timeoutMs??15000);
      try {
        const response=await (this.options.fetch??fetch)(this.base+'/'+path.replace(/^\//,''),{
          method,redirect:'error',signal:controller.signal,
          headers:{Authorization:'Bearer '+this.token,'Content-Type':'application/json'},body:payload===undefined?undefined:JSON.stringify(payload)});
        let body:unknown;
        try {body=await response.json();} catch {throw new MatrixDBError('invalid_response','Invalid Matrix DB response',response.status);}
        if(!response.ok) {
          const error=(body as {error?:{code?:string;message?:string;request_id?:string}})?.error;
          throw new MatrixDBError(error?.code??'http_error',error?.message??'Matrix DB request failed',response.status,error?.request_id);
        }
        return body as T;
      } catch(error) {
        if(error instanceof MatrixDBError) throw error;
        if(signal?.aborted) throw new MatrixDBError('aborted','Request cancelled');
        if(!readOnly||attempt===attempts-1) throw new MatrixDBError(timedOut?'timeout':'transport_error','Matrix DB request failed');
      } finally {clearTimeout(timer);signal?.removeEventListener('abort',abort);}
    }
    throw new MatrixDBError('transport_error','Matrix DB request failed');
  }
  datasets(limit=100,after?:string,signal?:AbortSignal):Promise<{items:Json[];next_cursor:string|null}> {
    const query=new URLSearchParams({limit:String(limit)});if(after)query.set('after',after);
    return this.request('GET','v1/datasets?'+query,undefined,signal,true);
  }
  query(query:ObservationQuery,signal?:AbortSignal):Promise<ObservationPage> {return this.request('POST','v1/observations/query',query,signal,true);}
  async *pages(query:ObservationQuery,signal?:AbortSignal):AsyncGenerator<ObservationPage> {
    query={...query};while(true){const page=await this.query(query,signal);yield page;if(!page.next_cursor)return;query.cursor=page.next_cursor;}
  }
  subset(query:SubsetQuery,signal?:AbortSignal):Promise<Json> {return this.request('POST','v1/arrays/subset',query,signal,true);}
  series(query:Json,signal?:AbortSignal):Promise<Json> {return this.request('POST','v1/series',query,signal,true);}
  playback(query:Json,signal?:AbortSignal):Promise<Json> {return this.request('POST','v1/playback',query,signal,true);}
  createScenario(request:Json,signal?:AbortSignal):Promise<Json> {return this.request('POST','v1/scenarios',request,signal);}
  createRun(request:Json,signal?:AbortSignal):Promise<Json> {return this.request('POST','v1/runs',request,signal);}
  transitionRun(request:Json,signal?:AbortSignal):Promise<Json> {return this.request('POST','v1/runs/transition',request,signal);}
  attachOutput(request:Json,signal?:AbortSignal):Promise<Json> {return this.request('POST','v1/runs/outputs',request,signal);}
  submitJob(operation:'observations'|'alignment'|'derivation'|'comparison',payload:unknown,signal?:AbortSignal):Promise<Job> {return this.request('POST','v1/jobs',{operation,payload},signal);}
  job(id:string,signal?:AbortSignal):Promise<Job> {return this.request('GET','v1/jobs/'+encodeURIComponent(id),undefined,signal,true);}
  cancelJob(id:string,signal?:AbortSignal):Promise<Job> {return this.request('POST','v1/jobs/'+encodeURIComponent(id)+'/cancel',undefined,signal);}
}

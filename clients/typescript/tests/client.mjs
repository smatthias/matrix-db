import {test} from 'node:test';
import assert from 'node:assert/strict';
import {MatrixDBClient,MatrixDBError} from '../dist/index.js';
const token='test-token-for-unit-tests-only';
test('cursor pagination and bounded read retry',async()=>{
 let calls=0;const seen=[];
 const client=new MatrixDBClient('https://matrix.invalid',token,{fetch:async(url,options)=>{
  calls++;if(calls===1)throw new TypeError('network');seen.push(JSON.parse(options.body));
  return new Response(JSON.stringify({items:[],next_cursor:calls===2?'next':null}),{status:200});}});
 const pages=[];for await(const page of client.pages({dataset_id:'weather'}))pages.push(page);
 assert.equal(pages.length,2);assert.equal(calls,3);assert.equal(seen[1].cursor,'next');
});
test('mutations are not automatically retried',async()=>{
 let calls=0;const client=new MatrixDBClient('https://matrix.invalid',token,{readRetries:3,fetch:async()=>{calls++;throw new TypeError('network');}});
 await assert.rejects(client.createRun({}),MatrixDBError);assert.equal(calls,1);
});
test('structured errors and cancellation',async()=>{
 const client=new MatrixDBClient('https://matrix.invalid',token,{fetch:async()=>new Response(JSON.stringify({error:{code:'forbidden',message:'access denied',request_id:'r1'}}),{status:403})});
 await assert.rejects(client.query({dataset_id:'x'}),error=>error.status===403&&error.requestId==='r1');
 const controller=new AbortController();controller.abort();
 await assert.rejects(client.query({},controller.signal),error=>error.code==='aborted');
});

test('capacity errors expose Retry-After without automatic replay',async()=>{
 let calls=0;const client=new MatrixDBClient('https://matrix.invalid',token,{readRetries:3,fetch:async()=>{calls++;return new Response(JSON.stringify({error:{code:'quota_exceeded',message:'capacity reached'}}),{status:429,headers:{'Retry-After':'12'}});}});
 await assert.rejects(client.query({dataset_id:'weather'}),error=>error.status===429&&error.retryAfter===12);assert.equal(calls,1);
});

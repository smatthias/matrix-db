import assert from 'node:assert/strict';
import {MatrixDBClient} from '../dist/index.js';
if(process.env.MATRIX_DB_TEST_API_URL){
 const client=new MatrixDBClient(process.env.MATRIX_DB_TEST_API_URL,process.env.MATRIX_DB_TEST_API_TOKEN);
 const catalogue=await client.datasets();assert.ok(catalogue.items.length);
 let records=0;for await(const page of client.pages({dataset_id:'demo-weather',limit:1}))records+=page.items.length;
 assert.ok(records>0);
}

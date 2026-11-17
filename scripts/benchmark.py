"""Disposable, rollback-only synthetic query baseline; never a production capacity claim."""
import argparse
from datetime import timedelta
import json
from pathlib import Path
import statistics
import time
from uuid import uuid4
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle
from matrix_db.queries import ObservationQuery,build_query,query_observations
from matrix_db.storage import connect


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--rows',type=int,default=2000)
    parser.add_argument('--repeats',type=int,default=5)
    args=parser.parse_args()
    if not 100 <= args.rows <= 100000 or not 3 <= args.repeats <= 100:
        parser.error('rows must be 100..100000 and repeats 3..100')
    bundle=Bundle.model_validate_json((Path(__file__).resolve().parents[1]/'examples/weather.json').read_text())
    dataset=bundle.dataset.model_copy(deep=True);dataset.id='benchmark-'+uuid4().hex
    from matrix_db.storage import PostgresStore
    connection=connect()
    try:
        connection.execute("SET LOCAL statement_timeout='60s'")
        store=PostgresStore(connection)
        store.register(CatalogueEntry(dataset=dataset,source_id=dataset.id))
        base=bundle.observations[0]
        records=[base.model_copy(update={'dataset_id':dataset.id,'id':f'point-{i:08d}',
            'longitude':13+(i%100)/100,'latitude':52+((i//100)%100)/100,
            'valid_time':base.valid_time+timedelta(seconds=i)}) for i in range(args.rows)]
        started=time.perf_counter();store.add_observations(dataset.id,records)
        ingestion=time.perf_counter()-started
        connection.execute('ANALYZE matrix_db.observations')
        queries={
            'time_page':ObservationQuery(dataset_id=dataset.id,limit=100),
            'viewport':ObservationQuery(dataset_id=dataset.id,bbox={'west':13.1,'east':13.5,'south':52,'north':52.5},limit=100),
            'radius':ObservationQuery(dataset_id=dataset.id,distance={'longitude':13.4,'latitude':52.2,'radius_metres':10000},order='nearest',limit=100)}
        output={'synthetic':True,'rows':args.rows,'repeats':args.repeats,'ingest_seconds':ingestion,'queries':{}}
        for name,query in queries.items():
            timings=[]
            for _ in range(args.repeats):
                started=time.perf_counter();page=query_observations(connection,query)
                timings.append((time.perf_counter()-started)*1000)
                assert 0 < len(page.items) <= 100
            sql,params=build_query(query)
            plan=connection.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+sql,params).fetchone()[0]
            output['queries'][name]={'median_ms':statistics.median(timings),'max_ms':max(timings),'rows_returned':len(page.items),'plan':plan}
        print(json.dumps(output,indent=2))
    finally:
        connection.rollback();connection.close()

if __name__=='__main__':main()

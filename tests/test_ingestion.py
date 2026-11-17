import csv
import gzip
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import psycopg
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle
from matrix_db.ingestion import records, normalize, import_file, job_report
from matrix_db.storage import migrate, PostgresStore

FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'
DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')

class AdapterTest(unittest.TestCase):
    def test_stream_formats_and_mapping(self):
        bundle=Bundle.model_validate_json(FIXTURE.read_text())
        rows=[o.model_dump(mode='json',exclude_none=True) for o in bundle.observations]
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for format in ('csv','json','jsonl'):
                path=root/(format+'.gz')
                with gzip.open(path,'wt') as stream:
                    if format=='json': json.dump(rows,stream)
                    elif format=='jsonl': stream.write('\n'.join(json.dumps(r) for r in rows))
                    else:
                        writer=csv.DictWriter(stream,fieldnames=rows[0].keys());writer.writeheader();writer.writerows(rows)
                imported=[normalize(r,bundle.dataset,format) for r in records(path,format)]
                self.assertEqual(imported,bundle.observations)
            renamed={**rows[0],'reading':rows[0]['value']};del renamed['value']
            mapping={k:k for k in rows[0]};mapping['value']='reading'
            self.assertEqual(normalize(renamed,bundle.dataset,'json',mapping),bundle.observations[0])

    def test_non_array_json_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'bad.json';path.write_text('{}')
            with self.assertRaises(ValueError): list(records(path,'json'))

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class ImportTest(unittest.TestCase):
    def setUp(self):
        self.connection=psycopg.connect(DSN,autocommit=True)
        self.addCleanup(self.connection.close)
        self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
        migrate(self.connection)
        self.bundle=Bundle.model_validate_json(FIXTURE.read_text())
        self.store=PostgresStore(self.connection)
        self.store.register(CatalogueEntry(dataset=self.bundle.dataset,source_id='weather'))
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'records.json'
        self.rows=[o.model_dump(mode='json') for o in self.bundle.observations]
        self.path.write_text(json.dumps(self.rows))

    def tearDown(self):
        self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')

    def test_duplicates_conflicts_rejections_and_metadata(self):
        self.path.write_text(json.dumps(self.rows+[self.rows[0],{**self.rows[0],'value':20},{'invalid':True}]))
        job=import_file(self.connection,'demo-weather',self.path,format='json',batch_size=2)
        report=job_report(self.connection,job)
        self.assertEqual((report['processed'],report['accepted'],report['duplicates'],report['rejected']),(6,3,1,2))
        again=import_file(self.connection,'demo-weather',self.path,format='json')
        self.assertEqual(job_report(self.connection,again)['accepted'],0)
        self.assertEqual(len(self.store.observations('demo-weather')),3)
        self.assertIsNotNone(self.store.get('demo-weather').last_successful_ingestion)
        self.assertEqual([r['record_number'] for r in report['rejections']],[5,6])

    def test_resume_after_committed_batch(self):
        def interrupted(*args):
            yield self.rows[0]
            raise RuntimeError('simulated worker interruption')
        with patch('matrix_db.ingestion.records',interrupted),self.assertRaises(ValueError):
            import_file(self.connection,'demo-weather',self.path,format='json',batch_size=1)
        job=self.connection.execute('SELECT id FROM matrix_db.import_jobs').fetchone()[0]
        self.assertEqual(job_report(self.connection,job)['accepted'],1)
        import_file(self.connection,'demo-weather',self.path,format='json',batch_size=1,job_id=job)
        report=job_report(self.connection,job)
        self.assertEqual((report['accepted'],report['processed'],report['status']),(3,3,'completed'))
        import_file(self.connection,'demo-weather',self.path,format='json',job_id=job)
        self.assertEqual(job_report(self.connection,job)['processed'],3)
        self.path.write_text('[]')
        with self.assertRaises(ValueError): import_file(self.connection,'demo-weather',self.path,format='json',job_id=job)

    def test_jsonl_parse_errors_are_reported(self):
        self.path.write_text(json.dumps(self.rows[0])+'\ninvalid\n'+json.dumps(self.rows[1]))
        job=import_file(self.connection,'demo-weather',self.path,format='jsonl',batch_size=1)
        report=job_report(self.connection,job)
        self.assertEqual((report['accepted'],report['rejected']),(2,1))

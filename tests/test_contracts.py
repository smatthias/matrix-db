import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from pydantic import ValidationError
from matrix_db.contracts import Bundle

EXAMPLE = Path(__file__).resolve().parents[1] / 'examples/weather.json'

class ContractsTest(unittest.TestCase):
    def setUp(self):
        self.data = json.loads(EXAMPLE.read_text())

    def reject(self, change):
        change(self.data)
        with self.assertRaises((ValidationError, ValueError)):
            Bundle.model_validate(self.data)

    def test_round_trip_and_utc(self):
        bundle = Bundle.model_validate(self.data)
        self.assertEqual(bundle.observations[0].valid_time.hour, 8)
        self.assertEqual(Bundle.model_validate_json(bundle.model_dump_json()), bundle)

    def test_missing_is_not_zero(self):
        for value in (None, 0):
            self.data['observations'][0]['value'] = value
            self.assertEqual(Bundle.model_validate(self.data).observations[0].value, value)

    def test_invalid_fields(self):
        for field, value in [('latitude',91),('longitude',181),('valid_time','2026-11-17T09:00:00'),('valid_time',123),('value',True),('value','15.2'),('value',float('nan')),('value',float('inf')),('unit','K'),('variable_id','absent'),('dataset_id','absent'),('longitude',15),('quality','excellent'),('value',99)]:
            with self.subTest(field=field,value=value):
                original=copy.deepcopy(self.data)
                self.reject(lambda d: d['observations'][0].__setitem__(field,value))
                self.data=original

    def test_non_nullable(self):
        self.data['dataset']['variables'][0]['nullable']=False
        self.reject(lambda d:d['observations'][0].__setitem__('value',None))

    def test_duplicate_variables(self):
        self.reject(lambda d:d['dataset']['variables'].append(d['dataset']['variables'][0]))

    def test_duplicate_observations(self):
        self.reject(lambda d:d['observations'].append(d['observations'][0]))

    def test_integer_type(self):
        self.data['dataset']['variables'][0]['data_type']='integer'
        self.reject(lambda d:d['observations'][0].__setitem__('value',15.0))
        self.data['observations'][0]['value']=15
        Bundle.model_validate(self.data)

    def test_invalid_ranges_and_extra_fields(self):
        self.reject(lambda d:d['dataset']['coverage'].__setitem__('south',54))
        self.setUp()
        self.reject(lambda d:d['dataset']['variables'][0].__setitem__('minimum',100))
        self.setUp()
        self.reject(lambda d:d.__setitem__('unexpected',True))

    def test_antimeridian(self):
        self.data['dataset']['coverage'].update(west=170,east=-170)
        for row in self.data['observations']: row['longitude']=179
        Bundle.model_validate(self.data)
        self.reject(lambda d:d['observations'][0].__setitem__('longitude',0))

    def test_forecast_issue_time(self):
        self.data['dataset']['origin']='forecast'
        self.reject(lambda d:None)
        for row in self.data['observations']: row['forecast_issue_time']='2026-11-17T07:00:00Z'
        Bundle.model_validate(self.data)
        self.reject(lambda d:d['observations'][0].__setitem__('forecast_issue_time','2026-11-18T00:00:00Z'))

    def test_vertical_reference(self):
        self.data['observations'][0]['vertical']={'value':10,'positive':'down','reference':'sea_surface','unit':'m'}
        Bundle.model_validate(self.data)
        self.reject(lambda d:d['observations'][0]['vertical'].pop('reference'))

    def test_cli(self):
        result=subprocess.run([sys.executable,'-m','matrix_db.cli','validate',str(EXAMPLE)],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        with tempfile.TemporaryDirectory() as directory:
            invalid=Path(directory)/'invalid.json'
            invalid.write_text('{bad json')
            result=subprocess.run([sys.executable,'-m','matrix_db.cli','validate',str(invalid)],capture_output=True,text=True)
            self.assertEqual(result.returncode,1)
            self.assertTrue(result.stderr)

if __name__=='__main__': unittest.main()

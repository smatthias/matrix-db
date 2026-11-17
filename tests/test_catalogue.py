import json
from pathlib import Path
import subprocess
import sys
import unittest
from matrix_db.catalogue import Catalogue, CatalogueEntry, TimeCoverage
from matrix_db.contracts import Coverage

FIXTURE=Path(__file__).resolve().parents[1]/'examples/catalogue.json'

class CatalogueTest(unittest.TestCase):
    def setUp(self):
        self.entries=[CatalogueEntry.model_validate(e) for e in json.loads(FIXTURE.read_text())]
        self.catalogue=Catalogue(self.entries)

    def test_combined_filters(self):
        result=self.catalogue.search(variable='humidity',provider='Matrix DB examples',origin='observation',status='active',region=Coverage(west=13,south=52,east=14,north=53))
        self.assertEqual([e.dataset.id for e in result],['demo-weather'])
        self.assertEqual(self.catalogue.search(variable='absent'),[])

    def test_duplicate_and_update(self):
        entry=self.entries[0]
        with self.assertRaises(ValueError): self.catalogue.register(entry)
        duplicate=entry.model_copy(deep=True);duplicate.dataset.id='another'
        with self.assertRaises(ValueError): self.catalogue.register(duplicate)
        replacement=entry.model_copy(deep=True);replacement.status='retired'
        self.catalogue.update(entry.dataset.id,replacement)
        self.assertEqual(self.catalogue.get(entry.dataset.id).status,'retired')
        with self.assertRaises(ValueError): self.catalogue.update(entry.dataset.id,duplicate)
        with self.assertRaises(KeyError): self.catalogue.get('absent')

    def test_snapshots_are_isolated(self):
        self.entries[0].dataset.name='changed'
        result=self.catalogue.get('demo-weather');result.dataset.name='changed again'
        self.catalogue.search()[0].dataset.variables.clear()
        self.assertNotEqual(self.catalogue.get('demo-weather').dataset.name,'changed again')
        self.assertEqual(len(self.catalogue.get('demo-weather').dataset.variables),3)

    def test_spatial_boundaries_and_antimeridian(self):
        for west,east in [(175,180),(-180,-175),(178,-178)]:
            result=self.catalogue.search(region=Coverage(west=west,east=east,south=-1,north=1))
            self.assertEqual([e.dataset.id for e in result],['pacific-weather'])
        self.assertEqual(self.catalogue.search(region=Coverage(west=0,east=1,south=0,north=1)),[])
        self.assertEqual(len(self.catalogue.search(region=Coverage(west=14,east=15,south=53,north=54))),2)

    def test_declared_and_ingested_are_separate(self):
        window=TimeCoverage(start='2026-11-20T00:00:00Z',end='2026-11-20T01:00:00Z')
        self.assertEqual(len(self.catalogue.search(time=window)),3)
        self.assertEqual(self.catalogue.search(time=window,availability='ingested'),[])
        boundary=TimeCoverage(start='2026-11-17T09:00:00Z',end='2026-11-17T10:00:00Z')
        self.assertEqual(len(self.catalogue.search(time=boundary,availability='ingested')),1)
        with self.assertRaises(ValueError): self.catalogue.search(availability='other')

    def test_invalid_metadata(self):
        for patch in [{'declared_time':{'start':'2026-11-01','end':'2026-10-01'}},{'update_interval_seconds':0},{'vertical_coverage':[0,10]},{'last_successful_ingestion':'2026-11-17T08:00:00'}]:
            with self.subTest(patch=patch),self.assertRaises(ValueError):
                CatalogueEntry.model_validate({**self.entries[0].model_dump(),**patch})

    def test_cli_filters_and_invalid_input(self):
        command=[sys.executable,'-m','matrix_db.cli','catalogue',str(FIXTURE)]
        result=subprocess.run(command+['--status','active','--variable','temperature'],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(len(json.loads(result.stdout)),1)
        result=subprocess.run(command+['--start','2026-11-17T00:00:00Z'],capture_output=True,text=True)
        self.assertEqual(result.returncode,1)

import base64
import json
import os
from pathlib import Path
import unittest
import psycopg
from matrix_db.catalogue import CatalogueEntry
from matrix_db.contracts import Bundle, Coverage
from matrix_db.queries import ObservationQuery, Polygon, build_query
from matrix_db.storage import migrate, PostgresStore

DSN=os.environ.get('MATRIX_DB_TEST_DATABASE_URL')
FIXTURE=Path(__file__).resolve().parents[1]/'examples/weather.json'

class QueryContractTest(unittest.TestCase):
    def test_limits_and_distances(self):
        for value in [0,1001,True,'10']:
            with self.subTest(value=value),self.assertRaises(ValueError):ObservationQuery(limit=value)
        with self.assertRaises(ValueError):ObservationQuery(order='nearest')
        for radius in [-1,float('nan'),float('inf')]:
            with self.assertRaises(ValueError):ObservationQuery(distance={'longitude':0,'latitude':0,'radius_metres':radius})

    def test_invalid_polygon_structure(self):
        for coords in [[],[[[0,0],[1,0],[1,1]]],[[[0,0],[181,0],[0,1],[0,0]]],[[[170,0],[-170,0],[-170,1],[170,0]]]]:
            with self.subTest(coords=coords),self.assertRaises(ValueError):Polygon(type='Polygon',coordinates=coords)

    def test_parameterization_and_invalid_cursor(self):
        query=ObservationQuery(dataset_id='demo-weather',limit=2)
        sql,params=build_query(query)
        self.assertNotIn('demo-weather',sql)
        self.assertEqual(params['dataset_id'],'demo-weather')
        for cursor in ['bad!',base64.urlsafe_b64encode(b'[]').decode()]:
            with self.assertRaises(ValueError):build_query(query.model_copy(update={'cursor':cursor}))

@unittest.skipUnless(DSN,'MATRIX_DB_TEST_DATABASE_URL required')
class SpatialTest(unittest.TestCase):
    def setUp(self):
        self.connection=psycopg.connect(DSN,autocommit=True)
        self.addCleanup(self.connection.close)
        self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')
        migrate(self.connection)
        bundle=Bundle.model_validate_json(FIXTURE.read_text())
        self.dataset=bundle.dataset.model_copy(deep=True)
        self.dataset.coverage=Coverage(west=-180,east=180,south=-90,north=90)
        self.store=PostgresStore(self.connection)
        self.store.register(CatalogueEntry(dataset=self.dataset,source_id='spatial'))
        template=bundle.observations[0]
        rows=[]
        for ident,lon,lat in [('berlin',13.4,52.5),('boundary',14,53),('far',0,0),('east',179,0),('west',-179,0),('edge',180,0)]:
            row=template.model_copy(deep=True)
            row.id=ident;row.longitude=lon;row.latitude=lat
            if ident=='berlin':
                from matrix_db.contracts import VerticalCoordinate
                row.vertical=VerticalCoordinate(value=10,positive='up',reference='ground')
            rows.append(row)
        self.store.add_observations(self.dataset.id,rows)

    def tearDown(self):self.connection.execute('DROP SCHEMA IF EXISTS matrix_db CASCADE')

    def query(self,**kwargs):return self.store.query_observations(ObservationQuery(**kwargs))
    def ids(self,page):return [m.observation.id for m in page.items]

    def test_bbox_boundaries_empty_and_antimeridian(self):
        self.assertEqual(self.ids(self.query(bbox={'west':13,'east':14,'south':52,'north':53})),['berlin','boundary'])
        self.assertEqual(self.ids(self.query(bbox={'west':170,'east':-170,'south':-1,'north':1})),['east','edge','west'])
        self.assertEqual(self.ids(self.query(bbox={'west':30,'east':31,'south':20,'north':21})),[])
        self.assertEqual(self.ids(self.query(bbox={'west':14,'east':14,'south':53,'north':53})),['boundary'])

    def test_polygon_boundary_holes_and_invalid_geometry(self):
        polygon={'type':'Polygon','coordinates':[[[13,52],[14,52],[14,53],[13,53],[13,52]]]}
        self.assertEqual(self.ids(self.query(polygon=polygon)),['berlin','boundary'])
        hole=[[13.3,52.4],[13.5,52.4],[13.5,52.6],[13.3,52.6],[13.3,52.4]]
        polygon['coordinates'].append(hole)
        self.assertEqual(self.ids(self.query(polygon=polygon)),['boundary'])
        invalid={'type':'Polygon','coordinates':[[[13,52],[14,53],[14,52],[13,53],[13,52]]]}
        with self.assertRaisesRegex(ValueError,'invalid polygon'):self.query(polygon=invalid)

    def test_split_antimeridian_multipolygon(self):
        polygon={'type':'MultiPolygon','coordinates':[
            [[[170,-1],[180,-1],[180,1],[170,1],[170,-1]]],
            [[[-180,-1],[-170,-1],[-170,1],[-180,1],[-180,-1]]]]}
        self.assertEqual(self.ids(self.query(polygon=polygon)),['east','edge','west'])

    def test_radius_nearest_and_geodesic_distance(self):
        center={'longitude':13.4,'latitude':52.5,'radius_metres':100}
        page=self.query(distance=center,order='nearest')
        self.assertEqual(self.ids(page),['berlin'])
        self.assertAlmostEqual(page.items[0].distance_metres,0,places=6)
        page=self.query(distance={'longitude':179,'latitude':0,'radius_metres':250000},order='nearest')
        self.assertEqual(self.ids(page),['east','edge','west'])
        self.assertAlmostEqual(page.items[-1].distance_metres,222638.9816,delta=1)
        zero=self.query(distance={'longitude':13.4,'latitude':52.5,'radius_metres':0})
        self.assertEqual(self.ids(zero),['berlin'])

    def test_combined_filters_and_vertical_reference(self):
        options={'dataset_id':self.dataset.id,'variable_id':'temperature',
            'time':{'start':'2026-11-17T08:00:00Z','end':'2026-11-17T08:00:00Z'},
            'bbox':{'west':13,'east':14,'south':52,'north':53},
            'vertical':{'minimum':10,'maximum':10,'reference':'ground','positive':'up'}}
        self.assertEqual(self.ids(self.query(**options)),['berlin'])
        options['vertical']['reference']='sea_surface'
        self.assertEqual(self.ids(self.query(**options)),[])
        self.assertEqual(self.ids(self.query(variable_id='humidity')),[])

    def test_pagination_and_cursor_filter_binding(self):
        twin=self.dataset.model_copy(deep=True);twin.id='another-dataset'
        self.store.register(CatalogueEntry(dataset=twin,source_id='another-source'))
        record=self.store.observations(self.dataset.id)[0].model_copy(deep=True)
        record.dataset_id=twin.id
        self.store.add_observations(twin.id,[record])
        expected=[(m.observation.dataset_id,m.observation.id) for m in self.query().items]
        for order,extra in [('time',{}),('nearest',{'distance':{'longitude':0,'latitude':0,'radius_metres':20040000}})]:
            seen=[];cursor=None
            while True:
                page=self.query(limit=2,cursor=cursor,order=order,**extra)
                seen.extend((m.observation.dataset_id,m.observation.id) for m in page.items)
                if not page.next_cursor:break
                cursor=page.next_cursor
            self.assertEqual(set(seen),set(expected));self.assertEqual(len(seen),len(expected))
        first=self.query(limit=2)
        with self.assertRaises(ValueError):self.query(limit=2,cursor=first.next_cursor,variable_id='humidity')
        self.assertEqual(len(self.query(limit=1,cursor=first.next_cursor).items),1)

    def test_spatial_index_access_paths(self):
        # Small fixtures favour scans; disable them to prove the predicates have eligible index paths.
        with self.connection.transaction():
            self.connection.execute('SET LOCAL enable_seqscan=off')
            for query,index in [
                (ObservationQuery(bbox={'west':13,'east':14,'south':52,'north':53}),'observations_location_idx'),
                (ObservationQuery(distance={'longitude':13.4,'latitude':52.5,'radius_metres':100},order='nearest'),'observations_geography_idx')]:
                sql,params=build_query(query)
                plan=self.connection.execute('EXPLAIN (FORMAT JSON) '+sql,params).fetchone()[0]
                self.assertIn(index,json.dumps(plan))

"""Validate and inspect a version-one JSON dataset bundle."""
import argparse
import json
import psycopg
from pathlib import Path
import sys
from .contracts import Bundle, Coverage
from .catalogue import Catalogue, CatalogueEntry, TimeCoverage

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["validate", "inspect", "catalogue", "db-migrate", "db-ready", "db-seed", "db-inspect"])
    parser.add_argument("path", type=Path, nargs="?")
    parser.add_argument('--variable')
    parser.add_argument('--provider')
    parser.add_argument('--origin', choices=['observation', 'forecast', 'simulation'])
    parser.add_argument('--status', choices=['active', 'stale', 'retired'])
    parser.add_argument('--bbox', nargs=4, type=float, metavar=('WEST','SOUTH','EAST','NORTH'))
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--availability', choices=['declared','ingested'], default='declared')
    args = parser.parse_args()
    try:
        if args.command.startswith('db-'):
            from .storage import connect, migrate, PostgresStore
            with connect() as connection:
                if args.command == 'db-migrate':
                    print(f'Schema version {migrate(connection)}')
                elif args.command == 'db-ready':
                    version = connection.execute('SELECT max(version) FROM matrix_db.schema_migrations').fetchone()[0]
                    from .storage import LATEST_VERSION
                    if version != LATEST_VERSION:
                        raise ValueError('database requires migration')
                    connection.execute('SELECT PostGIS_Version()')
                    print('Database ready')
                elif args.command == 'db-seed':
                    if args.path is None:
                        raise ValueError('db-seed requires a bundle path')
                    bundle = Bundle.model_validate_json(args.path.read_text(encoding='utf-8'))
                    store = PostgresStore(connection)
                    store.register(CatalogueEntry(dataset=bundle.dataset, source_id=bundle.dataset.source_reference))
                    store.add_observations(bundle.dataset.id, bundle.observations)
                    print(f'Stored {len(bundle.observations)} observations')
                else:
                    if args.path is None:
                        raise ValueError('db-inspect requires a dataset ID')
                    store = PostgresStore(connection)
                    print(json.dumps({'entry':store.get(str(args.path)).model_dump(mode='json'),
                        'observations':[r.model_dump(mode='json') for r in store.observations(str(args.path))]},indent=2))
            return 0
        if args.path is None:
            raise ValueError('a file path is required')
        if args.command == 'catalogue':
            entries = [CatalogueEntry.model_validate(e) for e in json.loads(args.path.read_text(encoding='utf-8'))]
            catalogue = Catalogue(entries)
            if bool(args.start) != bool(args.end):
                raise ValueError('--start and --end must be supplied together')
            region = Coverage(**dict(zip(('west','south','east','north'), args.bbox))) if args.bbox else None
            time = TimeCoverage(start=args.start, end=args.end) if args.start else None
            matches = catalogue.search(variable=args.variable, provider=args.provider, origin=args.origin,
                status=args.status, region=region, time=time, availability=args.availability)
            print(json.dumps([e.model_dump(mode='json') for e in matches], indent=2))
            return 0
        bundle = Bundle.model_validate_json(args.path.read_text(encoding="utf-8"))
    except psycopg.Error:
        print("Database operation failed; check connectivity, migrations and record uniqueness. Connection details are withheld.", file=sys.stderr)
        return 1
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(str(error), file=sys.stderr)
        return 1
    if args.command == "inspect":
        print(bundle.model_dump_json(indent=2))
    else:
        print(f"Valid dataset {bundle.dataset.id}: {len(bundle.observations)} observations")
    return 0

if __name__ == "__main__":
    sys.exit(main())

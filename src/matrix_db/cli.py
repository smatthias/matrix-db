"""Validate and inspect a version-one JSON dataset bundle."""
import argparse
import json
import os
import psycopg
from pathlib import Path
import sys
from .contracts import Bundle, Coverage
from .catalogue import Catalogue, CatalogueEntry, TimeCoverage

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["validate", "inspect", "catalogue", "db-migrate", "db-ready", "db-seed", "db-inspect", "db-import", "db-job", "db-query", "db-revise", "db-withdraw", "db-history", "db-series", "db-playback", "db-array-demo", "db-array-read", "db-array-inventory", "db-align", "db-derived-register", "db-derived-associate", "db-derived-run", "db-derived-inspect", "db-scenario-register", "db-scenario-resolve", "db-run-create", "db-run-change", "db-run-attach", "db-run-inspect", "db-run-compare"])
    parser.add_argument("path", type=Path, nargs="?")
    parser.add_argument('--variable')
    parser.add_argument('--provider')
    parser.add_argument('--origin', choices=['observation', 'forecast', 'simulation'])
    parser.add_argument('--status', choices=['active', 'stale', 'retired'])
    parser.add_argument('--bbox', nargs=4, type=float, metavar=('WEST','SOUTH','EAST','NORTH'))
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--availability', choices=['declared','ingested'], default='declared')
    parser.add_argument('--dataset')
    parser.add_argument('--format', choices=['csv','json','jsonl'])
    parser.add_argument('--batch-size',type=int,default=500)
    parser.add_argument('--job-id')
    parser.add_argument('--mapping',type=Path)
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
                elif args.command == 'db-import':
                    from .ingestion import import_file, job_report
                    if args.path is None or args.dataset is None or args.format is None:
                        raise ValueError('db-import requires a path, --dataset and --format')
                    mapping=json.loads(args.mapping.read_text()) if args.mapping else None
                    job_id=import_file(connection,args.dataset,args.path,format=args.format,
                        batch_size=args.batch_size,job_id=args.job_id,mapping=mapping)
                    print(json.dumps(job_report(connection,job_id),default=str,indent=2))
                elif args.command in ('db-revise','db-withdraw','db-history','db-series','db-playback'):
                    from .temporal import RevisionRequest, WithdrawalRequest, SeriesQuery, PlaybackQuery, history, series, playback
                    if args.path is None: raise ValueError('temporal command requires a JSON request file')
                    payload=json.loads(args.path.read_text(encoding='utf-8'))
                    store=PostgresStore(connection)
                    if args.command == 'db-revise':
                        request=RevisionRequest.model_validate(payload)
                        result=store.revise(request.observation,expected_revision=request.expected_revision,reason=request.reason,actor=request.actor)
                    elif args.command == 'db-withdraw':
                        request=WithdrawalRequest.model_validate(payload)
                        result=store.withdraw(request.dataset_id,request.observation_id,expected_revision=request.expected_revision,reason=request.reason,actor=request.actor)
                    elif args.command == 'db-history':
                        result=history(connection,**payload)
                    elif args.command == 'db-series':
                        result=series(connection,SeriesQuery.model_validate(payload))
                    else:
                        result=playback(connection,PlaybackQuery.model_validate(payload))
                    print(json.dumps(result,default=str,indent=2))
                elif args.command in ('db-scenario-register','db-scenario-resolve','db-run-create','db-run-change','db-run-attach','db-run-inspect','db-run-compare'):
                    from .arrays import ArrayStore
                    from .scenarios import Scenario,ScenarioRef,RunRequest,RunChange,OutputAttachment,ComparisonRequest,register_scenario,resolve_inputs,create_run,transition_run,attach_output,get_run,compare_and_publish
                    if args.path is None:raise ValueError('simulation command requires a JSON request file')
                    payload=json.loads(args.path.read_text())
                    root=os.environ.get('MATRIX_DB_ARRAY_ROOT')
                    if not root:raise ValueError('MATRIX_DB_ARRAY_ROOT is required')
                    arrays=ArrayStore(connection,root)
                    if args.command=='db-scenario-register':result=register_scenario(arrays,Scenario.model_validate(payload))
                    elif args.command=='db-scenario-resolve':result=resolve_inputs(arrays,ScenarioRef.model_validate(payload))
                    elif args.command=='db-run-create':result=create_run(arrays,RunRequest.model_validate(payload))
                    elif args.command=='db-run-change':result=transition_run(arrays,RunChange.model_validate(payload))
                    elif args.command=='db-run-attach':result=attach_output(arrays,OutputAttachment.model_validate(payload))
                    elif args.command=='db-run-compare':result=compare_and_publish(arrays,ComparisonRequest.model_validate(payload))
                    else:result=get_run(connection,**payload)
                    print(json.dumps(result,default=str,allow_nan=False,indent=2))
                elif args.command in ('db-derived-register','db-derived-associate','db-derived-run','db-derived-inspect'):
                    from .derived import Definition, DefinitionRef, Association, DerivationRequest, register_definitions, register_association, derive_and_publish, load_definitions
                    if args.path is None:raise ValueError('derived command requires a JSON request file')
                    payload=json.loads(args.path.read_text())
                    if args.command == 'db-derived-register':
                        if not isinstance(payload,list):raise ValueError('registration requires a JSON array of definitions')
                        result={'registered':register_definitions(connection,[Definition.model_validate(item) for item in payload])}
                    elif args.command == 'db-derived-associate':result={'id':register_association(connection,Association.model_validate(payload))}
                    elif args.command == 'db-derived-inspect':
                        reference=DefinitionRef.model_validate(payload)
                        result=load_definitions(connection)[reference.key()].model_dump(mode='json')
                    else:
                        from .arrays import ArrayStore
                        root=os.environ.get('MATRIX_DB_ARRAY_ROOT')
                        if not root:raise ValueError('MATRIX_DB_ARRAY_ROOT is required')
                        result=derive_and_publish(ArrayStore(connection,root),DerivationRequest.model_validate(payload))
                    print(json.dumps(result,allow_nan=False,indent=2))
                elif args.command in ('db-array-demo','db-array-read','db-array-inventory','db-align'):
                    import numpy as np
                    from .arrays import ArrayStore, GridSpec, SubsetQuery
                    root=os.environ.get('MATRIX_DB_ARRAY_ROOT')
                    if not root:raise ValueError('MATRIX_DB_ARRAY_ROOT is required')
                    arrays=ArrayStore(connection,root)
                    if args.command == 'db-align':
                        from .alignment import AlignmentRequest, align_and_publish
                        if args.path is None:raise ValueError('db-align requires an alignment request')
                        request=AlignmentRequest.model_validate_json(args.path.read_text())
                        result=align_and_publish(arrays,request)
                    elif args.command == 'db-array-inventory':
                        result=arrays.unreferenced_directories()
                    elif args.command == 'db-array-demo':
                        if args.path is None:raise ValueError('db-array-demo requires a grid specification')
                        spec=GridSpec.model_validate_json(args.path.read_text())
                        def synthetic(variable_id,slices):
                            variable=next(v for v in spec.variables if v.id==variable_id)
                            coordinates=np.meshgrid(*[np.arange(s.start,s.stop) for s in slices],indexing='ij')
                            data=sum((i+1)*coordinate for i,coordinate in enumerate(coordinates)).astype(variable.dtype)
                            mask=np.zeros(data.shape,dtype=bool)
                            return data,mask
                        result={'version_id':str(arrays.publish(spec,synthetic))}
                    else:
                        if args.path is None:raise ValueError('db-array-read requires a subset query')
                        result=arrays.read(SubsetQuery.model_validate_json(args.path.read_text()))
                        for variable in result['variables'].values():
                            values=variable['data'].astype(object)
                            values[variable['mask']]=None
                            variable['data']=values.tolist();variable['mask']=variable['mask'].tolist()
                            if variable.get('coverage') is not None:variable['coverage']=variable['coverage'].tolist()
                            if variable.get('quality') is not None:variable['quality']=variable['quality'].tolist()
                    print(json.dumps(result,allow_nan=False,indent=2))
                elif args.command == 'db-query':
                    from .queries import ObservationQuery
                    if args.path is None: raise ValueError('db-query requires a query JSON file')
                    query=ObservationQuery.model_validate_json(args.path.read_text(encoding='utf-8'))
                    print(PostgresStore(connection).query_observations(query).model_dump_json(indent=2))
                elif args.command == 'db-job':
                    from .ingestion import job_report
                    if args.path is None: raise ValueError('db-job requires a job ID')
                    print(json.dumps(job_report(connection,str(args.path)),default=str,indent=2))
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

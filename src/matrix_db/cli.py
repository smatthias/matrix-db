"""Validate and inspect a version-one JSON dataset bundle."""
import argparse
import json
from pathlib import Path
import sys
from .contracts import Bundle, Coverage
from .catalogue import Catalogue, CatalogueEntry, TimeCoverage

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["validate", "inspect", "catalogue"])
    parser.add_argument("path", type=Path)
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
    except (OSError, ValueError, TypeError) as error:
        print(str(error), file=sys.stderr)
        return 1
    if args.command == "inspect":
        print(bundle.model_dump_json(indent=2))
    else:
        print(f"Valid dataset {bundle.dataset.id}: {len(bundle.observations)} observations")
    return 0

if __name__ == "__main__":
    sys.exit(main())

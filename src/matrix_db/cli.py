"""Validate and inspect a version-one JSON dataset bundle."""
import argparse
from pathlib import Path
import sys
from pydantic import ValidationError
from .contracts import Bundle

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["validate", "inspect"])
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    try:
        bundle = Bundle.model_validate_json(args.path.read_text(encoding="utf-8"))
    except (OSError, ValidationError) as error:
        print(str(error), file=sys.stderr)
        return 1
    if args.command == "inspect":
        print(bundle.model_dump_json(indent=2))
    else:
        print(f"Valid dataset {bundle.dataset.id}: {len(bundle.observations)} observations")
    return 0

if __name__ == "__main__":
    sys.exit(main())

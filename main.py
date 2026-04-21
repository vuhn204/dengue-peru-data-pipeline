from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from dengue_pipeline.pipeline import run_ingestion, run_reporting, run_transformations


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline de vigilancia epidemiologica de dengue en Peru."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("ingest", "transform", "report", "run-all"):
        subparsers.add_parser(command)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "ingest":
        run_ingestion()
    elif args.command == "transform":
        run_transformations()
    elif args.command == "report":
        run_reporting()
    elif args.command == "run-all":
        run_ingestion()
        run_transformations()
        run_reporting()


if __name__ == "__main__":
    main()

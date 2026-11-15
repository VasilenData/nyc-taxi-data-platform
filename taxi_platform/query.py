"""Run a useful, read-only SQL recipe against the persistent sample warehouse."""
import argparse
import csv
from pathlib import Path
import sys

import duckdb

from .analyst import ROOT


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recipe", choices=["boroughs", "quality"])
    parser.add_argument("--workspace", type=Path, default=ROOT / "data" / "sample-workspace")
    args = parser.parse_args()
    database = args.workspace / "warehouse.duckdb"
    if not database.is_file():
        parser.error("warehouse.duckdb is missing; run python -m taxi_platform.sample first")
    sql = (ROOT / "docs" / "sql" / f"{args.recipe}.sql").read_text(encoding="utf-8")
    with duckdb.connect(str(database), read_only=True) as con:
        result = con.execute(sql)
        writer = csv.writer(sys.stdout, lineterminator="\n")
        writer.writerow([column[0] for column in result.description])
        writer.writerows(result.fetchall())


if __name__ == "__main__":
    main()

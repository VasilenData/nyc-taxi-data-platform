"""Reproduce stale-plan rejection on the bundled TLC sample; no cloud calls."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

from taxi_platform.change_plan import create_plan, validate_plan, write_new  # noqa: E402
from taxi_platform.release_bundle import sha256  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Evidence already exists; choose a new output filename")
    sample = ROOT / "data/sample"
    record = {
        "schema_version": 1, "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "controlled local changes to the 30,000-row historical TLC sample",
        "cloud_execution": False, "python": platform.python_version(), "duckdb": duckdb.__version__,
        "implementation_sha256": {name: sha256(ROOT / name) for name in
            ("taxi_platform/change_plan.py", "scripts/change_plan_drift_exercise.py")},
        "source_sha256": {path.name: sha256(path) for path in sorted(sample.glob("*.parquet"))},
        "limits": "Single-writer exercise. Commutative row hashes detect accidental drift, not malicious tampering. No full-data timing or cloud claim.",
        "scenarios": []}
    cases = [
        ("unchanged", None, False),
        ("raw_value_edit_same_count_and_load_time", "update raw.trips set fare_amount = fare_amount + 0.01", True),
        ("incremental_classifier_edit", "update build.stg_trips_classified set fare_amount = fare_amount + 0.01", True),
        ("published_fact_edit_same_version", "update main.fct_trips set fare_amount = fare_amount + 0.01", True),
        ("nullable_column_added", "alter table raw.trips add column maintenance_note varchar", True),
        ("published_table_removed", "drop table main.fct_trips", True),
        ("row_order_only", "create or replace table raw.trips as select * from raw.trips order by fare_amount desc", False),
    ]
    with tempfile.TemporaryDirectory(prefix="taxi-plan-drift-") as temporary:
        work = Path(temporary)
        source = work / "baseline.duckdb"
        env = os.environ | {"TAXI_DB": str(source), "TAXI_DATA_DIR": str(sample),
                            "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", ""),
                            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
        run = subprocess.run([sys.executable, "run_pipeline.py", "2023-01", "2023-02"],
                             cwd=ROOT, env=env, capture_output=True, timeout=300)
        if run.returncode:
            raise RuntimeError((run.stdout + run.stderr).decode(errors="replace")[-6000:])
        with duckdb.connect(str(source), read_only=True) as con:
            record["raw_rows"] = con.execute("select count(*) from raw.trips").fetchone()[0]
            record["fact_rows"] = con.execute("select count(*) from main.fct_trips").fetchone()[0]
        for name, sql, expected_rejection in cases:
            database = work / f"{name}.duckdb"
            shutil.copy2(source, database)
            plan = create_plan(database, sample, [])
            if sql:
                with duckdb.connect(str(database)) as con:
                    con.execute(sql)
            before = sha256(database)
            error = None
            try:
                validate_plan(plan)
            except ValueError as exc:
                error = str(exc)
            unchanged = sha256(database) == before
            rejected = error is not None
            record["scenarios"].append({"name": name, "sql": sql,
                "expected_rejection": expected_rejection, "rejected": rejected,
                "error": error, "validation_left_database_bytes_unchanged": unchanged,
                "passed": rejected == expected_rejection and unchanged
                    and (not rejected or "Warehouse changed" in error)})
    record["success"] = all(item["passed"] for item in record["scenarios"])
    write_new(args.output, record)
    print(json.dumps({"output": str(args.output), "scenarios": len(cases), "success": record["success"]}))
    return 0 if record["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

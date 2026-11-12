"""Run the real bundled TLC sample through publication and restore a serving backup.

No AWS calls. This deliberately records local recovery evidence separately from
cloud deployment, IAM denial verification and cloud billing, which it cannot prove.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

from taxi_platform.release_bundle import export_release, literal, RELATIONS, restore_into, sha256  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    record = {"schema_version": 1, "measured_at_utc": datetime.now(timezone.utc).isoformat(),
              "scope": "local serving-backup exercise on bundled historical TLC sample",
              "cloud_deployed": False, "aws_iam_denial_tests": "not run",
              "athena_query_measurements": "not run", "cloud_billed_cost_usd": None,
              "python": platform.python_version(), "duckdb": duckdb.__version__,
              "operating_system": platform.system(),
              "implementation_sha256": {str(path.relative_to(ROOT)).replace("\\", "/"): sha256(path)
                  for path in [ROOT / "taxi_platform/release_bundle.py", ROOT / "scripts/cloud_readiness_exercise.py"]}}
    months = ["2023-01", "2023-02"]
    record["inputs"] = [{"file": f"yellow_tripdata_{month}.parquet",
                         "sha256": sha256(ROOT / "data/sample" / f"yellow_tripdata_{month}.parquet")}
                        for month in months]
    with tempfile.TemporaryDirectory(prefix="taxi-cloud-readiness-") as temporary:
        work = Path(temporary)
        source, restored, bundle = work / "source.duckdb", work / "restored.duckdb", work / "release"
        env = os.environ | {"TAXI_DB": str(source), "TAXI_DATA_DIR": str(ROOT / "data/sample"),
                            "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", ""),
                            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "NO_COLOR": "1"}
        run = subprocess.run([sys.executable, "run_pipeline.py", *months], cwd=ROOT, env=env,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300)
        record["pipeline_exit_code"] = run.returncode
        record["pipeline_log_sha256"] = hashlib.sha256(run.stdout).hexdigest()
        if run.returncode:
            print(run.stdout.decode(errors="replace")[-8000:])
            raise RuntimeError("Sample pipeline failed")
        manifest = export_release(source, bundle)
        record["release"] = {key: manifest[key] for key in ("release_id", "total_bytes", "export_seconds")}
        with duckdb.connect(str(restored)) as con:
            record["restore"] = restore_into(con, bundle)
            con.execute(f"attach {literal(source)} as expected (read_only)")
            differences = {}
            schemas = {}
            for name in RELATIONS:
                differences[name] = con.execute(f'''select count(*) from (
                    (select * from main."{name}" except all select * from expected.main."{name}")
                    union all
                    (select * from expected.main."{name}" except all select * from main."{name}")
                    )''').fetchone()[0]
                schemas[name] = (con.execute(f'describe main."{name}"').fetchall()
                                 == con.execute(f'describe expected.main."{name}"').fetchall())
            record["multiset_differences"] = differences
            record["schemas_equal"] = schemas
            record["row_counts"] = {name: con.execute(f'select count(*) from main."{name}"').fetchone()[0]
                                    for name in RELATIONS}
            record["reconciliation"] = {
                "classified_equals_valid_plus_quarantine": record["row_counts"]["stg_trips_classified"]
                    == record["row_counts"]["stg_trips"] + record["row_counts"]["trips_quarantine"],
                "daily_trips_equal_facts": con.execute("select sum(trips) from mart_daily_borough").fetchone()[0]
                    == record["row_counts"]["fct_trips"],
                "hourly_trips_equal_facts": con.execute("select sum(trips) from mart_hourly_demand").fetchone()[0]
                    == record["row_counts"]["fct_trips"]}
        record["success"] = (all(value == 0 for value in differences.values()) and all(schemas.values())
                             and all(record["reconciliation"].values()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(record, indent=2))
    return 0 if record["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

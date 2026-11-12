"""Record a real sample correction, failed candidate, recovery and model change.

The January/February source extracts are public TLC data. Corrections and failures
are deliberate synthetic exercises, not observed source incidents.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New evidence directory")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    outcomes = []
    with tempfile.TemporaryDirectory(prefix="taxi-change-") as temporary:
        work = Path(temporary)
        database, source_dir, dbt_dir = work / "warehouse.duckdb", work / "sources", work / "dbt"
        source_dir.mkdir()
        shutil.copytree(ROOT / "dbt", dbt_dir, ignore=shutil.ignore_patterns("target", "logs", ".user.yml"))
        for source in (ROOT / "data/sample").glob("*.parquet"):
            shutil.copy2(source, source_dir / source.name)
        env = {**os.environ, "TAXI_DB": str(database), "TAXI_DATA_DIR": str(source_dir),
               "TAXI_DBT_DIR": str(dbt_dir), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
               "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", "")}

        def run(label, arguments, expected=0):
            process = subprocess.run([sys.executable, *arguments], cwd=ROOT, env=env,
                                     capture_output=True, text=True, timeout=600)
            log = (process.stdout + process.stderr).replace(str(work), "<EXERCISE>").replace(str(ROOT), "<REPOSITORY>")
            log_path = args.output / f"{label}.log"
            log_path.write_text(log, encoding="utf-8")
            if process.returncode != expected:
                raise RuntimeError(f"{label} returned {process.returncode}, expected {expected}; see {log_path}")
            return {"exit_code": process.returncode, "log": log_path.name,
                    "log_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest()}

        def change(label, expected=0):
            plan_path, report_path = work / f"{label}-plan.json", work / f"{label}-report.json"
            planned = run(label + "-plan", ["-m", "taxi_platform.change_plan", "plan", "--database", str(database),
                          "--data-dir", str(source_dir), "--months", "2023-01", "--output", str(plan_path)])
            applied = run(label + "-apply", ["-m", "taxi_platform.change_plan", "apply", "--plan", str(plan_path),
                          "--output", str(report_path)], expected=expected)
            plan = json.loads(plan_path.read_text())
            report = json.loads(report_path.read_text())
            return {"name": label, "plan_execution": planned, "apply_execution": applied,
                    "plan": {k: v for k, v in plan.items() if k not in {"database", "data_dir"}}, "report": report}

        outcomes.append({"name": "initial", **run("initial", ["run_pipeline.py", "2023-01", "2023-02"])})
        source = source_dir / "yellow_tripdata_2023-01.parquet"
        original = ROOT / "data/sample" / source.name

        def replace_source(expression):
            target = source_dir / "replacement.parquet"
            with duckdb.connect() as con:
                con.execute(f"copy ({expression}) to ? (format parquet)", [str(original), str(target)])
            target.replace(source)

        corrected = "select * replace(total_amount + 1 as total_amount), 'synthetic correction' source_note from read_parquet(?)"
        replace_source(corrected)
        outcomes.append(change("corrected-month-and-compatible-extra-column"))
        assert outcomes[-1]["report"]["unaffected_classifier_months_unchanged"]
        assert outcomes[-1]["report"]["reload_months"] == ["2023-01"]

        replace_source("select * exclude(fare_amount) from read_parquet(?)")
        schema_failure = run("missing-required-column", ["-m", "taxi_platform.change_plan", "plan", "--database", str(database),
                             "--data-dir", str(source_dir), "--months", "2023-01", "--output", str(work / "invalid-plan.json")], 1)
        outcomes.append({"name": "missing-required-column", "rejected_before_apply": True, **schema_failure})

        replace_source("select * replace(case when row_number() over () % 5 < 2 then -5 else fare_amount end as fare_amount) from read_parquet(?)")
        outcomes.append(change("invalid-candidate", 1))
        assert outcomes[-1]["report"]["consumer_output_preserved"]

        replace_source(corrected)
        outcomes.append(change("recover-with-valid-source"))
        project_file = dbt_dir / "dbt_project.yml"
        old = project_file.read_text()
        assert "max_trip_miles: 200" in old
        project_file.write_text(old.replace("max_trip_miles: 200", "max_trip_miles: 100"))
        outcomes.append(change("business-rule-change"))
        assert outcomes[-1]["report"]["full_reclassification"]
        assert outcomes[-1]["report"]["reload_months"] == []
        with duckdb.connect(str(database), read_only=True) as con:
            invalid = con.execute("select count(*) from main.fct_trips where distance_miles > 100").fetchone()[0]
        assert invalid == 0
        outcomes[-1]["new_rule_validated"] = {"maximum_published_distance_miles": 100, "violations": invalid}

    files = [p for folder in ("taxi_platform", "dbt", "scripts") for p in (ROOT / folder).rglob("*")
             if p.is_file() and p.suffix in {".py", ".sql", ".yml", ".csv"}
             and not {"target", "logs", "__pycache__"}.intersection(p.parts)]
    report = {"schema_version": 1, "completed_utc": datetime.now(timezone.utc).isoformat(), "success": True,
              "scope": "30,000 public sample rows; synthetic corrections and faults; single-writer local DuckDB",
              "outcomes": outcomes,
              "source_sha256": {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}}
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "success": True, "scenarios": len(outcomes)}, indent=2))


if __name__ == "__main__":
    main()

"""Capture a real crash / transaction repair / recovery exercise.

Only an isolated teaching copy is deliberately broken. The project publisher is
never edited. This is an AI-assisted synthetic demonstration, not a historical
incident or an assessment of the portfolio owner's independent performance.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import difflib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import duckdb
from taxi_platform.publication import TABLES, publish


def sha(data):
    return hashlib.sha256(data).hexdigest()


def load_variant(path):
    spec = importlib.util.spec_from_file_location("taxi_platform.teaching_publication", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.publish


def candidate(con, size, value):
    """Same bounded contract as the existing publication regression fixture."""
    con.execute("create schema if not exists raw")
    con.execute("create schema if not exists build")
    con.execute("""create or replace table raw.trips as select '2023-01' _source_month,
        'synthetic.parquet' _source_file, timestamp '2023-01-01' _loaded_at from range(?)""", [size])
    for table in TABLES:
        con.execute(f'create or replace table build."{table}" as select ? value from range(?)', [value, size])
    con.execute("create or replace table build.stg_trips as select *, ? value from raw.trips", [value])
    con.execute("create or replace table build.trips_quarantine as select * from build.stg_trips where false")
    con.execute("""create or replace table build.fct_trips as
        select _source_month source_month, ? value from raw.trips""", [value])
    for table in ("mart_daily_borough", "mart_hourly_demand"):
        con.execute(f'create or replace table build."{table}" as select ? trips, ? value', [size, value])
    con.execute("""create or replace table build.mart_data_quality as
        select '2023-01' source_month, 0 rejected_rows, ? raw_rows""", [size])


def snapshot(con):
    return {name: con.execute(f'select * from main."{name}"').fetchall()
            for name in (*TABLES, "publication")}


def digest(rows):
    return sha(json.dumps(rows, default=str, sort_keys=True).encode())


def crash_worker(database, module):
    con = duckdb.connect(str(database))

    class CrashConnection:
        def execute(self, sql, parameters=None):
            result = con.execute(sql, parameters) if parameters is not None else con.execute(sql)
            if sql.startswith('create or replace table main."fct_trips"'):
                print("Child: fact table copy completed; calling os._exit(37).", flush=True)
                os._exit(37)  # No close, exception handler or Python rollback runs.
            return result

    load_variant(module)(CrashConnection())
    raise RuntimeError("Crash boundary was not reached")


def exercise(database, module, result_path):
    with duckdb.connect(str(database)) as con:
        candidate(con, 4, 1)
        publish(con)
        before = snapshot(con)
        candidate(con, 7, 2)
    print("Synthetic contract: raw = valid + rejected; marts sum to fact rows.", flush=True)
    print("Published: 4 valid rows. Candidate: 7 valid rows. Rejected: 0.", flush=True)
    child = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--crash-worker",
                            "--database", str(database), "--module", str(module)], cwd=ROOT, timeout=30)
    if child.returncode != 37:
        raise RuntimeError(f"Unexpected crash worker status: {child.returncode}")
    with duckdb.connect(str(database)) as con:
        after = snapshot(con)
        changed = [name for name in before if after[name] != before[name]]
        fact_rows = con.execute("select count(*) from main.fct_trips").fetchone()[0]
        mart_rows = con.execute("select sum(trips) from main.mart_hourly_demand").fetchone()[0]
        print(f"After reopen: fact rows = {fact_rows}; hourly mart trips = {mart_rows}.", flush=True)
        print(f"Unchanged consumer relations (including metadata): {9 - len(changed)}/9.", flush=True)
        print("Changed relations: " + (", ".join(changed) or "none"), flush=True)
        result = {"crash_exit_code": child.returncode, "before_digest": digest(before),
                  "after_crash_digest": digest(after), "changed_relations": changed,
                  "fact_rows_after_crash": fact_rows, "mart_trips_after_crash": mart_rows,
                  "all_previous_relations_preserved": not changed, "recovery_verified": False}
        if changed:
            print("FAIL: previous complete dataset was not preserved.", flush=True)
            result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
            return 1
        print("PASS: previous complete dataset survived abrupt process exit.", flush=True)
        recovered = load_variant(module)(con)
        recovered_fact = con.execute("select count(*) from main.fct_trips").fetchone()[0]
        recovered_mart = con.execute("select sum(trips) from main.mart_hourly_demand").fetchone()[0]
        versions = con.execute("select count(*) from audit.publications").fetchone()[0]
        assert recovered_fact == recovered_mart == recovered["fact_rows"] == 7
        assert versions == 2
        result.update(recovery_verified=True, recovered_fact_rows=recovered_fact,
                      recovered_mart_trips=recovered_mart, publication_history_rows=versions)
        print("PASS: retry published 7 fact rows / 7 mart trips; 2 successful versions.", flush=True)
        result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return 0


def record(output):
    output.mkdir(parents=True, exist_ok=True)
    if (output / "evidence.json").exists():
        raise ValueError("Use a new output directory; recorded evidence is immutable")
    original = (ROOT / "taxi_platform/publication.py").read_text(encoding="utf-8")
    broken = original
    for statement in ("begin", "commit", "rollback"):
        old = f'con.execute("{statement}")'
        assert original.count(old) == 1
        broken = broken.replace(old, 'con.execute("select 1")  # TEACHING DEFECT: transaction boundary removed')
    steps = []

    def command(title, caption, args, expected):
        start = time.perf_counter()
        process = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=120)
        output_text = process.stdout + process.stderr
        print(f"{title}: exit {process.returncode}", flush=True)
        if process.returncode != expected:
            raise RuntimeError(output_text)
        steps.append({"title": title, "caption": caption, "exit_code": process.returncode,
                      "elapsed_seconds": round(time.perf_counter() - start, 3), "output": output_text})

    with tempfile.TemporaryDirectory(prefix="nyc-demo-") as temp:
        temp = Path(temp)
        module = temp / "teaching_publication.py"
        module.write_text(broken, encoding="utf-8")
        command("1. Reproduce the failure", "A deliberately broken teaching copy commits each table separately. Stop the child immediately after the fact-table write.",
                [sys.executable, str(Path(__file__).resolve()), "--exercise", "--module", str(module),
                 "--database", str(temp / "broken.duckdb"), "--result", str(temp / "broken.json")], 1)
        broken_result = json.loads((temp / "broken.json").read_text())
        patch = "".join(difflib.unified_diff(broken.splitlines(True), original.splitlines(True),
                                          fromfile="teaching_publication.py (broken)", tofile="teaching_publication.py (fixed)", n=2))
        module.write_text(original, encoding="utf-8")
        assert module.read_text(encoding="utf-8") == original
        steps.append({"title": "2. Restore the transaction", "caption": "This actual file edit restores BEGIN, COMMIT and ROLLBACK in the isolated copy. The repository publisher already contains this fix.",
                      "exit_code": 0, "elapsed_seconds": 0, "output": patch})
        command("3. Crash again, then recover", "The same crash now rolls back all eight tables and publication metadata. Reopen, compare every row, then retry successfully.",
                [sys.executable, str(Path(__file__).resolve()), "--exercise", "--module", str(module),
                 "--database", str(temp / "fixed.duckdb"), "--result", str(temp / "fixed.json")], 0)
        fixed_result = json.loads((temp / "fixed.json").read_text())
        command("4. Run the publisher regressions", "The existing suite also checks invalid candidates, exceptions during copy and before commit, and upgrade from old views.",
                [sys.executable, "-m", "pytest", "-q", "tests/test_publication.py"], 0)
    evidence = {"schema_version": 1, "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                "scope": "AI-assisted synthetic local teaching exercise; not a historical incident, human performance assessment or power-loss guarantee.",
                "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "runtime": {"python": platform.python_version(), "duckdb": duckdb.__version__, "platform": platform.system()},
                "source_sha256": {name: sha((ROOT / name).read_bytes()) for name in (
                    "taxi_platform/publication.py", "taxi_platform/quality.py", "tests/test_publication.py", "scripts/record_publication_demo.py")},
                "teaching_copy_sha256": {"broken": sha(broken.encode()), "fixed": sha(original.encode())},
                "broken": broken_result, "fixed": fixed_result, "steps": steps}
    (output / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    (output / "repair.diff").write_text(patch, encoding="utf-8")
    transcript = "\n\n".join(f'{step["title"]}\n{step["caption"]}\n{step["output"]}\nExit status: {step["exit_code"]}' for step in steps)
    (output / "transcript.txt").write_text(transcript + "\n", encoding="utf-8")
    print(f"Captured evidence: {output}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--exercise", action="store_true")
    parser.add_argument("--crash-worker", action="store_true")
    parser.add_argument("--database", type=Path)
    parser.add_argument("--module", type=Path)
    parser.add_argument("--result", type=Path)
    args = parser.parse_args()
    if args.crash_worker:
        crash_worker(args.database, args.module)
    elif args.exercise:
        return exercise(args.database, args.module, args.result)
    elif args.output:
        record(args.output)
    else:
        parser.error("Provide --output with a fresh directory")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

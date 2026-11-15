"""Build and retain an offline sample workspace. Repeat runs verify and reuse it."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import duckdb

# Ignore full-data overrides before importing config. The child receives only
# settings constructed below; ordinary run_pipeline.py keeps its existing API.
if "--worker" not in sys.argv:
    for _name in ("TAXI_DB", "TAXI_DATA_DIR", "TAXI_DBT_DIR", "TAXI_DUCKDB_MEMORY_LIMIT", "TAXI_MAX_REJECT_RATE"):
        os.environ.pop(_name, None)

from .analyst import ROOT, SAMPLE_INPUTS, check_inputs, export_analyst, verify_analyst, write_json, remove_new_directory
from .release_bundle import sha256
from .revision import model_signature


def worker():
    # No fallback resolver is reachable in this explicit starter. The ordinary
    # full-data pipeline retains its documented download behaviour.
    from . import ingest

    def exact_sample(month):
        if month not in SAMPLE_INPUTS:
            raise ValueError(f"Sample-only command refuses source month {month}")
        check_inputs()
        return ROOT / "data" / "sample" / f"yellow_tripdata_{month}.parquet"

    ingest.resolve_source = exact_sample
    from run_pipeline import run
    success = run(list(SAMPLE_INPUTS))
    check_inputs()
    return 0 if success else 1


def verify_workspace(destination):
    destination = Path(destination)
    try:
        receipt = json.loads((destination / "sample-receipt.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError("Destination is not a complete sample workspace; choose a new --output directory. Existing files preserved.") from exc
    required = {"schema_version", "inputs", "model_sha256", "release_id", "warehouse_sha256", "independent_checks"}
    if not isinstance(receipt, dict) or not required <= receipt.keys() or receipt["schema_version"] != 1:
        raise ValueError("Incomplete sample receipt; choose a new --output directory. Existing files preserved.")
    if receipt["inputs"] != check_inputs() or receipt["model_sha256"] != model_signature():
        raise ValueError("Sample inputs or model changed; choose a new --output directory. Prior workspace preserved.")
    database = destination / "warehouse.duckdb"
    if not database.is_file() or sha256(database) != receipt["warehouse_sha256"]:
        raise ValueError("Sample warehouse changed; choose a new --output directory. Existing files preserved.")
    package = verify_analyst(destination / "analyst")
    with duckdb.connect(str(database), read_only=True) as con:
        version, signature = con.execute("select version, model_sha256 from main.publication").fetchone()
        raw, valid, rejected, fact = [con.execute(f"select count(*) from {table}").fetchone()[0]
                                      for table in ("raw.trips", "main.stg_trips", "main.trips_quarantine", "main.fct_trips")]
    if not (version == package["release_id"] == receipt["release_id"]
            and signature == package["model_sha256"] == receipt["model_sha256"]
            and raw == 30000 == valid + rejected and valid == fact):
        raise ValueError("Sample warehouse identity or independent count check failed")
    return {**receipt, "reused": True, "workspace": str(destination.resolve())}


def build_sample(destination):
    inputs = check_inputs()  # Refuse missing/changed inputs before touching outputs.
    destination = Path(destination).resolve()
    if destination.exists():
        return verify_workspace(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".sample-", dir=destination.parent))
    try:
        dbt = staging / "dbt"
        shutil.copytree(ROOT / "dbt", dbt, ignore=shutil.ignore_patterns("target", "logs", ".user.yml"))
        home = staging / "dagster-home"
        home.mkdir()
        (home / "dagster.yaml").write_text("telemetry:\n  enabled: false\n", encoding="utf-8")
        database = staging / "warehouse.duckdb"
        env = {**os.environ, "TAXI_DB": str(database), "TAXI_DATA_DIR": str(ROOT / "data" / "sample"),
               "TAXI_DBT_DIR": str(dbt), "TAXI_MAX_REJECT_RATE": "0.10", "TAXI_DUCKDB_MEMORY_LIMIT": "1GB",
               "DAGSTER_HOME": str(home), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "NO_COLOR": "1",
               "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", "")}
        with (staging / "pipeline.log").open("w", encoding="utf-8") as log:
            process = subprocess.run([sys.executable, "-m", "taxi_platform.sample", "--worker"],
                                     cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=900)
        if process.returncode:
            details = (staging / "pipeline.log").read_text(encoding="utf-8")[-4000:]
            raise ValueError(f"Sample pipeline failed; previous results preserved. Check installed requirements.\n{details}")
        check_inputs()
        manifest = export_analyst(database, staging / "analyst")
        receipt = {"schema_version": 1, "inputs": inputs, "release_id": manifest["release_id"],
                   "model_sha256": manifest["model_sha256"], "warehouse_sha256": sha256(database),
                   "independent_checks": manifest["reconciliation"], "network_data_downloads": False}
        write_json(staging / "sample-receipt.json", receipt)
        # dbt/Dagster logs contain absolute staging paths and aren't prerequisites
        # for querying the retained warehouse. Keep the single pipeline transcript.
        for temporary in (dbt, home):
            if not temporary.resolve().is_relative_to(staging.resolve()):
                raise ValueError("Refusing cleanup outside the new sample staging directory")
            remove_new_directory(temporary, staging)
        verify_workspace(staging)
        staging.rename(destination)
        return {**receipt, "reused": False, "workspace": str(destination)}
    finally:
        if staging.exists():
            if not staging.resolve().is_relative_to(destination.parent) or not staging.name.startswith(".sample-"):
                raise ValueError("Refusing cleanup outside the new sample staging directory")
            remove_new_directory(staging, destination.parent)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "sample-workspace")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        raise SystemExit(worker())
    try:
        result = build_sample(args.output)
    except (ValueError, OSError, duckdb.Error, subprocess.TimeoutExpired) as exc:
        parser.exit(1, f"Sample refused: {exc}\n")
    print(json.dumps(result, indent=2))
    print("Query retained data: python -m taxi_platform.query boroughs --workspace " + str(args.output))


if __name__ == "__main__":
    main()

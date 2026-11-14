"""Compare a pinned publisher with the working publisher on identical database copies.

This measures publication only, not ingestion/dbt or an end-to-end speedup. Every
run starts from the same checkpointed input file. File copying, fingerprint
verification and the final checkpoint are outside the publication timer. The OS
cache is not cleared. Warmups are retained but excluded from summary statistics.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import duckdb
import psutil

from scripts.benchmark import ResourceSampler
from taxi_platform.publication import TABLES
from taxi_platform.revision import model_signature

BASELINE = "5a8003f3d4c9c9f2ceea9340c9a4484f1f1cf246"


def checksum(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def fingerprints(con, schema):
    """Order-insensitive diagnostics, not cryptographic proof of row equality."""
    result = {}
    for name in TABLES:
        relation = f'{schema}."{name}"'
        n, total, xor = con.execute(f"""select count(*),
            coalesce(sum(cast(hash(t) as hugeint)),0), coalesce(bit_xor(hash(t)),0)
            from {relation} t""").fetchone()
        result[name] = {
            "columns": [list(row[:2]) for row in con.execute(f"describe {relation}").fetchall()],
            "rows": n, "row_hash_sum": str(total), "row_hash_xor": str(xor),
        }
    return result


def summary(runs):
    output = {}
    for variant in ("baseline", "candidate"):
        selected = [run for run in runs if run["variant"] == variant and not run["warmup"]]
        output[variant] = {}
        for metric in ("publication_wall_seconds", "cpu_seconds", "copy_seconds",
                       "peak_process_rss_bytes", "peak_run_directory_bytes", "checkpointed_database_bytes"):
            values = [run[metric] for run in selected]
            output[variant][metric] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
    before = output["baseline"]["publication_wall_seconds"]["median"]
    after = output["candidate"]["publication_wall_seconds"]["median"]
    output["publication_median_reduction_percent"] = round((before - after) / before * 100, 3)
    return output


def worker(args):
    spec = importlib.util.spec_from_file_location("taxi_platform.measured_publication", args.module)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    con = duckdb.connect(str(args.database), config={"memory_limit": args.memory_limit, "threads": args.threads})
    con.execute("set preserve_insertion_order = true")
    timings = {}

    class TimedConnection:
        def execute(self, sql, parameters=None):
            start = time.perf_counter()
            result = con.execute(sql, parameters) if parameters is not None else con.execute(sql)
            match = re.match(r'create or replace table main\."([a-z_]+)"', sql)
            if match:
                timings[match[1]] = time.perf_counter() - start
            return result

    print("READY", flush=True)
    if sys.stdin.readline().strip() != "GO":
        raise RuntimeError("Supervisor did not authorize timing")
    cpu_start = psutil.Process().cpu_times()
    start = time.perf_counter()
    published = module.publish(TimedConnection())
    elapsed = time.perf_counter() - start
    cpu_end = psutil.Process().cpu_times()
    print(json.dumps({"publication_wall_seconds": elapsed, "publication": published,
                      "copy_seconds": published["copy_seconds"], "per_table_copy_seconds": timings,
                      "cpu_seconds": cpu_end.user + cpu_end.system - cpu_start.user - cpu_start.system}), flush=True)
    if sys.stdin.readline().strip() != "VERIFY":
        raise RuntimeError("Supervisor did not end resource sampling")
    expected = json.loads(args.reference.read_text(encoding="utf-8"))
    observed = fingerprints(con, "main")
    settings_restored = con.execute("select current_setting('preserve_insertion_order')").fetchone()[0] is True
    pub = con.execute("select version, row_counts, model_sha256, source_loads from main.publication").fetchall()
    checks = {
        "all_eight_relation_fingerprints_match_candidate": observed == expected["build_fingerprints"],
        "setting_restored": settings_restored,
        "one_current_version": len(pub) == 1,
        "new_publication_version": len(pub) == 1 and pub[0][0] != expected["previous_version"],
        "model_signature_matches": len(pub) == 1 and pub[0][2] == expected["model_sha256"],
        "metadata_counts_match": len(pub) == 1 and json.loads(pub[0][1]) == {
            key: value["rows"] for key, value in observed.items()},
        "source_loads_unchanged": len(pub) == 1 and json.loads(pub[0][3]) == expected["source_loads"],
        "one_audit_version_appended": con.execute("select count(*) from audit.publications").fetchone()[0]
        == expected["audit_versions"] + 1,
    }
    started = time.perf_counter()
    con.execute("checkpoint")
    checkpoint_seconds = time.perf_counter() - started
    con.close()
    print(json.dumps({"checks": checks, "published_fingerprints": observed,
                      "checkpoint_seconds_outside_timer": checkpoint_seconds,
                      "checkpointed_database_bytes": args.database.stat().st_size}), flush=True)
    return 0 if all(checks.values()) else 1


def benchmark(args):
    output = args.output.resolve()
    if output.exists():
        raise ValueError("Output exists; choose a new immutable evidence filename")
    database = args.database.resolve()
    if not database.is_file() or Path(str(database) + ".wal").exists():
        raise ValueError("Use an existing closed, checkpointed database without a WAL")
    if args.repeats < 2 or args.warmups < 1:
        raise ValueError("Use at least two pairs and one warmup pair")
    if not re.fullmatch(r"[0-9a-f]{40}", args.baseline):
        raise ValueError("Baseline must be a full immutable Git commit")
    # Relative imports use current helpers: prove they are unchanged from baseline.
    dependencies = ("taxi_platform/config.py", "taxi_platform/ingest.py", "taxi_platform/quality.py",
                    "taxi_platform/revision.py")
    for path in dependencies:
        original = subprocess.check_output(["git", "show", f"{args.baseline}:{path}"], cwd=ROOT)
        if original.replace(b"\r\n", b"\n") != (ROOT / path).read_bytes().replace(b"\r\n", b"\n"):
            raise ValueError(f"Baseline helper changed: {path}")
    baseline = subprocess.check_output(["git", "show", f"{args.baseline}:taxi_platform/publication.py"], cwd=ROOT)
    candidate = (ROOT / "taxi_platform/publication.py").read_bytes()
    initial_sha = checksum(database)
    with duckdb.connect(str(database), read_only=True) as con:
        pub = con.execute("select version, model_sha256, source_loads from main.publication").fetchone()
        reference = {"build_fingerprints": fingerprints(con, "build"), "previous_version": pub[0],
                     "model_sha256": pub[1], "source_loads": json.loads(pub[2]),
                     "audit_versions": con.execute("select count(*) from audit.publications").fetchone()[0],
                     "raw_rows": con.execute("select count(*) from raw.trips").fetchone()[0]}
        source_rows = con.execute("""select source_month, source_file, source_sha256, source_bytes, rows
            from audit.source_loads order by source_month, loaded_at, load_id""").fetchall()
        source_inputs = [dict(zip(("month", "file", "sha256", "bytes", "rows"), row)) for row in source_rows]
    if reference["model_sha256"] != model_signature():
        raise ValueError("Prepared database model signature differs from current model files")
    record = {
        "schema_version": 1, "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "local single-writer publication only; same checkpointed database copied before every run",
        "baseline_commit": args.baseline,
        "baseline_publisher_sha256": hashlib.sha256(baseline.replace(b"\r\n", b"\n")).hexdigest(),
        "candidate_publisher_sha256": hashlib.sha256(candidate.replace(b"\r\n", b"\n")).hexdigest(),
        "runner_sha256": checksum(Path(__file__)),
        "dependency_sha256": {path: checksum(ROOT / path) for path in dependencies},
        "source_inputs": source_inputs,
        "prepared_database": {"sha256": initial_sha, "bytes": database.stat().st_size, **reference},
        "conditions": {"duckdb_memory_limit": args.memory_limit, "threads": args.threads,
                       "initial_preserve_insertion_order": True, "repeats_per_variant": args.repeats,
                       "warmups_per_variant": args.warmups, "os_cache": "not cleared; file copy warms OS cache",
                       "excluded_from_timing": ["database preparation/copy/hash", "result verification", "explicit checkpoint"],
                       "host": "shared local workstation; not a dedicated or guaranteed idle benchmark host"},
        "environment": {"python": platform.python_version(), "duckdb": duckdb.__version__,
                        "platform": platform.platform(), "cpu": platform.processor(),
                        "logical_cpus": os.cpu_count(), "host_memory_bytes": psutil.virtual_memory().total},
        "fingerprint_limit": "count/schema plus sum and XOR of DuckDB row hashes: order-insensitive, noncryptographic; same engine version only",
        "success": False, "runs": [],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    work_root = ROOT / "data/benchmark_work"
    work_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="publication-", dir=work_root) as directory:
        work = Path(directory)
        baseline_path = work / "baseline.py"
        baseline_path.write_bytes(baseline)
        candidate_path = work / "candidate.py"
        candidate_path.write_bytes(candidate)
        reference_path = work / "reference.json"
        reference_path.write_text(json.dumps(reference), encoding="utf-8")
        for pair in range(args.warmups + args.repeats):
            order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
            for variant in order:
                with tempfile.TemporaryDirectory(prefix="run-", dir=work) as run_dir:
                    run_path = Path(run_dir)
                    trial = run_path / "warehouse.duckdb"
                    shutil.copyfile(database, trial)
                    if checksum(trial) != initial_sha:
                        raise RuntimeError("Database copy differs from prepared input")
                    module = baseline_path if variant == "baseline" else candidate_path
                    command = [sys.executable, str(Path(__file__).resolve()), "--worker", "--database", str(trial),
                               "--module", str(module), "--reference", str(reference_path),
                               "--memory-limit", args.memory_limit, "--threads", str(args.threads)]
                    process = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE, text=True)
                    deadline = threading.Timer(600, process.kill)
                    deadline.daemon = True
                    deadline.start()
                    sampler = None
                    try:
                        if process.stdout.readline().strip() != "READY":
                            raise RuntimeError(process.stderr.read())
                        sampler = ResourceSampler(process.pid, run_path)
                        sampler.start()
                        process.stdin.write("GO\n")
                        process.stdin.flush()
                        timed = json.loads(process.stdout.readline())
                        resources = sampler.finish()
                        process.stdin.write("VERIFY\n")
                        process.stdin.flush()
                        verified = json.loads(process.stdout.readline())
                        _, errors = process.communicate(timeout=600)
                        if process.returncode != 0:
                            raise RuntimeError(errors or str(verified))
                    finally:
                        deadline.cancel()
                        if sampler is not None and not sampler.stop.is_set():
                            sampler.finish()
                        if process.poll() is None:
                            process.kill()
                            process.wait()
                    record["runs"].append({"pair": pair, "warmup": pair < args.warmups, "variant": variant,
                                           **timed, **verified, "resources": resources,
                                           "peak_process_rss_bytes": resources["peak_summed_process_rss_bytes"],
                                           "peak_run_directory_bytes": resources["peak_working_directory_bytes"]})
                    print(f"{'warmup' if pair < args.warmups else 'measured'} {pair} {variant}: "
                          f"{timed['publication_wall_seconds']:.3f}s; checks passed", flush=True)
                    output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    record["prepared_database_unchanged"] = checksum(database) == initial_sha
    record["dependencies_unchanged"] = all(checksum(ROOT / path) == expected
                                           for path, expected in record["dependency_sha256"].items())
    record["success"] = record["prepared_database_unchanged"] and record["dependencies_unchanged"] and all(
        all(run["checks"].values()) for run in record["runs"])
    record["summary"] = summary(record["runs"])
    record["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(record["summary"], indent=2))
    return 0 if record["success"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--baseline", default=BASELINE)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--memory-limit", default="4GB")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--module", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--reference", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        return worker(args)
    if args.output is None or args.threads < 1:
        parser.error("--output and a positive --threads value are required")
    return benchmark(args)


if __name__ == "__main__":
    raise SystemExit(main())

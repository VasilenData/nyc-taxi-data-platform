"""Measure an isolated pipeline run and retain its inputs, environment, checks and log.

Downloads and source checksums happen before timing. Every run uses a new warehouse;
the OS page cache is not cleared. Only the explicitly selected files are accepted,
so a full-data run cannot silently fall back to the bundled January/February samples.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

import duckdb

ROOT = Path(__file__).resolve().parents[1]
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def read_text(path: str) -> str | None:
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def environment() -> dict:
    cpuinfo = read_text("/proc/cpuinfo") or ""
    cpu_model = next((line.split(":", 1)[1].strip() for line in cpuinfo.splitlines()
                      if line.startswith("model name")), platform.processor())
    meminfo = read_text("/proc/meminfo") or ""
    memory_kib = next((int(line.split()[1]) for line in meminfo.splitlines()
                       if line.startswith("MemTotal:")), None)
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_model": cpu_model,
        "logical_cpus": os.cpu_count(),
        "cpu_affinity_count": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "host_memory_bytes": memory_kib * 1024 if memory_kib else None,
        "cgroup_memory_max": read_text("/sys/fs/cgroup/memory.max"),
        "cgroup_cpu_max": read_text("/sys/fs/cgroup/cpu.max"),
        "packages": dict(sorted((dist.metadata["Name"], dist.version)
                                for dist in importlib.metadata.distributions())),
    }


def source_revision() -> dict:
    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()
    paths = [ROOT / "run_pipeline.py", ROOT / "scripts" / "benchmark.py"]
    for folder in ("taxi_platform", "dbt"):
        paths.extend(path for path in (ROOT / folder).rglob("*")
                     if path.suffix in {".py", ".sql", ".yml", ".csv"}
                     and not {"target", "logs", "__pycache__"}.intersection(path.parts))
    return {
        "base_commit": git("rev-parse", "HEAD"),
        "working_tree_dirty": bool(git("status", "--porcelain")),
        "source_sha256": {str(path.relative_to(ROOT)): sha256(path) for path in sorted(set(paths))},
    }


def prepare_input(month: str, data_dir: Path, download: bool) -> dict:
    source = data_dir / f"yellow_tripdata_{month}.parquet"
    downloaded = False
    download_seconds = 0.0
    if not source.is_file():
        if not download:
            raise FileNotFoundError(f"Missing {source}; provide all selected files or use --download with --dataset full")
        temporary = source.with_suffix(".download")
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(TLC_URL.format(month=month), timeout=120) as response:
                with temporary.open("wb") as output:
                    shutil.copyfileobj(response, output)
            temporary.replace(source)
        finally:
            temporary.unlink(missing_ok=True)
        downloaded = True
        download_seconds = time.perf_counter() - started
    with duckdb.connect() as con:
        rows = con.execute("select count(*) from read_parquet(?)", [str(source)]).fetchone()[0]
    result = {
        "month": month, "file": source.name, "bytes": source.stat().st_size,
        "rows": rows, "sha256": sha256(source), "downloaded_this_run": downloaded,
        "download_seconds": round(download_seconds, 3),
    }
    if download:
        result["download_url"] = TLC_URL.format(month=month)
    print(f"Prepared {month}: {rows:,} rows", flush=True)
    return result


def inspect_warehouse(database: Path, expected_rows: int) -> dict:
    with duckdb.connect(str(database), read_only=True) as con:
        counts = {label: con.execute(f"select count(*) from {table}").fetchone()[0]
                  for label, table in {
                      "raw": "raw.trips", "valid": "main.stg_trips",
                      "quarantined": "main.trips_quarantine", "fact": "main.fct_trips",
                  }.items()}
        reasons = dict(con.execute("select reject_reason, count(*) from main.trips_quarantine group by 1").fetchall())
        by_month = con.execute(
            "select source_month, sum(rejected_rows), max(raw_rows) "
            "from main.mart_data_quality group by 1 order by 1"
        ).fetchall()
    return {
        "rows": counts,
        "rejection_reasons": reasons,
        "rejected_share": counts["quarantined"] / counts["raw"] if counts["raw"] else None,
        "months": [{"month": month, "rejected_rows": rejected, "raw_rows": raw}
                   for month, rejected, raw in by_month],
        "reconciliation": {
            "raw_equals_input_rows": counts["raw"] == expected_rows,
            "raw_equals_valid_plus_quarantined": counts["raw"] == counts["valid"] + counts["quarantined"],
            "valid_equals_fact": counts["valid"] == counts["fact"],
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=("sample", "full"), default="sample")
    parser.add_argument("--months", nargs="+", default=["2023-01", "2023-02"])
    parser.add_argument("--data-dir", type=Path, help="Full Parquet input directory; sample mode uses bundled files")
    parser.add_argument("--download", action="store_true", help="Download missing full files before the timed phase")
    parser.add_argument("--memory-limit", default="4GB",
                        help="Per-process DuckDB limit for ingestion and dbt, not a total process-memory cap")
    parser.add_argument("--timeout", type=int, default=900, help="Maximum timed pipeline seconds")
    parser.add_argument("--output", type=Path, required=True, help="New measurement JSON file; log is saved beside it")
    args = parser.parse_args()
    if not all(MONTH_RE.fullmatch(month) for month in args.months) or len(set(args.months)) != len(args.months):
        parser.error("months must be unique YYYY-MM values")
    if args.dataset == "sample" and (args.download or args.data_dir):
        parser.error("--data-dir and --download apply only to --dataset full")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    output = args.output.resolve()
    if output.suffix.lower() != ".json":
        parser.error("--output must have a .json extension so the measurement and .log remain separate")
    log_path = output.with_suffix(".log")
    if output.exists() or log_path.exists() or log_path.is_symlink():
        parser.error("output or log already exists; choose a new filename")
    data_dir = ((args.data_dir or ROOT / "data" / "raw") if args.dataset == "full"
                else ROOT / "data" / "sample").resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    work_root = ROOT / "data" / "benchmark_work"
    work_root.mkdir(parents=True, exist_ok=True)
    prepared_at = datetime.now(timezone.utc).isoformat()
    prep_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=3) as pool:
        inputs = list(pool.map(lambda month: prepare_input(month, data_dir, args.download), args.months))
    preparation_seconds = time.perf_counter() - prep_start
    record = {
        "schema_version": 1, "prepared_at_utc": prepared_at,
        "dataset": args.dataset, "months": args.months, "inputs": inputs,
        "input_rows": sum(source["rows"] for source in inputs),
        "environment": environment(), "source": source_revision(),
        "conditions": {
            "warehouse": "new empty database for this run",
            "source_downloads_in_timing": False,
            "source_checksums_in_timing": False,
            "os_page_cache": "not cleared; inputs read for SHA-256 before timing",
            "dbt_manifest_present_at_start": (ROOT / "dbt" / "target" / "manifest.json").exists(),
            "duckdb_memory_limit": args.memory_limit,
            "dbt_threads": 4,
            "timed_scope": "Python startup, monthly ingestion, dbt build with tests, Dagster asset checks, transactional publication",
            "preparation_seconds": round(preparation_seconds, 3),
        },
        "command": ["python", "run_pipeline.py", *args.months],
        "log_file": log_path.name,
    }
    with tempfile.TemporaryDirectory(prefix="run-", dir=work_root) as temporary:
        work = Path(temporary)
        database = work / "warehouse.duckdb"
        env = os.environ.copy()
        env.update({"TAXI_DB": str(database), "TAXI_DATA_DIR": str(data_dir),
                    "TAXI_DUCKDB_MEMORY_LIMIT": args.memory_limit,
                    "PATH": str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", ""),
                    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "NO_COLOR": "1"})
        record["started_at_utc"] = datetime.now(timezone.utc).isoformat()
        print(f"Running {args.dataset} benchmark on {record['input_rows']:,} rows; log: {log_path.name}", flush=True)
        started = time.perf_counter()
        timed_out = False
        with log_path.open("w") as log:
            process = subprocess.Popen([sys.executable, "run_pipeline.py", *args.months], cwd=ROOT,
                                       env=env, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=os.name == "posix")
            try:
                process.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGTERM)
                else:
                    process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    if os.name == "posix":
                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
                    process.wait()
        record.update({"finished_at_utc": datetime.now(timezone.utc).isoformat(),
                       "wall_seconds": round(time.perf_counter() - started, 3),
                       "exit_code": process.returncode, "timed_out": timed_out})
        log_text = ANSI_RE.sub("", log_path.read_text()).replace(str(work), "<BENCHMARK_WORKDIR>")
        log_text = log_text.replace(str(ROOT), "<REPOSITORY>")
        log_path.write_text(log_text)
        record["log_sha256"] = sha256(log_path)
        record["asset_checks"] = dict(re.findall(r"^check\s+(\S+)\s+(PASS|FAIL)$", log_text, re.MULTILINE))
        try:
            record["warehouse"] = inspect_warehouse(database, record["input_rows"])
        except duckdb.Error as exc:
            record["warehouse_inspection_error"] = str(exc).replace(str(work), "<BENCHMARK_WORKDIR>")
        expected_checks = {"reconciliation", "reject_rate", "coverage"}
        reconciliation = record.get("warehouse", {}).get("reconciliation", {})
        record["success"] = (process.returncode == 0 and not timed_out
                             and expected_checks <= record["asset_checks"].keys()
                             and set(record["asset_checks"].values()) == {"PASS"}
                             and bool(reconciliation) and all(reconciliation.values()))
    output.write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps({"measurement": str(output.relative_to(ROOT)) if output.is_relative_to(ROOT) else str(output),
                      "success": record["success"], "wall_seconds": record["wall_seconds"],
                      "rows": record.get("warehouse", {}).get("rows")}, indent=2))
    return 0 if record["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

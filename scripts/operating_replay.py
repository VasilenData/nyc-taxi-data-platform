"""Run bounded historical sample replays; record actual clocks, failures and recovery."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import threading
import time

import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from taxi_platform.operations import (check_and_deliver, consumer_snapshot, identifier,
                                     immutable_json, plan_slot, read_json, timestamp, utcnow)

MONTHS = ("2023-01", "2023-02")
LOG_LIMIT = 65536


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def source_identity():
    paths = ["scripts/operating_replay.py", "taxi_platform/operations.py",
             "taxi_platform/publication.py", "taxi_platform/ingest.py",
             "taxi_platform/quality.py", "taxi_platform/revision.py", "taxi_platform/config.py",
             "scripts/check_replay_schedule.py", "operations/replay-window.json", "requirements.txt"]
    paths += [path.relative_to(ROOT).as_posix() for path in sorted((ROOT / "dbt").rglob("*"))
              if path.is_file() and path.suffix in {".sql", ".yml", ".csv"}
              and "target" not in path.parts and "logs" not in path.parts and path.name != ".user.yml"]
    return {path: hashlib.sha256((ROOT / path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for path in paths}


def terminate_tree(pid):
    """Freeze trusted child trees before killing them, including Windows launchers."""
    processes = {}
    try:
        root = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return []
    pending = [root]
    try:
        while pending:
            process = pending.pop()
            if process.pid in processes:
                continue
            processes[process.pid] = process
            try:
                process.suspend()
                pending.extend(process.children())
            except psutil.NoSuchProcess:
                continue
    finally:
        for process in reversed(list(processes.values())):
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
    _, alive = psutil.wait_procs(list(processes.values()), timeout=10)
    if alive:
        raise RuntimeError("Timed-out worker tree could not be confirmed stopped")
    return sorted(processes)


def bounded_command(command, log, env, timeout):
    """Drain output, cap retained bytes, terminate/confirm timed-out child trees."""
    total = 0
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("xb") as stream:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        def drain():
            nonlocal total
            while chunk := process.stdout.read(8192):
                remaining = max(0, LOG_LIMIT - total)
                stream.write(chunk[:remaining])
                total += len(chunk)

        thread = threading.Thread(target=drain, daemon=True)
        thread.start()
        timed_out = False
        terminated = []
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            terminated = terminate_tree(process.pid)
            process.wait(timeout=10)
        thread.join(timeout=10)
        if thread.is_alive():
            raise RuntimeError("Child output stream did not close")
    return {"exit_code": process.returncode, "timed_out": timed_out, "process_id": process.pid,
            "terminated_process_ids": terminated,
            "log_bytes_emitted": total, "log_bytes_retained": log.stat().st_size,
            "log_truncated": total > LOG_LIMIT, "log_sha256": sha(log)}


def phase_worker(phase, database, output, request, fault):
    import duckdb
    from taxi_platform import ingest
    from taxi_platform.publication import publish

    try:
        with ingest.connect(database) as con:
            con.execute("set TimeZone = 'UTC'")
            if phase == "ingest":
                rows = []
                for month in MONTHS:
                    path = ROOT / "data/sample" / f"yellow_tripdata_{month}.parquet"
                    if not path.is_file():
                        raise ValueError("Bundled sample missing; downloads are forbidden in this replay")
                    rows.append(ingest.load_month(month, con, expected_sha256=request["inputs"][month]))
                if sum(row["rows"] for row in rows) != 30000:
                    raise ValueError("Expected exactly the bundled 30,000-row workload")
                payload = {"status": "success", "completed_at_utc": utcnow().isoformat(), "loads": rows,
                           "raw_rows": con.execute("select count(*) from raw.trips").fetchone()[0]}
            else:
                if fault:
                    con.execute("update build.mart_hourly_demand set trips = 0")
                    print("Controlled fault: set candidate hourly-mart trip counts to zero.", flush=True)
                payload = {"status": "success", "completed_at_utc": None, "publication": publish(con)}
                payload["completed_at_utc"] = utcnow().isoformat()
        payload["duckdb_version"] = duckdb.__version__
        payload["worker_pid"] = os.getpid()
        immutable_json(output, payload)
        return 0
    except Exception as error:
        immutable_json(output, {"status": "failed", "completed_at_utc": utcnow().isoformat(),
                                "worker_pid": os.getpid(), "error_type": type(error).__name__, "detail": str(error)[:500]})
        print(f"{phase} failed: {type(error).__name__}: {str(error)[:500]}", flush=True)
        return 1


def run_attempt(history, database, slot_id, attempt_id, *, fault=False):
    history, database = Path(history).resolve(), Path(database).resolve()
    identifier(slot_id)
    identifier(attempt_id)
    plan = read_json(history / "plans" / f"{slot_id}.json")
    directory = history / "attempts" / slot_id / attempt_id
    request = {"slot_id": slot_id, "attempt_id": attempt_id, "controlled_fault": fault,
               "database": database.name, "inputs": {month: sha(ROOT / "data/sample" / f"yellow_tripdata_{month}.parquet")
                                                      for month in MONTHS},
               "source_sha256_lf": source_identity(), "plan_sha256": sha(history / "plans" / f"{slot_id}.json")}
    if (directory / "started.json").exists():
        started = read_json(directory / "started.json")
        if started["request"] != request:
            raise ValueError("Attempt identity already belongs to a different request")
        if (directory / "result.json").exists():
            return read_json(directory / "result.json")
        raise ValueError("Attempt has no terminal record; inspect it, then use a new attempt ID")
    started_at = utcnow()
    clock = time.perf_counter()
    immutable_json(directory / "started.json", {"slot_id": slot_id, "attempt_id": attempt_id,
                   "started_at_utc": started_at.isoformat(), "request": request})
    before, preflight_error = None, None
    try:
        before = consumer_snapshot(database)
    except Exception as error:
        preflight_error = type(error).__name__
    env = {**os.environ, "TAXI_DB": str(database), "TAXI_DATA_DIR": str(ROOT / "data/sample"),
           "TAXI_DBT_DIR": str(ROOT / "dbt"), "TAXI_DUCKDB_MEMORY_LIMIT": "2GB",
           "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
           "PYTHONPATH": os.pathsep.join([str(ROOT), *[p for p in sys.path if "site-packages" in p]])}
    request_path = directory / "started.json"
    steps, classification, detail = {}, "ingestion_failed", "Ingestion did not complete"
    ingestion_elapsed = None
    published = None
    interpreter = getattr(sys, "_base_executable", sys.executable)
    command = [interpreter, str(Path(__file__).resolve()), "worker", "--database", str(database),
               "--request", str(request_path)]
    with tempfile.TemporaryDirectory(prefix="nyc-replay-dbt-") as temporary:
        try:
            if preflight_error:
                classification = "warehouse_unavailable"
                raise RuntimeError(f"Preflight warehouse read failed: {preflight_error}")
            steps["ingest"] = bounded_command(command + ["--phase", "ingest", "--output", str(directory / "ingested.json")],
                                               directory / "ingest.log", env, plan["ingestion_budget_seconds"])
            ingestion_elapsed = time.perf_counter() - clock
            if steps["ingest"]["timed_out"]:
                classification = "process_timeout"
                raise RuntimeError("Ingestion child exceeded its budget")
            if steps["ingest"]["exit_code"]:
                raise RuntimeError(read_json(directory / "ingested.json").get("detail", detail))
            classification, detail = "transformation_failed", "dbt build did not complete"
            steps["transform"] = bounded_command(
                [interpreter, "-c", "from dbt.cli.main import cli; cli()", "build", "--full-refresh", "--project-dir", str(ROOT / "dbt"),
                 "--profiles-dir", str(ROOT / "dbt"), "--target-path", str(Path(temporary) / "target"),
                 "--log-path", str(Path(temporary) / "logs")], directory / "transform.log", env, 240)
            if steps["transform"]["timed_out"]:
                classification = "process_timeout"
                raise RuntimeError("dbt child exceeded its 240-second budget")
            if steps["transform"]["exit_code"]:
                raise RuntimeError(detail)
            dbt_result = read_json(Path(temporary) / "target/run_results.json")
            statuses = [result["status"] for result in dbt_result["results"]]
            if not statuses or any(status not in {"success", "pass", "warn"} for status in statuses):
                raise RuntimeError("dbt exit did not agree with model/test results")
            immutable_json(directory / "transformed.json", {"completed_at_utc": utcnow().isoformat(),
                            "result_counts": {status: statuses.count(status) for status in sorted(set(statuses))}})
            classification, detail = "publication_failed", "Publication did not complete"
            flags = ["--controlled-fault"] if fault else []
            steps["publish"] = bounded_command(command + ["--phase", "publish", "--output", str(directory / "publication.json"), *flags],
                                                directory / "publish.log", env, 60)
            if steps["publish"]["timed_out"]:
                classification = "process_timeout"
                raise RuntimeError("Publication child exceeded its 60-second budget")
            event = read_json(directory / "publication.json")
            if steps["publish"]["exit_code"]:
                detail = event.get("detail", detail)
                if detail.startswith("Publication refused:"):
                    classification = "publication_refused"
                raise RuntimeError(detail)
            published = consumer_snapshot(database)
            if not published or published["version"] != event["publication"]["version"]:
                raise RuntimeError("Read-only snapshot disagrees with publication result")
            classification, detail = "completed", "All eight consumer tables and metadata published"
        except Exception as error:
            detail = str(error)[:500]
    snapshot_error = None
    try:
        after = consumer_snapshot(database)
    except Exception as error:
        after, snapshot_error = None, type(error).__name__
        if classification == "completed":
            classification, detail = "warehouse_unavailable", "Post-run warehouse snapshot failed"
    result = {"slot_id": slot_id, "attempt_id": attempt_id,
              "started_at_utc": started_at.isoformat(), "finished_at_utc": utcnow().isoformat(),
              "elapsed_seconds": time.perf_counter() - clock,
              "ingestion_elapsed_seconds": ingestion_elapsed,
              "status": "success" if classification == "completed" else "failed",
              "classification": classification, "detail": detail,
              "controlled_fault": fault, "steps": steps, "published": published,
              "previous_complete_output_preserved": None if preflight_error or snapshot_error else before == after,
              "preflight_snapshot_error": preflight_error, "post_run_snapshot_error": snapshot_error,
              "before": before, "after": after}
    immutable_json(directory / "result.json", result)
    return result


def independent_check(history, database, check_id):
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), *[p for p in sys.path if "site-packages" in p]])}
    process = subprocess.run([getattr(sys, "_base_executable", sys.executable), str(Path(__file__).resolve()), "check", "--history", str(history),
                              "--database", str(database), "--check-id", check_id], cwd=ROOT,
                             env=env, capture_output=True, text=True, timeout=60)
    if process.returncode not in (0, 2):
        raise RuntimeError(process.stderr[-2000:])
    return read_json(Path(history) / "checks" / f"{check_id}.json")


def drill(history, database):
    history = Path(history).resolve()
    if history.exists() or Path(database).exists():
        raise ValueError("Use fresh history and database paths; past exercises are immutable")
    plan_slot(history, "baseline", utcnow().isoformat())
    baseline = run_attempt(history, database, "baseline", "initial")
    if baseline["status"] != "success":
        raise RuntimeError(baseline["detail"])
    independent_check(history, database, "baseline")
    plan_slot(history, "incident", utcnow().isoformat())
    failed = run_attempt(history, database, "incident", "fault", fault=True)
    repeated_attempt = run_attempt(history, database, "incident", "fault", fault=True)
    detected = independent_check(history, database, "failure")
    repeated = independent_check(history, database, "repeat-detection")
    recovered = run_attempt(history, database, "incident", "recovery")
    clear = independent_check(history, database, "recovery")
    checks = {"controlled_fault_refused": failed["classification"] == "publication_refused",
              "same_attempt_id_reuses_immutable_result": failed == repeated_attempt,
              "all_nine_previous_relations_preserved": failed["previous_complete_output_preserved"],
              "separate_checker_detected_failure": detected["failed_or_overdue_slots"] == 1,
              "failure_delivered_to_local_inbox": len(detected["deliveries"]) == 1 and detected["deliveries"][0]["delivered"],
              "repeat_detection_reused_delivery": detected["deliveries"] == repeated["deliveries"],
              "recovery_published": recovered["status"] == "success",
              "both_expected_slots_completed": clear["expected_slots"] == clear["completed_slots"] == 2,
              "checker_verified_recovered_warehouse": clear["warehouse_matches_latest_success"] and clear["failed_or_overdue_slots"] == 0,
              "recovery_notice_delivered": len(clear["deliveries"]) == 1 and clear["deliveries"][0]["delivered"]}
    evidence = {"schema_version": 1, "recorded_at_utc": utcnow().isoformat(),
                "scope": "One actual local 30k-row historical replay exercise with a controlled candidate fault; not multiweek operation or a production incident",
                "source_sha256_lf": source_identity(), "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "runtime": {"python": platform.python_version(), "platform": platform.platform()},
                "checks": checks, "success": all(checks.values()),
                "attempts": [{key: result[key] for key in ("slot_id", "attempt_id", "started_at_utc", "finished_at_utc", "elapsed_seconds", "status", "classification")}
                             for result in (baseline, failed, recovered)],
                "detection_seconds_after_failure": (timestamp(detected["checked_at_utc"]) - timestamp(failed["finished_at_utc"])).total_seconds(),
                "inbox": detected["deliveries"] + clear["deliveries"],
                "expected_slots": clear["expected_slots"], "completed_slots": clear["completed_slots"],
                "artifacts": {path.relative_to(history).as_posix(): sha(path) for path in sorted(history.rglob("*")) if path.is_file()}}
    immutable_json(history / "evidence.json", evidence)
    if not evidence["success"]:
        raise RuntimeError("Operating exercise acceptance failed")
    print(json.dumps({"success": True, "evidence": str(history / "evidence.json"), "checks": checks}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("drill", "run", "check", "worker"))
    parser.add_argument("--history", type=Path)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--slot-id", default="manual")
    parser.add_argument("--attempt-id", default="initial")
    parser.add_argument("--due-at")
    parser.add_argument("--scheduled", action="store_true")
    parser.add_argument("--check-id")
    parser.add_argument("--phase", choices=("ingest", "publish"))
    parser.add_argument("--request", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--controlled-fault", action="store_true")
    args = parser.parse_args()
    if args.command == "worker":
        return phase_worker(args.phase, args.database, args.output, read_json(args.request)["request"], args.controlled_fault)
    if args.history is None:
        parser.error("--history is required")
    if args.command == "drill":
        drill(args.history, args.database)
        return 0
    if args.command == "check":
        report = check_and_deliver(args.history, args.database, args.check_id)
        print(json.dumps(report))
        return 2 if report["failed_or_overdue_slots"] else 0
    previous_plan = args.history / "plans" / f"{identifier(args.slot_id)}.json"
    due = args.due_at or (read_json(previous_plan)["due_at_utc"] if previous_plan.exists() else utcnow().isoformat())
    plan_slot(args.history, args.slot_id, due,
              kind="scheduled-historical-replay" if args.scheduled else "manual-exercise")
    result = run_attempt(args.history, args.database, args.slot_id, args.attempt_id, fault=args.controlled_fault)
    print(json.dumps({key: result[key] for key in ("status", "classification", "elapsed_seconds")}))
    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())

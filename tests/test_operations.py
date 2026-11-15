"""Clock-controlled monitor properties; no simulated dates are operating history."""
from datetime import datetime, timedelta, timezone
import os
import sys
import subprocess
import time

import duckdb
import pytest

from scripts.operating_replay import LOG_LIMIT, bounded_command
from scripts import operating_replay as replay
from scripts.record_publication_demo import candidate
from taxi_platform import operations as ops
from taxi_platform.publication import publish

NOW = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)


def record(history, slot, attempt, seconds, snapshot, status="success"):
    directory = history / "attempts" / slot / attempt
    started = (NOW - timedelta(seconds=seconds)).isoformat()
    ops.immutable_json(directory / "started.json", {"slot_id": slot, "attempt_id": attempt, "started_at_utc": started})
    result = {"slot_id": slot, "attempt_id": attempt, "started_at_utc": started,
              "finished_at_utc": (NOW - timedelta(seconds=seconds - 1)).isoformat(),
              "status": status, "classification": "completed" if status == "success" else "publication_refused",
              "elapsed_seconds": 1, "ingestion_elapsed_seconds": .1,
              "published": snapshot if status == "success" else None}
    ops.immutable_json(directory / "result.json", result)


@pytest.fixture
def history(tmp_path, monkeypatch):
    monkeypatch.setattr(ops, "utcnow", lambda: NOW)
    root, database = tmp_path / "history", tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as con:
        candidate(con, 4, 1)
        publish(con)
    ops.plan_slot(root, "cycle", (NOW - timedelta(seconds=60)).isoformat())
    snapshot = ops.consumer_snapshot(database)
    record(root, "cycle", "first", 50, snapshot)
    return root, database, snapshot


def test_immutable_files_are_idempotent_and_refuse_replacement(tmp_path):
    path = tmp_path / "one.json"
    digest = ops.immutable_json(path, {"a": 1})
    assert digest == ops.immutable_json(path, {"a": 1})
    with pytest.raises(ValueError, match="immutable"):
        ops.immutable_json(path, {"a": 2})
    assert ops.read_json(path) == {"a": 1}
    assert list(tmp_path.iterdir()) == [path]


def test_delivery_timestamp_records_verified_write_not_detection_start(tmp_path, monkeypatch):
    alert = {"slot_id": "cycle", "attempt_id": "a", "state": "publication_failed",
             "observed_at_utc": (NOW - timedelta(seconds=2)).isoformat()}
    monkeypatch.setattr(ops, "utcnow", lambda: NOW)
    receipt = ops.local_delivery(tmp_path, alert)
    assert receipt["delivered_at_utc"] == NOW.isoformat()
    monkeypatch.setattr(ops, "utcnow", lambda: NOW + timedelta(seconds=5))
    assert ops.local_delivery(tmp_path, alert) == receipt


@pytest.mark.parametrize("slot,due", [("../escape", NOW.isoformat()), ("safe", "2026-01-01T00:00:00")])
def test_plan_requires_safe_ids_and_explicit_utc(tmp_path, slot, due):
    with pytest.raises(ValueError):
        ops.plan_slot(tmp_path, slot, due)


def test_missing_run_is_detected_without_any_pipeline_record(tmp_path):
    ops.plan_slot(tmp_path, "never-started", (NOW - timedelta(hours=4)).isoformat())
    report = ops.assess(tmp_path, tmp_path / "missing.duckdb", observed_at=NOW)
    assert report["expected_slots"] == 1 and report["completed_slots"] == 0
    assert report["slots"][0]["state"] == "missing_run"
    assert report["failed_or_overdue_slots"] == 1


def test_latest_failure_is_not_hidden_by_previous_success(history):
    root, database, snapshot = history
    record(root, "cycle", "second", 20, snapshot, "failed")
    report = ops.assess(root, database, observed_at=NOW)
    assert report["completed_slots"] == 0
    assert report["ever_completed_slots"] == 1
    assert report["warehouse_matches_latest_success"]
    assert report["slots"][0]["state"] == "publication_refused"


def test_repeated_detection_and_later_healthy_retries_do_not_duplicate_recovery(history):
    root, database, snapshot = history
    record(root, "cycle", "failed", 30, snapshot, "failed")
    first = ops.check_and_deliver(root, database, "one")
    second = ops.check_and_deliver(root, database, "two")
    assert first["deliveries"] == second["deliveries"]
    incident = first["deliveries"][0]["alert_id"]
    record(root, "cycle", "fixed", 20, snapshot)
    recovered = ops.check_and_deliver(root, database, "three")
    notice = ops.read_json(root / recovered["deliveries"][0]["artifact"])
    assert notice["state"] == "recovered" and notice["resolved_alert_ids"] == [incident]
    record(root, "cycle", "another-healthy-retry", 10, snapshot)
    assert ops.check_and_deliver(root, database, "four")["deliveries"] == []
    assert len(list((root / "inbox").glob("*.json"))) == 2


def test_warehouse_drift_can_recover_and_recur_as_a_new_incident(history):
    root, database, _ = history
    with duckdb.connect(str(database)) as con:
        con.execute("update main.fct_trips set value=99")
    first = ops.check_and_deliver(root, database, "drift")
    incident = first["deliveries"][0]["alert_id"]
    with duckdb.connect(str(database)) as con:
        con.execute("update main.fct_trips set value=1")
    recovery = ops.check_and_deliver(root, database, "repair")
    notice = ops.read_json(root / recovery["deliveries"][0]["artifact"])
    assert notice["slot_id"] == "warehouse" and notice["resolved_alert_ids"] == [incident]
    assert ops.check_and_deliver(root, database, "healthy")["deliveries"] == []
    with duckdb.connect(str(database)) as con:
        con.execute("update main.fct_trips set value=99")
    # A later real observation has a distinct incident identity.
    from unittest.mock import patch
    with patch.object(ops, "utcnow", return_value=NOW + timedelta(seconds=1)):
        again = ops.check_and_deliver(root, database, "drift-again")
    assert again["deliveries"][0]["alert_id"] != incident


def test_interrupted_ingestion_and_publication_have_distinct_deadlines(tmp_path):
    ops.plan_slot(tmp_path, "cycle", (NOW - timedelta(seconds=400)).isoformat())
    directory = tmp_path / "attempts/cycle/a"
    ops.immutable_json(directory / "started.json", {"slot_id": "cycle", "attempt_id": "a",
                       "started_at_utc": (NOW - timedelta(seconds=400)).isoformat()})
    assert ops.assess(tmp_path, tmp_path / "none", observed_at=NOW)["slots"][0]["state"] == "ingestion_overdue"
    ops.immutable_json(directory / "ingested.json", {"status": "success"})
    assert ops.assess(tmp_path, tmp_path / "none", observed_at=NOW)["slots"][0]["state"] == "publication_overdue"


def test_logs_are_drained_but_retained_bytes_are_bounded(tmp_path):
    log = tmp_path / "bounded.log"
    result = bounded_command([sys.executable, "-c", "import sys; sys.stdout.write('x'*200000)"], log, os.environ.copy(), 10)
    assert result["exit_code"] == 0 and result["log_truncated"]
    assert result["log_bytes_emitted"] == 200000
    assert log.stat().st_size == LOG_LIMIT


def test_stuck_child_is_killed_with_a_timeout_classification(tmp_path):
    result = bounded_command([sys.executable, "-c", "import time; time.sleep(5)"], tmp_path / "timeout.log", os.environ.copy(), .05)
    assert result["timed_out"] and result["exit_code"] != 0


def test_timeout_stops_an_observed_descendant_writer(tmp_path, monkeypatch):
    ready, marker = tmp_path / "ready", tmp_path / "marker"
    interpreter = getattr(sys, "_base_executable", sys.executable)
    child = ("import os,time; from pathlib import Path; "
             f"Path({str(ready)!r}).write_text(str(os.getpid())); "
             f"marker=Path({str(marker)!r}); "
             "exec('while True:\\n marker.write_text(str(time.monotonic_ns()))\\n time.sleep(.02)')")
    parent = f"import subprocess,time; subprocess.Popen({[interpreter, '-c', child]!r}); time.sleep(30)"
    original_popen = subprocess.Popen

    def observed_timeout(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        real_wait = process.wait
        first = True

        def wait(timeout=None):
            nonlocal first
            if first:
                first = False
                deadline = time.monotonic() + 10
                while not (ready.exists() and marker.exists()):
                    if time.monotonic() > deadline:
                        replay.terminate_tree(process.pid)
                        raise AssertionError("Descendant did not reach its observed write boundary")
                    time.sleep(.01)
                raise subprocess.TimeoutExpired(args[0], timeout)
            return real_wait(timeout=timeout)

        process.wait = wait
        return process

    monkeypatch.setattr(replay.subprocess, "Popen", observed_timeout)
    result = replay.bounded_command([interpreter, "-c", parent], tmp_path / "tree.log", os.environ.copy(), 10)
    assert result["timed_out"]
    assert int(ready.read_text()) in result["terminated_process_ids"]
    stopped = marker.read_text()
    time.sleep(.1)
    assert marker.read_text() == stopped


def test_snapshot_failure_after_timeout_keeps_terminal_error_evidence(tmp_path, monkeypatch):
    history, database = tmp_path / "history", tmp_path / "warehouse.duckdb"
    ops.plan_slot(history, "cycle", ops.utcnow().isoformat())
    snapshots = iter([None, OSError("reopen failed")])

    def snapshot(_):
        value = next(snapshots)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(replay, "consumer_snapshot", snapshot)
    monkeypatch.setattr(replay, "bounded_command", lambda *a, **kw: {"timed_out": True, "exit_code": -1})
    result = replay.run_attempt(history, database, "cycle", "one")
    assert result["classification"] == "process_timeout"
    assert result["post_run_snapshot_error"] == "OSError"
    assert result["previous_complete_output_preserved"] is None
    assert ops.read_json(history / "attempts/cycle/one/result.json") == result

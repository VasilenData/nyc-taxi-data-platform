"""Append-only local replay history and a checker that never writes to the warehouse.

The inbox is an inspectable local file delivery channel, not email or an on-call
service. Caller-supplied clocks are for deterministic unit tests only.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from uuid import uuid4


def utcnow():
    return datetime.now(timezone.utc)


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("Use an explicit UTC timestamp")
    return parsed


def identifier(value):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", value):
        raise ValueError("Invalid history identifier")
    return value


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def read_json(path):
    return json.loads(Path(path).read_bytes())


def immutable_json(path, value):
    """Publish a complete file exclusively; identical retries are harmless.

    Hard links keep the final name invisible until all bytes are written. This
    requires a local filesystem supporting hard links, not an object-store path.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = encoded(value)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != data:
                raise ValueError(f"Refusing to overwrite immutable evidence: {path.name}") from None
    finally:
        temporary.unlink()
    return hashlib.sha256(data).hexdigest()


def plan_slot(history, slot_id, due_at, *, kind="manual-exercise", ingestion_seconds=60,
              publication_seconds=360, freshness_seconds=10800):
    identifier(slot_id)
    timestamp(due_at)
    if not (0 < ingestion_seconds <= publication_seconds <= freshness_seconds <= 86400):
        raise ValueError("Require positive, ordered deadlines bounded to one day")
    if kind not in {"manual-exercise", "scheduled-historical-replay"}:
        raise ValueError("Unsupported replay kind")
    plan = {"slot_id": slot_id, "due_at_utc": due_at, "kind": kind,
            "ingestion_budget_seconds": ingestion_seconds,
            "publication_budget_seconds": publication_seconds,
            "freshness_budget_seconds": freshness_seconds}
    immutable_json(Path(history) / "plans" / f"{slot_id}.json", plan)
    return plan


def consumer_snapshot(database):
    """Hash every row as a sorted multiset on the bounded 30k-row replay only."""
    import duckdb
    from .publication import TABLES

    if not Path(database).is_file():
        return None
    with duckdb.connect(str(database), read_only=True) as con:
        if not con.execute("select count(*) from information_schema.tables where table_schema='main' "
                           "and table_name='publication'").fetchone()[0]:
            return None
        result = {}
        for table in (*TABLES, "publication"):
            relation = f'main."{table}"'
            columns = [list(row[:2]) for row in con.execute(f"describe {relation}").fetchall()]
            if con.execute(f"select count(*) from {relation}").fetchone()[0] > 50000:
                raise ValueError("Operating replay snapshots are bounded to 50k rows per relation")
            rows = con.execute(f"select * from {relation}").fetchall()
            canonical = sorted(json.dumps(row, default=str, separators=(",", ":"), allow_nan=False)
                               for row in rows)
            digest = hashlib.sha256(encoded(columns))
            for row in canonical:
                digest.update(row.encode() + b"\n")
            result[table] = {"rows": len(rows), "schema": columns, "sha256": digest.hexdigest()}
        version, published_at = con.execute("select version, published_at from main.publication").fetchone()
        return {"version": version, "published_at_warehouse": published_at.isoformat(),
                "relations": result}


def local_delivery(history, alert):
    """Deliver once to a local inbox and verify the written bytes before receipt."""
    alert_id = hashlib.sha256(encoded(alert)).hexdigest()
    path = Path(history) / "inbox" / f"{alert_id}.json"
    receipt = Path(history) / "deliveries" / f"{alert_id}.json"
    digest = immutable_json(path, alert)
    if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        raise OSError("Local inbox delivery verification failed")
    if receipt.exists():
        previous = read_json(receipt)
        if previous["sha256"] != digest or previous["alert_id"] != alert_id:
            raise ValueError("Delivery receipt disagrees with immutable inbox artifact")
        return previous
    delivered = {"alert_id": alert_id, "channel": "local-file", "delivered": True,
                 "artifact": path.relative_to(history).as_posix(), "sha256": digest,
                 "delivered_at_utc": utcnow().isoformat()}
    immutable_json(receipt, delivered)
    return delivered


def assess(history, database, *, observed_at=None):
    """Independent read-only inspection, without trusting the last command's exit."""
    history = Path(history)
    now = observed_at or utcnow()
    plans = [read_json(path) for path in sorted((history / "plans").glob("*.json"))]
    if len(plans) > 1000:
        raise ValueError("Review/rotate the bounded history before 1,000 planned slots")
    slots, successes = [], []
    for plan in plans:
        slot_id = plan["slot_id"]
        due = timestamp(plan["due_at_utc"])
        attempts = []
        for path in (history / "attempts" / slot_id).glob("*/started.json"):
            start = read_json(path)
            if start["slot_id"] != slot_id:
                raise ValueError("Attempt/plan slot mismatch")
            result_path = path.parent / "result.json"
            result = read_json(result_path) if result_path.exists() else None
            attempts.append((timestamp(start["started_at_utc"]), start, result, path.parent))
            if result and result["status"] == "success":
                if not result.get("published"):
                    raise ValueError("Success record has no publication proof")
                successes.append(result)
        attempts.sort(key=lambda row: (row[0], row[1]["attempt_id"]))
        state, attempt_id, detail = "not_due", "none", "No scheduled obligation yet"
        ever_completed = any(row[2] and row[2]["status"] == "success" for row in attempts)
        completed = bool(attempts and attempts[-1][2] and attempts[-1][2]["status"] == "success")
        if due <= now:
            if not attempts:
                state = "missing_run" if now > due + timedelta(seconds=plan["freshness_budget_seconds"]) else "awaiting_run"
                detail = "No attempt record for the expected slot"
            else:
                started, start, result, directory = attempts[-1]
                attempt_id = start["attempt_id"]
                elapsed = (now - started).total_seconds()
                ingestion = read_json(directory / "ingested.json") if (directory / "ingested.json").exists() else None
                publication = read_json(directory / "publication.json") if (directory / "publication.json").exists() else None
                if result:
                    state = result["classification"] if result["status"] != "success" else "healthy"
                    detail = result.get("detail", "A complete publication was observed")
                    if result["status"] == "success":
                        if timestamp(result["finished_at_utc"]) > due + timedelta(seconds=plan["freshness_budget_seconds"]):
                            state, detail = "late_publication", "Publication completed after the slot freshness deadline"
                        elif result["ingestion_elapsed_seconds"] > plan["ingestion_budget_seconds"]:
                            state, detail = "late_ingestion", "Ingestion exceeded its elapsed-time budget"
                        elif result["elapsed_seconds"] > plan["publication_budget_seconds"]:
                            state, detail = "late_publication", "Publication exceeded its elapsed-time budget"
                elif ingestion and ingestion.get("status") == "failed":
                    state, detail = "ingestion_failed", "Ingestion failure recorded before supervisor stopped"
                elif publication and publication.get("status") == "failed":
                    state = "publication_refused" if publication.get("detail", "").startswith("Publication refused:") else "publication_failed"
                    detail = "Publication failure recorded before supervisor stopped"
                elif not ingestion and elapsed > plan["ingestion_budget_seconds"]:
                    state, detail = "ingestion_overdue", "Started attempt has no ingestion completion record"
                elif elapsed > plan["publication_budget_seconds"]:
                    state, detail = "publication_overdue", "Started attempt has no terminal result"
                else:
                    state, detail = "running", "Attempt is within its elapsed-time budgets"
        slots.append({"slot_id": slot_id, "attempt_id": attempt_id, "due_at_utc": plan["due_at_utc"],
                      "state": state, "detail": detail, "completed": completed,
                      "ever_completed": ever_completed,
                      "expected": due <= now, "attempt_count": len(attempts)})
    latest = max(successes, key=lambda r: timestamp(r["finished_at_utc"]), default=None)
    try:
        actual = consumer_snapshot(database)
        warehouse_matches = actual == latest["published"] if latest else actual is None
        warehouse_error = None
    except Exception as error:
        actual, warehouse_matches, warehouse_error = None, False, type(error).__name__
    slots.append({"slot_id": "warehouse", "attempt_id": latest["attempt_id"] if latest else "none",
                  "state": "healthy" if warehouse_matches else ("warehouse_unavailable" if warehouse_error else "consumer_drift"),
                  "detail": ("Consumer snapshot matches the latest successful record" if warehouse_matches else
                             warehouse_error or "Consumer snapshot differs from the latest successful record"),
                  "expected": False, "completed": False, "ever_completed": False, "attempt_count": 0})
    bad = {"missing_run", "ingestion_overdue", "publication_overdue", "ingestion_failed",
           "transformation_failed", "publication_refused", "publication_failed", "process_timeout",
           "late_ingestion", "late_publication", "consumer_drift", "warehouse_unavailable"}
    completed_at = now if observed_at is not None else utcnow()
    return {"check_started_at_utc": now.isoformat(), "checked_at_utc": completed_at.isoformat(),
            "expected_slots": sum(s["expected"] for s in slots),
            "completed_slots": sum(s["completed"] and s["expected"] for s in slots),
            "ever_completed_slots": sum(s["ever_completed"] and s["expected"] for s in slots),
            "failed_or_overdue_slots": sum(s["state"] in bad for s in slots),
            "warehouse_matches_latest_success": warehouse_matches,
            "slots": slots, "unhealthy_states": sorted(bad),
            "scope": "Local historical replay; checker opens the warehouse read-only; freshness means replay completion, not recent taxi events"}


def check_and_deliver(history, database, check_id=None):
    history = Path(history)
    check_id = identifier(check_id or str(uuid4()))
    destination = history / "checks" / f"{check_id}.json"
    if destination.exists():
        return read_json(destination)
    report = assess(history, database)
    deliveries = []
    existing = {path.stem: read_json(path) for path in (history / "inbox").glob("*.json")}
    resolved = {key for alert in existing.values() for key in alert.get("resolved_alert_ids", [])}
    open_alerts = {key: alert for key, alert in existing.items()
                   if key not in resolved and alert["state"] != "recovered"}
    for slot in report["slots"]:
        unhealthy = slot["state"] in report["unhealthy_states"]
        slot_alerts = {key: alert for key, alert in open_alerts.items() if alert["slot_id"] == slot["slot_id"]}
        duplicate = next((alert for alert in slot_alerts.values()
                          if alert["attempt_id"] == slot["attempt_id"] and alert["state"] == slot["state"]), None)
        if unhealthy or (slot["state"] == "healthy" and slot_alerts):
            alert = {"slot_id": slot["slot_id"], "attempt_id": slot["attempt_id"],
                     "state": slot["state"] if unhealthy else "recovered",
                     "detail": slot["detail"], "observed_at_utc": report["checked_at_utc"],
                     "scope": "Local historical-replay alert; no outbound email configured"}
            if not unhealthy:
                alert["resolved_alert_ids"] = sorted(slot_alerts)
            if duplicate:
                alert = duplicate
            deliveries.append(local_delivery(history, alert))
    report["deliveries"] = deliveries
    immutable_json(destination, report)
    return report

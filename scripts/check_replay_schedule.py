"""Check scheduled-run history independently of the replay worker's local plans.

Expected slots come from committed configuration. This detects missing GitHub
runs, but both workflows still depend on GitHub's scheduler and availability.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time as daytime, timedelta, timezone
from email.utils import parsedate_to_datetime
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from taxi_platform.operations import immutable_json, local_delivery, read_json, timestamp, utcnow

CONFIG = ROOT / "operations/replay-window.json"
REQUIRED_STEPS = {"Replay the bundled historical sample",
                  "Check actual publication independently and deliver local notices",
                  "Retain immutable attempt records and bounded logs"}
HTTP_ATTEMPTS = 3
HTTP_RETRY_BUDGET_SECONDS = 30
HTTP_REQUEST_TIMEOUT_SECONDS = 8


def slots(config):
    start, end = date.fromisoformat(config["start_date_utc"]), date.fromisoformat(config["end_date_utc"])
    if not 0 <= (end - start).days < 31:
        raise ValueError("Schedule window must contain 1–31 days")
    due_time = daytime.fromisoformat(config["due_time_utc"])
    if due_time.tzinfo is not None:
        raise ValueError("Due time is UTC by contract; omit a second timezone")
    return [datetime.combine(start + timedelta(days=i), due_time, timezone.utc)
            for i in range((end - start).days + 1)]


def retry_delay(error, attempt, epoch):
    """Return a bounded-policy delay, or None for a permanent HTTP error.

    Do not shorten a server's requested wait. The caller refuses a retry when
    the full wait cannot fit its remaining budget.
    """
    headers = error.headers or {}
    limited = error.code == 429 or (
        error.code == 403 and
        (headers.get("Retry-After") is not None or headers.get("X-RateLimit-Remaining") == "0")
    )
    if not limited and error.code not in {408, 500, 502, 503, 504}:
        return None
    delays = [2 ** attempt]
    retry_after = headers.get("Retry-After")
    if retry_after is not None:
        try:
            seconds = int(retry_after)
            if seconds < 0:
                return None
            delays.append(seconds)
        except ValueError:
            try:
                delays.append(max(0, parsedate_to_datetime(retry_after).timestamp() - epoch()))
            except (TypeError, ValueError, OverflowError):
                return None  # An invalid server hint is not permission to retry early.
    if headers.get("X-RateLimit-Remaining") == "0":
        try:
            delays.append(max(0, int(headers["X-RateLimit-Reset"]) - epoch()))
        except (KeyError, TypeError, ValueError, OverflowError):
            return None
    elif limited and retry_after is None:
        delays.append(60)  # GitHub's minimum wait without a reset or Retry-After hint.
    return max(delays)


def fetch_json(url, *, opener=None, monotonic=None, sleep=None, epoch=None):
    """Fetch metadata with finite attempts, request timeouts and a retry deadline.

    Payload and JSON failures remain permanent. A response that returns after
    the deadline is refused; no additional request or sleep starts beyond it.
    Socket timeouts are not an OS-level hard process deadline.
    """
    opener = opener or urllib.request.urlopen
    clock, pause, wall_time = monotonic or time.monotonic, sleep or time.sleep, epoch or time.time
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "nyc-operating-replay-monitor"}
    if os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = "Bearer " + os.environ["GITHUB_TOKEN"]
    request = urllib.request.Request(url, headers=headers)
    deadline = clock() + HTTP_RETRY_BUDGET_SECONDS
    for attempt in range(HTTP_ATTEMPTS):
        remaining = deadline - clock()
        if remaining <= 0:
            raise TimeoutError("GitHub metadata retry budget exhausted")
        try:
            with opener(request, timeout=min(HTTP_REQUEST_TIMEOUT_SECONDS, remaining)) as response:
                payload = response.read(2_000_001)
            if clock() > deadline:
                raise TimeoutError("GitHub metadata response exceeded retry budget")
            if len(payload) > 2_000_000:
                raise ValueError("GitHub response exceeded the bounded monitor payload")
            return json.loads(payload)
        except urllib.error.HTTPError as error:
            delay = retry_delay(error, attempt, wall_time)
            error.close()
            if delay is None:
                raise
            failure = error
        except (TimeoutError, ConnectionError, urllib.error.URLError) as error:
            if isinstance(error, urllib.error.URLError) and not isinstance(error.reason, (TimeoutError, ConnectionError)):
                raise
            delay, failure = 2 ** attempt, error
        remaining = deadline - clock()
        if attempt + 1 == HTTP_ATTEMPTS or delay >= remaining:
            raise failure
        print(f"GitHub metadata retry {attempt + 2}/{HTTP_ATTEMPTS} after {type(failure).__name__}; "
              f"waiting {delay:g}s", file=sys.stderr)
        pause(delay)
    raise AssertionError("Unreachable retry state")


def github_history(config, fetch=fetch_json):
    repository = config["repository"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("Invalid repository")
    workflow = config["workflow"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+\.yml", workflow):
        raise ValueError("Invalid workflow")
    query = urllib.parse.urlencode({"event": "schedule", "branch": config["branch"],
                                   "created": ">=" + config["start_date_utc"], "per_page": 100})
    root = f"https://api.github.com/repos/{repository}/actions"
    result = fetch(f"{root}/workflows/{workflow}/runs?{query}")
    rows = result["workflow_runs"]
    if result["total_count"] > len(rows):
        raise ValueError("History response truncated; refusing to report incomplete coverage")
    unique = {row["id"]: row for row in rows}
    if len(unique) > 62:
        raise ValueError("More than 62 run identities; review the bounded schedule")
    observed = []
    for row in unique.values():
        if row["event"] != "schedule" or row["head_branch"] != config["branch"]:
            raise ValueError("GitHub response did not match requested schedule/branch")
        jobs = fetch(f'{root}/runs/{int(row["id"])}/jobs?filter=latest&per_page=100')
        if jobs["total_count"] != len(jobs["jobs"]):
            raise ValueError("Job response truncated")
        completed = [job["completed_at"] for job in jobs["jobs"] if job["completed_at"]]
        complete = bool(jobs["jobs"]) and all(job["status"] == "completed" for job in jobs["jobs"])
        passed_steps = {step["name"] for job in jobs["jobs"] for step in job.get("steps", [])
                        if step["status"] == "completed" and step["conclusion"] == "success"}
        executed = REQUIRED_STEPS <= passed_steps
        success = complete and executed and all(job["conclusion"] == "success" for job in jobs["jobs"])
        observed.append({"id": row["id"], "attempt": row["run_attempt"], "url": row["html_url"],
                         "created_at_utc": row["created_at"], "head_sha": row["head_sha"],
                         "completed_at_utc": max(completed, key=timestamp) if complete else None,
                         "status": "completed" if complete else row["status"],
                         "required_replay_steps_passed": executed,
                         "conclusion": "success" if success else ("required_steps_not_successful" if complete else row["conclusion"])})
    return sorted(observed, key=lambda row: (timestamp(row["created_at_utc"]), row["id"]))


def assess_schedule(config, observed, now):
    result = []
    for due in slots(config):
        if due > now:
            continue
        deadline = due + timedelta(seconds=config["completion_grace_seconds"])
        matches = [row for row in observed if due <= timestamp(row["created_at_utc"]) < due + timedelta(days=1)]
        if not matches:
            state = "missing_scheduled_run" if now > deadline else "awaiting_run"
        elif len(matches) > 1:
            state = "ambiguous_multiple_runs"
        else:
            run = matches[0]
            if run["status"] == "completed" and run["conclusion"] != "success":
                state = "scheduled_run_failed"
            elif run["status"] == "completed":
                state = "healthy" if timestamp(run["completed_at_utc"]) <= deadline else "late_completion"
            else:
                state = "scheduled_run_overdue" if now > deadline else "running"
        result.append({"slot_id": due.date().isoformat(), "due_at_utc": due.isoformat(),
                       "deadline_utc": deadline.isoformat(), "state": state,
                       "run_ids": [row["id"] for row in matches],
                       "completed": len(matches) == 1 and matches[0]["status"] == "completed" and matches[0]["conclusion"] == "success"})
    failed = [row for row in result if row["state"] not in {"healthy", "awaiting_run", "running"}]
    return {"checked_at_utc": now.isoformat(), "expected_slots": len(result),
            "completed_slots": sum(row["completed"] for row in result), "failed_or_overdue_slots": len(failed),
            "slots": result, "observed_runs": observed,
            "scope": "GitHub scheduled-workflow completion windows, not direct warehouse or artifact-content verification",
            "scheduler_independence": "Separate workflow; shared GitHub scheduler and control plane"}


def monitor(config, output):
    if output.exists():
        raise ValueError("Use a new monitor output directory")
    now = utcnow()
    try:
        observed = github_history(config) if any(due <= now for due in slots(config)) else []
        report = assess_schedule(config, observed, now)
    except Exception as error:
        report = {"checked_at_utc": now.isoformat(), "expected_slots": sum(due <= now for due in slots(config)),
                  "completed_slots": None, "failed_or_overdue_slots": None,
                  "slots": [{"slot_id": "monitor", "state": "monitor_unavailable", "run_ids": []}],
                  "error_type": type(error).__name__, "scope": "Unknown coverage; monitor did not obtain complete run evidence"}
    deliveries = []
    for slot in report["slots"]:
        if slot["state"] not in {"healthy", "awaiting_run", "running"}:
            deliveries.append(local_delivery(output, {"slot_id": slot["slot_id"], "attempt_id": "schedule-check",
                               "state": slot["state"], "observed_at_utc": report["checked_at_utc"],
                               "detail": "Inspect the committed schedule and observed workflow history; no outbound email configured"}))
    report["deliveries"] = deliveries
    immutable_json(output / "report.json", report)
    print(json.dumps({"expected_slots": report["expected_slots"], "completed_slots": report["completed_slots"],
                      "local_alerts": len(deliveries), "scope": report["scope"]}))
    return 1 if deliveries else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("eligibility", "monitor"))
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--monitor", action="store_true")
    args = parser.parse_args()
    config = read_json(args.config)
    if args.command == "monitor":
        if args.output is None:
            parser.error("--output is required")
        return monitor(config, args.output)
    now, window = utcnow(), slots(config)
    manual = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
    end = window[-1].date() + timedelta(days=1 if args.monitor else 0)
    active = manual or window[0].date() <= now.date() <= end
    anchor = now
    if os.environ.get("GITHUB_EVENT_NAME") == "schedule" and not args.monitor:
        run_id = os.environ.get("GITHUB_RUN_ID", "")
        if not run_id.isdigit():
            raise ValueError("Scheduled invocation requires a GitHub run identity")
        run = fetch_json(f'https://api.github.com/repos/{config["repository"]}/actions/runs/{run_id}')
        if run["event"] != "schedule" or run["head_branch"] != config["branch"]:
            raise ValueError("Run metadata differs from configured schedule")
        anchor = timestamp(run["created_at"])
    due = now if manual else datetime.combine(anchor.date(), window[0].timetz())
    if not manual and due > anchor:
        due -= timedelta(days=1)
    if not manual and not args.monitor:
        active = active and window[0] <= due <= window[-1]
    print("active=" + str(active).lower())
    print("due_at=" + due.isoformat())
    suffix = os.environ.get("GITHUB_RUN_ID", "local")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", suffix):
        raise ValueError("Invalid run identity")
    print("slot_id=" + ("manual-" + suffix if manual else due.date().isoformat()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

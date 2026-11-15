"""Synthetic GitHub API/clock cases; these are not observed scheduled runs."""
from datetime import datetime, timezone

import pytest

from scripts import check_replay_schedule as monitor
from taxi_platform.operations import read_json

CONFIG = {"start_date_utc": "2026-11-15", "end_date_utc": "2026-11-16", "due_time_utc": "03:17",
          "completion_grace_seconds": 10800, "repository": "example/project", "branch": "main",
          "workflow": "operating-replay.yml"}
NOW = datetime(2026, 11, 16, 7, tzinfo=timezone.utc)


def run(id=1, day=15, conclusion="success", finished="04:00:00"):
    return {"id": id, "created_at_utc": f"2026-11-{day}T03:20:00Z", "status": "completed",
            "conclusion": conclusion, "completed_at_utc": f"2026-11-{day}T{finished}Z"}


def test_expected_days_exist_even_when_worker_never_created_plans():
    report = monitor.assess_schedule(CONFIG, [run()], NOW)
    assert report["expected_slots"] == 2 and report["completed_slots"] == 1
    assert report["slots"][1]["state"] == "missing_scheduled_run"


@pytest.mark.parametrize("observed,state", [
    ([run(conclusion="failure")], "scheduled_run_failed"),
    ([run(finished="07:00:00")], "late_completion"),
    ([run(), run(id=2)], "ambiguous_multiple_runs"),
])
def test_failure_lateness_and_ambiguous_duplicates_are_not_success(observed, state):
    assert monitor.assess_schedule(CONFIG, observed, NOW)["slots"][0]["state"] == state


def test_future_dates_are_not_manufactured_as_operating_history():
    earlier = datetime(2026, 11, 14, 7, tzinfo=timezone.utc)
    report = monitor.assess_schedule(CONFIG, [], earlier)
    assert report["expected_slots"] == report["completed_slots"] == 0
    assert report["slots"] == []


def test_api_truncation_fails_closed():
    with pytest.raises(ValueError, match="truncated"):
        monitor.github_history(CONFIG, fetch=lambda url: {"total_count": 101, "workflow_runs": []})


def test_observed_job_completion_time_not_mutable_run_updated_time():
    row = {"id": 1, "event": "schedule", "head_branch": "main", "run_attempt": 2,
           "html_url": "https://example.test/run/1", "created_at": "2026-11-15T03:20:00Z",
           "head_sha": "a" * 40, "status": "in_progress", "conclusion": None,
           "updated_at": "2026-12-01T00:00:00Z"}
    replies = iter([{"total_count": 1, "workflow_runs": [row]},
                    {"total_count": 1, "jobs": [{"status": "completed", "conclusion": "success",
                                                  "completed_at": "2026-11-15T04:00:00Z",
                                                  "steps": [{"name": name, "status": "completed", "conclusion": "success"}
                                                            for name in monitor.REQUIRED_STEPS]}]}])
    observed = monitor.github_history(CONFIG, fetch=lambda url: next(replies))
    assert observed[0]["completed_at_utc"] == "2026-11-15T04:00:00Z"
    assert monitor.assess_schedule(CONFIG, observed, NOW)["slots"][0]["state"] == "healthy"


def test_skipped_replay_with_green_wrapper_is_not_counted_completed():
    row = {"id": 1, "event": "schedule", "head_branch": "main", "run_attempt": 1,
           "html_url": "https://example.test/run/1", "created_at": "2026-11-15T03:20:00Z",
           "head_sha": "a" * 40, "status": "completed", "conclusion": "success"}
    replies = iter([{"total_count": 1, "workflow_runs": [row]},
                    {"total_count": 1, "jobs": [{"status": "completed", "conclusion": "success",
                                                  "completed_at": "2026-11-15T04:00:00Z", "steps": []}]}])
    observed = monitor.github_history(CONFIG, fetch=lambda url: next(replies))
    report = monitor.assess_schedule(CONFIG, observed, NOW)
    assert report["slots"][0]["state"] == "scheduled_run_failed"
    assert report["completed_slots"] == 0


def test_api_failure_reports_unknown_coverage_and_delivers_local_notice(tmp_path, monkeypatch):
    monkeypatch.setattr(monitor, "utcnow", lambda: NOW)

    def unavailable(config):
        raise TimeoutError("service unavailable")

    monkeypatch.setattr(monitor, "github_history", unavailable)
    assert monitor.monitor(CONFIG, tmp_path / "check") == 1
    report = read_json(tmp_path / "check/report.json")
    assert report["completed_slots"] is None
    assert report["deliveries"][0]["delivered"]
    assert report["slots"][0]["state"] == "monitor_unavailable"


def test_delayed_worker_uses_original_created_at_window(tmp_path, monkeypatch, capsys):
    from taxi_platform.operations import immutable_json
    config = tmp_path / "window.json"
    immutable_json(config, CONFIG)
    monkeypatch.setattr(monitor, "utcnow", lambda: datetime(2026, 11, 16, 0, 10, tzinfo=timezone.utc))
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setattr(monitor, "fetch_json", lambda url: {"event": "schedule", "head_branch": "main",
                                                          "created_at": "2026-11-15T03:20:00Z"})
    monkeypatch.setattr("sys.argv", ["check_replay_schedule.py", "eligibility", "--config", str(config)])
    assert monitor.main() == 0
    output = capsys.readouterr().out
    assert "due_at=2026-11-15T03:17:00+00:00" in output and "slot_id=2026-11-15" in output


def test_future_year_cron_does_not_reactivate_expired_window(tmp_path, monkeypatch, capsys):
    from taxi_platform.operations import immutable_json
    config = tmp_path / "window.json"
    immutable_json(config, CONFIG)
    monkeypatch.setattr(monitor, "utcnow", lambda: datetime(2027, 11, 15, 7, tzinfo=timezone.utc))
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setattr("sys.argv", ["check_replay_schedule.py", "eligibility", "--monitor", "--config", str(config)])
    assert monitor.main() == 0
    assert "active=false" in capsys.readouterr().out

"""Synthetic GitHub API/clock cases; these are not observed scheduled runs."""
from datetime import datetime, timezone
from email.message import Message
import io
import json
import ssl
import urllib.error

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


class FakeClock:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def http_error(code, **headers):
    message = Message()
    for key, value in headers.items():
        message[key.replace("_", "-")] = value
    return urllib.error.HTTPError("https://api.github.com/example", code, "Synthetic error", message, io.BytesIO())


def fetch_fixture(replies, clock):
    """Use deterministic network outcomes; no real waiting or GitHub requests."""
    calls = []
    outcomes = iter(replies)

    def open_response(request, timeout):
        calls.append(timeout)
        value = next(outcomes)
        if isinstance(value, TimeoutError):
            clock.now += timeout
        if isinstance(value, Exception):
            raise value
        return io.BytesIO(value)

    def fetch():
        return monitor.fetch_json("https://api.github.com/example", opener=open_response,
                                  monotonic=clock.monotonic, sleep=clock.sleep, epoch=clock.monotonic)
    return fetch, calls


def test_metadata_timeout_then_success_keeps_eligibility_stdout_clean(capsys):
    clock = FakeClock()
    fetch, calls = fetch_fixture([TimeoutError("read timed out"), b'{"event":"schedule"}'], clock)
    assert fetch() == {"event": "schedule"}
    assert calls == [8, 8] and clock.sleeps == [1]
    output = capsys.readouterr()
    assert output.out == "" and "retry 2/3" in output.err


def test_metadata_timeouts_exhaust_attempts_within_retry_budget():
    clock = FakeClock()
    final = TimeoutError("third read timed out")
    fetch, calls = fetch_fixture([TimeoutError(), TimeoutError(), final], clock)
    with pytest.raises(TimeoutError) as caught:
        fetch()
    assert caught.value is final
    assert calls == [8, 8, 8] and clock.sleeps == [1, 2]
    assert clock.now == 27 < monitor.HTTP_RETRY_BUDGET_SECONDS


@pytest.mark.parametrize("status", [401, 403, 404, 422])
def test_metadata_permanent_http_errors_are_not_retried(status):
    clock = FakeClock()
    failure = http_error(status)
    fetch, calls = fetch_fixture([failure], clock)
    with pytest.raises(urllib.error.HTTPError) as caught:
        fetch()
    assert caught.value is failure and len(calls) == 1 and clock.sleeps == []


@pytest.mark.parametrize("failure,wait", [
    (http_error(503), 1),
    (http_error(429, Retry_After="3"), 3),
    (http_error(403, X_RateLimit_Remaining="0", X_RateLimit_Reset="4"), 4),
    (http_error(503, Retry_After="Thu, 01 Jan 1970 00:00:05 GMT"), 5),
])
def test_metadata_temporary_http_or_explicit_rate_limit_retries(failure, wait):
    clock = FakeClock()
    fetch, calls = fetch_fixture([failure, b'{"ok":true}'], clock)
    assert fetch() == {"ok": True}
    assert len(calls) == 2 and clock.sleeps == [wait]


@pytest.mark.parametrize("failure", [
    http_error(429, Retry_After="3600"),
    http_error(403, X_RateLimit_Remaining="0", X_RateLimit_Reset="3600"),
    http_error(429),  # GitHub recommends at least one minute without a reset hint.
    http_error(429, Retry_After="not-a-duration"),
    http_error(429, Retry_After="-1"),
])
def test_metadata_retry_hint_never_causes_excess_wait_or_early_retry(failure):
    clock = FakeClock()
    fetch, calls = fetch_fixture([failure], clock)
    with pytest.raises(urllib.error.HTTPError) as caught:
        fetch()
    assert caught.value is failure and len(calls) == 1 and clock.sleeps == []


def test_metadata_deadline_reduces_final_request_timeout_and_rejects_late_result():
    clock = FakeClock()
    calls = []

    def slow_response(request, timeout):
        calls.append(timeout)
        if len(calls) == 1:
            clock.now += 8
            raise http_error(503, Retry_After="20")
        clock.now += 3  # Deliberately simulate a transport returning after its timeout.
        return io.BytesIO(b'{"ok":true}')

    with pytest.raises(TimeoutError, match="exceeded retry budget"):
        monitor.fetch_json("https://api.github.com/example", opener=slow_response,
                           monotonic=clock.monotonic, sleep=clock.sleep, epoch=clock.monotonic)
    assert calls == [8, 2] and clock.sleeps == [20]


@pytest.mark.parametrize("payload,error", [(b'not-json', json.JSONDecodeError), (b' ' * 2_000_001, ValueError)])
def test_metadata_invalid_or_oversized_payload_is_not_retried(payload, error):
    clock = FakeClock()
    fetch, calls = fetch_fixture([payload], clock)
    with pytest.raises(error):
        fetch()
    assert len(calls) == 1 and clock.sleeps == []


def test_metadata_wrapped_timeout_retries_but_other_url_errors_do_not():
    clock = FakeClock()
    fetch, calls = fetch_fixture([urllib.error.URLError(TimeoutError()), b'{}'], clock)
    assert fetch() == {} and len(calls) == 2
    failure = urllib.error.URLError("certificate verify failed")
    fetch, calls = fetch_fixture([failure], clock)
    with pytest.raises(urllib.error.URLError) as caught:
        fetch()
    assert caught.value is failure and len(calls) == 1


def test_metadata_connection_reset_while_reading_body_then_success():
    clock = FakeClock()
    calls = []

    class InterruptedResponse(io.BytesIO):
        def read(self, size=-1):
            raise ConnectionResetError("connection reset during metadata body")

    interrupted = InterruptedResponse()

    def open_response(request, timeout):
        calls.append(timeout)
        return interrupted if len(calls) == 1 else io.BytesIO(b'{"ok":true}')

    assert monitor.fetch_json("https://api.github.com/example", opener=open_response,
                              monotonic=clock.monotonic, sleep=clock.sleep) == {"ok": True}
    assert calls == [8, 8] and clock.sleeps == [1]
    assert interrupted.closed


@pytest.mark.parametrize("failure", [
    ssl.SSLCertVerificationError("certificate verify failed"),
    urllib.error.URLError(ssl.SSLCertVerificationError("certificate verify failed")),
])
def test_metadata_certificate_failures_are_not_retried(failure):
    clock = FakeClock()
    fetch, calls = fetch_fixture([failure], clock)
    with pytest.raises(type(failure)) as caught:
        fetch()
    assert caught.value is failure and len(calls) == 1 and clock.sleeps == []

"""Offline experiment orchestration; no AWS calls or actual IAM evidence."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import io
import json
from unittest.mock import Mock

import duckdb
import pytest

from taxi_platform.cloud_experiment import execute_plan, fact_read_denial, make_plan, parse_answer
from taxi_platform.release_bundle import export_release
from test_release_bundle import warehouse


def error(code="AccessDenied", status=403):
    result = RuntimeError("private upstream message must not leak")
    result.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}
    return result


@pytest.fixture
def experiment(tmp_path):
    database, bundle = tmp_path / "source.duckdb", tmp_path / "release"
    warehouse(database)
    with duckdb.connect(str(database)) as con:
        # Hand-computed oracle: two daily groups (1+2), three hourly groups (1+1+1), three facts.
        con.execute("create or replace table mart_daily_borough as select * from (values (1::bigint),(2::bigint)) t(trips)")
        con.execute("create or replace table mart_hourly_demand as select 1::bigint trips from range(3)")
        counts = json.loads(con.execute("select row_counts from publication").fetchone()[0])
        counts["mart_daily_borough"] = 2
        con.execute("update publication set row_counts=?", [json.dumps(counts)])
    export_release(database, bundle)
    settings = {"account_id": "123456789012", "region": "eu-west-1", "data_bucket": "private-data",
                "results_bucket": "private-results", "database": "taxi_dev", "workgroup": "taxi-dev",
                "publisher_role_arn": "arn:aws:iam::123456789012:role/publisher",
                "query_role_arn": "arn:aws:iam::123456789012:role/analyst", "budget_usd": "5.00",
                "expires_at_utc": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
                "experiment_id": "offline-fixture-not-an-approved-AWS-experiment"}
    return bundle, make_plan(bundle, settings)


def result(rows, trips):
    return {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "aggregate_rows"}, {"Name": "trips"}]},
                         "Rows": [{"Data": [{"VarCharValue": "aggregate_rows"}, {"VarCharValue": "trips"}]},
                                  {"Data": [{"VarCharValue": str(rows)}, {"VarCharValue": str(trips)}]}]}}


def runner(monkeypatch, experiment, tmp_path, state="SUCCEEDED"):
    bundle, plan = experiment
    # Actual acceptance and denial functions have separate concrete fixture tests;
    # these spies establish ordering and that a failure cannot start any query.
    accepted, denied = Mock(return_value={"passed": True}), Mock(return_value={"query_read": "AccessDenied"})
    monkeypatch.setattr("taxi_platform.cloud_experiment.inspect_release", accepted)
    monkeypatch.setattr("taxi_platform.cloud_experiment.fact_read_denial", denied)
    clients = [Mock() for _ in range(6)]
    athena = clients[1]
    submitted = {}

    def start(**kwargs):
        assert accepted.called and denied.called
        assert json.loads((tmp_path / "journal.json").read_text(encoding="utf-8"))["query_attempts"][-1]["request_token"] == kwargs["ClientRequestToken"]
        token = kwargs["ClientRequestToken"]
        query_id = "query-" + str([q["sql"] for q in plan["queries"]].index(kwargs["QueryString"]))
        submitted[token] = kwargs
        return {"QueryExecutionId": query_id}

    def execution(**kwargs):
        query = plan["queries"][int(kwargs["QueryExecutionId"][-1])]
        return {"QueryExecution": {"QueryExecutionId": kwargs["QueryExecutionId"], "WorkGroup": "taxi-dev",
                 "Query": query["sql"], "StatementType": "DML", "Status": {"State": state},
                 "Statistics": {"DataScannedInBytes": 80, "ResultReuseInformation": {"ReusedPreviousResult": False}}}}

    athena.start_query_execution.side_effect = start
    athena.get_query_execution.side_effect = execution
    athena.get_query_results.side_effect = lambda **kwargs: result(*plan["queries"][int(kwargs["QueryExecutionId"][-1])]["expected"])
    return clients, submitted, accepted, denied


def test_offline_plan_contains_exact_two_queries_and_hand_calculated_totals(experiment):
    bundle, plan = experiment
    assert [q["expected"] for q in plan["queries"]] == [[2, 3], [3, 3]]
    assert plan["max_queries"] == 2
    assert plan == make_plan(bundle, plan["settings"])
    assert plan["cost_limit"].startswith("Declared budget is not automatically enforced")


@pytest.mark.parametrize("change", ["missing_budget", "zero_budget", "nan_budget", "sql_identifier", "foreign_role", "same_role"])
def test_invalid_plan_inputs_fail_without_cloud_clients(experiment, change):
    bundle, plan = experiment
    settings = deepcopy(plan["settings"])
    if change == "missing_budget":
        settings.pop("budget_usd")
    elif change == "zero_budget":
        settings["budget_usd"] = "0"
    elif change == "nan_budget":
        settings["budget_usd"] = "NaN"
    elif change == "sql_identifier":
        settings["database"] = 'db";drop table x;--'
    elif change == "foreign_role":
        settings["publisher_role_arn"] = "arn:aws:iam::999999999999:role/publisher"
    else:
        settings["publisher_role_arn"] = settings["query_role_arn"]
    with pytest.raises(ValueError):
        make_plan(bundle, settings)


def test_queries_reconcile_and_restarts_reuse_tokens(monkeypatch, experiment, tmp_path):
    clients, submitted, _, _ = runner(monkeypatch, experiment, tmp_path)
    report = execute_plan(*experiment, clients, tmp_path / "journal.json")
    assert report["status"] == "passed"
    assert [a["actual"] for a in report["query_attempts"]] == [[2, 3], [3, 3]]
    assert report["usage"]["summary"]["known_scanned_bytes"] == 160
    assert report["cloud_billed_cost_usd"] is None
    assert len(submitted) == 2
    first_tokens = set(submitted)
    restarted = execute_plan(*experiment, clients, tmp_path / "journal.json")
    assert set(submitted) == first_tokens
    assert len(restarted["previous_queries"]) == 2
    for call in clients[1].start_query_execution.call_args_list:
        assert call.kwargs["ResultReuseConfiguration"] == {"ResultReuseByAgeConfiguration": {"Enabled": False}}
        assert call.kwargs["WorkGroup"] == "taxi-dev"


@pytest.mark.parametrize("stage", ["acceptance", "denial"])
def test_preflight_failure_cannot_start_queries(monkeypatch, experiment, tmp_path, stage):
    clients, _, accepted, denied = runner(monkeypatch, experiment, tmp_path)
    (accepted if stage == "acceptance" else denied).side_effect = ValueError("not accepted")
    with pytest.raises(ValueError):
        execute_plan(*experiment, clients, tmp_path / "journal.json")
    clients[1].start_query_execution.assert_not_called()
    assert json.loads((tmp_path / "journal.json").read_text(encoding="utf-8"))["status"] == "failed"


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_terminal_failure_retains_id_and_usage_without_second_query(monkeypatch, experiment, tmp_path, state):
    clients, _, _, _ = runner(monkeypatch, experiment, tmp_path, state)
    with pytest.raises(ValueError, match="did not succeed"):
        execute_plan(*experiment, clients, tmp_path / "journal.json")
    clients[1].start_query_execution.assert_called_once()
    report = json.loads((tmp_path / "journal.json").read_text(encoding="utf-8"))
    assert report["query_attempts"][0]["query_id"] == "query-0"
    assert report["usage"]["queries"][0]["state"] == state
    assert report["usage"]["summary"]["known_scanned_bytes"] == 80


def test_timeout_requests_cancel_and_retains_provisional_usage(monkeypatch, experiment, tmp_path):
    clients, _, _, _ = runner(monkeypatch, experiment, tmp_path, "RUNNING")
    with pytest.raises(TimeoutError):
        execute_plan(*experiment, clients, tmp_path / "journal.json", clock=iter([0, 121]).__next__, sleep=lambda _: None)
    clients[1].stop_query_execution.assert_called_once_with(QueryExecutionId="query-0")
    report = json.loads((tmp_path / "journal.json").read_text(encoding="utf-8"))
    assert report["cancellation"]["terminal_state_verified"] is False
    assert report["usage"]["summary"]["all_terminal_scan_counters_present"] is False


def test_mismatching_answer_stops_before_next_query(monkeypatch, experiment, tmp_path):
    clients, _, _, _ = runner(monkeypatch, experiment, tmp_path)
    clients[1].get_query_results.side_effect = None
    clients[1].get_query_results.return_value = result(2, 4)
    with pytest.raises(ValueError, match="oracle"):
        execute_plan(*experiment, clients, tmp_path / "journal.json")
    clients[1].start_query_execution.assert_called_once()
    report = json.loads((tmp_path / "journal.json").read_text(encoding="utf-8"))
    assert report["query_attempts"][0]["actual"] == [2, 4]
    assert report["query_attempts"][0]["expected"] == [2, 3]


def test_network_failure_after_submission_retains_id_without_leaking_sdk_error(monkeypatch, experiment, tmp_path):
    clients, _, _, _ = runner(monkeypatch, experiment, tmp_path)
    clients[1].get_query_execution.side_effect = RuntimeError("sensitive upstream error text")
    with pytest.raises(RuntimeError):
        execute_plan(*experiment, clients, tmp_path / "journal.json")
    clients[1].start_query_execution.assert_called_once()
    clients[1].stop_query_execution.assert_called_once_with(QueryExecutionId="query-0")
    journal = (tmp_path / "journal.json").read_text(encoding="utf-8")
    assert "sensitive upstream" not in journal
    report = json.loads(journal)
    assert report["query_attempts"][0]["query_id"] == "query-0"
    assert report["cancellation"]["requested"] is True


def test_failed_restart_retains_prior_known_measurements(monkeypatch, experiment, tmp_path):
    clients, _, accepted, _ = runner(monkeypatch, experiment, tmp_path)
    execute_plan(*experiment, clients, tmp_path / "journal.json")
    accepted.side_effect = ValueError("later preflight failed")
    with pytest.raises(ValueError):
        execute_plan(*experiment, clients, tmp_path / "journal.json")
    journal = json.loads((tmp_path / "journal.json").read_text(encoding="utf-8"))
    assert len(journal["previous_queries"]) == 2
    assert sum(q["measurement"]["data_scanned_bytes"] for q in journal["previous_queries"].values()) == 160


def test_expired_or_altered_plan_is_rejected_before_sdk_calls(experiment, tmp_path):
    bundle, plan = experiment
    clients = [Mock() for _ in range(6)]
    altered = deepcopy(plan)
    altered["max_queries"] = 3
    with pytest.raises(ValueError, match="Plan differs"):
        execute_plan(bundle, altered, clients, tmp_path / "journal.json")
    plan["settings"]["expires_at_utc"] = "2020-01-01T00:00:00+00:00"
    expired = make_plan(bundle, plan["settings"])
    with pytest.raises(ValueError, match="expired"):
        execute_plan(bundle, expired, clients, tmp_path / "journal.json")
    assert all(not client.mock_calls for client in clients)


def test_fact_probe_requires_positive_publisher_checksum_then_explicit_denial(experiment):
    import base64
    bundle, plan = experiment
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    fact = next(f for f in manifest["files"] if f["table"] == "fct_trips")
    sts, writer, reader = Mock(), Mock(), Mock()
    sts.get_caller_identity.return_value = {"Account": "123456789012", "Arn": "arn:aws:sts::123456789012:assumed-role/publisher/fixture"}
    writer.head_object.return_value = {"ContentLength": fact["bytes"], "ChecksumSHA256": base64.b64encode(bytes.fromhex(fact["sha256"])).decode()}
    reader.get_object.side_effect = error()
    observed = fact_read_denial(sts, writer, reader, bundle, plan)
    assert observed["query_read"] == "AccessDenied"
    assert reader.get_object.call_args.kwargs["Range"] == "bytes=0-0"
    reader.get_object.side_effect = error("NoSuchKey", 404)
    with pytest.raises(ValueError, match="inconclusive"):
        fact_read_denial(sts, writer, reader, bundle, plan)
    reader.get_object.reset_mock()
    writer.head_object.return_value["ContentLength"] = 0
    with pytest.raises(ValueError, match="establish"):
        fact_read_denial(sts, writer, reader, bundle, plan)
    reader.get_object.assert_not_called()


def test_unexpected_allowed_read_closes_body_and_does_not_disclose_it(experiment):
    import base64
    bundle, plan = experiment
    fact = next(f for f in json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))["files"] if f["table"] == "fct_trips")
    sts, writer, reader = Mock(), Mock(), Mock()
    sts.get_caller_identity.return_value = {"Account": "123456789012", "Arn": "arn:aws:sts::123456789012:assumed-role/publisher/fixture"}
    writer.head_object.return_value = {"ContentLength": fact["bytes"], "ChecksumSHA256": base64.b64encode(bytes.fromhex(fact["sha256"])).decode()}
    payload = io.BytesIO(b"private byte")
    body = Mock(wraps=payload)
    reader.get_object.return_value = {"Body": body}
    with pytest.raises(ValueError, match="unexpectedly read"):
        fact_read_denial(sts, writer, reader, bundle, plan)
    body.close.assert_called_once()
    body.read.assert_not_called()
    assert payload.closed


@pytest.mark.parametrize("damage", ["pagination", "null", "negative", "header"])
def test_result_shape_fails_closed(damage):
    response = result(2, 3)
    if damage == "pagination":
        response["NextToken"] = "more"
    elif damage == "null":
        response["ResultSet"]["Rows"][1]["Data"][1] = {}
    elif damage == "negative":
        response["ResultSet"]["Rows"][1]["Data"][1]["VarCharValue"] = "-1"
    else:
        response["ResultSet"]["Rows"][0]["Data"][0]["VarCharValue"] = "wrong"
    with pytest.raises(ValueError):
        parse_answer(response)

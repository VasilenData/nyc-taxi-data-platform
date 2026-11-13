"""Offline AWS boundary tests with real local Parquet and read-only SDK doubles."""
import base64
from copy import deepcopy
import io
import json
from unittest.mock import Mock

import pytest

from taxi_platform.cloud_acceptance import inspect_release
from taxi_platform.cloud_release import AGGREGATES, query_ledger, query_measurement, upload_release
from taxi_platform.release_bundle import export_release
from test_cloud_controls import FakeGlue, FakeS3
from test_release_bundle import warehouse


@pytest.fixture
def acceptance(tmp_path):
    source, bundle = tmp_path / "source.duckdb", tmp_path / "release"
    warehouse(source)
    manifest = export_release(source, bundle)
    publisher_s3 = FakeS3()
    publisher_glue = FakeGlue(publisher_s3)
    upload_release(publisher_s3, publisher_glue, bundle, "private", "taxi_dev", "123456789012", 1000000)
    identity = {"Account": "123456789012", "Arn": "arn:aws:sts::123456789012:assumed-role/analyst/acceptance"}
    group = {"Name": "taxi-dev", "State": "ENABLED", "Configuration": {
        "EnforceWorkGroupConfiguration": True, "BytesScannedCutoffPerQuery": 104857600,
        "PublishCloudWatchMetricsEnabled": True,
        "EngineVersion": {"EffectiveEngineVersion": "Athena engine version 3"},
        "ResultConfiguration": {"OutputLocation": "s3://results/dev/", "ExpectedBucketOwner": "123456789012",
                                "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}}}}
    remote_manifest = {"body": (bundle / "manifest.json").read_bytes()}
    tables = {item["Name"]: item for item in publisher_glue.tables}
    heads, listings = {}, {}
    for item in manifest["files"]:
        if item["table"] in AGGREGATES:
            prefix = f'releases/{manifest["release_id"]}/tables/{item["table"]}/'
            heads[prefix + "data.parquet"] = {"ContentLength": item["bytes"], "ServerSideEncryption": "AES256",
                "ChecksumSHA256": base64.b64encode(bytes.fromhex(item["sha256"])).decode()}
            listings[prefix] = {"IsTruncated": False, "Contents": [{"Key": prefix + "data.parquet"}]}
    sts, athena, s3, glue = (Mock(spec=methods) for methods in
        (["get_caller_identity"], ["get_work_group"], ["get_object", "list_objects_v2", "head_object"], ["get_table"]))
    sts.get_caller_identity.return_value = identity
    athena.get_work_group.return_value = {"WorkGroup": group}
    s3.get_object.side_effect = lambda **kwargs: {
        "ContentLength": len(remote_manifest["body"]), "Body": io.BytesIO(remote_manifest["body"])}
    s3.head_object.side_effect = lambda **kwargs: heads[kwargs["Key"]]
    s3.list_objects_v2.side_effect = lambda **kwargs: listings[kwargs["Prefix"]]
    glue.get_table.side_effect = lambda **kwargs: {"Table": tables[kwargs["Name"]]}
    kwargs = dict(data_bucket="private", results_bucket="results", database="taxi_dev", workgroup="taxi-dev",
                  owner="123456789012", query_role_arn="arn:aws:iam::123456789012:role/analyst",
                  environment="dev", max_scan_bytes=104857600)
    return dict(clients=(sts, athena, s3, glue), bundle=bundle, kwargs=kwargs, identity=identity, group=group,
                manifest=remote_manifest, heads=heads, listings=listings, tables=tables)


def inspect(case):
    return inspect_release(*case["clients"], case["bundle"], **case["kwargs"])


def test_read_only_acceptance_matches_actual_bundle_and_uploaded_catalog(acceptance):
    result = inspect(acceptance)
    assert result["configuration_and_release_checks_passed"] is True
    assert result["aws_queries_started"] == 0
    assert result["cloud_billed_cost_usd"] is None
    assert result["iam_denial_tests"] == "not performed"
    assert len(result["tables"]) == 2
    sts, athena, s3, glue = acceptance["clients"]
    assert [len(client.mock_calls) for client in (sts, athena, s3, glue)] == [1, 1, 5, 2]
    athena.get_work_group.assert_called_once_with(WorkGroup="taxi-dev")
    for call in s3.mock_calls:
        assert call.kwargs["ExpectedBucketOwner"] == "123456789012"
        assert call.kwargs["Bucket"] == "private"
    for call in glue.mock_calls:
        assert call.kwargs["CatalogId"] == "123456789012"
        assert call.kwargs["DatabaseName"] == "taxi_dev"


@pytest.mark.parametrize("damage", ["operator_role", "foreign_account", "disabled_group", "unenforced",
    "unbounded", "result_location", "result_owner", "encryption", "metrics", "engine", "manifest",
    "extra_object", "truncated_listing", "checksum", "catalog_location", "catalog_schema", "catalog_format", "partition_keys"])
def test_acceptance_rejects_drift_before_issuing_any_query(acceptance, damage):
    config = acceptance["group"]["Configuration"]
    result = config["ResultConfiguration"]
    descriptor = next(iter(acceptance["tables"].values()))["StorageDescriptor"]
    if damage == "operator_role":
        acceptance["identity"]["Arn"] = "arn:aws:sts::123456789012:assumed-role/operator/acceptance"
    elif damage == "foreign_account":
        acceptance["identity"]["Account"] = "999999999999"
    elif damage == "disabled_group":
        acceptance["group"]["State"] = "DISABLED"
    elif damage == "unenforced":
        config["EnforceWorkGroupConfiguration"] = False
    elif damage == "unbounded":
        config["BytesScannedCutoffPerQuery"] = 104857601
    elif damage == "result_location":
        result["OutputLocation"] = "s3://results/prod/"
    elif damage == "result_owner":
        result["ExpectedBucketOwner"] = "999999999999"
    elif damage == "encryption":
        result.pop("EncryptionConfiguration")
    elif damage == "metrics":
        config["PublishCloudWatchMetricsEnabled"] = False
    elif damage == "engine":
        config["EngineVersion"]["EffectiveEngineVersion"] = "Athena engine version 2"
    elif damage == "manifest":
        acceptance["manifest"]["body"] += b" "
    elif damage == "extra_object":
        next(iter(acceptance["listings"].values()))["Contents"].append({"Key": "unexpected.parquet"})
    elif damage == "truncated_listing":
        next(iter(acceptance["listings"].values()))["IsTruncated"] = True
    elif damage == "checksum":
        next(iter(acceptance["heads"].values()))["ChecksumSHA256"] = "not the release checksum"
    elif damage == "catalog_location":
        descriptor["Location"] = "s3://private/releases/another-release/"
    elif damage == "catalog_schema":
        descriptor["Columns"][0]["Type"] = "string"
    elif damage == "catalog_format":
        descriptor["InputFormat"] = "org.apache.hadoop.mapred.TextInputFormat"
    else:
        next(iter(acceptance["tables"].values()))["PartitionKeys"] = [{"Name": "year", "Type": "int"}]
    with pytest.raises(ValueError):
        inspect(acceptance)


def queries():
    baseline = {"QueryExecutionId": "success", "WorkGroup": "taxi-dev", "Query": "select count(*) from private_data",
                "StatementType": "DML", "Status": {"State": "SUCCEEDED"}, "Statistics": {
                    "DataScannedInBytes": 100, "ResultReuseInformation": {"ReusedPreviousResult": False}}}
    result = []
    for identity, state, scanned, reused in (("success", "SUCCEEDED", 100, False), ("failed", "FAILED", 40, False),
            ("cancelled", "CANCELLED", 60, False), ("cached", "SUCCEEDED", 0, True), ("running", "RUNNING", None, None)):
        query = deepcopy(baseline)
        query.update(QueryExecutionId=identity, Status={"State": state}, Statistics={})
        if scanned is not None:
            query["Statistics"]["DataScannedInBytes"] = scanned
        if reused is not None:
            query["Statistics"]["ResultReuseInformation"] = {"ReusedPreviousResult": reused}
        result.append(query)
    return result


def test_ledger_retains_failed_and_cancelled_usage_without_zero_filling_unknowns():
    result = query_ledger(queries(), "taxi-dev")
    assert result["summary"] == {"query_count": 5, "counts_by_state": {
        "SUCCEEDED": 2, "FAILED": 1, "CANCELLED": 1, "RUNNING": 1, "QUEUED": 0},
        "known_scanned_bytes": 200, "unknown_scan_query_count": 1,
        "all_terminal_scan_counters_present": False, "eligible_comparison_query_count": 1, "billed_cost_usd": None}
    assert result["queries"][-1]["data_scanned_bytes"] is None
    assert "select count" not in json.dumps(result)
    assert "private_data" not in json.dumps(result)


def test_duplicate_query_ids_cannot_inflate_observed_usage():
    with pytest.raises(ValueError, match="Duplicate"):
        query_ledger(queries()[:1] * 2, "taxi-dev")


@pytest.mark.parametrize("value", [-1, True, "100", 1.5])
def test_invalid_usage_counter_fails_closed(value):
    query = queries()[0]
    query["Statistics"]["DataScannedInBytes"] = value
    with pytest.raises(ValueError, match="counter"):
        query_measurement(query, "taxi-dev")


def test_unknown_result_reuse_cannot_be_compared_as_a_cold_query():
    query = queries()[0]
    query["Statistics"].pop("ResultReuseInformation")
    result = query_measurement(query, "taxi-dev")
    assert result["reused_result"] is None
    assert result["eligible_for_scan_comparison"] is False

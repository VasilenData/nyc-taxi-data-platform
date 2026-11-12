"""Static policy contracts and SDK-boundary tests; these do not prove AWS deployment."""
import base64
import hashlib
import json
from pathlib import Path

import pytest

from taxi_platform.cloud_release import query_measurement, upload_release
from taxi_platform.release_bundle import export_release
from test_release_bundle import warehouse

TEMPLATE = Path(__file__).resolve().parents[1] / "cloud" / "aws" / "template.json"


def test_cloud_template_is_private_scoped_encrypted_and_scan_bounded():
    template = json.loads(TEMPLATE.read_text())
    resources = template["Resources"]
    for bucket in ("DataBucket", "ResultsBucket"):
        properties = resources[bucket]["Properties"]
        assert all(properties["PublicAccessBlockConfiguration"].values())
        assert properties["VersioningConfiguration"]["Status"] == "Enabled"
        assert properties["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]["ServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
        assert resources[bucket]["DeletionPolicy"] == "Retain"
    for role in ("QueryRole", "PublisherRole"):
        properties = resources[role]["Properties"]
        trust = properties["AssumeRolePolicyDocument"]["Statement"][0]
        assert trust["Principal"] == {"AWS": {"Ref": "OperatorPrincipalArn"}}
        for statement in properties["Policies"][0]["PolicyDocument"]["Statement"]:
            assert statement["Resource"] != "*"
            assert all(action != "*" and not action.endswith(":*") for action in statement["Action"])
    create_only = resources["DataBucketPolicy"]["Properties"]["PolicyDocument"]["Statement"][1]
    assert create_only["Effect"] == "Deny"
    assert create_only["Condition"]["Null"]["s3:if-none-match"] == "true"
    query_policy = resources["QueryRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    writes = [statement for statement in query_policy if "s3:PutObject" in statement["Action"]]
    assert len(writes) == 1 and "ResultsBucket.Arn" in str(writes[0]["Resource"])
    assert "DataBucket.Arn" not in str(writes[0]["Resource"])
    assert all("s3:DeleteObject" not in statement["Action"] for statement in query_policy)
    workgroup = resources["Workgroup"]["Properties"]["WorkGroupConfiguration"]
    assert workgroup["EnforceWorkGroupConfiguration"] is True
    assert workgroup["BytesScannedCutoffPerQuery"] == {"Ref": "PerQueryScanLimitBytes"}
    assert workgroup["ResultConfiguration"]["ExpectedBucketOwner"] == {"Ref": "AWS::AccountId"}


def test_measurement_records_observed_counters_without_inventing_a_bill():
    query = {"QueryExecutionId": "observed-id", "WorkGroup": "taxi-dev", "Query": "select count(*) from trips",
             "Status": {"State": "SUCCEEDED"}, "Statistics": {"DataScannedInBytes": 12748,
             "EngineExecutionTimeInMillis": 900, "TotalExecutionTimeInMillis": 1200,
             "ResultReuseInformation": {"ReusedPreviousResult": False}}}
    result = query_measurement(query, "taxi-dev")
    assert result["data_scanned_bytes"] == 12748
    assert result["billed_cost_usd"] is None
    with pytest.raises(ValueError, match="workgroup"):
        query_measurement(query, "other")
    query["Status"]["State"] = "FAILED"
    with pytest.raises(ValueError, match="successful"):
        query_measurement(query, "taxi-dev")


class FakeS3:
    def __init__(self, fail_at=None):
        self.objects = {}
        self.calls = []
        self.fail_at = fail_at

    def put_object(self, **kwargs):
        assert kwargs["IfNoneMatch"] == "*"
        assert kwargs["ExpectedBucketOwner"] == "123456789012"
        if len(self.calls) == self.fail_at:
            raise OSError("injected network interruption")
        self.calls.append(kwargs["Key"])
        self.objects[kwargs["Key"]] = kwargs["Body"]

    def head_object(self, **kwargs):
        body = self.objects[kwargs["Key"]]
        return {"ContentLength": len(body), "ChecksumSHA256": base64.b64encode(hashlib.sha256(body).digest()).decode()}


class FakeGlue:
    def __init__(self, s3):
        self.s3 = s3
        self.tables = []

    def create_table(self, **kwargs):
        assert self.s3.calls[-1].endswith("/manifest.json"), "Catalog exposed before verified commit marker"
        self.tables.append(kwargs["TableInput"])


def test_upload_checks_budget_and_remote_checksums_before_registering_tables(tmp_path):
    source, bundle = tmp_path / "source.duckdb", tmp_path / "release"
    warehouse(source)
    export_release(source, bundle)
    s3 = FakeS3()
    glue = FakeGlue(s3)
    with pytest.raises(ValueError, match="budget"):
        upload_release(s3, glue, bundle, "private", "taxi_dev", "123456789012", 1)
    assert not s3.calls
    result = upload_release(s3, glue, bundle, "private", "taxi_dev", "123456789012", 1000000)
    assert len(s3.calls) == 10
    assert len(glue.tables) == 2
    assert len(result["catalog_tables"]) == 2


def test_interrupted_upload_cannot_expose_a_commit_marker_or_catalog(tmp_path):
    source, bundle = tmp_path / "source.duckdb", tmp_path / "release"
    warehouse(source)
    export_release(source, bundle)
    s3 = FakeS3(fail_at=4)
    glue = FakeGlue(s3)
    with pytest.raises(OSError, match="network interruption"):
        upload_release(s3, glue, bundle, "private", "taxi_dev", "123456789012", 1000000)
    assert not any(key.endswith("manifest.json") for key in s3.calls)
    assert not glue.tables

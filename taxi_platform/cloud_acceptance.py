"""Read-only acceptance of an existing AWS release from the scoped query role.

Never deploys, starts queries, changes access or deletes objects. Successful reads
prove only the listed observed configuration and objects, not denied permissions.
"""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

from taxi_platform.cloud_release import AGGREGATES, glue_type, release_token
from taxi_platform.release_bundle import verify_release

MAX_MANIFEST_BYTES = 1024 * 1024


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_workgroup(group, name, results_bucket, owner, environment, max_scan_bytes):
    _require(group.get("Name") == name and group.get("State") == "ENABLED", "Workgroup name/state mismatch")
    config = group.get("Configuration", {})
    _require(config.get("EnforceWorkGroupConfiguration") is True, "Workgroup overrides are not enforced")
    cutoff = config.get("BytesScannedCutoffPerQuery")
    _require(type(cutoff) is int and 10_000_000 <= cutoff <= max_scan_bytes, "Scan cutoff missing or above approved bound")
    result = config.get("ResultConfiguration", {})
    _require(result.get("OutputLocation") == f"s3://{results_bucket}/{environment}/", "Unexpected query result location")
    _require(result.get("ExpectedBucketOwner") == owner, "Query results owner is not enforced")
    _require(result.get("EncryptionConfiguration", {}).get("EncryptionOption") == "SSE_S3", "Unexpected query result encryption")
    _require(config.get("PublishCloudWatchMetricsEnabled") is True, "Workgroup metrics are disabled")
    _require(config.get("EngineVersion", {}).get("EffectiveEngineVersion") == "Athena engine version 3",
             "Unexpected effective Athena engine")
    return {"name": name, "scan_cutoff_bytes": cutoff, "output_location": result["OutputLocation"],
            "enforced_configuration": True, "encrypted_results": True}


def inspect_release(sts, athena, s3, glue, bundle, *, data_bucket, results_bucket, database,
                    workgroup, owner, query_role_arn, environment, max_scan_bytes):
    """Check one locally verified release without downloading the fact dataset."""
    _require(re.fullmatch(r"[0-9]{12}", owner) is not None, "Expected a 12-digit account ID")
    _require(environment in {"dev", "prod"}, "Unknown environment")
    _require(type(max_scan_bytes) is int and max_scan_bytes >= 10_000_000, "Invalid approved scan bound")
    role_match = re.fullmatch(rf"arn:(aws(?:-us-gov|-cn)?):iam::{owner}:role/([A-Za-z0-9+=,.@_/-]+)", query_role_arn)
    _require(role_match is not None, "Query role must belong to the expected account")
    manifest = verify_release(bundle)
    release_id = manifest["release_id"]
    token = release_token(release_id)
    local_bytes = (Path(bundle) / "manifest.json").read_bytes()
    _require(len(local_bytes) <= MAX_MANIFEST_BYTES, "Manifest exceeds bounded read size")

    identity = sts.get_caller_identity()
    role_name = role_match.group(2).split("/")[-1]
    prefix = f"arn:{role_match.group(1)}:sts::{owner}:assumed-role/{role_name}/"
    _require(identity.get("Account") == owner and identity.get("Arn", "").startswith(prefix),
             "Acceptance must run as the expected scoped query role")
    observed_group = validate_workgroup(athena.get_work_group(WorkGroup=workgroup)["WorkGroup"], workgroup,
                                       results_bucket, owner, environment, max_scan_bytes)
    key_prefix = f"releases/{release_id}"
    remote = s3.get_object(Bucket=data_bucket, Key=f"{key_prefix}/manifest.json", ExpectedBucketOwner=owner)
    stream = remote["Body"]
    try:
        _require(remote.get("ContentLength") == len(local_bytes), "Commit manifest size differs")
        remote_bytes = stream.read(MAX_MANIFEST_BYTES + 1)
    finally:
        stream.close()
    _require(remote_bytes == local_bytes, "Remote commit manifest differs from the trusted local bundle")
    observed_tables = []
    for name in AGGREGATES:
        item = next(item for item in manifest["files"] if item["table"] == name)
        table_prefix = f"{key_prefix}/tables/{name}/"
        key = table_prefix + "data.parquet"
        listing = s3.list_objects_v2(Bucket=data_bucket, Prefix=table_prefix, MaxKeys=2, ExpectedBucketOwner=owner)
        _require(not listing.get("IsTruncated", False) and [obj["Key"] for obj in listing.get("Contents", [])] == [key],
                 f"Aggregate prefix has missing or unexpected objects: {name}")
        head = s3.head_object(Bucket=data_bucket, Key=key, ChecksumMode="ENABLED", ExpectedBucketOwner=owner)
        _require(head.get("ContentLength") == item["bytes"] and head.get("ChecksumSHA256") ==
                 base64.b64encode(bytes.fromhex(item["sha256"])).decode(), f"Aggregate checksum/size mismatch: {name}")
        _require(head.get("ServerSideEncryption") == "AES256", f"Unexpected aggregate encryption: {name}")
        table_name = f"release_{token}_{name}"
        table = glue.get_table(CatalogId=owner, DatabaseName=database, Name=table_name)["Table"]
        descriptor = table.get("StorageDescriptor", {})
        _require(table.get("Name") == table_name and table.get("TableType") == "EXTERNAL_TABLE"
                 and table.get("Parameters", {}).get("release_id") == release_id, f"Catalog identity mismatch: {name}")
        _require(table.get("PartitionKeys") == [], f"Unexpected catalog partition keys: {name}")
        _require(descriptor.get("Location") == f"s3://{data_bucket}/{table_prefix}", f"Catalog location mismatch: {name}")
        _require(descriptor.get("Columns") == [{"Name": column, "Type": glue_type(kind)} for column, kind in item["schema"]],
                 f"Catalog schema mismatch: {name}")
        _require(descriptor.get("InputFormat") == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
                 and descriptor.get("OutputFormat") == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
                 and descriptor.get("SerdeInfo", {}).get("SerializationLibrary") ==
                 "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                 and descriptor.get("SerdeInfo", {}).get("Parameters") in (None, {}), f"Catalog format mismatch: {name}")
        observed_tables.append({"table": table_name, "rows_in_local_verified_bundle": item["rows"],
                                "verified_object_bytes": item["bytes"], "sha256": item["sha256"]})
    return {"schema_version": 1, "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": "read-only observations of existing AWS configuration and two aggregate objects",
            "release_id": release_id, "manifest_sha256": hashlib.sha256(local_bytes).hexdigest(),
            "caller_account": owner, "query_role_arn": query_role_arn, "workgroup": observed_group,
            "tables": observed_tables, "configuration_and_release_checks_passed": True,
            "aws_queries_started": 0, "iam_denial_tests": "not performed", "query_result_reconciliation": "not performed",
            "cloud_billed_cost_usd": None,
            "limitations": ["A read-only check cannot prove effective negative permissions.",
                            "This is a point-in-time check, not protection against subsequent changes.",
                            "It does not establish SQL execution, sustained uptime or invoiced costs."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    for field in ("profile", "region", "data-bucket", "results-bucket", "database", "workgroup",
                  "account-id", "query-role-arn", "environment"):
        parser.add_argument("--" + field, required=True)
    parser.add_argument("--max-scan-bytes", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    config = Config(retries={"mode": "standard", "total_max_attempts": 3})
    result = inspect_release(*(session.client(service, config=config) for service in ("sts", "athena", "s3", "glue")),
                             args.bundle, data_bucket=args.data_bucket, results_bucket=args.results_bucket,
                             database=args.database, workgroup=args.workgroup, owner=args.account_id,
                             query_role_arn=args.query_role_arn, environment=args.environment,
                             max_scan_bytes=args.max_scan_bytes)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

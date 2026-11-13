"""Explicit AWS release upload and read-only Athena measurement collection.

Never provisions infrastructure or starts a query. Credentials come from the AWS
SDK credential chain; no key arguments or credential output are accepted.
"""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from uuid import UUID

from taxi_platform.release_bundle import verify_release

AGGREGATES = ("mart_daily_borough", "mart_hourly_demand")


def release_token(value):
    parsed = UUID(value)
    if str(parsed) != value:
        raise ValueError("Release ID must be a canonical lower-case UUID")
    return parsed.hex


def glue_type(value):
    types = {"VARCHAR": "string", "DATE": "date", "TIMESTAMP": "timestamp",
             "TIMESTAMP WITH TIME ZONE": "timestamp", "BIGINT": "bigint", "INTEGER": "int",
             "SMALLINT": "smallint", "TINYINT": "tinyint", "DOUBLE": "double",
             "FLOAT": "float", "BOOLEAN": "boolean", "HUGEINT": "decimal(38,0)"}
    if value in types:
        return types[value]
    if re.fullmatch(r"DECIMAL\([1-9][0-9]?,[0-9]+\)", value):
        return value.lower()
    raise ValueError(f"Unsupported Glue type: {value}")


def _put_immutable(s3, bucket, key, body, owner):
    checksum = base64.b64encode(hashlib.sha256(body).digest()).decode("ascii")
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*",
                      ChecksumSHA256=checksum, ServerSideEncryption="AES256",
                      ExpectedBucketOwner=owner)
    except Exception as exc:
        # An interrupted upload may safely resume only if the complete object agrees.
        if getattr(exc, "response", {}).get("Error", {}).get("Code") not in {"PreconditionFailed", "412"}:
            raise
    head = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED", ExpectedBucketOwner=owner)
    if head.get("ChecksumSHA256") != checksum or head.get("ContentLength") != len(body):
        raise ValueError(f"Remote object does not match immutable release: {key}")


def upload_release(s3, glue, bundle, bucket, database, owner, max_bytes):
    """Upload verified bytes, then a commit manifest, then two immutable catalog tables."""
    bundle = Path(bundle)
    manifest = verify_release(bundle)
    token = release_token(manifest["release_id"])
    manifest_bytes = (bundle / "manifest.json").read_bytes()
    if manifest["total_bytes"] + len(manifest_bytes) > max_bytes:
        raise ValueError("Release exceeds the explicit upload byte budget")
    # Validate the entire catalog definition before starting any cloud write.
    tables = []
    prefix = f'releases/{manifest["release_id"]}'
    for item in manifest["files"]:
        if item["table"] in AGGREGATES:
            columns = [{"Name": name, "Type": glue_type(kind)} for name, kind in item["schema"]]
            tables.append({"Name": f'release_{token}_{item["table"]}', "TableType": "EXTERNAL_TABLE", "PartitionKeys": [],
                           "Parameters": {"classification": "parquet", "release_id": manifest["release_id"]},
                           "StorageDescriptor": {"Columns": columns,
                               "Location": f's3://{bucket}/{prefix}/tables/{item["table"]}/',
                               "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                               "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                               "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}}})
    for item in manifest["files"]:
        body = (bundle / item["file"]).read_bytes()
        if hashlib.sha256(body).hexdigest() != item["sha256"]:
            raise ValueError("Release changed after verification")
        _put_immutable(s3, bucket, f'{prefix}/tables/{item["table"]}/data.parquet', body, owner)
    _put_immutable(s3, bucket, f"{prefix}/manifest.json", manifest_bytes, owner)
    for table in tables:
        try:
            glue.create_table(CatalogId=owner, DatabaseName=database, TableInput=table)
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") != "AlreadyExistsException":
                raise
            existing = glue.get_table(CatalogId=owner, DatabaseName=database, Name=table["Name"])["Table"]
            expected = table["StorageDescriptor"]
            actual = existing["StorageDescriptor"]
            if (existing.get("Name") != table["Name"] or existing.get("TableType") != table["TableType"]
                    or existing.get("Parameters", {}).get("release_id") != manifest["release_id"]
                    or existing.get("PartitionKeys") != []
                    or any(actual.get(key) != expected[key] for key in
                           ("Location", "Columns", "InputFormat", "OutputFormat"))
                    or actual.get("SerdeInfo", {}).get("SerializationLibrary") != expected["SerdeInfo"]["SerializationLibrary"]
                    or actual.get("SerdeInfo", {}).get("Parameters") not in (None, {})):
                raise ValueError("Existing catalog table differs from the immutable release")
    return {"release_id": manifest["release_id"], "verified_bytes": manifest["total_bytes"],
            "catalog_tables": [table["Name"] for table in tables], "cloud_execution": True,
            "scope": "uploaded serving backup; two aggregate tables queryable"}


def query_measurement(execution, expected_workgroup):
    """Retain every query state; failed/cancelled work can consume scanned bytes."""
    if execution["WorkGroup"] != expected_workgroup:
        raise ValueError("Query belongs to a different workgroup")
    state = execution["Status"]["State"]
    if state not in {"SUCCEEDED", "FAILED", "CANCELLED", "QUEUED", "RUNNING"}:
        raise ValueError("Unknown Athena query state")
    stats = execution.get("Statistics", {})
    counters = {key: stats.get(key) for key in
                ("DataScannedInBytes", "EngineExecutionTimeInMillis", "TotalExecutionTimeInMillis")}
    if any(value is not None and (type(value) is not int or value < 0) for value in counters.values()):
        raise ValueError("Invalid nonnegative integer execution counter")
    scanned = counters["DataScannedInBytes"]
    reused = stats.get("ResultReuseInformation", {}).get("ReusedPreviousResult")
    if reused is not None and type(reused) is not bool:
        raise ValueError("Invalid result-reuse indicator")
    terminal = state in {"SUCCEEDED", "FAILED", "CANCELLED"}
    return {"query_execution_id": execution["QueryExecutionId"], "workgroup": expected_workgroup,
            "sql_sha256": hashlib.sha256(execution["Query"].encode()).hexdigest(),
            "state": state, "terminal": terminal,
            "data_scanned_bytes": scanned, "engine_milliseconds": counters["EngineExecutionTimeInMillis"],
            "total_milliseconds": counters["TotalExecutionTimeInMillis"], "reused_result": reused,
            "eligible_for_scan_comparison": (state == "SUCCEEDED" and reused is False
                                              and scanned is not None and execution.get("StatementType") == "DML"),
            "billed_cost_usd": None, "billing_note": "AWS execution counters; invoice charges are not measured",
            "cloud_execution": True}


def query_ledger(executions, expected_workgroup):
    """A bounded supplied-query ledger, never a complete account/monthly bill."""
    if not 1 <= len(executions) <= 100:
        raise ValueError("Supply between 1 and 100 query executions")
    identities = [item["QueryExecutionId"] for item in executions]
    if len(set(identities)) != len(identities):
        raise ValueError("Duplicate query execution IDs would double-count usage")
    queries = [query_measurement(item, expected_workgroup) for item in executions]
    complete = all(item["terminal"] and item["data_scanned_bytes"] is not None for item in queries)
    return {"schema_version": 2, "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": "only the supplied query IDs; not workgroup-wide or invoice accounting",
            "queries": queries,
            "summary": {"query_count": len(queries),
                        "counts_by_state": {state: sum(item["state"] == state for item in queries)
                                            for state in ("SUCCEEDED", "FAILED", "CANCELLED", "RUNNING", "QUEUED")},
                        "known_scanned_bytes": sum(item["data_scanned_bytes"] or 0 for item in queries),
                        "unknown_scan_query_count": sum(item["data_scanned_bytes"] is None for item in queries),
                        "all_terminal_scan_counters_present": complete,
                        "eligible_comparison_query_count": sum(item["eligible_for_scan_comparison"] for item in queries),
                        "billed_cost_usd": None},
            "limitations": ["Missing counters are unknown, not zero.",
                            "Running-query counters are provisional.",
                            "Eligibility does not prove that compared SQL has equivalent semantics.",
                            "Excluded query IDs, S3, Glue and other charges are not measured."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="Existing named AWS SDK profile")
    parser.add_argument("--region", required=True)
    commands = parser.add_subparsers(dest="action", required=True)
    upload = commands.add_parser("upload")
    upload.add_argument("bundle", type=Path)
    for name in ("bucket", "database", "account-id"):
        upload.add_argument("--" + name, required=True)
    upload.add_argument("--max-bytes", type=int, required=True)
    measure = commands.add_parser("measure")
    measure.add_argument("--workgroup", required=True)
    measure.add_argument("--query-id", action="append", required=True)
    measure.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    config = Config(retries={"mode": "standard", "total_max_attempts": 3})
    if args.action == "upload":
        if not re.fullmatch(r"[0-9]{12}", args.account_id) or args.max_bytes <= 0:
            parser.error("A 12-digit account ID and positive byte budget are required")
        result = upload_release(session.client("s3", config=config), session.client("glue", config=config),
                                args.bundle, args.bucket, args.database, args.account_id, args.max_bytes)
    else:
        if not 1 <= len(args.query_id) <= 100 or len(set(args.query_id)) != len(args.query_id):
            parser.error("Supply 1 to 100 distinct query IDs")
        athena = session.client("athena", config=config)
        result = query_ledger([athena.get_query_execution(QueryExecutionId=q)["QueryExecution"]
                               for q in args.query_id], args.workgroup)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

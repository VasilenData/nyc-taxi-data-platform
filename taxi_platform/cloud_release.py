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
            tables.append({"Name": f'release_{token}_{item["table"]}', "TableType": "EXTERNAL_TABLE",
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
            if actual["Location"] != expected["Location"] or actual["Columns"] != expected["Columns"]:
                raise ValueError("Existing catalog table differs from the immutable release")
    return {"release_id": manifest["release_id"], "verified_bytes": manifest["total_bytes"],
            "catalog_tables": [table["Name"] for table in tables], "cloud_execution": True,
            "scope": "uploaded serving backup; two aggregate tables queryable"}


def query_measurement(execution, expected_workgroup):
    """Observed AWS counters, not a pricing estimate or billing statement."""
    if execution["WorkGroup"] != expected_workgroup:
        raise ValueError("Query belongs to a different workgroup")
    if execution["Status"]["State"] != "SUCCEEDED":
        raise ValueError("Only completed successful queries are comparable")
    stats = execution["Statistics"]
    scanned = stats["DataScannedInBytes"]
    if not isinstance(scanned, int) or scanned < 0:
        raise ValueError("Invalid scanned-byte counter")
    return {"query_execution_id": execution["QueryExecutionId"], "workgroup": expected_workgroup,
            "sql_sha256": hashlib.sha256(execution["Query"].encode()).hexdigest(),
            "data_scanned_bytes": scanned, "engine_milliseconds": stats.get("EngineExecutionTimeInMillis"),
            "total_milliseconds": stats.get("TotalExecutionTimeInMillis"),
            "reused_result": stats.get("ResultReuseInformation", {}).get("ReusedPreviousResult", False),
            "billed_cost_usd": None, "billing_note": "AWS execution counters; invoice charges are not measured",
            "cloud_execution": True}


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
        athena = session.client("athena", config=config)
        result = {"recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                  "queries": [query_measurement(athena.get_query_execution(QueryExecutionId=q)["QueryExecution"],
                                                args.workgroup) for q in args.query_id]}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

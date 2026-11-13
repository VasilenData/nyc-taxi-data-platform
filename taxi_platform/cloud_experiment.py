"""Explicit two-query AWS sandbox exercise; plan creation is entirely offline.

Execution can incur AWS charges. A declared budget is a review requirement, not
an enforced spending ceiling. It does not deploy infrastructure or change source
data or IAM. Athena writes query results to the existing private results bucket.
"""
import argparse
import base64
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import time

import duckdb

from taxi_platform.cloud_acceptance import inspect_release
from taxi_platform.cloud_release import AGGREGATES, query_ledger, release_token
from taxi_platform.release_bundle import literal, verify_release


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def make_plan(bundle, settings):
    """Bind exact read-only queries, local answers and limits to one trusted bundle."""
    fields = {"account_id", "region", "data_bucket", "results_bucket", "database", "workgroup",
              "publisher_role_arn", "query_role_arn", "budget_usd", "expires_at_utc", "experiment_id"}
    require(set(settings) == fields, "Unexpected or missing experiment settings")
    owner = settings["account_id"]
    require(isinstance(owner, str) and re.fullmatch(r"[0-9]{12}", owner), "Expected a 12-digit account ID")
    require(re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-[0-9]", settings["region"]), "Invalid region")
    for field in ("data_bucket", "results_bucket"):
        require(re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", settings[field]), "Invalid bucket name")
    require(settings["data_bucket"] != settings["results_bucket"], "Data and result buckets must differ")
    for field in ("database", "workgroup", "experiment_id"):
        require(re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,63}", settings[field]), "Unsafe identifier")
    for field in ("publisher_role_arn", "query_role_arn"):
        require(re.fullmatch(rf"arn:aws:iam::{owner}:role/[A-Za-z0-9+=,.@_/-]+", settings[field]), "Role/account mismatch")
    require(settings["publisher_role_arn"] != settings["query_role_arn"], "Publisher and query roles must differ")
    amount = settings["budget_usd"]
    require(isinstance(amount, str) and re.fullmatch(r"[0-9]+(?:\.[0-9]{1,2})?", amount)
            and Decimal(amount) > 0, "A positive explicit USD experiment budget is required")
    expiry = datetime.fromisoformat(settings["expires_at_utc"])
    require(expiry.tzinfo is not None and expiry.utcoffset().total_seconds() == 0, "Expiry must be UTC")
    manifest = verify_release(bundle)
    require(manifest["total_bytes"] + (Path(bundle) / "manifest.json").stat().st_size <= 16 * 1024 * 1024,
            "This exercise accepts only bundles up to 16 MiB including the manifest")
    token = release_token(manifest["release_id"])
    fact_rows = next(row["rows"] for row in manifest["files"] if row["table"] == "fct_trips")
    queries = []
    with duckdb.connect() as con:
        for table in AGGREGATES:
            path = Path(bundle) / (table + ".parquet")
            rows, trips = con.execute(f"select count(*), coalesce(sum(trips),0) from read_parquet({literal(path)})").fetchone()
            require(trips == fact_rows, "Local aggregate trip total differs from the fact count")
            name = f"release_{token}_{table}"
            sql = (f'SELECT CAST(count(*) AS bigint) AS aggregate_rows, '
                   f'CAST(coalesce(sum(trips),0) AS bigint) AS trips FROM "{settings["database"]}"."{name}"')
            queries.append({"table": name, "sql": sql, "expected": [int(rows), int(trips)]})
    plan = {"schema_version": 1, "settings": settings, "release_id": manifest["release_id"],
            "manifest_sha256": hashlib.sha256((Path(bundle) / "manifest.json").read_bytes()).hexdigest(),
            "environment": "dev", "max_queries": 2, "per_query_scan_limit_bytes": 104857600,
            "max_poll_seconds": 120, "queries": queries,
            "scope": "historical serving aggregates; two reconciliation queries and one bounded fact-read denial probe",
            "cost_limit": "Declared budget is not automatically enforced. Per-query scan limits are not total spending limits.",
            "authorization": "A plan or checksum is not permission; the operator must approve this exact scope before execution."}
    return {**plan, "plan_sha256": digest(plan)}


def verify_role(sts, role, owner):
    identity = sts.get_caller_identity()
    name = role.rsplit("/", 1)[1]
    require(identity.get("Account") == owner and identity.get("Arn", "").startswith(
        f"arn:aws:sts::{owner}:assumed-role/{name}/"), "Unexpected scoped role")
    return identity["Arn"]


def fact_read_denial(publisher_sts, publisher_s3, query_s3, bundle, plan):
    """Positive publisher existence/hash check, then a one-byte query-role read."""
    settings = plan["settings"]
    owner, bucket = settings["account_id"], settings["data_bucket"]
    verify_role(publisher_sts, settings["publisher_role_arn"], owner)
    manifest = verify_release(bundle)
    fact = next(item for item in manifest["files"] if item["table"] == "fct_trips")
    key = f'releases/{plan["release_id"]}/tables/fct_trips/data.parquet'
    head = publisher_s3.head_object(Bucket=bucket, Key=key, ExpectedBucketOwner=owner, ChecksumMode="ENABLED")
    require(head.get("ContentLength") == fact["bytes"] and head.get("ChecksumSHA256") ==
            base64.b64encode(bytes.fromhex(fact["sha256"])).decode(), "Publisher could not establish the expected fact object")
    try:
        result = query_s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-0", ExpectedBucketOwner=owner)
    except Exception as error:
        response = getattr(error, "response", {})
        require(response.get("Error", {}).get("Code") == "AccessDenied"
                and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 403,
                "Probe did not observe an explicit AccessDenied/403; inconclusive")
        return {"operation": "GetObject Range bytes=0-0", "fact_sha256": fact["sha256"],
                "publisher_existence_verified": True, "query_read": "AccessDenied", "http_status": 403,
                "limit": "One point-in-time read denial, not proof of all IAM denials; concurrent object deletion is not excluded."}
    else:
        result["Body"].close()
        raise ValueError("Query role unexpectedly read the fact object; no payload retained")


def parse_answer(response):
    require(not response.get("NextToken"), "Unexpected paginated aggregate result")
    result = response["ResultSet"]
    require([row["Name"] for row in result["ResultSetMetadata"]["ColumnInfo"]] == ["aggregate_rows", "trips"],
            "Unexpected result columns")
    rows = result["Rows"]
    require(len(rows) == 2 and [cell.get("VarCharValue") for cell in rows[0]["Data"]] == ["aggregate_rows", "trips"],
            "Expected one header and one aggregate result")
    values = [cell.get("VarCharValue") for cell in rows[1]["Data"]]
    require(len(values) == 2 and all(isinstance(v, str) and re.fullmatch(r"[0-9]+", v) for v in values),
            "Expected two nonnegative integer totals")
    return list(map(int, values))


def execute_plan(bundle, plan, clients, output, *, clock=time.monotonic, sleep=time.sleep):
    """Invoke only the reviewed two-query plan. Injectable clients enable offline tests."""
    expected = make_plan(bundle, plan["settings"])
    require(plan == expected, "Plan differs from verified bundle, queries or fixed limits")
    require(datetime.fromisoformat(plan["settings"]["expires_at_utc"]) > datetime.now(timezone.utc), "Plan has expired")
    output = Path(output)
    previous_queries = {}
    if output.exists():
        previous = json.loads(output.read_text(encoding="utf-8"))
        require(previous["plan_sha256"] == plan["plan_sha256"], "Journal belongs to another plan")
        previous_queries.update(previous.get("previous_queries", {}))
        previous_queries.update({row["request_token"]: row for row in previous["query_attempts"] if row.get("query_id")})
    report = {"schema_version": 1, "plan_sha256": plan["plan_sha256"], "release_id": plan["release_id"],
              "recorded_at_utc": datetime.now(timezone.utc).isoformat(), "status": "in_progress",
              "query_attempts": [], "previous_queries": previous_queries, "cloud_billed_cost_usd": None,
              "limits": ["Exactly two SQL statements per plan; repeated plans with new identities can incur new charges.",
                         "Idempotency tokens permit safe restart; cancellation is best-effort and can still incur charges.",
                         "Point-in-time exercise, not operating history or a guarantee of financial spending limits."]}

    def save():
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        temporary.replace(output)

    # Persist the plan identity before any cloud calls or query submission.
    save()
    sts, athena, s3, glue, publisher_sts, publisher_s3 = clients
    settings = plan["settings"]
    executions = []
    active = None
    try:
        report["acceptance"] = inspect_release(sts, athena, s3, glue, bundle,
            data_bucket=settings["data_bucket"], results_bucket=settings["results_bucket"], database=settings["database"],
            workgroup=settings["workgroup"], owner=settings["account_id"], query_role_arn=settings["query_role_arn"],
            environment="dev", max_scan_bytes=plan["per_query_scan_limit_bytes"])
        report["fact_read_denial"] = fact_read_denial(publisher_sts, publisher_s3, s3, bundle, plan)
        save()
        for query in plan["queries"]:
            request_token = digest({"plan": plan["plan_sha256"], "table": query["table"]})
            attempt = {"table": query["table"], "request_token": request_token,
                       "query_id": None, "status": "submission_pending"}
            report["query_attempts"].append(attempt)
            save()
            # SDK retries use the same token. Repeating this plan also returns its prior IDs.
            active = athena.start_query_execution(ClientRequestToken=request_token, QueryString=query["sql"],
                QueryExecutionContext={"Database": settings["database"], "Catalog": "AwsDataCatalog"},
                WorkGroup=settings["workgroup"], ResultReuseConfiguration={"ResultReuseByAgeConfiguration": {"Enabled": False}})["QueryExecutionId"]
            attempt["query_id"] = active
            attempt["status"] = "submitted"
            save()
            deadline = clock() + plan["max_poll_seconds"]
            while True:
                execution = athena.get_query_execution(QueryExecutionId=active)["QueryExecution"]
                require(execution["QueryExecutionId"] == active and execution["WorkGroup"] == settings["workgroup"]
                        and execution["Query"] == query["sql"], "Execution identity differs from submitted plan")
                state = execution["Status"]["State"]
                attempt["status"] = state
                if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                    executions.append(execution)
                    report["usage"] = query_ledger(executions, settings["workgroup"])
                    attempt["measurement"] = report["usage"]["queries"][-1]
                    save()
                    active = None
                    require(state == "SUCCEEDED", "Query did not succeed; no further statement submitted")
                    break
                require(state in {"QUEUED", "RUNNING"}, "Unknown query state")
                if clock() >= deadline:
                    executions.append(execution)
                    report["usage"] = query_ledger(executions, settings["workgroup"])
                    attempt["measurement"] = report["usage"]["queries"][-1]
                    raise TimeoutError("Query wait exceeded 120 seconds")
                sleep(1)
            actual = parse_answer(athena.get_query_results(QueryExecutionId=attempt["query_id"], MaxResults=3))
            attempt["actual"] = actual
            attempt["expected"] = query["expected"]
            require(actual == query["expected"], "Athena result differs from local Parquet oracle")
            attempt["reconciled"] = True
            save()
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["error_type"] = type(error).__name__
        # Do not copy exception messages/SDK payloads into shareable evidence.
        if active is not None:
            try:
                athena.stop_query_execution(QueryExecutionId=active)
                report["cancellation"] = {"query_id": active, "requested": True, "terminal_state_verified": False}
            except Exception:
                report["cancellation"] = {"query_id": active, "requested": False, "terminal_state_verified": False}
        save()
        raise
    save()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["plan", "run"])
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--settings", type=Path, help="Local settings JSON; no secrets")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--query-profile")
    parser.add_argument("--publisher-profile")
    parser.add_argument("--execute-reviewed-plan", action="store_true")
    args = parser.parse_args()
    if args.action == "plan":
        if args.settings is None:
            parser.error("plan requires --settings")
        result = make_plan(args.bundle, json.loads(args.settings.read_text(encoding="utf-8")))
        args.plan.parent.mkdir(parents=True, exist_ok=True)
        args.plan.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"plan_sha256": result["plan_sha256"], "cloud_calls": 0, "queries_planned": 2}))
        return
    if not (args.execute_reviewed_plan and args.output and args.query_profile and args.publisher_profile):
        parser.error("run requires --execute-reviewed-plan, --output and both existing scoped profiles")
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    # Reject altered/expired plans before even constructing a credential-chain client.
    require(plan == make_plan(args.bundle, plan["settings"]), "Plan differs from current trusted bundle")
    require(datetime.fromisoformat(plan["settings"]["expires_at_utc"]) > datetime.now(timezone.utc), "Plan has expired")
    import boto3
    from botocore.config import Config
    config = Config(connect_timeout=5, read_timeout=15, retries={"mode": "standard", "total_max_attempts": 2})
    query = boto3.Session(profile_name=args.query_profile, region_name=plan["settings"]["region"])
    publisher = boto3.Session(profile_name=args.publisher_profile, region_name=plan["settings"]["region"])
    clients = [query.client(service, config=config) for service in ("sts", "athena", "s3", "glue")]
    clients.extend(publisher.client(service, config=config) for service in ("sts", "s3"))
    result = execute_plan(args.bundle, plan, clients, args.output)
    print(json.dumps({"status": result["status"], "output": str(args.output), "cloud_billed_cost_usd": None}))


if __name__ == "__main__":
    main()

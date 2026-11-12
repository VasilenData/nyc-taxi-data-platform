"""Review a bounded source correction, then apply that exact plan.

Single-writer maintenance workflow. Raw working data can change on a failed build;
the eight consumer tables retain their last good transaction. A correction reloads
only changed source months; downstream fact/mart tables are still rebuilt together.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

import duckdb

from . import config, ingest
from .publication import TABLES
from .revision import model_files, model_signature, published_revision

PLAN_VERSION = 2


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def raw_state(con) -> list:
    return [list(row) for row in con.execute("""select _source_month, count(*),
        cast(min(_loaded_at) as varchar), cast(max(_loaded_at) as varchar),
        cast(coalesce(sum(hash(t)), 0) as varchar)
        from raw.trips t group by 1 order by 1""").fetchall()]


def relation_state(con) -> dict:
    """Order-independent drift guards, not cryptographic/tamper-proof hashes.

    The persisted classifier can affect a future incremental build even when the
    publication ID and raw load timestamps have not changed. Schema guards also
    detect added nullable columns, which row counts alone cannot identify.
    """
    existing = {tuple(row) for row in con.execute("""select table_schema, table_name
        from information_schema.tables where table_catalog = current_database()
        """).fetchall()}
    relations = [("raw", "trips"), ("build", "stg_trips_classified"),
                 *(("main", name) for name in (*TABLES, "publication"))]
    result = {}
    for namespace, name in relations:
        key = f"{namespace}.{name}"
        if (namespace, name) not in existing:
            result[key] = None
            continue
        query_name = f'"{namespace}"."{name}"'
        result[key] = {
            "schema": [list(row[:2]) for row in con.execute(f"describe {query_name}").fetchall()],
            "rows_and_fingerprint": list(con.execute(f"""select count(*),
                cast(coalesce(sum(hash(t)), 0) as varchar) from {query_name} t""").fetchone())}
    return result


def consumer_state(con) -> dict:
    # Commutative fingerprints detect changed contents without relying on row
    # order. These are regression checksums, not cryptographic integrity proofs.
    relations = {name: list(con.execute(f'''select count(*),
        cast(coalesce(sum(hash(t)), 0) as varchar) from main."{name}" t''').fetchone())
        for name in (*TABLES, "publication")}
    classified = {month: [count, fingerprint] for month, count, fingerprint in
                  con.execute("""select _source_month, count(*),
                    cast(sum(hash(t)) as varchar) from main.stg_trips_classified t
                    group by 1 order by 1""").fetchall()}
    return {"relations": relations, "classified_months": classified}


def create_plan(database: Path, data_dir: Path, months: list[str]) -> dict:
    if len(set(months)) != len(months):
        raise ValueError("Source months must be unique")
    for month in months:
        ingest.validate_month(month)
    database, data_dir = database.resolve(), data_dir.resolve()
    with duckdb.connect(str(database), read_only=True) as con:
        publication = published_revision(con)
        if publication["version"] is None:
            raise ValueError("Create an initial successful publication before planning a correction")
        existing = raw_state(con)
        relations = relation_state(con)
        published_columns = {r[0] for r in con.execute("describe main.publication").fetchall()}
        old_files_json = con.execute("select model_files from main.publication").fetchone()[0] \
            if "model_files" in published_columns else None
        old_files = json.loads(old_files_json) if old_files_json else {}
        sources = []
        for month in sorted(months):
            path = data_dir / f"yellow_tripdata_{month}.parquet"
            _, schema = ingest.inspect_source(con, path)
            checksum = ingest.source_fingerprint(path)
            rows = con.execute("select count(*) from read_parquet(?)", [str(path)]).fetchone()[0]
            if not rows:
                raise ValueError("Empty correction cannot replace an existing month")
            previous = con.execute("""select source_sha256 from audit.source_loads
                where source_month = ? order by loaded_at desc, load_id desc limit 1""", [month]).fetchone()
            sources.append({"month": month, "file": path.name, "sha256": checksum,
                            "rows": rows, "schema": schema,
                            "reload": previous is None or previous[0] != checksum})
    signature = model_signature()
    full_refresh = publication["model_sha256"] != signature
    plan = {"schema_version": PLAN_VERSION, "created_utc": datetime.now(timezone.utc).isoformat(),
            "database": str(database), "data_dir": str(data_dir),
            "expected_publication": publication, "expected_raw_state": existing,
            "expected_relation_state": relations, "fingerprint_duckdb_version": duckdb.__version__,
            "model_sha256": signature, "model_files": model_files(), "sources": sources,
            "changed_transformation_files": sorted(name for name in set(old_files) | set(model_files())
                                                    if old_files.get(name) != model_files().get(name)),
            "reload_months": [s["month"] for s in sources if s["reload"]],
            "full_reclassification": full_refresh,
            "affected_classification_months": sorted({r[0] for r in existing} | set(months)) if full_refresh
                else [s["month"] for s in sources if s["reload"]],
            "consumer_tables_rebuilt": list(TABLES),
            "publication_boundary": "all eight consumer tables and current metadata, one DuckDB transaction",
            "scope": "single writer; source-month incremental classification, full downstream fact/mart rebuild"}
    plan["plan_sha256"] = digest(plan)
    return plan


def validate_plan(plan: dict) -> None:
    if plan.get("schema_version") != PLAN_VERSION:
        raise ValueError("Unsupported change plan version; create a new plan")
    unsigned = {k: v for k, v in plan.items() if k != "plan_sha256"}
    if digest(unsigned) != plan.get("plan_sha256"):
        raise ValueError("Change plan checksum does not match")
    if model_signature() != plan["model_sha256"]:
        raise ValueError("Transformation changed since planning; create a new plan")
    if duckdb.__version__ != plan["fingerprint_duckdb_version"]:
        raise ValueError("DuckDB version changed since planning; create a new plan")
    with duckdb.connect(plan["database"], read_only=True) as con:
        if relation_state(con) != plan["expected_relation_state"]:
            raise ValueError("Warehouse changed since planning; create a new plan")
        if published_revision(con) != plan["expected_publication"] or raw_state(con) != plan["expected_raw_state"]:
            raise ValueError("Warehouse changed since planning; create a new plan")
        for source in plan["sources"]:
            path = Path(plan["data_dir"]) / source["file"]
            if ingest.source_fingerprint(path) != source["sha256"]:
                raise ValueError("Source changed since planning; create a new plan")
            ingest.inspect_source(con, path)


def apply_plan(plan: dict) -> dict:
    validate_plan(plan)  # All sources validated before the first replacement.
    config.DB_PATH = Path(plan["database"])
    config.DATA_DIR = Path(plan["data_dir"])
    started = time.perf_counter()
    with ingest.connect() as con:
        before = consumer_state(con)
    loaded = []
    error = None
    success = False
    try:
        for source in plan["sources"]:
            if source["reload"]:
                loaded.append(ingest.load_month(source["month"], expected_sha256=source["sha256"]))
        if plan["reload_months"] or plan["full_reclassification"]:
            from run_pipeline import run
            success = run([])
        else:
            success = True
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    with ingest.connect() as con:
        after = consumer_state(con)
        publication = published_revision(con)
    unaffected = set(before["classified_months"]) - set(plan["affected_classification_months"])
    unchanged = all(before["classified_months"][m] == after["classified_months"].get(m)
                    for m in unaffected)
    return {"schema_version": 1, "completed_utc": datetime.now(timezone.utc).isoformat(),
            "plan_sha256": plan["plan_sha256"], "success": success and unchanged,
            "pipeline_success": success, "error": error,
            "duration_seconds": round(time.perf_counter() - started, 3),
            "reload_months": [r["month"] for r in loaded],
            "full_reclassification": plan["full_reclassification"],
            "unaffected_classifier_months": sorted(unaffected),
            "unaffected_classifier_months_unchanged": unchanged,
            "consumer_output_preserved": before == after,
            "publication": publication, "before": before, "after": after,
            "limits": "Controlled single-writer maintenance; raw working data may contain a failed candidate. No old-version retention."}


def write_new(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    plan_parser = sub.add_parser("plan")
    plan_parser.add_argument("--database", type=Path, required=True)
    plan_parser.add_argument("--data-dir", type=Path, required=True)
    plan_parser.add_argument("--months", nargs="*", default=[])
    plan_parser.add_argument("--output", type=Path, required=True)
    apply_parser = sub.add_parser("apply")
    apply_parser.add_argument("--plan", type=Path, required=True)
    apply_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Evidence output already exists; choose a new filename")
    if args.action == "plan":
        record = create_plan(args.database, args.data_dir, args.months)
    else:
        plan = json.loads(args.plan.read_text(encoding="utf-8"))
        # dbt itself reads TAXI_DB; import Dagster only after setting the same path.
        import os
        os.environ["TAXI_DB"] = plan["database"]
        os.environ["TAXI_DATA_DIR"] = plan["data_dir"]
        record = apply_plan(plan)
    write_new(args.output, record)
    print(json.dumps({"output": str(args.output), "reload_months": record["reload_months"],
                      "full_reclassification": record["full_reclassification"],
                      "success": record.get("success")}, indent=2))
    return 0 if record.get("success", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())

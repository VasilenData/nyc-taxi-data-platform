"""Publish a validated build snapshot atomically inside one DuckDB database.

DuckDB rolls the transaction back after an exception or process death before commit.
This is a single-writer warehouse, not a multi-process serving layer. Readers needing
one version across several SELECTs must use a read transaction.
"""
import json
import time
from uuid import uuid4

from . import config, ingest
from .quality import evaluate
from .revision import model_files, model_signature

TABLES = (
    "taxi_zone_lookup", "stg_trips_classified", "stg_trips", "trips_quarantine",
    "fct_trips", "mart_daily_borough", "mart_hourly_demand", "mart_data_quality",
)


def publish(con=None) -> dict:
    own = con is None
    con = con or ingest.connect()
    version = str(uuid4())
    started = time.perf_counter()
    try:
        con.execute("begin")
        try:
            failed = [name for name, (passed, _) in evaluate(con, config.MAX_REJECT_RATE).items() if not passed]
            if failed:
                raise ValueError("Publication refused: " + ", ".join(failed))
            counts = {name: con.execute(f'select count(*) from build."{name}"').fetchone()[0]
                      for name in TABLES}
            if counts["stg_trips"] != counts["fct_trips"]:
                raise ValueError("Publication refused: fact/valid row mismatch")
            for name in ("mart_daily_borough", "mart_hourly_demand"):
                total = con.execute(f'select coalesce(sum(trips),0) from build."{name}"').fetchone()[0]
                if total != counts["fct_trips"]:
                    raise ValueError(f"Publication refused: {name}/fact row mismatch")
            validated_at = time.perf_counter()
            # Older installs have main views referencing the old classifier.
            views = {r[0] for r in con.execute("""select table_name from information_schema.views
                where table_catalog = current_database() and table_schema = 'main'""").fetchall()}
            for name in reversed(TABLES):
                if name in views:
                    con.execute(f'drop view main."{name}"')
            for name in TABLES:
                con.execute(f'create or replace table main."{name}" as select * from build."{name}"')
            copied_at = time.perf_counter()
            loads = con.execute("""select _source_month, _source_file, cast(_loaded_at as varchar), count(*)
                from raw.trips group by 1,2,3 order by 1,2,3""").fetchall()
            con.execute("create schema if not exists audit")
            con.execute("""create table if not exists audit.publications
                (version varchar primary key, published_at timestamp, source_loads json, row_counts json)""")
            con.execute("alter table audit.publications add column if not exists model_sha256 varchar")
            con.execute("alter table audit.publications add column if not exists model_files json")
            con.execute("""insert into audit.publications
                (version, published_at, source_loads, row_counts, model_sha256, model_files)
                values (?, current_timestamp, ?, ?, ?, ?)""",
                        [version, json.dumps(loads), json.dumps(counts), model_signature(), json.dumps(model_files())])
            con.execute("""create or replace table main.publication as
                select * from audit.publications where version = ?""", [version])
            con.execute("commit")
        except BaseException:
            con.execute("rollback")
            raise
        return {"version": version, "fact_rows": counts["fct_trips"], "tables": len(TABLES),
                "publication_seconds": round(time.perf_counter() - started, 6),
                "validation_seconds": round(validated_at - started, 6),
                "copy_seconds": round(copied_at - validated_at, 6)}
    finally:
        if own:
            con.close()

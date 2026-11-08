"""Publish a validated build snapshot atomically inside one DuckDB database.

DuckDB rolls the transaction back after an exception or process death before commit.
This is a single-writer warehouse, not a multi-process serving layer. Readers needing
one version across several SELECTs must use a read transaction.
"""
import json
from uuid import uuid4

from . import config, ingest
from .quality import evaluate

TABLES = (
    "taxi_zone_lookup", "stg_trips_classified", "stg_trips", "trips_quarantine",
    "fct_trips", "mart_daily_borough", "mart_hourly_demand", "mart_data_quality",
)


def publish(con=None) -> dict:
    own = con is None
    con = con or ingest.connect()
    version = str(uuid4())
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
            # Older installs have main views referencing the old classifier.
            views = {r[0] for r in con.execute("""select table_name from information_schema.views
                where table_catalog = current_database() and table_schema = 'main'""").fetchall()}
            for name in reversed(TABLES):
                if name in views:
                    con.execute(f'drop view main."{name}"')
            for name in TABLES:
                con.execute(f'create or replace table main."{name}" as select * from build."{name}"')
            loads = con.execute("""select _source_month, _source_file, cast(_loaded_at as varchar), count(*)
                from raw.trips group by 1,2,3 order by 1,2,3""").fetchall()
            con.execute("create schema if not exists audit")
            con.execute("""create table if not exists audit.publications
                (version varchar primary key, published_at timestamp, source_loads json, row_counts json)""")
            con.execute("insert into audit.publications values (?, current_timestamp, ?, ?)",
                        [version, json.dumps(loads), json.dumps(counts)])
            con.execute("""create or replace table main.publication as
                select * from audit.publications where version = ?""", [version])
            con.execute("commit")
        except BaseException:
            con.execute("rollback")
            raise
        return {"version": version, "fact_rows": counts["fct_trips"], "tables": len(TABLES)}
    finally:
        if own:
            con.close()

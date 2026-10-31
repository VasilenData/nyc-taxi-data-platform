"""Raw-layer loader: one TLC parquet file per month into raw.trips.

Design rules
- Idempotent: loading a month twice leaves one copy (delete the month, then insert).
- Append-only lineage columns: _source_month, _source_file, _loaded_at.
- Schema drift tolerant: columns are selected by name and cast, so TLC's renamed columns
  (Airport_fee vs airport_fee) do not break the load.
- Nothing is cleaned here. Bad rows are kept raw and judged by dbt, so every rejection is auditable.
"""
from __future__ import annotations

import re
import urllib.request
from pathlib import Path

import duckdb

from . import config

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

COLUMNS = {
    "VendorID": "BIGINT",
    "tpep_pickup_datetime": "TIMESTAMP",
    "tpep_dropoff_datetime": "TIMESTAMP",
    "passenger_count": "DOUBLE",
    "trip_distance": "DOUBLE",
    "RatecodeID": "DOUBLE",
    "store_and_fwd_flag": "VARCHAR",
    "PULocationID": "BIGINT",
    "DOLocationID": "BIGINT",
    "payment_type": "BIGINT",
    "fare_amount": "DOUBLE",
    "extra": "DOUBLE",
    "mta_tax": "DOUBLE",
    "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "improvement_surcharge": "DOUBLE",
    "total_amount": "DOUBLE",
    "congestion_surcharge": "DOUBLE",
}


def validate_month(month: str) -> str:
    if not MONTH_RE.match(month):
        raise ValueError(f"month must look like 2023-01, got {month!r}")
    return month


def resolve_source(month: str) -> Path:
    """Find the parquet for a month: data dir, then bundled sample, then download."""
    validate_month(month)
    name = f"yellow_tripdata_{month}.parquet"
    for folder in (config.DATA_DIR, config.SAMPLE_DIR):
        candidate = folder / name
        if candidate.exists():
            return candidate
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = config.DATA_DIR / name
    tmp = target.with_suffix(".part")
    urllib.request.urlretrieve(config.TLC_URL.format(month=month), tmp)  # noqa: S310 (fixed https URL)
    tmp.rename(target)
    return target


def connect(path: Path | None = None) -> duckdb.DuckDBPyConnection:
    path = Path(path or config.DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    cols = ",\n    ".join(f'"{c}" {t}' for c, t in COLUMNS.items())
    con.execute("create schema if not exists raw")
    con.execute(
        f"""create table if not exists raw.trips (
    {cols},
    airport_fee DOUBLE,
    _source_month VARCHAR,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP
)"""
    )


def load_month(month: str, con: duckdb.DuckDBPyConnection | None = None) -> dict:
    """Load one month. Returns {'month', 'rows', 'file'} for the Dagster asset metadata."""
    validate_month(month)
    own = con is None
    con = con or connect()
    try:
        ensure_schema(con)
        src = resolve_source(month)
        available = {r[0].lower(): r[0] for r in con.execute(
            "select name from parquet_schema(?)", [str(src)]).fetchall()}
        select = []
        for col, typ in COLUMNS.items():
            select.append(f'try_cast("{available[col.lower()]}" as {typ}) as "{col}"' if col.lower() in available
                          else f'cast(null as {typ}) as "{col}"')
        fee = available.get("airport_fee")
        select.append(f'try_cast("{fee}" as DOUBLE) as airport_fee' if fee else "cast(null as DOUBLE) as airport_fee")
        con.execute("begin")
        con.execute("delete from raw.trips where _source_month = ?", [month])
        con.execute(
            f"""insert into raw.trips
            select {', '.join(select)}, ?, ?, now()::timestamp
            from read_parquet(?)""",
            [month, src.name, str(src)],
        )
        con.execute("commit")
        rows = con.execute("select count(*) from raw.trips where _source_month = ?", [month]).fetchone()[0]
        return {"month": month, "rows": rows, "file": src.name}
    finally:
        if own:
            con.close()

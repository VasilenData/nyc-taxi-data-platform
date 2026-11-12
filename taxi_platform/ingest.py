"""Raw-layer loader: one TLC parquet file per month into raw.trips.

Design rules
- Idempotent: loading a month twice leaves one copy (delete the month, then insert).
- Append-only lineage columns: _source_month, _source_file, _loaded_at.
- Schema drift tolerant: columns are selected by name and cast, so TLC's renamed columns
  (Airport_fee vs airport_fee) do not break the load.
- Required source fields must exist before replacing a month; optional fee fields remain nullable.
- Row-level bad values are cast/kept raw and judged by dbt. Source schema and SHA-256 are recorded.
"""
from __future__ import annotations

import hashlib
import json
import re
from uuid import uuid4
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


# Columns used by the classifier, trip identity or published metrics. Optional
# historical surcharges stay nullable; a missing business field is schema drift.
REQUIRED_COLUMNS = {
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "pulocationid", "dolocationid", "payment_type", "fare_amount",
    "tip_amount", "tolls_amount", "total_amount",
}


def source_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_source(con, path: Path) -> tuple[dict[str, str], list[dict]]:
    # DESCRIBE reads top-level logical columns: parquet_schema also includes
    # nested children that must not masquerade as a required top-level field.
    described = con.execute("describe select * from read_parquet(?)", [str(path)]).fetchall()
    available = {row[0].lower(): row[0] for row in described}
    missing = sorted(REQUIRED_COLUMNS - available.keys())
    if missing:
        raise ValueError(f"Source schema missing required columns: {', '.join(missing)}; "
                         "previous month preserved")
    nested = [name for name, typ, *_ in described
              if name.lower() in REQUIRED_COLUMNS and any(token in typ for token in ("STRUCT", "[]", "MAP("))]
    if nested:
        raise ValueError(f"Source schema has nested required columns: {', '.join(nested)}; "
                         "previous month preserved")
    return available, [{"name": row[0], "type": row[1]} for row in described]


def validate_month(month: str) -> str:
    if not MONTH_RE.fullmatch(month):
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
    return duckdb.connect(str(path), config={"memory_limit": config.DUCKDB_MEMORY_LIMIT})


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
    con.execute("create schema if not exists audit")
    con.execute("""create table if not exists audit.source_loads (
        load_id varchar primary key, source_month varchar, source_file varchar,
        source_sha256 varchar, source_bytes bigint, source_schema json,
        missing_optional_columns json, rows bigint, loaded_at timestamp)""")


def load_month(month: str, con: duckdb.DuckDBPyConnection | None = None,
               expected_sha256: str | None = None) -> dict:
    """Load one month. Returns {'month', 'rows', 'file'} for the Dagster asset metadata."""
    validate_month(month)
    own = con is None
    con = con or connect()
    try:
        ensure_schema(con)
        src = resolve_source(month)
        available, source_schema = inspect_source(con, src)
        fingerprint = source_fingerprint(src)
        if expected_sha256 is not None and fingerprint != expected_sha256:
            raise ValueError("Source no longer matches reviewed change plan; previous month preserved")
        source_bytes = src.stat().st_size
        load_id = str(uuid4())
        missing_optional = sorted((set(c.lower() for c in COLUMNS) | {"airport_fee"}) - available.keys())
        select = []
        for col, typ in COLUMNS.items():
            select.append(f'try_cast("{available[col.lower()]}" as {typ}) as "{col}"' if col.lower() in available
                          else f'cast(null as {typ}) as "{col}"')
        fee = available.get("airport_fee")
        select.append(f'try_cast("{fee}" as DOUBLE) as airport_fee' if fee else "cast(null as DOUBLE) as airport_fee")
        con.execute("begin")
        try:
            con.execute("delete from raw.trips where _source_month = ?", [month])
            con.execute(
                f"""insert into raw.trips
                select {', '.join(select)}, ?, ?, now()::timestamp
                from read_parquet(?)""",
                [month, src.name, str(src)],
            )
            rows = con.execute("select count(*) from raw.trips where _source_month = ?", [month]).fetchone()[0]
            if rows == 0:
                raise ValueError(f"Refusing empty input for {month}: {src.name}; previous rows have been preserved")
            if source_fingerprint(src) != fingerprint:
                raise ValueError("Source changed during ingestion; previous month preserved")
            con.execute("""insert into audit.source_loads values (?, ?, ?, ?, ?, ?, ?, ?, now()::timestamp)""",
                        [load_id, month, src.name, fingerprint, source_bytes,
                         json.dumps(source_schema), json.dumps(missing_optional), rows])
            con.execute("commit")
        except BaseException:
            # A caller-owned connection must be usable again after a failed replacement,
            # and a malformed/empty corrected file must not erase the previous month.
            con.execute("rollback")
            raise
        return {"month": month, "rows": rows, "file": src.name,
                "load_id": load_id, "sha256": fingerprint}
    finally:
        if own:
            con.close()

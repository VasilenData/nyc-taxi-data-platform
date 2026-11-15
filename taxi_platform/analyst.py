"""A bounded analyst release from one verified sample publication; no cloud calls."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import shutil
import tempfile

import duckdb

from .mart_contract import validate_marts
from .release_bundle import export_release, literal, sha256, verify_release

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_INPUTS = {
    "2023-01": "f517dc1958e3bfc29c74a7fb7c7e1c112e2fca05c48e06c970d6ab3aab2f6558",
    "2023-02": "ef77b0d663ca4927a2cd9fc12ab77fd3d55252b12a5673ef37581ca260a730bd",
}
FIELDS = ["pickup_date", "pickup_borough", "trips", "total_amount_cents"]
MAX_SAFE_INTEGER = 2**53 - 1
MAX_ROWS = 10000
METRICS = {
    "grain": "One observed NYC-local pickup date and pickup borough; absent groups are missing observations, not measured zero demand.",
    "pickup_date": "YYYY-MM-DD from the source pickup timestamp; naive NYC local civil time, no UTC conversion.",
    "pickup_borough": "Pickup taxi-zone lookup borough; null mapped to Unknown. EWR and Unknown are retained.",
    "trips": "Valid fact rows, deduplicated within source month using the pipeline trip identity; integer count.",
    "total_amount_cents": "Sum of recorded total_amount in USD cents (fare, taxes, tolls, tips and other recorded components). Not driver earnings, net revenue or average fare. Every source fact must be representable to cents; summed as integers.",
    "quality": "Whole-release source-month totals and first failing rejection rule; not filtered borough/date statistics.",
    "missing": "No aggregate row means no observed valid sample rows. It does not establish zero activity; percent change is unavailable when the comparison has no observations or a zero denominator.",
    "scope": "Historical January/February 2023 public TLC extracts: exactly 15,000 bundled source rows per month. A convenience sample, not a random or representative estimate of NYC demand.",
    "exclusions": "Missing/nonpositive/6-hour-or-longer timestamps or durations; pickups outside source month; nonfinite measures; distance outside (0,200] miles; missing/negative fare or nonpositive total; zone outside 1..265; repeated trip identity within month. First failing rule is retained in quarantine.",
}


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def remove_new_directory(path, boundary):
    """Remove only a checked, newly-created child; support deep dbt paths on Windows."""
    path, boundary = Path(path).resolve(), Path(boundary).resolve()
    if path == boundary or not path.is_relative_to(boundary):
        raise ValueError("Refusing cleanup outside the new staging directory")
    target = str(path)
    if os.name == "nt" and not target.startswith("\\\\?\\"):
        target = "\\\\?\\UNC\\" + target[2:] if target.startswith("\\\\") else "\\\\?\\" + target
    shutil.rmtree(target)


def check_inputs(directory=None):
    directory = Path(directory or ROOT / "data" / "sample")
    result = []
    for month, expected in SAMPLE_INPUTS.items():
        path = directory / f"yellow_tripdata_{month}.parquet"
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"Bundled sample file missing or not regular: {path.name}; restore it from Git. No download attempted.")
        if sha256(path) != expected:
            raise ValueError(f"Bundled sample checksum mismatch: {path.name}; restore it from Git. Prior results preserved.")
        result.append({"source_month": month, "file": path.name, "sha256": expected,
                       "bytes": path.stat().st_size, "rows": 15000})
    return result


def _source_identity(database, publication):
    """Join immutable load history to the exact load IDs represented in the backup."""
    expected = check_inputs()
    sources = []
    with duckdb.connect(str(database), read_only=True) as con:
        for month, filename, loaded_at, rows in json.loads(publication["source_loads"]):
            matches = con.execute("""select source_sha256, source_bytes, rows from audit.source_loads
                where source_month = ? and source_file = ? and loaded_at = cast(? as timestamp)""",
                                  [month, filename, loaded_at]).fetchall()
            if len(matches) != 1:
                raise ValueError(f"Missing or ambiguous audited source identity for {month}")
            checksum, size, audited_rows = matches[0]
            if rows != audited_rows:
                raise ValueError(f"Source row count mismatch for {month}")
            sources.append({"source_month": month, "file": Path(filename).name, "sha256": checksum,
                            "bytes": size, "rows": rows})
    if sources != expected:
        raise ValueError("Analyst sample release requires exactly the two bundled 15,000-row inputs; this publication differs")
    return sources


def _records(con, sql):
    cursor = con.execute(sql)
    return [dict(zip([c[0] for c in cursor.description], row)) for row in cursor.fetchall()]


def export_analyst(database, destination):
    """Stage, reconcile and rename one immutable package; previous packages survive failures."""
    destination = Path(destination).resolve()
    if destination.exists():
        raise FileExistsError("Analyst releases are immutable; choose a new destination")
    if not Path(database).is_file():
        raise ValueError("Warehouse is missing; run python -m taxi_platform.sample first")
    # Bound work before the general backup exporter touches fact data. A full
    # 31.6M-row warehouse must be refused here, not after making a large backup.
    with duckdb.connect(str(database), read_only=True) as con:
        publications = _records(con, "select * from main.publication")
    if len(publications) != 1:
        raise ValueError("Exactly one published snapshot is required")
    preflight = publications[0]
    _source_identity(database, preflight)
    limits = {"taxi_zone_lookup": 265, "stg_trips_classified": 30000, "stg_trips": 30000,
              "trips_quarantine": 30000, "fct_trips": 30000, "mart_daily_borough": MAX_ROWS,
              "mart_hourly_demand": 168, "mart_data_quality": 100}
    if any(not 0 <= json.loads(preflight["row_counts"]).get(name, -1) <= maximum for name, maximum in limits.items()):
        raise ValueError("Published counts exceed bounded sample-release limits")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".analyst-", dir=destination.parent))
    try:
        # Reuse the existing nine-relation snapshot exporter and verifier. This is
        # an analyst projection of that snapshot, not a second backup format.
        backup = staging / "snapshot"
        export_release(database, backup)
        receipt = verify_release(backup)
        if receipt["release_id"] != preflight["version"]:
            raise ValueError("Published snapshot changed during export; retry against one stable publication")
        package = staging / "package"
        package.mkdir()
        with duckdb.connect() as con:
            con.execute("create schema build")
            for item in receipt["files"]:
                con.execute(f"create view build.{item['table']} as select * from read_parquet({literal(backup / item['file'])})")
            validate_marts(con)
            publication = _records(con, "select * from build.publication")[0]
            inputs = _source_identity(database, publication)
            bad = con.execute("""select count(*) from build.fct_trips where total_amount is null
                or not isfinite(total_amount) or abs(total_amount * 100 - round(total_amount * 100)) > 0.000001""").fetchone()[0]
            if bad:
                raise ValueError(f"total_amount is not representable in USD cents in {bad} fact rows")
            # The original daily mart already has these dimensions. Reconcile its
            # count and rounded amount with a separately aggregated fact oracle.
            con.execute("""create table daily as select cast(pickup_date as varchar) pickup_date,
                pickup_borough, cast(trips as bigint) trips,
                cast(round(revenue * 100) as bigint) total_amount_cents
                from build.mart_daily_borough order by 1,2""")
            oracle = """select cast(pickup_date as varchar), coalesce(pickup_borough,'Unknown'),
                count(*)::bigint, sum(cast(round(total_amount*100) as bigint))::bigint
                from build.fct_trips group by 1,2"""
            difference = con.execute(f"""select count(*) from (
                (select * from daily except all {oracle}) union all
                ({oracle} except all select * from daily))""").fetchone()[0]
            if difference:
                raise ValueError(f"Daily-borough exact reconciliation failed: {difference} groups")
            rows = _records(con, "select * from daily order by 1,2")
            if not 0 < len(rows) <= MAX_ROWS:
                raise ValueError(f"Analyst release requires 1..{MAX_ROWS} daily-borough rows")
            if any(not 0 <= r[key] <= MAX_SAFE_INTEGER for r in rows for key in ("trips", "total_amount_cents")):
                raise ValueError("Aggregate exceeds safe browser integer bounds")
            counts = json.loads(publication["row_counts"])
            classified = con.execute("select count(*) from build.stg_trips_classified").fetchone()[0]
            if classified != 30000 or counts["fct_trips"] != counts["stg_trips"] or classified != counts["stg_trips"] + counts["trips_quarantine"]:
                raise ValueError("Input, classified, valid, rejected and fact totals do not reconcile")
            amount = sum(row["total_amount_cents"] for row in rows)
            if amount > MAX_SAFE_INTEGER:
                raise ValueError("Release amount exceeds safe browser integer bound")
            quality = {"input_rows": 30000, "valid_rows": counts["fct_trips"],
                       "rejected_rows": counts["trips_quarantine"],
                       "by_month_and_reason": _records(con, "select * from build.mart_data_quality order by source_month, reject_reason")}
            quality_oracle = """with totals as (select _source_month source_month, count(*) raw_rows
                from build.stg_trips_classified group by 1), rejected as (
                select _source_month source_month, reject_reason, count(*) rejected_rows
                from build.trips_quarantine group by 1,2)
                select t.source_month, t.raw_rows, coalesce(r.reject_reason,'none'), coalesce(r.rejected_rows,0),
                    coalesce(r.rejected_rows,0)*1.0/t.raw_rows from totals t left join rejected r using(source_month)"""
            if con.execute(f"select count(*) from ((select * from build.mart_data_quality except all ({quality_oracle})) union all (({quality_oracle}) except all select * from build.mart_data_quality))").fetchone()[0]:
                raise ValueError("Quality summary does not reconcile to classified and quarantined rows")
            coverage = {"source_months": list(SAMPLE_INPUTS), "first_observed_date": rows[0]["pickup_date"],
                        "last_observed_date": rows[-1]["pickup_date"],
                        "observed_dates": sorted({row["pickup_date"] for row in rows}),
                        "boroughs": sorted({row["pickup_borough"] for row in rows}),
                        "daily_borough_rows": len(rows), "full_source_files": False,
                        "observed_date_count": len({row["pickup_date"] for row in rows}),
                        "source_calendar_days": 59,
                        "coverage_note": "Observed dates may cover every calendar day while trip coverage remains partial; absent borough/day groups are unobserved, not measured zero."}
            identity = {"release_id": publication["version"], "model_sha256": publication["model_sha256"],
                        "model_files": json.loads(publication["model_files"]), "inputs": inputs}
            data = {"schema_version": 1, **identity, "scope": METRICS["scope"], "coverage": coverage,
                    "quality": quality, "totals": {"trips": counts["fct_trips"], "total_amount_cents": amount},
                    "rows": rows}
            write_json(package / "data.json", data)
            write_json(package / "metric-dictionary.json", METRICS)
            write_json(package / "quality-coverage.json", {"release_id": identity["release_id"], "quality": quality, "coverage": coverage})
            with (package / "daily-borough.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            con.execute(f"copy (select * from daily order by 1,2) to {literal(package / 'daily-borough.parquet')} (format parquet, compression zstd)")
            if con.execute(f"select count(*) from ((select * from daily except all select * from read_parquet({literal(package / 'daily-borough.parquet')})) union all (select * from read_parquet({literal(package / 'daily-borough.parquet')}) except all select * from daily))").fetchone()[0]:
                raise ValueError("Parquet round-trip mismatch")
        files = [{"file": path.name, "sha256": sha256(path), "bytes": path.stat().st_size} for path in sorted(package.iterdir())]
        manifest = {"schema_version": 1, "kind": "historical-bundled-sample-analyst", **identity,
                    "created_at_utc": datetime.now(timezone.utc).isoformat(),
                    "runtime": {"python": platform.python_version(), "duckdb": duckdb.__version__},
                    "exporter_sha256": sha256(Path(__file__)), "files": files,
                    "reconciliation": {"daily_borough_fact_differences": 0, "raw_valid_rejected_balanced": True,
                                       "parquet_round_trip_differences": 0, **data["totals"]},
                    "scope": METRICS["scope"], "limits": "Static sample; no live service or representative citywide inference. Amounts are exact cents; source marts use DOUBLE, reconciled with their existing tolerance before cent reconciliation."}
        write_json(package / "manifest.json", manifest)
        verify_analyst(package)
        package.rename(destination)
        return manifest
    finally:
        if staging.exists():
            if not staging.resolve().is_relative_to(destination.parent) or not staging.name.startswith(".analyst-"):
                raise ValueError("Refusing cleanup outside the new analyst staging directory")
            remove_new_directory(staging, destination.parent)


def verify_analyst(directory):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    names = {"data.json", "daily-borough.csv", "daily-borough.parquet", "quality-coverage.json", "metric-dictionary.json"}
    if manifest.get("schema_version") != 1 or {item["file"] for item in manifest["files"]} != names:
        raise ValueError("Incomplete analyst manifest")
    if {p.name for p in directory.iterdir()} != names | {"manifest.json"}:
        raise ValueError("Unexpected analyst release files")
    for item in manifest["files"]:
        path = directory / item["file"]
        if path.is_symlink() or not path.is_file() or path.stat().st_size != item["bytes"] or sha256(path) != item["sha256"]:
            raise ValueError(f"Analyst checksum mismatch: {item['file']}")
    data = json.loads((directory / "data.json").read_text(encoding="utf-8"))
    if any(data[key] != manifest[key] for key in ("release_id", "inputs", "model_sha256", "model_files")):
        raise ValueError("Analyst source identity mismatch")
    totals = {key: sum(row[key] for row in data["rows"]) for key in ("trips", "total_amount_cents")}
    if totals != data["totals"] or any(totals[key] != manifest["reconciliation"][key] for key in totals):
        raise ValueError("Analyst totals mismatch")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["export", "verify"])
    parser.add_argument("destination", type=Path)
    parser.add_argument("--database", type=Path)
    args = parser.parse_args()
    if args.action == "export" and not args.database:
        parser.error("--database is required for export")
    try:
        result = export_analyst(args.database, args.destination) if args.action == "export" else verify_analyst(args.destination)
    except (ValueError, OSError, duckdb.Error) as exc:
        parser.exit(1, f"Analyst release refused: {exc}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

"""Complete-month ingestion and an explicitly covered zone/day count panel."""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import urllib.request
from pathlib import Path

import duckdb
import pandas as pd

SOURCE_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def months_between(start: str, end: str) -> list[str]:
    if not all(re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value) for value in (start, end)):
        raise ValueError("months must have YYYY-MM format")
    if start > end:
        raise ValueError("start month must precede end month")
    return pd.period_range(start, end, freq="M").astype(str).tolist()


def download_month(month: str, directory: Path) -> Path:
    months_between(month, month)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"yellow_tripdata_{month}.parquet"
    if destination.exists():
        return destination  # Full Parquet read and hash are checked during ingestion.
    temporary = destination.with_suffix(".parquet.partial")
    url = f"{SOURCE_BASE}/{destination.name}"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=60) as response, temporary.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            with duckdb.connect() as connection:
                connection.execute("SELECT count(*) FROM read_parquet(?)", [str(temporary)]).fetchone()
            os.replace(temporary, destination)
            return destination
        except (OSError, duckdb.Error):
            temporary.unlink(missing_ok=True)
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError("unreachable")


def complete_panel(counts: pd.DataFrame, start: str, end: str, zones: list[int]) -> pd.DataFrame:
    """Fill zero zone/days only when every day has some accepted source records."""
    days = pd.date_range(start, end, inclusive="left", freq="D")
    if len(days) == 0 or not zones or len(set(zones)) != len(zones):
        raise ValueError("nonempty date interval and unique zone IDs required")
    if any(not isinstance(zone, int) or not 1 <= zone <= 263 for zone in zones):
        raise ValueError("zones must be named TLC zone IDs 1..263")
    required = {"date", "zone", "pickups"}
    if set(counts.columns) != required:
        raise ValueError("count data must contain exactly date, zone, pickups")
    counts = counts.copy()
    counts["date"] = pd.to_datetime(counts["date"])
    if counts.duplicated(["date", "zone"]).any():
        raise ValueError("duplicate zone/day counts")
    if not counts["date"].isin(days).all() or not counts["zone"].isin(zones).all():
        raise ValueError("counts outside declared coverage")
    if counts["pickups"].isna().any() or (counts["pickups"] <= 0).any():
        raise ValueError("aggregated observed counts must be positive")
    if (counts["pickups"] % 1 != 0).any():
        raise ValueError("pickup counts must be integers")
    missing = days.difference(counts["date"].unique())
    if len(missing):
        raise ValueError(f"source has uncovered days; cannot interpret as zero: {missing.astype(str).tolist()}")
    index = pd.MultiIndex.from_product([zones, days], names=["zone", "date"])
    panel = counts.set_index(["zone", "date"]).reindex(index, fill_value=0).reset_index()
    panel["pickups"] = panel["pickups"].astype("int64")
    return panel.sort_values(["zone", "date"]).reset_index(drop=True)


def aggregate_sources(directory: Path, months: list[str]) -> tuple[pd.DataFrame, dict]:
    if not months or months != months_between(months[0], months[-1]):
        raise ValueError("declare every month once, contiguously and in order")
    sources, frames = [], []
    with duckdb.connect() as connection:
        for month in months:
            path = directory / f"yellow_tripdata_{month}.parquet"
            if not path.is_file():
                raise FileNotFoundError(path)
            start = pd.Timestamp(f"{month}-01")
            end = start + pd.offsets.MonthBegin(1)
            connection.execute("""
                CREATE OR REPLACE TEMP TABLE classified AS
                SELECT CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_time,
                       PULocationID AS zone,
                       CASE
                         WHEN tpep_pickup_datetime IS NULL THEN 'missing_pickup_time'
                         WHEN tpep_pickup_datetime < ? OR tpep_pickup_datetime >= ? THEN 'outside_source_month'
                         WHEN PULocationID IS NULL OR PULocationID < 1 OR PULocationID > 263
                              OR PULocationID != floor(PULocationID) THEN 'unnamed_zone'
                         ELSE 'accepted'
                       END AS disposition
                FROM read_parquet(?)
            """, [start.to_pydatetime(), end.to_pydatetime(), str(path)])
            disposition = dict(connection.execute(
                "SELECT disposition,count(*) FROM classified GROUP BY disposition"
            ).fetchall())
            frame = connection.execute("""
                SELECT CAST(pickup_time AS DATE) AS date, zone, count(*) AS pickups
                FROM classified WHERE disposition='accepted' GROUP BY 1,2
            """).df()
            if len(frame) == 0:
                raise ValueError(f"no accepted pickup records for {month}")
            frames.append(frame)
            sources.append({"month": month, "filename": path.name, "url": f"{SOURCE_BASE}/{path.name}",
                            "bytes": path.stat().st_size, "sha256": sha256(path),
                            "rows": sum(disposition.values()), "disposition": disposition})
    counts = pd.concat(frames, ignore_index=True)
    end = (pd.Timestamp(months[-1] + "-01") + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    panel = complete_panel(counts, months[0] + "-01", end, list(range(1, 264)))
    accepted = sum(item["disposition"].get("accepted", 0) for item in sources)
    if int(panel["pickups"].sum()) != accepted:
        raise AssertionError("source-to-panel count reconciliation failed")
    return panel, {"sources": sources, "raw_rows": sum(s["rows"] for s in sources),
                   "accepted_rows": accepted, "panel_rows": len(panel), "zone_count": 263,
                   "start": months[0] + "-01", "end_exclusive": end,
                   "grain": "named pickup zone / NYC local calendar day",
                   "target": "observed yellow-taxi pickup records; not unmet or total transport demand",
                   "duplicate_policy": "source records counted; no assumed unique trip key or deduplication",
                   "coverage_rule": "all declared files parse and every calendar day has accepted records",
                   "coverage_limit": "checks do not prove complete vendor submissions",
                   "time_zone": "TLC NYC local timestamps; daily buckets retain 23/25-hour DST days"}


def write_panel_cache(panel: pd.DataFrame, provenance: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(path, index=False, lineterminator="\n")
    provenance = {**provenance, "panel_sha256": sha256(path)}
    path.with_suffix(".provenance.json").write_text(json.dumps(provenance, indent=2), encoding="utf-8")


def read_panel_cache(path: Path) -> tuple[pd.DataFrame, dict]:
    provenance = json.loads(path.with_suffix(".provenance.json").read_text(encoding="utf-8"))
    if provenance["panel_sha256"] != sha256(path):
        raise ValueError("cached panel checksum does not match provenance")
    panel = pd.read_csv(path, parse_dates=["date"])
    if len(panel) != provenance["panel_rows"] or int(panel.pickups.sum()) != provenance["accepted_rows"]:
        raise ValueError("cached panel does not reconcile to provenance")
    positive = panel.loc[panel.pickups > 0, ["date", "zone", "pickups"]]
    expected = complete_panel(positive, provenance["start"], provenance["end_exclusive"], list(range(1, 264)))
    observed = panel.sort_values(["zone", "date"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(observed, expected, check_dtype=False)
    return observed, provenance

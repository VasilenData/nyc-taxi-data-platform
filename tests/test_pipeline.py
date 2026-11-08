"""End-to-end: load the bundled sample, build dbt, run every asset check, then try to break the data."""
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    db = tmp_path_factory.mktemp("wh") / "w.duckdb"
    env = {"TAXI_DB": str(db), "TAXI_DATA_DIR": str(tmp_path_factory.mktemp("none")), "PATH": __import__("os").environ["PATH"]}
    proc = subprocess.run([sys.executable, "run_pipeline.py", "2023-01", "2023-02"], cwd=ROOT, env=env,
                          capture_output=True, text=True, timeout=600)
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    return db, env, proc.stdout


def q(db, sql):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_all_checks_reported_and_passed(warehouse):
    out = warehouse[2]
    assert "FAIL" not in out
    for name in ("reconciliation", "reject_rate", "coverage"):
        assert f"check {name}" in out


def test_raw_equals_valid_plus_rejected(warehouse):
    db = warehouse[0]
    raw = q(db, "select count(*) from raw.trips")[0][0]
    valid = q(db, "select count(*) from main.stg_trips")[0][0]
    bad = q(db, "select count(*) from main.trips_quarantine")[0][0]
    assert raw == 30000 and raw == valid + bad and bad > 0


def test_every_rejection_has_a_known_reason(warehouse):
    reasons = {r[0] for r in q(warehouse[0], "select distinct reject_reason from main.trips_quarantine")}
    assert reasons <= {"missing_timestamp", "non_positive_duration", "implausible_duration", "out_of_period",
                       "bad_distance", "bad_fare", "unknown_zone", "duplicate"}


def test_fact_table_has_no_impossible_trips(warehouse):
    bad = q(warehouse[0], "select count(*) from main.fct_trips where duration_min <= 0 or distance_miles <= 0 or fare_amount < 0")
    assert bad[0][0] == 0


def test_marts_agree_with_fact(warehouse):
    db = warehouse[0]
    fact = q(db, "select count(*) from main.fct_trips")[0][0]
    assert q(db, "select sum(trips) from main.mart_daily_borough")[0][0] == fact
    assert q(db, "select sum(trips) from main.mart_hourly_demand")[0][0] == fact


def test_incremental_rerun_changes_nothing(warehouse):
    db, env, _ = warehouse
    before = q(db, "select count(*), round(sum(total_amount), 2) from main.fct_trips")[0]
    proc = subprocess.run([sys.executable, "run_pipeline.py", "2023-01"], cwd=ROOT, env=env,
                          capture_output=True, text=True, timeout=600)
    assert proc.returncode == 0, proc.stdout[-2000:]
    assert q(db, "select count(*), round(sum(total_amount), 2) from main.fct_trips")[0] == before


def test_poisoned_month_trips_the_quality_gate(tmp_path):
    """Corrupt a month so >10% of rows are bad. The CLI must report a failed run."""
    data = tmp_path / "raw"
    data.mkdir()
    src = ROOT / "data" / "sample" / "yellow_tripdata_2023-01.parquet"
    duckdb.sql(f"""copy (select * replace (case when random() < 0.4 then -5.0 else fare_amount end as fare_amount)
                    from read_parquet('{src}')) to '{data}/yellow_tripdata_2023-01.parquet' (format parquet)""")
    env = {"TAXI_DB": str(tmp_path / "p.duckdb"), "TAXI_DATA_DIR": str(data), "PATH": __import__("os").environ["PATH"]}
    proc = subprocess.run([sys.executable, "run_pipeline.py", "2023-01"], cwd=ROOT, env=env,
                          capture_output=True, text=True, timeout=600)
    assert proc.returncode != 0
    assert "reject_rate" in proc.stdout and "FAIL" in proc.stdout

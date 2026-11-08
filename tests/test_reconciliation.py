from pathlib import Path

import duckdb
from jinja2 import Template
import pytest

from taxi_platform import checks, config


@pytest.mark.parametrize("valid,quarantine,expected", [
    (["2023-01"], ["2023-01"], []),
    (["2023-01", "2023-02"], ["2023-01"], [("2023-02", 0, 1, 0)]),
    (["2023-01"], ["2023-01", "2023-03"], [("2023-03", 0, 0, 1)]),
    ([], ["2023-01"], [("2023-01", 2, 0, 1)]),
    (["2023-01"] * 3, ["2023-01"], [("2023-01", 2, 3, 1)]),
])
def test_asset_and_dbt_reconciliation_include_missing_and_orphan_months(tmp_path, monkeypatch,
                                                                       valid, quarantine, expected):
    database = tmp_path / "reconcile.duckdb"
    with duckdb.connect(str(database)) as con:
        con.execute("create schema raw")
        for table, months in {"raw.trips": ["2023-01"] * 2, "main.stg_trips": valid,
                              "main.trips_quarantine": quarantine}.items():
            con.execute(f"create table {table} (_source_month varchar)")
            if months:
                con.executemany(f"insert into {table} values (?)", [(month,) for month in months])
    monkeypatch.setattr(config, "DB_PATH", database)
    assert sorted(checks.reconciliation_mismatches()) == expected
    template = Path(__file__).resolve().parents[1] / "dbt/tests/assert_raw_equals_valid_plus_rejected.sql"
    sql = Template(template.read_text()).render(source=lambda schema, table: f"{schema}.{table}",
                                                ref=lambda table: f"main.{table}")
    with duckdb.connect(str(database), read_only=True) as con:
        assert sorted(con.execute(sql).fetchall()) == expected


def test_coverage_is_blocking_in_dagster_as_well_as_the_cli():
    assert all(spec.blocking for spec in checks.coverage.check_specs)

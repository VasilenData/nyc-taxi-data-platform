"""Blocking checks read candidate tables; consumers stay on the last publication."""
import dagster as dg
import duckdb

from . import config
from .quality import RECONCILIATION_SQL, evaluate


def reconciliation_mismatches() -> list[tuple]:
    with duckdb.connect(str(config.DB_PATH), read_only=True) as con:
        return con.execute(RECONCILIATION_SQL).fetchall()


def _check(name: str) -> dg.AssetCheckResult:
    with duckdb.connect(str(config.DB_PATH), read_only=True) as con:
        passed, metadata = evaluate(con, config.MAX_REJECT_RATE)[name]
    return dg.AssetCheckResult(passed=passed, metadata=metadata, severity=dg.AssetCheckSeverity.ERROR)


@dg.asset_check(asset=dg.AssetKey("fct_trips"), blocking=True,
                description="Candidate raw rows = valid + quarantined rows, including output-only months.")
def reconciliation() -> dg.AssetCheckResult:
    return _check("reconciliation")


@dg.asset_check(asset=dg.AssetKey("fct_trips"), blocking=True,
                description=f"No candidate month may reject more than {config.MAX_REJECT_RATE:.0%} of rows.")
def reject_rate() -> dg.AssetCheckResult:
    return _check("reject_rate")


@dg.asset_check(asset=dg.AssetKey("fct_trips"), blocking=True,
                description="Candidate fact table is not empty and covers every raw month.")
def coverage() -> dg.AssetCheckResult:
    return _check("coverage")

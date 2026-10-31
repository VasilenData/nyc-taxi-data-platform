"""Asset checks that run after the dbt graph. They read the warehouse, not the dbt artifacts, so they
verify what is actually stored."""
import dagster as dg
import duckdb

from . import config


def _rows(sql: str) -> list[tuple]:
    con = duckdb.connect(str(config.DB_PATH), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


@dg.asset_check(asset=dg.AssetKey("fct_trips"), blocking=True,
                description="raw rows = valid rows + quarantined rows, for every loaded month.")
def reconciliation() -> dg.AssetCheckResult:
    bad = _rows(
        """select r.m, r.n, coalesce(v.n,0), coalesce(q.n,0) from
           (select _source_month m, count(*) n from raw.trips group by 1) r
           left join (select _source_month m, count(*) n from main.stg_trips group by 1) v using (m)
           left join (select _source_month m, count(*) n from main.trips_quarantine group by 1) q using (m)
           where r.n <> coalesce(v.n,0) + coalesce(q.n,0)"""
    )
    return dg.AssetCheckResult(passed=not bad, metadata={"mismatched_months": len(bad)})


@dg.asset_check(asset=dg.AssetKey("fct_trips"), blocking=True,
                description=f"No month may reject more than {config.MAX_REJECT_RATE:.0%} of its rows.")
def reject_rate() -> dg.AssetCheckResult:
    rows = _rows(
        """select source_month, sum(rejected_rows) * 1.0 / max(raw_rows)
           from main.mart_data_quality group by 1 order by 1"""
    )
    worst = max(rows, key=lambda r: r[1], default=("none", 0.0))
    return dg.AssetCheckResult(
        passed=worst[1] <= config.MAX_REJECT_RATE,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"worst_month": worst[0], "worst_reject_rate": float(worst[1]), "months_checked": len(rows)},
    )


@dg.asset_check(asset=dg.AssetKey("fct_trips"), description="Fact table is not empty and covers every raw month.")
def coverage() -> dg.AssetCheckResult:
    raw = {r[0] for r in _rows("select distinct _source_month from raw.trips")}
    fact = {r[0] for r in _rows("select distinct source_month from main.fct_trips")}
    return dg.AssetCheckResult(passed=raw == fact and bool(fact),
                               metadata={"raw_months": len(raw), "fact_months": len(fact)})

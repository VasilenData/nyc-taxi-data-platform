"""Quality predicates shared by Dagster and the publication transaction."""

RECONCILIATION_SQL = """select coalesce(r.m, v.m, q.m), coalesce(r.n,0), coalesce(v.n,0), coalesce(q.n,0) from
    (select _source_month m, count(*) n from raw.trips group by 1) r
    full outer join (select _source_month m, count(*) n from build.stg_trips group by 1) v using (m)
    full outer join (select _source_month m, count(*) n from build.trips_quarantine group by 1) q using (m)
    where coalesce(r.n,0) <> coalesce(v.n,0) + coalesce(q.n,0)"""


def evaluate(con, max_reject_rate: float) -> dict:
    mismatches = con.execute(RECONCILIATION_SQL).fetchall()
    rates = con.execute("""select source_month, sum(rejected_rows) * 1.0 / max(raw_rows)
        from build.mart_data_quality group by 1 order by 1""").fetchall()
    worst = max(rates, key=lambda r: r[1], default=("none", 0.0))
    raw = {r[0] for r in con.execute("select distinct _source_month from raw.trips").fetchall()}
    fact = {r[0] for r in con.execute("select distinct source_month from build.fct_trips").fetchall()}
    return {
        "reconciliation": (not mismatches, {"mismatched_months": len(mismatches)}),
        "reject_rate": (worst[1] <= max_reject_rate, {
            "worst_month": worst[0], "worst_reject_rate": float(worst[1]), "months_checked": len(rates)}),
        "coverage": (raw == fact and bool(fact), {"raw_months": len(raw), "fact_months": len(fact)}),
    }

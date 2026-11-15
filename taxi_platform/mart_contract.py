"""Reconcile each candidate reporting grain to its fact rows before publication.

Counts/keys are exact. Floating aggregates allow 1e-8 absolute + 1e-12 relative
roundoff; neither NaN nor infinity is accepted. This guards the listed report
metrics, not an independent reproduction of every fact-table transformation.
"""

CONTRACTS = {
    "mart_daily_borough": {
        "keys": ("pickup_date", "pickup_borough"),
        "dimensions": ("pickup_date", "coalesce(pickup_borough, 'Unknown') as pickup_borough"),
        "metrics": {
            "revenue": "sum(total_amount)", "avg_fare": "avg(fare_amount)",
            "avg_distance_miles": "avg(distance_miles)", "avg_duration_min": "avg(duration_min)",
            "avg_tip_pct": "avg(tip_pct)",
        },
    },
    "mart_hourly_demand": {
        "keys": ("pickup_dow", "pickup_hour"),
        "dimensions": ("pickup_dow", "pickup_hour"),
        "metrics": {"avg_speed_mph": "avg(avg_speed_mph)", "avg_fare": "avg(fare_amount)"},
    },
}


def validate_marts(con) -> None:
    for name, contract in CONTRACTS.items():
        keys, metrics = contract["keys"], contract["metrics"]
        # A full join exposes missing AND output-only groups. Window counts expose
        # duplicate keys even where duplicated/split rows conserve global totals.
        measures = ", ".join(f"{expression} as {column}" for column, expression in metrics.items())
        conditions = ["e.present is null", "a.present is null", "a.grain_count <> 1",
                      "a.trips is distinct from e.trips"]
        for column in metrics:
            conditions.append(f"""(case
                when e.{column} is null then a.{column} is not null
                when a.{column} is null then true
                when not isfinite(e.{column}) or not isfinite(a.{column}) then true
                else abs(a.{column} - e.{column}) > 1e-8 + abs(e.{column}) * 1e-12
                end)""")
        join = " and ".join(f"e.{key} is not distinct from a.{key}" for key in keys)
        bad = con.execute(f"""with expected as (
            select {', '.join(contract['dimensions'])}, count(*) trips, {measures}, true present
            from build.fct_trips group by {', '.join(str(i + 1) for i in range(len(keys)))}
        ), actual as (
            select *, true present, count(*) over (partition by {', '.join(keys)}) grain_count
            from build.{name}
        )
        select count(*) from expected e full outer join actual a on {join}
        where {' or '.join(conditions)}""").fetchone()[0]
        if bad:
            raise ValueError(f"Publication refused: {name} grain/metric mismatch ({bad} rows)")

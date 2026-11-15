"""Hand-computed report grains; corruptions deliberately preserve global counts."""
import duckdb
import pytest

from scripts.record_publication_demo import candidate, snapshot
from taxi_platform.mart_contract import validate_marts
from taxi_platform.publication import publish


def reporting_candidate(con):
    candidate(con, 4, 10)
    con.execute("""create or replace table build.fct_trips as select * from (values
        ('2023-01', date '2023-01-01', 'A', 7, 10, 10., 8., 1., 10., 0.1, 6.),
        ('2023-01', date '2023-01-01', 'A', 7, 10, 20., 16., 3., 20., null, 9.),
        ('2023-01', date '2023-01-01', null, 7, 11, 5., 4., 1., 5., 0.0, 12.),
        ('2023-01', date '2023-01-02', 'B', 1, 10, 30., 25., 5., 10., 0.2, 30.)
    ) t(source_month,pickup_date,pickup_borough,pickup_dow,pickup_hour,total_amount,
        fare_amount,distance_miles,duration_min,tip_pct,avg_speed_mph)""")
    con.execute("""create or replace table build.mart_daily_borough as select * from (values
        (date '2023-01-01','A',2,30.0,12.0,2.0,15.0,0.1),
        (date '2023-01-01','Unknown',1,5.0,4.0,1.0,5.0,0.0),
        (date '2023-01-02','B',1,30.0,25.0,5.0,10.0,0.2)
    ) t(pickup_date,pickup_borough,trips,revenue,avg_fare,avg_distance_miles,avg_duration_min,avg_tip_pct)""")
    con.execute("""create or replace table build.mart_hourly_demand as select * from (values
        (7,10,2,7.5,12.0), (7,11,1,12.0,4.0), (1,10,1,30.0,25.0)
    ) t(pickup_dow,pickup_hour,trips,avg_speed_mph,avg_fare)""")


@pytest.mark.parametrize("damage", [
    "update build.mart_daily_borough set revenue = revenue+1 where pickup_borough='A'",
    """update build.mart_daily_borough set revenue = revenue +
        case when pickup_borough='A' then 1 when pickup_borough='B' then -1 else 0 end""",
    "update build.mart_daily_borough set pickup_borough='C' where pickup_borough='B'",
    """update build.mart_hourly_demand set trips = trips +
        case when pickup_hour=10 and pickup_dow=7 then -1 when pickup_hour=11 then 1 else 0 end""",
    "update build.mart_hourly_demand set avg_speed_mph=99 where pickup_hour=11",
    "update build.mart_daily_borough set avg_tip_pct=null where pickup_borough='A'",
    """create or replace table build.mart_daily_borough as
        select * replace (trips/2 as trips,revenue/2 as revenue) from build.mart_daily_borough
        where pickup_borough='A' union all
        select * replace (trips/2 as trips,revenue/2 as revenue) from build.mart_daily_borough
        where pickup_borough='A' union all
        select * from build.mart_daily_borough where pickup_borough<>'A'""",
])
def test_same_count_reporting_corruption_preserves_all_consumers(tmp_path, damage):
    with duckdb.connect(str(tmp_path / "report.duckdb")) as con:
        candidate(con, 4, 1)
        publish(con)
        before = snapshot(con)
        reporting_candidate(con)
        validate_marts(con)  # The hand-computed candidate is valid before mutation.
        con.execute(damage)
        assert con.execute("select sum(trips) from build.mart_daily_borough").fetchone()[0] == 4
        assert con.execute("select sum(trips) from build.mart_hourly_demand").fetchone()[0] == 4
        with pytest.raises(ValueError, match="grain/metric mismatch"):
            publish(con)
        assert snapshot(con) == before
        assert con.execute("select count(*) from audit.publications").fetchone()[0] == 1
        reporting_candidate(con)
        publish(con)
        assert con.execute("select sum(revenue) from main.mart_daily_borough").fetchone()[0] == 65


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_report_measure_is_rejected_even_if_counts_match(value):
    with duckdb.connect() as con:
        candidate(con, 4, 10)
        con.execute("update build.mart_daily_borough set revenue=?::double", [value])
        with pytest.raises(ValueError, match="grain/metric mismatch"):
            publish(con)


def test_nullable_metrics_roundoff_and_order_do_not_create_false_drift():
    with duckdb.connect() as con:
        candidate(con, 4, 10)  # NULL tip_pct averages stay NULL in both sources.
        con.execute("update build.mart_daily_borough set revenue=revenue+1e-12")
        validate_marts(con)
        reporting_candidate(con)
        con.execute("create or replace table build.fct_trips as select * from build.fct_trips order by total_amount desc")
        con.execute("create or replace table build.mart_daily_borough as select * from build.mart_daily_borough order by revenue desc")
        validate_marts(con)

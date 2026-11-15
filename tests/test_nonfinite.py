"""Execute the actual dbt classifier over bounded adversarial source rows."""
from jinja2 import Template
import duckdb
import pytest

from taxi_platform import config, ingest


def classify(con):
    source = config.DBT_DIR / "models/staging/stg_trips_classified.sql"
    sql = Template(source.read_text()).render(
        config=lambda **kwargs: "", source=lambda *args: "raw.trips",
        is_incremental=lambda: False,
        var=lambda key: {"max_trip_hours": 6, "max_trip_miles": 200}[key],
    )
    return con.execute(sql).fetchdf()


@pytest.mark.parametrize("column", ["passenger_count", "trip_distance", "fare_amount", "tip_amount", "tolls_amount", "total_amount"])
@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_consumed_measure_is_quarantined(column, value):
    with duckdb.connect() as con:
        ingest.ensure_schema(con)
        con.execute("""insert into raw.trips (VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,
            passenger_count,trip_distance,PULocationID,DOLocationID,payment_type,fare_amount,
            tip_amount,tolls_amount,total_amount,_source_month,_source_file,_loaded_at)
            values (1,'2023-01-05 10:00','2023-01-05 10:10',1,2,10,20,1,10,2,0,12,
                '2023-01','fixture.parquet','2023-02-01')""")
        con.execute(f'update raw.trips set "{column}"=?::double', [value])
        result = classify(con)
        assert len(result) == 1 and result.iloc[0]["reject_reason"] == "non_finite_value"


def test_range_test_rejects_nonfinite_but_keeps_explicit_null_policy():
    text = (config.DBT_DIR / "macros/accepted_range.sql").read_text()
    body = text.split("%}", 1)[1].rsplit("{% endtest %}", 1)[0]
    sql = Template(body).render(model="values_under_test", column_name="amount", min_value=0,
                                max_value=None, inclusive=False)
    with duckdb.connect() as con:
        con.execute("""create table values_under_test as select unnest(
            [null,0.,1.,'NaN'::double,'Infinity'::double,'-Infinity'::double]) amount""")
        assert len(con.execute(sql).fetchall()) == 4  # zero, NaN and both infinities.

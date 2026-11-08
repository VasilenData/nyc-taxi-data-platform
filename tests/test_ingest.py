import duckdb
import pytest

from taxi_platform import config, ingest


@pytest.fixture()
def con(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "none")
    c = duckdb.connect(str(tmp_path / "t.duckdb"))
    yield c
    c.close()


def test_month_validation_rejects_path_tricks():
    for bad in ["2023-13", "../etc/passwd", "2023-1", "abcd-01", "2023-01\n"]:
        with pytest.raises(ValueError):
            ingest.validate_month(bad)


def test_load_is_idempotent(con):
    first = ingest.load_month("2023-01", con)
    second = ingest.load_month("2023-01", con)
    assert first["rows"] == second["rows"] == 15000
    assert con.execute("select count(*) from raw.trips").fetchone()[0] == 15000


def test_months_are_independent(con):
    ingest.load_month("2023-01", con)
    ingest.load_month("2023-02", con)
    ingest.load_month("2023-01", con)  # reload must not touch February
    counts = dict(con.execute("select _source_month, count(*) from raw.trips group by 1").fetchall())
    assert counts == {"2023-01": 15000, "2023-02": 15000}


def test_lineage_columns_filled(con):
    ingest.load_month("2023-01", con)
    nulls = con.execute(
        "select count(*) from raw.trips where _source_file is null or _loaded_at is null").fetchone()[0]
    assert nulls == 0


def test_renamed_columns_do_not_break_load(con, tmp_path, monkeypatch):
    """TLC has shipped both Airport_fee and airport_fee; a case change must not fail the load."""
    src = config.SAMPLE_DIR / "yellow_tripdata_2023-01.parquet"
    out = tmp_path / "drift"
    out.mkdir()
    duckdb.sql(f"copy (select * exclude (airport_fee), airport_fee as \"Airport_fee\", "
               f"tpep_pickup_datetime as tpep_pickup_datetime from "
               f"(select *, 1.25 as airport_fee from read_parquet('{src}') limit 100)) "
               f"to '{out}/yellow_tripdata_2023-03.parquet' (format parquet)")
    monkeypatch.setattr(config, "DATA_DIR", out)
    assert ingest.load_month("2023-03", con)["rows"] == 100


def test_empty_replacement_preserves_existing_month(con, tmp_path, monkeypatch):
    ingest.load_month("2023-01", con)
    before = con.execute("select count(*), sum(total_amount) from raw.trips").fetchone()
    empty = tmp_path / "empty.parquet"
    con.execute("copy (select * from raw.trips where false) to ? (format parquet)", [str(empty)])
    monkeypatch.setattr(ingest, "resolve_source", lambda month: empty)

    with pytest.raises(ValueError, match="Refusing empty input"):
        ingest.load_month("2023-01", con)

    assert con.execute("select count(*), sum(total_amount) from raw.trips").fetchone() == before
    # The borrowed connection must have no failed transaction left open.
    con.execute("begin")
    con.execute("rollback")


def test_failed_insert_rolls_back_borrowed_connection(con, monkeypatch):
    ingest.load_month("2023-01", con)
    before = con.execute("select count(*), sum(total_amount) from raw.trips").fetchone()

    class FailingInsert:
        def execute(self, sql, parameters=None):
            if sql.lstrip().startswith("insert into raw.trips"):
                # A real SQL failure puts DuckDB's transaction into an aborted state.
                return con.execute("select cast('broken-input' as integer)")
            return con.execute(sql, parameters) if parameters is not None else con.execute(sql)

    with pytest.raises(duckdb.ConversionException):
        ingest.load_month("2023-01", FailingInsert())

    assert con.execute("select count(*), sum(total_amount) from raw.trips").fetchone() == before
    assert ingest.load_month("2023-01", con)["rows"] == 15000

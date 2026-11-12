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


def test_reviewed_hash_is_checked_before_replacing_rows(con):
    ingest.load_month("2023-01", con)
    before = con.execute("select * from raw.trips order by all").fetchall()
    with pytest.raises(ValueError, match="reviewed change plan"):
        ingest.load_month("2023-01", con, expected_sha256="0" * 64)
    assert con.execute("select * from raw.trips order by all").fetchall() == before


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


def test_missing_required_schema_cannot_replace_good_month(con, tmp_path, monkeypatch):
    ingest.load_month("2023-01", con)
    before = con.execute("select * from raw.trips order by all").fetchall()
    audit = con.execute("select * from audit.source_loads").fetchall()
    wrong = tmp_path / "wrong.parquet"
    con.execute("copy (select 1 as unrelated) to ? (format parquet)", [str(wrong)])
    monkeypatch.setattr(ingest, "resolve_source", lambda month: wrong)
    with pytest.raises(ValueError, match="missing required columns.*fare_amount"):
        ingest.load_month("2023-01", con)
    assert con.execute("select * from raw.trips order by all").fetchall() == before
    assert con.execute("select * from audit.source_loads").fetchall() == audit


def test_optional_fields_are_nullable_and_audited_with_original_bytes(con, tmp_path, monkeypatch):
    import hashlib
    import json

    source = tmp_path / "minimal.parquet"
    columns = ",".join(f'"{c}"' for c in ingest.COLUMNS if c.lower() in ingest.REQUIRED_COLUMNS)
    con.execute(f"copy (select {columns} from read_parquet(?) limit 5) to ? (format parquet)",
                [str(config.SAMPLE_DIR / "yellow_tripdata_2023-01.parquet"), str(source)])
    monkeypatch.setattr(ingest, "resolve_source", lambda month: source)
    result = ingest.load_month("2023-01", con)
    digest, size, schema, missing, rows, stamp = con.execute("""select source_sha256, source_bytes,
        source_schema, missing_optional_columns, rows, loaded_at from audit.source_loads
        where load_id=?""", [result["load_id"]]).fetchone()
    assert digest == result["sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert size == source.stat().st_size and rows == 5
    assert "airport_fee" in json.loads(missing)
    assert {c["name"].lower() for c in json.loads(schema)} == ingest.REQUIRED_COLUMNS
    assert con.execute("select count(*) from raw.trips where airport_fee is not null").fetchone() == (0,)
    assert con.execute("select distinct _loaded_at from raw.trips").fetchall() == [(stamp,)]


def test_changed_source_or_failed_audit_rolls_back_both_rows_and_lineage(con, monkeypatch):
    ingest.load_month("2023-01", con)
    before = con.execute("select * from raw.trips order by all").fetchall()
    audit = con.execute("select * from audit.source_loads").fetchall()
    hashes = iter(["a" * 64, "b" * 64])
    with monkeypatch.context() as patch:
        patch.setattr(ingest, "source_fingerprint", lambda path: next(hashes))
        with pytest.raises(ValueError, match="Source changed"):
            ingest.load_month("2023-01", con)
    assert con.execute("select * from raw.trips order by all").fetchall() == before
    assert con.execute("select * from audit.source_loads").fetchall() == audit

    class FailedAudit:
        def execute(self, sql, parameters=None):
            if sql.startswith("insert into audit.source_loads"):
                raise OSError("audit unavailable")
            return con.execute(sql, parameters) if parameters is not None else con.execute(sql)

    with pytest.raises(OSError, match="audit unavailable"):
        ingest.load_month("2023-01", FailedAudit())
    assert con.execute("select * from raw.trips order by all").fetchall() == before
    assert con.execute("select * from audit.source_loads").fetchall() == audit
    assert ingest.load_month("2023-01", con)["rows"] == 15000


def test_nested_child_cannot_impersonate_top_level_required_field(con, tmp_path, monkeypatch):
    source = tmp_path / "nested.parquet"
    con.execute("copy (select {'tpep_pickup_datetime': timestamp '2023-01-01'} nested) "
                "to ? (format parquet)", [str(source)])
    monkeypatch.setattr(ingest, "resolve_source", lambda month: source)
    with pytest.raises(ValueError, match="missing required columns.*tpep_pickup_datetime"):
        ingest.load_month("2023-01", con)

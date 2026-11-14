"""Publication invariants over small generated candidates and an abrupt process exit."""
import os
from pathlib import Path
import random
import subprocess
import sys

import duckdb
import pytest

from taxi_platform.publication import TABLES, publish


def candidate(con, size, value):
    con.execute("create schema if not exists raw")
    con.execute("create schema if not exists build")
    con.execute("""create or replace table raw.trips as select '2023-01' _source_month,
        'exercise.parquet' _source_file, timestamp '2023-01-01' _loaded_at from range(?)""", [size])
    for table in TABLES:
        con.execute(f'create or replace table build."{table}" as select ? value from range(?)', [value, size])
    con.execute("""create or replace table build.stg_trips as
        select *, ? value from raw.trips""", [value])
    con.execute("create or replace table build.trips_quarantine as select * from build.stg_trips where false")
    con.execute("""create or replace table build.fct_trips as
        select _source_month source_month, ? value from raw.trips""", [value])
    for table in ("mart_daily_borough", "mart_hourly_demand"):
        con.execute(f'create or replace table build."{table}" as select ? trips, ? value', [size, value])
    con.execute("""create or replace table build.mart_data_quality as
        select '2023-01' source_month, 0 rejected_rows, ? raw_rows""", [size])


def snapshot(con):
    return {name: con.execute(f'select * from main."{name}"').fetchall()
            for name in (*TABLES, "publication")}


@pytest.mark.parametrize("failure_after", [1, 4, 8, "commit"])
def test_failed_publication_preserves_every_consumer_relation(tmp_path, failure_after):
    rng = random.Random(904)
    with duckdb.connect(str(tmp_path / 'warehouse.duckdb')) as con:
        candidate(con, 3, -1)
        publish(con)
        before = snapshot(con)
        for _ in range(5):
            candidate(con, rng.randint(1, 30), rng.randint(0, 10_000))

            class FailingConnection:
                copied = 0

                def execute(self, sql, parameters=None):
                    if failure_after == 'commit' and sql == 'commit':
                        raise OSError('interrupted before commit')
                    result = con.execute(sql, parameters) if parameters is not None else con.execute(sql)
                    if sql.startswith('create or replace table main.'):
                        self.copied += 1
                        if self.copied == failure_after:
                            raise OSError('interrupted during copy')
                    return result

            with pytest.raises(OSError, match='interrupted'):
                publish(FailingConnection())
            assert snapshot(con) == before
            assert con.execute('select count(*) from audit.publications').fetchone()[0] == 1
        result = publish(con)
        assert result['fact_rows'] == con.execute('select count(*) from main.fct_trips').fetchone()[0]
        assert con.execute('select count(*) from audit.publications').fetchone()[0] == 2
        assert snapshot(con) != before


@pytest.mark.parametrize('damage', [
    "update build.mart_data_quality set rejected_rows = raw_rows",
    "delete from build.stg_trips",
    "delete from build.fct_trips",
    "update build.mart_hourly_demand set trips = 0",
])
def test_invalid_candidate_cannot_replace_previous_publication(tmp_path, damage):
    with duckdb.connect(str(tmp_path / 'warehouse.duckdb')) as con:
        candidate(con, 5, 1)
        publish(con)
        before = snapshot(con)
        candidate(con, 9, 2)
        con.execute(damage)
        with pytest.raises(ValueError, match='Publication refused'):
            publish(con)
        assert snapshot(con) == before


def test_process_death_rolls_back_partial_publication_and_can_recover(tmp_path):
    database = tmp_path / 'warehouse.duckdb'
    with duckdb.connect(str(database)) as con:
        candidate(con, 4, 1)
        publish(con)
        before = snapshot(con)
        candidate(con, 7, 2)
    script = '''
import os, duckdb
from taxi_platform.publication import publish
con = duckdb.connect(os.environ['CRASH_TEST_DB'])
class CrashConnection:
    def execute(self, sql, parameters=None):
        result = con.execute(sql, parameters) if parameters is not None else con.execute(sql)
        if sql.startswith('create or replace table main."fct_trips"'):
            os._exit(37)  # No finally, connection close or rollback code runs.
        return result
publish(CrashConnection())
'''
    process = subprocess.run([sys.executable, '-c', script],
                             env={**os.environ, 'CRASH_TEST_DB': str(database)},
                             cwd=Path(__file__).resolve().parents[1], timeout=30)
    assert process.returncode == 37
    with duckdb.connect(str(database)) as con:
        assert snapshot(con) == before
        publish(con)
        assert con.execute('select count(*) from main.fct_trips').fetchone()[0] == 7


def test_upgrade_replaces_old_dependent_views_transactionally(tmp_path):
    with duckdb.connect(str(tmp_path / 'warehouse.duckdb')) as con:
        candidate(con, 3, 1)
        con.execute('create table main.stg_trips_classified as select * from build.stg_trips')
        con.execute('create view main.stg_trips as select * from main.stg_trips_classified')
        con.execute('create view main.trips_quarantine as select * from main.stg_trips_classified where false')
        publish(con)
        assert con.execute('select count(*) from main.stg_trips').fetchone()[0] == 3


@pytest.mark.parametrize('original_order', [True, False])
@pytest.mark.parametrize('failure', ['none', 'validation', 'copy'])
def test_copy_tuning_restores_callers_setting_and_preserves_rows(tmp_path, original_order, failure):
    with duckdb.connect(str(tmp_path / 'warehouse.duckdb')) as con:
        candidate(con, 9, 5)
        publish(con)
        before = snapshot(con)
        candidate(con, 17, 8)
        if failure == 'validation':
            con.execute('update build.mart_hourly_demand set trips = 0')
        con.execute('set preserve_insertion_order = ?', [original_order])

        class FailingCopy:
            def execute(self, sql, parameters=None):
                result = con.execute(sql, parameters) if parameters is not None else con.execute(sql)
                if failure == 'copy' and sql.startswith('create or replace table main."fct_trips"'):
                    raise OSError('copy interrupted')
                return result

        if failure == 'none':
            publish(FailingCopy())
            # Exact bidirectional multiset comparison, preserving duplicate rows.
            for table in TABLES:
                differences = con.execute(f'''select * from (
                    (select * from main."{table}" except all select * from build."{table}")
                    union all
                    (select * from build."{table}" except all select * from main."{table}")
                )''').fetchall()
                assert not differences
        else:
            with pytest.raises((ValueError, OSError)):
                publish(FailingCopy())
            assert snapshot(con) == before
        assert con.execute("select current_setting('preserve_insertion_order')").fetchone()[0] is original_order


def test_existing_reader_keeps_complete_version_across_publication(tmp_path):
    database = str(tmp_path / 'warehouse.duckdb')
    with duckdb.connect(database) as writer:
        candidate(writer, 4, 1)
        publish(writer)
        with duckdb.connect(database) as reader:
            reader.execute('begin')
            before = snapshot(reader)
            candidate(writer, 7, 2)
            publish(writer)
            assert snapshot(reader) == before
            reader.execute('commit')
            assert reader.execute('select count(*) from main.fct_trips').fetchone()[0] == 7
            assert reader.execute('select sum(trips) from main.mart_hourly_demand').fetchone()[0] == 7

"""The comparison must retain duplicate sensitivity and exclude warmup timings."""
import duckdb

from scripts.benchmark_publication import TABLES, fingerprints, summary


def test_row_fingerprints_ignore_order_but_detect_values_and_duplicates():
    with duckdb.connect() as con:
        con.execute('create schema build')
        for table in TABLES:
            con.execute(f'create table build."{table}" as select * from (values (1), (2), (2), (null)) t(x)')
            con.execute(f'create table main."{table}" as select * from build."{table}" order by x desc nulls first')
        expected = fingerprints(con, 'build')
        assert fingerprints(con, 'main') == expected
        con.execute('update main.fct_trips set x = 3 where x = 2')
        assert fingerprints(con, 'main')['fct_trips'] != expected['fct_trips']
        con.execute('create or replace table main.fct_trips as select distinct * from build.fct_trips')
        assert fingerprints(con, 'main')['fct_trips'] != expected['fct_trips']


def test_summary_excludes_warmups_and_preserves_range():
    def run(variant, warmup, value):
        return {'variant': variant, 'warmup': warmup,
                **{metric: value for metric in ('publication_wall_seconds', 'cpu_seconds', 'copy_seconds',
                                                'peak_process_rss_bytes', 'peak_run_directory_bytes',
                                                'checkpointed_database_bytes')}}
    records = [run('baseline', True, 10000), run('candidate', True, 10000),
               run('baseline', False, 10), run('baseline', False, 14),
               run('candidate', False, 6), run('candidate', False, 10)]
    measured = summary(records)
    assert measured['baseline']['publication_wall_seconds'] == {'median': 12, 'min': 10, 'max': 14}
    assert measured['candidate']['publication_wall_seconds']['median'] == 8
    assert measured['publication_median_reduction_percent'] == 33.333

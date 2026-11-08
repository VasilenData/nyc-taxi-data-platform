# Runbook

## What a failed run means

`python run_pipeline.py ...` exits non-zero when loading, dbt, or any asset check
fails. The CLI explicitly evaluates every asset-check result. In Dagster,
`reconciliation` and `reject_rate` are configured as blocking checks; `coverage`
is recorded as a check but is not itself configured as blocking. Inspect the
individual check results when operating through the UI.

Checks execute after tables have been materialized. A failure does not roll back
the entire warehouse or atomically hide newly built tables from readers. Treat
the result as unsuitable for downstream publication until it has been inspected
and rerun successfully. An automated consumer needs a separate
staging/validate/promote mechanism before this can serve as an atomic publication
boundary.

## A check failed

| Failing check | Meaning | First thing to look at | Fix |
|---|---|---|---|
| `reconciliation` | raw rows are not equal to valid plus rejected for some month | `select * from main.mart_data_quality`, then dbt test `assert_raw_equals_valid_plus_rejected` | Re-run the month. If it persists the classifier dropped rows: check for a `where` or a join added to `stg_trips_classified`. |
| `reject_rate` | a month rejected more than 10% of its rows | `select reject_reason, rejected_rows from main.mart_data_quality where source_month = '2023-09'` | One dominant reason means upstream changed (TLC schema or unit change): inspect the raw file. Spread over many reasons means a corrupt file: re-download it. Raise `TAXI_MAX_REJECT_RATE` only after checking the rows. |
| `coverage` | a raw month has no rows in the fact table | The `fct_trips` dbt run log | A contract or test failure upstream stopped the build. Fix, then re-run `transform_and_check`. |
| dbt contract error | a column name or type in `fct_trips` changed | `dbt/models/marts/schema.yml` against the model SQL | Update both together, in one commit. |

## Backfill a month

Dagster UI, asset `raw_trips`, select the month, Materialize. Then run job `transform_and_check`. CLI: `python run_pipeline.py 2023-05`.

## Late or corrected file from the TLC

Replace the parquet in `TAXI_DATA_DIR` and load the month again. The loader deletes
and inserts the month in one transaction. Empty files and failed inserts roll back
the replacement, preserving the previous month and leaving a caller-owned
connection usable. An empty source is treated as an upstream failure, not as an
instruction to delete historical data.

The incremental classifier compares `_loaded_at` with that month's processed load
version rather than a global timestamp. This also reprocesses a corrected month
whose timestamp is older after a clock correction or warehouse restore. Use the
loader to make corrections; manually changing raw values without changing their
load timestamp is outside this versioning contract.

## Look at rejected rows

```sql
select reject_reason, count(*) from main.trips_quarantine group by 1 order by 2 desc;
select * from main.trips_quarantine where reject_reason = 'bad_distance' limit 20;
```

## Capture a reproducible run

Use the [benchmark runner](BENCHMARKS.md) for an isolated warehouse, source
checksums, environment details and retained JSON/log evidence. A full benchmark
requires all requested monthly files and never silently uses bundled extracts.
For memory-constrained machines, set `TAXI_DUCKDB_MEMORY_LIMIT` (for example,
`4GB`); the same configured limit is applied separately to ingestion and dbt.
This is not a total process-memory cap, and the asset-check connections retain
DuckDB's default memory setting.

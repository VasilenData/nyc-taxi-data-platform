# Runbook

## What a failed run means

`python run_pipeline.py ...` exits non-zero when loading, dbt, or any asset check
fails. The CLI explicitly evaluates every asset-check result. In Dagster,
`reconciliation`, `reject_rate` and `coverage` are all configured as blocking
checks. Inspect the individual check results when operating through the UI.

dbt materializes candidates in `build`, while consumers read `main`. The
`published_warehouse` asset runs after the complete dbt graph and its blocking
checks. It repeats the platform quality predicates in one DuckDB transaction,
checks fact and mart totals, then copies all eight managed tables and their
publication metadata. Validation errors, copy failures and process death before
commit leave the previous `main` publication intact. On a first-run failure there
is no published dataset yet. Raw loads and failed `build` candidates remain for
diagnosis; they are not consumer tables.

Inspect the current version with `select * from main.publication` and successful
publication history with `select * from audit.publications order by published_at`.
The history records source filenames/load timestamps, row counts, model revision
and individual transformation-file hashes, not retained
old table snapshots. Retry `transform_and_check` after correcting the candidate;
use the full job, not a manually selected publication asset, so dbt tests also run.
Direct `dbt build` updates only `build` and never publishes.

On upgrade, the first successful transform creates the new candidate schema and
replaces the previous `main` tables/views in the same publication transaction.
Keep one writer and do not modify raw/build data concurrently with a transform.
DuckDB does not provide general concurrent multi-process read/write serving.
A reader issuing several SELECTs must use a read transaction for one consistent
publication. See DuckDB's [transaction](https://duckdb.org/docs/stable/sql/statements/transactions)
and [concurrency](https://duckdb.org/docs/stable/connect/concurrency) documentation.
The process-exit test covers abrupt application termination before commit, not
power failure or a corrupt storage device.

## A check failed

| Failing check | Meaning | First thing to look at | Fix |
|---|---|---|---|
| `reconciliation` | raw rows are not equal to valid plus rejected for some month, including output-only months | dbt test `assert_raw_equals_valid_plus_rejected`, then the raw, valid and quarantine counts for the reported month | Investigate missing or stale classified rows. Re-run the affected load and transformation; a full refresh may be needed after manual raw-table changes. |
| `reject_rate` | a month rejected more than 10% of its rows | `select reject_reason, rejected_rows from build.mart_data_quality where source_month = '2023-09'` | One dominant reason means upstream changed (TLC schema or unit change): inspect the raw file. Spread over many reasons means a corrupt file: re-download it. Raise `TAXI_MAX_REJECT_RATE` only after checking the rows. |
| `coverage` | a raw month has no rows in the fact table | The `fct_trips` dbt run log | A contract or test failure upstream stopped the build. Fix, then re-run `transform_and_check`. |
| dbt contract error | a column name or type in `fct_trips` changed | `dbt/models/marts/schema.yml` against the model SQL | Update both together, in one commit. |

## Backfill a month

Dagster UI, asset `raw_trips`, select the month, Materialize. Then run job `transform_and_check`. CLI: `python run_pipeline.py 2023-05`.

For reviewed corrections to an existing publication, use the
[plan/apply workflow](CHANGE_MANAGEMENT.md). It binds the approved source bytes,
model revision and current warehouse state; stale plans fail before replacement.
The report records affected months, before/after consumer checksums and recovery
outcomes. Its executed six-scenario exercise includes a source correction,
breaking schema, rejected candidate, successful recovery and model-rule change.

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

Changes to SQL, project variables, macros, tests or seed content now trigger an
automatic full dbt refresh, including unchanged source months. Each publication
records its model hash. This prevents the incremental classifier from retaining
an old rule merely because raw load timestamps did not change. A model-only plan
can omit `--months`; all loaded months are reclassified conservatively. Revert a
bad accepted rule in source and rebuild to publish the corrected result; audit
history is not an old-data snapshot store.

## Look at rejected rows

```sql
select reject_reason, count(*) from build.trips_quarantine group by 1 order by 2 desc;
select * from build.trips_quarantine where reject_reason = 'bad_distance' limit 20;
```

## Capture a reproducible run

Use the [benchmark runner](BENCHMARKS.md) for an isolated warehouse, source
checksums, environment details and retained JSON/log evidence. A full benchmark
requires all requested monthly files and never silently uses bundled extracts.
For memory-constrained machines, set `TAXI_DUCKDB_MEMORY_LIMIT` (for example,
`4GB`); the same configured limit is applied separately to ingestion and dbt.
This is not a total process-memory cap, and the asset-check connections retain
DuckDB's default memory setting.

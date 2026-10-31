# Runbook

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

Replace the parquet in `TAXI_DATA_DIR` and load the month again. The loader deletes the month before inserting, and dbt reprocesses any month whose `_loaded_at` is newer than what it has seen.

## Look at rejected rows

```sql
select reject_reason, count(*) from main.trips_quarantine group by 1 order by 2 desc;
select * from main.trips_quarantine where reject_reason = 'bad_distance' limit 20;
```

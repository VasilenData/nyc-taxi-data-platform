# NYC Taxi Data Platform

[![CI](https://github.com/VasilenData/nyc-taxi-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/VasilenData/nyc-taxi-data-platform/actions/workflows/ci.yml)

A reproducible batch pipeline that turns public NYC taxi records into tested analytics tables and retains rejected records with an auditable reason.

**Stack:** Python, SQL, Dagster, dbt, DuckDB, Docker, GitHub Actions.

**Operating status:** Runs locally, in Docker and in CI. Cloud deployment and a continuously operated production service are outside the current scope.

## Results at a glance

Historical measurements from **November 8, 2026**, before staged publication was added, using a new warehouse for each run:

| Dataset | Input rows | Valid fact rows | Quarantined rows | Pipeline wall time | Evidence |
|---|---:|---:|---:|---:|---|
| Full January–October 2023 TLC files | 31,593,944 | 30,694,510 | 899,434 (2.85%) | 78.576 s | [JSON](measurements/full-2026-11-08.json) · [run log](measurements/full-2026-11-08.log) |
| Bundled January/February sample | 30,000 | 29,326 | 674 (2.25%) | 20.735 s | [JSON](measurements/sample-2026-11-08.json) · [run log](measurements/sample-2026-11-08.log) |

These timings do not include the new transactional publication copies; that full-data implementation has not been remeasured. Both recorded runs passed the dbt tests, the three platform asset checks, and independent input/raw/valid/quarantine/fact reconciliation. These are public-data project results, not employer production or customer-usage metrics.

The full-data measurement used an AMD EPYC host with an **8-CPU container quota**, **8 GiB container memory limit**, a **4 GB DuckDB limit for ingestion and dbt**, and four dbt threads. That DuckDB setting is not a total process-memory cap; asset-check connections keep their default setting. Input downloads and checksums are excluded from pipeline timing; the OS file cache was not cleared. The sample included the initial dbt parse, while the full run reused the manifest. Complete versions, input checksums and timing conditions are in the JSON files. See [how to reproduce and interpret the benchmarks](docs/BENCHMARKS.md).

## The problem

Raw trip files contain invalid distances, negative fares, impossible durations, missing timestamps and other inconsistencies. Silently dropping those rows makes analytics difficult to reconcile and failures difficult to investigate.

This pipeline preserves raw records, classifies each rejected row by its first failed rule, and builds a fact table and marts from valid records. Its CLI reports a failed run when reconciliation, rejection-rate or coverage checks fail.

## Architecture

```mermaid
flowchart TD
    A["NYC TLC monthly Parquet"] --> B["Dagster ingestion"]
    B --> C["DuckDB raw trips and lineage"]
    C --> D["dbt classification"]
    D --> E["Valid trips and fact table"]
    D --> F["Quarantine and rejection reasons"]
    E --> G["Candidate marts in build schema"]
    G --> P["Validate and commit one publication"]
    P --> O["main: last-good consumer tables"]
    C --> H["Run-level quality checks"]
    E --> H
    F --> H
```

- **Ingestion:** reloads a month transactionally without accumulating a second copy; empty or failed replacements roll back to the previous data. Records source month, source filename and load timestamp.
- **Transformation:** an incremental dbt classifier compares load versions per month, including corrected versions with older timestamps. The fact table has an enforced column/type contract and joins the zone lookup seed.
- **Consumption:** marts summarize daily borough activity, hourly demand and data quality.
- **Quality:** reconciliation includes output-only months as well as raw months, so stale valid or quarantined data cannot hide behind a raw-side join. Reconciliation, rejection-rate and coverage checks validate the candidate `build` schema. A downstream publication asset repeats those gates within its snapshot, checks fact/mart totals, then copies the eight managed tables and publication metadata to `main` in one DuckDB transaction. CLI, UI and scheduled transform jobs use this same path.

## Run the sample

Use Python 3.11 or later. The sample command uses two bundled 15,000-row extracts and requires no TLC data download.

```bash
git clone https://github.com/VasilenData/nyc-taxi-data-platform.git
cd nyc-taxi-data-platform
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python scripts/benchmark.py --dataset sample --output measurements/local-sample.json
pytest -q
```

On Windows, activate the environment with `.venv\Scripts\Activate.ps1` in PowerShell.

Open the Dagster UI with Docker:

```bash
docker compose up --build
# http://localhost:3000
```

The quickstart always selects the bundled sample, even if full TLC files have
previously been downloaded. It writes `measurements/local-sample.json` and its
companion `.log`. Choose a new `.json` filename to repeat the measurement; existing
evidence is not overwritten.

For a full-data run, follow [the benchmark commands](docs/BENCHMARKS.md#full-januaryoctober-2023-data). Full mode requires every requested input file in the full-data directory and prevents the loader's automatic fallback to bundled samples. Verify the recorded row counts and checksums when reusing files. For ordinary runs, `TAXI_DATA_DIR` selects the input directory, `TAXI_DB` selects the warehouse, and `TAXI_DUCKDB_MEMORY_LIMIT` optionally caps ingestion/dbt memory.

## Evidence to inspect

| Engineering question | Where to look |
|---|---|
| Can a month be reloaded without double-counting? | [Transactional monthly loader](taxi_platform/ingest.py) and [rerun integration test](tests/test_pipeline.py) |
| Will a bad replacement destroy the previous load? | [Empty-input and failed-insert rollback tests](tests/test_ingest.py); [out-of-order correction integration test](tests/test_pipeline.py) |
| Are invalid rows handled deliberately? | [Classification rules](dbt/models/staging/stg_trips_classified.sql) and [quarantine model](dbt/models/staging/trips_quarantine.sql) |
| Does a failed check actually fail the CLI run? | [Quality checks](taxi_platform/checks.py), [runner](run_pipeline.py) and the poisoned-month test in [integration tests](tests/test_pipeline.py) |
| Can the results be reproduced? | [Benchmark runner](scripts/benchmark.py), [measurement guide](docs/BENCHMARKS.md), [CI workflow](.github/workflows/ci.yml) and [Dockerfile](Dockerfile) |
| Can a failed candidate replace consumer tables? | [Transactional publisher](taxi_platform/publication.py), [rollback/process-exit tests](tests/test_publication.py) and poisoned-month integration test |
| Can another engineer operate it? | [Runbook: failed checks, backfills and corrected files](docs/RUNBOOK.md) |

The failure-injection test corrupts roughly 40% of a sample month's fares and requires a non-zero pipeline exit:

```bash
pytest -q tests/test_pipeline.py::test_poisoned_month_trips_the_quality_gate
```

## Decisions and current limits

- DuckDB provides a credential-free local warehouse and fast CI. Warehouse-specific cloud deployment and tuning have not been demonstrated here.
- Incremental classification operates at monthly partition level: a changed month is reprocessed as a whole. Fact and mart models are rebuilt by dbt.
- Invalid records remain available in quarantine with their first rejection reason; they are not silently discarded.
- Consumers read `main.*`; `raw` and `build` are working data and may contain a failed candidate. Publication replaces all managed consumer tables and their version metadata atomically. An exception or process death before commit leaves the previous publication intact.
- This remains a single-writer local DuckDB warehouse, not a concurrent multi-process serving database. Readers needing one version across multiple queries must use a read transaction. Publication copies add storage and runtime overhead; it does not retain old table versions or provide a cloud serving layer.
- CI uses the 30,000-row sample and retains its benchmark artifact for 14 days. The checked-in 31.6-million-row measurement is a separate full-data run.

## Next improvement

Measure the staged publication overhead on full data, then add explicit source-schema drift diagnostics. The current loader still accepts missing fields as NULL; its transactional replacement and the publication gate are separate safeguards.

## Author

[Vasilen Kanchev](https://github.com/VasilenData) · [Portfolio](https://vasilendata.github.io) · [LinkedIn](https://www.linkedin.com/in/vasilenkanchev/)

Data source: public NYC Taxi and Limousine Commission trip records. Code: [MIT license](LICENSE).

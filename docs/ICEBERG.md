# Optional Iceberg table-format comparison

This is an executed **five-row synthetic fixture**, using PyIceberg 0.12.0, a local SQLite catalog and Parquet files. It does not replace the main DuckDB pipeline, run the full TLC dataset on Iceberg, or deploy a cloud lakehouse.

[Measured JSON](../measurements/iceberg-2026-11-12.json) · [Implementation](../scripts/iceberg_comparison.py) · [Optional integration test](../tests/test_iceberg_comparison.py)

## Why compare it?

The main pipeline publishes eight managed consumer tables in one DuckDB transaction. Iceberg offers a different useful boundary: readers can select a consistent **single-table snapshot**, retain history and change a partition without rewriting unrelated files. The comparison demonstrates those semantics and their storage costs; it does not infer an eight-table transaction from eight independent Iceberg commits.

## Observed behavior

Eight assertions passed in the recorded local run on 12 November 2026:

1. A January correction reuses the exact February data-file paths.
2. Adding a nullable `quality_note` column preserves existing field IDs and returns null for old rows.
3. An exception after candidate Parquet files are written but before the catalog transaction commits leaves the published metadata location, snapshot ID and rows unchanged.
4. At least one unreferenced candidate Parquet file remains after that failed transaction. The exercise measures it rather than pretending the write left no storage behind.
5. Prior snapshots can still be read: the initial three rows and later five-row append match independent expected values.
6. Rolling back to the append snapshot restores its data. The added column remains part of the current schema: data-snapshot rollback does not reverse schema evolution.
7. Restoring the corrected snapshot returns the expected five rows and total of 15,200 synthetic fare cents.
8. A companion DuckDB current-state table agrees with the same expected corrected rows and survives its own pre-commit exception.

The Iceberg fixture is partitioned by `source_month`. The operations are initial append, second append, January-only overwrite, nullable-column addition, aborted append, rollback and restore. The file reader uses table metadata rather than listing every Parquet file in the directory as if it were committed.

## Interpreting the measurements

The JSON records one wall-clock observation for each operation, package versions, host platform/CPU count, a deterministic fixture hash and the normalized implementation-file hash. The Git head recorded at execution predates this newly added script; the implementation hash identifies the exact exercised source.

Physical storage is split into current referenced Parquet, all Parquet including historical and aborted files, Iceberg metadata and SQLite catalog bytes. A standalone current-state Parquet file and a checkpointed DuckDB database provide additional context. These figures include different structures and retention policies. Five tiny rows are dominated by file/database overhead; ratios and timings from this example are **not** a throughput, scalability, compression or engine-speed ranking.

The observed aborted candidate occupies storage while remaining invisible to table readers. A real deployment needs an explicitly safe orphan-file and snapshot-retention policy, coordinated with writers and retention requirements. This exercise intentionally retains all files for inspection.

## Reproduce

Use an isolated environment so these optional dependencies do not change the main Dagster/dbt environment:

```bash
python -m venv .venv-lakehouse
source .venv-lakehouse/bin/activate
pip install -r requirements-lakehouse.txt pytest
python scripts/iceberg_comparison.py --work-dir /tmp/taxi-iceberg-new --output measurements/iceberg-local.json
python -m pytest -q tests/test_iceberg_comparison.py
```

On Windows, use `.venv-lakehouse\Scripts\Activate.ps1` and a new local path for `--work-dir`. Both the work directory and evidence path must be new. A failed run raises instead of emitting a passing report. The unit test uses a real local catalog and Parquet files; the core pipeline test job skips this optional test if PyIceberg is not installed. A dedicated `iceberg` CI job installs the isolated requirements and runs this integration test on Linux.

## What this does not prove

No remote object store, concurrent writers, distributed catalog, multiple-table publication, host power failure, long-running operation or full-data performance test. The deliberate failure is a Python exception before catalog commit. The companion DuckDB file stores current state without a historical snapshot catalog; the comparison does not claim identical retention or storage designs.

References: [PyIceberg API](https://py.iceberg.apache.org/api/), [Iceberg reliability and atomic table commits](https://iceberg.apache.org/docs/latest/reliability/).

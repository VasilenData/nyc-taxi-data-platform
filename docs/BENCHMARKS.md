# Reproducing the measurements

The benchmark runner measures the actual pipeline in a new DuckDB database, then
queries that database to reconcile input, raw, valid, quarantined and fact rows.
It requires Python 3.11 or later and the development requirements.

## Bundled sample

From the repository root, after installing `requirements-dev.txt`:

```bash
python scripts/benchmark.py --dataset sample --output measurements/local-sample.json
```

This selects the bundled January and February 2023 extracts, with 15,000 rows each.
No input data is downloaded, and previously downloaded full files are ignored.
Use a new `.json` output filename for each run: the runner requires that extension
to keep the measurement separate from its `.log`, and refuses to overwrite an
existing measurement or log.

## Full January–October 2023 data

```bash
python scripts/benchmark.py \
  --dataset full --download \
  --months 2023-01 2023-02 2023-03 2023-04 2023-05 \
           2023-06 2023-07 2023-08 2023-09 2023-10 \
  --memory-limit 4GB --timeout 900 \
  --output measurements/local-full.json
```

Missing files are downloaded from the official TLC distribution into `data/raw/`
before timing. Existing files are reused, with their row counts and SHA-256 hashes
recorded. Downloads occupy approximately 520 MB; the temporary warehouse and
intermediate data need additional disk space. The runner removes its temporary
warehouse on completion and retains only the measurement and log.

To use files already downloaded elsewhere, add `--data-dir /path/to/full-files`
and omit `--download`. Every requested month must exist in that directory. Unlike
the general-purpose loader, full benchmark mode never falls back to bundled sample
files, so January and February cannot silently become 15,000-row extracts.

## What the JSON and log establish

Each JSON file records:

- UTC preparation, start and finish times; pipeline exit code and wall time.
- Input filenames, monthly row counts, byte sizes and SHA-256 hashes.
- Whether files were downloaded in this invocation and their download durations.
- Python, package versions, operating system, CPU model, logical CPU/affinity count,
  host memory, and Linux cgroup CPU/memory limits when available.
- Git base commit, whether the working tree had changes, and hashes of the pipeline,
  benchmark script, SQL, configuration and seed files used.
- The exact pipeline command, configured DuckDB memory budget, and whether a dbt
  manifest already existed.
- Independently queried output counts, rejection reasons, monthly totals and three
  reconciliation comparisons, plus the asset checks reported by the runner.
- The accompanying normalized pipeline log and its SHA-256 hash.

`success: true` requires a zero pipeline exit code, all three named asset checks
passing, and input/raw/valid/quarantine/fact reconciliation. A failed or timed-out
pipeline produces a failure record and a non-zero exit code. Preparation failures,
such as a missing input file, stop before a pipeline measurement is created.

The checked-in measurements were made in a working tree based on the recorded
commit. The source hashes identify the code used more precisely than the base
commit alone. Their `scripts/benchmark.py` hash matches the exact
[archived runner source](../measurements/benchmark-runner-2026-11-08.py.txt)
used for those measurements. The current runner adds an output-filename guard
after review; that change does not alter the timed pipeline. All recorded
pipeline/SQL/configuration/seed hashes still match the current files. Use the
current `scripts/benchmark.py` for new runs; the archive is provenance evidence.

## Timing boundaries and cache conditions

The timer includes Python/Dagster startup, monthly loading, `dbt build` (models,
seeds and tests), and Dagster asset checks. If the dbt manifest is missing, its
initial parse is also included. The timer excludes source downloads, input
inspection/checksums, environment inventory and the final independent SQL count
queries. Those preparation conditions are recorded separately.

Every run starts with a new warehouse. This is **not a cold OS-cache benchmark**:
source files are read for their checksums before timing, and the runner does not
clear the operating system page cache. Shared-host load and CPU quotas can also
affect results. Compare measurements only after checking these conditions; a
single recorded time is not a performance guarantee for another machine.

`--memory-limit` defaults to `4GB`, configured independently for ingestion and
dbt through `TAXI_DUCKDB_MEMORY_LIMIT`. It limits DuckDB's memory management in
those processes, not their total process memory or the combined pipeline memory.
The SQL asset-check connections and final inspection queries retain DuckDB's
default memory setting. The container memory limit is recorded separately.
Ordinary ingestion/dbt runs retain DuckDB's default `80%` unless that environment
variable is set. dbt uses four worker threads.

## CI evidence

The [CI workflow](../.github/workflows/ci.yml) runs the bundled-sample benchmark
and uploads its JSON and log as the `sample-benchmark` artifact, retained for
14 days. CI does not download or run the full ten-month dataset. The repository's
dated full-data measurement is separate from that sample CI evidence.

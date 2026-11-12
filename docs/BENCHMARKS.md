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
- Publication version and model revision; validation, copy and commit-path timing.
- Sampled aggregate pipeline-process RSS, peak warehouse-directory bytes, and
  checkpointed database bytes after independent inspection.

`success: true` requires a zero pipeline exit code, all three named asset checks
passing, and input/raw/valid/quarantine/fact reconciliation. A failed or timed-out
pipeline produces a failure record and a non-zero exit code. Preparation failures,
such as a missing input file, stop before a pipeline measurement is created.

Measurements identify the executed working tree through source hashes as well
as its Git base commit. The November 8 files predate atomic publication and
retain their [archived runner](../measurements/benchmark-runner-2026-11-08.py.txt).
They do not describe the current pipeline. The November 12 measurement uses the
new publisher, source checksums and model-revision guard; its code hashes are
the evidence for the implementation run. Use the current runner for new results.
The source-file hashes identify exact checkout bytes, including line endings:
unchanged files in the Windows measurement checkout can have CRLF where Git's
stored blob uses LF. The separate published `model_sha256` normalizes line
endings and is portable across Windows and Linux.

## Timing boundaries and cache conditions

The timer includes Python/Dagster startup, monthly loading, `dbt build` (models,
seeds and tests), Dagster asset checks and transactional publication. If the dbt manifest is missing, its
initial parse is also included. The timer excludes source downloads, input
preparation inspection/checksums, environment inventory and the final independent
SQL count queries. The loader's before/after integrity checksums ARE included in
pipeline timing. Those boundaries are recorded separately in schema version 2.

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
14 days. Normal CI does not download the full ten-month dataset. The separate
[manual full-data workflow](../.github/workflows/full-benchmark.yml) allows a
bounded Linux run and retains exact-head JSON/log artifacts for 30 days. The
checked-in November 12 full-data result was run locally on Windows, not by that
hosted workflow. The dedicated `iceberg` CI job exercises its optional local
snapshot semantics in a separate dependency environment.

## Publication-version timing

The November 12 full-data measurement includes the `build`-to-`main` publication
step. `publication_seconds` measures entry through transaction commit;
`validation_seconds` measures the repeated publication predicates;
`copy_seconds` includes removal of legacy views and the eight table copies.
Remaining time includes transaction setup, metadata and commit. These are
instrumented phases within one run, not a baseline/optimized comparison.

RSS is sampled every 0.1 seconds for the pipeline process and descendants.
Summed RSS can count shared pages more than once; short peaks between samples
can be missed. The supervisor is excluded. Warehouse-directory sampling includes
the database, WAL and temporary spill files. Checkpointed final bytes include raw,
build and consumer data: they are not solely publication overhead. Neither
measurement is a hard resource limit.

Hardware, operating system, dependency versions and pipeline work differ from
November 8. No speedup or regression claim can be inferred by comparing those
two wall times. Both use a warm input cache and a new warehouse.

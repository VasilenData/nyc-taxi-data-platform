# Measuring publication without weakening recovery

The November 12 end-to-end benchmark identified a concrete cost: 31.976 of the
33.074 publication seconds were spent copying eight reporting tables. Those
historical numbers remain unchanged. This experiment isolates the publication
stage and compares it with the pinned publisher from commit
`5a8003f3d4c9c9f2ceea9340c9a4484f1f1cf246` on the same prepared database.

## Decision and contract

All eight public relations remain physical tables. Candidate validation, the
single transaction, audit history and current-version metadata are unchanged.
The retained tuning allows DuckDB's table-copy sink to reorder rows, using
`preserve_insertion_order=false` during publication. No report promises a
physical row order: consumers requiring ordered output must use `ORDER BY`.

The publisher restores the caller's original setting after success, validation
failure and interrupted copying. This is a DuckDB **global** setting, not a
connection-local hint; other connections in the same database instance can see
the temporary value. The supported deployment remains one writer. Readers
requiring one version across multiple queries use a read transaction. This
change does not claim distributed or multi-process serving.

The choice follows DuckDB's [order-preservation contract](https://duckdb.org/docs/2.0/sql/dialect/order_preservation)
and [workload-tuning guidance](https://duckdb.org/docs/2.0/guides/performance/how_to_tune_workloads).
Those documents describe a possible memory benefit, not a guaranteed speedup.
The recommendation here depends on the measured result below.

## Observed result: 14 November 2026

The [complete comparison record](../measurements/publication-order-2026-11-14.json)
contains the frozen source hashes, ten source-file hashes, per-table copy timings,
all eight runs and their verification results. Preparation used the January–October
2023 TLC files: **31,593,944 raw rows, 30,694,510 valid rows and 899,434 quarantined
rows**. Both variants used DuckDB 2.0.0, Python 3.12.14, 20 threads and a 4 GB
DuckDB memory limit on the same Windows workstation.

After one excluded warmup per variant, three alternating pairs produced:

| Measured quantity | Baseline median (range) | Candidate median (range) |
|---|---:|---:|
| Publication wall time | 31.311 s (31.051–39.050) | **24.165 s (24.108–28.308)** |
| Eight-table copy time | 29.918 s (28.939–37.038) | 23.134 s (22.987–27.045) |
| Process CPU time | 162.016 s (160.562–162.734) | 169.234 s (166.359–176.703) |
| Sampled peak process RSS | 3.997 GiB (3.986–4.003) | 4.029 GiB (4.022–4.030) |
| Sampled peak run-directory size | 13.114 GiB (13.113–13.121) | 13.366 GiB (13.365–13.367) |
| Physical database after checkpoint | 13.160 GiB (13.159–13.168) | 13.402 GiB (13.399–13.402) |

Median publication wall time was **22.8% lower**, and the candidate was faster in
each measured pair. This buys shorter publication latency at a cost: median CPU
time rose **4.5%**, sampled peak RSS **0.8%**, and the checkpointed physical file
size **1.8%**. It is not a memory, storage or CPU optimization. The file-size
measurements include the whole warehouse after replacement; no compaction was
performed, and these are not logical table sizes. Explicit checkpoint times
(outside publication timing) were 0.743–0.887 s for the baseline and
0.657–0.870 s for the candidate.

Per-table timers locate the clearest reduction in the large `stg_trips` copy:
its median fell from 12.721 s to 7.474 s. Other copy stages did not all improve.
This supports removing unnecessary insertion-order preservation for this workload,
without claiming that every table or machine benefits.

Every run passed all eight acceptance checks, including eight-relation
schema/count/hash diagnostics, publication metadata and setting restoration.
The prepared database and shared helpers
were unchanged at completion. Baseline timings varied substantially; three pairs
on one shared machine establish an observed local result, not a statistical
guarantee. The older end-to-end measurements above are a separate experiment.

## Measurement boundary

`scripts/benchmark_publication.py`:

1. Requires a closed, checkpointed input database with no WAL; fingerprints its
   complete bytes, candidate schemas/rows and source-file audit records.
2. Loads the baseline publisher from its full Git commit, freezes both publisher
   files, and verifies their shared helpers before and after the experiment.
3. Makes and SHA256-checks a fresh database copy for **every** run. It never writes
   to the input database. Baseline and candidate use the same memory limit,
   thread count, initial setting, input and runtime.
4. Runs a warmup pair and three measured pairs, alternating which variant runs
   first. Warmups are retained but excluded from summary statistics.
5. Times the actual publisher, including validation and commit. It also records
   each table-copy duration, process CPU time, sampled RSS and database/WAL/spill
   disk usage. Copying the input, checking results and an explicit final
   checkpoint are outside this timer; checkpoint time/size are recorded separately.
6. Verifies all eight consumer tables and the new publication metadata after each
   run, and checks the original database bytes again at completion.

The OS cache is not cleared; copying and hashing warm it. This runs on a shared
Windows workstation, not a dedicated idle benchmark host. RSS/disk are sampled
every 100 ms and can miss brief peaks. DuckDB's memory limit is not a process RSS
cap. Full-table diagnostics compare column types, counts, summed row hashes and
XOR row hashes: they ignore row order but are not collision-free or
cryptographic proofs. Small regression fixtures additionally use exact,
bidirectional `EXCEPT ALL` comparisons, including duplicates.

This is **publication-only** evidence. It does not establish an end-to-end
pipeline speedup, cloud throughput or a universal tuning recommendation.

### Source-byte identity

The baseline and candidate publisher hashes normalize CRLF to LF, as the runner
explicitly records in its implementation. The runner hash uses its actual LF
bytes. The four unchanged helper hashes use the actual **CRLF Windows checkout
bytes** loaded during the run. In the pinned Git baseline, `config.py` and
`ingest.py` are CRLF; `quality.py` and `revision.py` are LF. All four measured
helpers exactly match their baseline blobs after reconstructing CRLF endings.
Verify that byte rule without changing the immutable report:

```python
import hashlib, json, subprocess
from pathlib import Path
report = json.loads(Path("measurements/publication-order-2026-11-14.json").read_text())
for path, expected in report["dependency_sha256"].items():
    blob = subprocess.check_output(["git", "show", f'{report["baseline_commit"]}:{path}'])
    measured_bytes = blob.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")
    assert hashlib.sha256(measured_bytes).hexdigest() == expected
```

## Recovery gates

The existing rollback and abrupt-process-exit tests remain enabled. Added checks
cover both original setting values on successful, invalid and interrupted
publications; exact duplicate-sensitive equality against the candidate; and a
reader transaction that keeps the old complete version while the writer commits
the new one. A reader starting a new transaction then sees the new fact and mart.

```bash
python -m pytest -q tests/test_publication.py tests/test_publication_benchmark.py
```

## Reproduce

Use a new local database and the project environment. Preparation is outside the
publication comparison. For the historical full workload, provide all January
through October 2023 TLC Parquet inputs through `TAXI_DATA_DIR` and set
`TAXI_DB` to a new path. The ordinary pipeline must finish successfully first.

```bash
python run_pipeline.py 2023-01 2023-02 2023-03 2023-04 2023-05 \
  2023-06 2023-07 2023-08 2023-09 2023-10
python scripts/benchmark_publication.py --database /path/to/prepared.duckdb \
  --memory-limit 4GB --threads 20 --warmups 1 --repeats 3 \
  --output measurements/my-new-publication-comparison.json
```

The immutable baseline commit must exist in your clone; a shallow clone may need
`git fetch origin 5a8003f3d4c9c9f2ceea9340c9a4484f1f1cf246`. Use a new output
filename. A partially written report remains `success: false`. Do not run other
benchmarks concurrently. Database copies and intermediate spill files can require
substantially more disk than the Parquet inputs.

## Engineering prompt

> Identify one measured publication bottleneck. Preserve physical consumer
> tables, exact values, version metadata and the transaction/recovery boundary.
> Change one setting only when its semantics fit the public contract; restore it
> for callers on both successful and failed runs. Compare frozen baseline and
> candidate implementations on byte-identical database copies with interleaved
> repeats. Keep warmups, per-run measurements, source/runtime identity and
> correctness checks. Report regressions and limits as well as improvements;
> never infer an end-to-end speedup from a publication-only experiment.

Implementation, tests and this investigation are AI-assisted. They are not an
assessment of the owner's unaided performance or evidence of a production incident.

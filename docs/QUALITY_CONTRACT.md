# Prevent plausible-looking reports from publishing incorrect values

The 15 November 2026 review reproduced two weaknesses in the earlier contract:

1. `NaN` fare/tip and positive-infinite total amount were classified as valid.
   DuckDB orders NaN above other floating-point numbers, so a minimum-value test
   does not reject it. A valid-looking row count could accompany invalid revenue.
2. Publication reconciled global trip counts, but accepted changed borough revenue.
   Moving one unit of revenue from borough B to borough A also passed: even total
   revenue stayed unchanged while the report was wrong.

The [dated counterexample receipt](metric-contract-2026-11-15.json) records two
synthetic executions of the original pinned publisher and the new refusal. This
is a local correctness exercise, not a production incident or customer impact.

## Source classification

The classifier preserves every row and assigns `non_finite_value` when a consumed
numeric measure contains NaN or either infinity: passenger count, distance, fare,
tip, tolls or total amount. Existing null handling remains unchanged. The rule
follows timestamp/duration/period rules and precedes distance/fare rules, retaining
the documented first-failed-rule convention. Generic numeric range tests also
reject non-finite values, including when only a lower bound is configured.

The changed SQL and test contract alter the existing model signature. The normal
runner therefore fully reclassifies retained raw history once; unchanged old
incremental months cannot retain the former rule. Source reloads are not required.

## Consumer contract at publication

Inside the same publication transaction, the new gate independently aggregates
candidate facts and compares them with each reporting grain:

| Output | Grain | Compared metrics |
|---|---|---|
| Daily borough | Pickup date + borough, including the existing `Unknown` mapping | Trips, revenue, average fare, distance, duration and tip percentage |
| Hourly demand | Pickup weekday + hour | Trips, average speed and fare |

Missing groups, output-only groups, duplicate grain keys and changed counts fail.
Nullable metric results must agree on nullness. Other measures must be finite and
differ by no more than `1e-8 + abs(expected) * 1e-12`, allowing floating aggregation
roundoff. This is a report reconciliation tolerance, not currency rounding or a
claim of exact decimal accounting. Existing DOUBLE source/metric definitions are
preserved. The gate does not independently reconstruct every fact transformation.

A refused candidate leaves all eight previously published tables, current-version
metadata and successful publication history unchanged. Tests use hand-computed
four-trip reports and deliberately preserve global counts while moving revenue,
shifting hourly counts, splitting a group into duplicates or replacing its key.
The corrected candidate must then publish successfully. Existing process-exit and
reader-snapshot tests continue to protect the transaction boundary.

## Reproduction and measurement limits

```sh
pytest -q tests/test_nonfinite.py tests/test_mart_contract.py tests/test_publication.py
python scripts/benchmark.py --output measurements/new-sample-quality-check.json
```

Use a new output name. Earlier full-data measurements and the 22.8% publication
comparison remain immutable dated evidence for their recorded implementations.
The new validation adds work and changes the model identity; those older timings
must not be advertised as measurements of this version. The publication comparison
runner now requires a baseline with the same validation helper before timing it.
Operating/recovery records include that helper's source fingerprint.

The [15 November full-workload receipt](../measurements/full-quality-2026-11-15.json)
and [actual run log](../measurements/full-quality-2026-11-15.log) revalidate the
changed contract over **31,593,944** January–October 2023 source rows in a new local
warehouse: **30,694,510** valid facts plus **899,434** quarantined rows reconcile
exactly. All checks passed and publication completed. No rows received the new
`non_finite_value` rejection reason in this historical workload; the injected
regression fixtures establish that rejection behavior.

This single run took 106.544 seconds including ingestion, tests and publication;
publication took 22.728 seconds, with 0.339 seconds in its validation stage. Sampled
pipeline RSS peaked at 4,942,057,472 bytes and working-directory storage at
9,438,822,203 bytes. These are recorded observations, **not a measured improvement**:
other local verification work was active, inputs were warm, and no paired baseline
was run. The receipt retains source/input hashes and the sampling limitations.

References: [DuckDB floating-point semantics](https://duckdb.org/docs/2.0/sql/data_types/numeric#floating-point-types),
[finite-number predicates](https://duckdb.org/docs/2.0/sql/functions/numeric#isfinitex).

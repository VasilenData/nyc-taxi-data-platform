# Reviewed corrections and model changes

Monthly source corrections and SQL changes must not silently leave historical
classifications under an old rule. The maintenance CLI produces a reviewable
plan, binds it to the source bytes and current publication, and applies that
plan through the existing loader, dbt graph and eight-table publisher.

[Recorded six-scenario exercise](../measurements/change-exercise-2026-11-12-final/report.json)
· [Executable exercise](../scripts/change_exercise.py)
· [Integration tests](../tests/test_change_plan.py)

## Plan, inspect, apply

Create an initial valid warehouse using the ordinary pipeline first. Then, with
one writer and immutable input files:

```bash
python -m taxi_platform.change_plan plan \
  --database data/warehouse.duckdb --data-dir data/raw \
  --months 2023-01 --output measurements/january-plan.json

# Read the plan before applying it.
python -m taxi_platform.change_plan apply \
  --plan measurements/january-plan.json \
  --output measurements/january-result.json
```

Use the actual warehouse and input paths for your installation. Evidence files
are never overwritten. A plan records required-field validation, source SHA-256
hashes, monthly row counts/content fingerprints, the current publication and raw load versions,
changed transformation files, affected classification months and all rebuilt
consumer tables. Unchanged source files are skipped. Missing required columns or
empty corrections are rejected before application.

Version 2 plans also record schemas and order-independent content fingerprints
for raw input, the persisted incremental classifier and all nine serving
relations. Apply refuses a changed plan, source file, model revision or warehouse
state, including edits that retain row counts, load timestamps and publication
IDs. Physical row reordering alone is accepted. Missing tables and nullable-column
additions invalidate the plan. Old version 1 plans and plans created under a
different DuckDB version must be recreated; row-hash behavior is not promised to
remain stable across runtime versions.

The loader checks the reviewed checksum again before replacement. These are
controlled single-writer maintenance safeguards, not a distributed lock or a
cryptographic signature authenticating an approver. Recreate a stale plan rather
than editing its checksum. The fingerprints add full content scans during both
planning and validation; this trades review latency for stronger drift detection.
Their collision risk means they are not an authenticity or malicious-tampering
proof. Do not manually edit raw values without using the loader: detecting a
stale plan does not automatically repair existing inconsistent data. These
guards compare the reviewed state; they do not certify arbitrary preexisting
state as correct or lock out a competing writer after validation.

## Corrected source versus changed business rule

For a source correction, only changed months are loaded and reclassified.
Fact and mart tables are still rebuilt in full and published together; this is
not a fully incremental downstream warehouse.

Every successful publication records the hash of the dbt project, SQL, macros,
tests and seed inputs, plus their individual file hashes. A different revision
automatically runs `dbt build --full-refresh`, including all loaded months, even
if no source timestamp changed. This deliberately conservative rule prevents
stale classifications after a threshold or SQL change. Legacy publications
without a revision also trigger a full refresh. Newline normalization makes
the model revision independent of Windows/Linux line endings.

To review a model-only change, omit `--months` from the plan command. The plan
lists changed transformation files and all months needing reclassification.
The transformation also refuses publication if files change during its build.
This does not replace code review: a logically wrong rule can still satisfy
the data-quality tests.

## Executed exercise, 12 November 2026

The exercise uses 30,000 public sample rows. Source modifications and faults
are **synthetic exercises**, not reported TLC incidents.

| Scenario | Observed result |
|---|---|
| Initial January/February build | Valid baseline published |
| January total correction plus nullable extra source column | January alone reloaded; February classification checksum unchanged |
| Remove required `fare_amount` | Plan rejected before apply |
| Make roughly 40% of January fares negative | Quality gate failed; all eight consumer tables and publication metadata unchanged |
| Restore the valid correction | New valid publication committed |
| Change `max_trip_miles` from 200 to 100 in an isolated dbt project | Both months reclassified with no source reload; no published fact exceeded 100 miles |

The report includes command exit codes, normalized log hashes, source-code
hashes, plans, publication versions and before/after row counts and content
checksums. Content checksums use a commutative DuckDB row hash for regression
comparison, not cryptographic integrity. Reproduce with a new output directory:

```bash
python scripts/change_exercise.py --output measurements/local-change-exercise
```

### Stale-state regression exercise

[Seven-scenario evidence](change-plan-drift-2026-11-12.json) uses a fresh
30,000-row historical TLC sample publication. Before the fix, four regression
tests showed that value edits in raw input, the incremental classifier and
published facts, plus a schema-only change, passed validation. The version 2
guard rejects all four and a dropped serving table, while accepting an unchanged
warehouse and a row-order-only rewrite. Every validation leaves database bytes
unchanged. Mutations are controlled exercises, not observed TLC incidents.

```bash
python scripts/change_plan_drift_exercise.py --output measurements/local-plan-drift.json
```

The dated record includes input and implementation hashes and each injected SQL
change. It does not establish a full-data planning runtime or cloud behavior.

## What changes downstream?

This map follows the repository's SQL definitions. It is deliberately small;
the plan conservatively rebuilds all consumer tables instead of pretending to
provide column-level SQL lineage inference.

| Source or rule | Definition affected | Consumer consequence |
|---|---|---|
| Pickup/drop-off timestamps | Identity, duration, date/hour and classification | Valid/quarantine membership, fact durations, daily and hourly marts |
| Fare, total amount or distance | Classification and fact measures | Financial/distance aggregates and rejection counts |
| Pickup/drop-off location IDs or zone seed | Fact zone joins | Borough grouping and zone/hour counts |
| `max_trip_miles` or another classifier threshold | `stg_trips_classified` and valid/quarantine split | Every loaded month reclassified; fact and marts rebuilt |

Graph: `raw.trips` → `stg_trips_classified` → `stg_trips` /
`trips_quarantine` → `fct_trips` and the daily, hourly and quality marts.
The zone lookup seed also feeds the fact joins. All eight managed consumer
tables cross the same publication transaction.

## Recovery boundary

On a failed candidate, consumers retain their last good `main` publication.
Raw and `build` may contain the failed candidate for diagnosis. Correct the
source/rule, produce a fresh plan and retry. Audit metadata retains successful
publication history; the DuckDB pipeline does not retain old table versions for
arbitrary time travel. Reverting a previously accepted rule requires rebuilding
and republishing its valid data. The optional [Iceberg comparison](ICEBERG.md)
demonstrates single-table snapshot rollback separately, without claiming an
eight-table Iceberg transaction.

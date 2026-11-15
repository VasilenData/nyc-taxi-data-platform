# Explore and query the historical sample

Open the [static explorer](https://vasilendata.github.io/nyc-taxi-data-platform/explorer/) to select pickup dates and a borough, compare two periods, and download the current selection. It runs entirely in your browser against the checked-in analyst release. No account, backend, AWS query or credentials are involved.

The data is **30,000 historical public TLC source rows**, exactly two bundled 15,000-row extracts for January and February 2023. The release contains **29,326 valid trips**, **674 quarantined rows**, and **$798,964.25 recorded total amount**. It has observations on all 59 calendar dates, in 265 borough/date groups, but includes only a small subset of monthly trips and has borough/day gaps. It is not representative of citywide activity. These are sample counts, separate from the immutable 31.6-million-row correctness receipt.

## Keep a queryable local result

Use Python **3.11 or newer**; Python 3.11 is configured in CI, and the delivery was executed on Python 3.12. The benchmark uses `hashlib.file_digest`, introduced in 3.11, so the project metadata and instructions now agree. Install the normal requirements once; installation may access PyPI. On Windows, a short clone path such as `C:\src\nyc-taxi` avoids dependency installer path-length limits.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m taxi_platform.sample
python -m taxi_platform.query boroughs
python -m taxi_platform.query quality
python -m taxi_platform.sample
```

In PowerShell, activate with `.venv\Scripts\Activate.ps1` instead of `source`. Run commands from the repository root. The sample starter ignores inherited `TAXI_*` overrides and selects only the two bundled files, with exact SHA-256 checks before and after execution. It never reaches the loader's download fallback. Computation works offline after dependencies are installed.

The first command stages the ordinary Dagster/dbt pipeline, runs its checks, independently reconciles the analyst projection, and publishes a complete `data/sample-workspace/` directory only after success. It retains:

- `warehouse.duckdb`: raw, build and published `main` tables; read `main` for consumer queries.
- `analyst/`: verified CSV, JSON, Parquet, quality/coverage, dictionary and manifest.
- `sample-receipt.json`: source/model/release identity, database checksum and reconciliation.
- `pipeline.log`: local execution transcript.

**Repeating the same command verifies and reuses that complete snapshot**, preserving its publication UUID and avoiding duplicate loads. A changed warehouse, source or model is refused; choose a new `--output data/sample-workspace-v2` to rebuild. Supply the same directory to `query --workspace`. A fresh build has equivalent aggregate values but a new publication UUID, timestamps and database bytes. No byte-identical-build claim is made. A failed first build never exposes a partial destination; an existing unrelated directory is never overwritten. Use one writer at a time. The final rename is a local filesystem visibility boundary, not a promise of power-loss durability.

The earlier `scripts/benchmark.py --dataset sample` remains a timing/evidence tool: it deliberately removes its temporary warehouse. Use the starter above when you want to keep querying the result.

## Two useful SQL recipes

[`boroughs.sql`](sql/boroughs.sql) answers which pickup boroughs have observed valid trips and the recorded total amount in the sample. [`quality.sql`](sql/quality.sql) shows source-month input, valid and quarantine counts. Both run read-only and print CSV; redirect stdout to keep a result:

```bash
python -m taxi_platform.query boroughs > borough-summary.csv
python -m taxi_platform.query quality > quality-summary.csv
```

For a custom query, use a DuckDB connection with `read_only=True` to `data/sample-workspace/warehouse.duckdb`. Date/borough analysis uses `main.mart_daily_borough`; the existing weekday/hour mart has no date or borough dimensions and cannot implement those filters.

## Analyst package contract

The exporter reuses `taxi_platform.release_bundle.export_release` and `verify_release` to obtain one consistent snapshot of all published consumer relations, then projects a bounded daily-borough package. It checks the exact two audited source fingerprints before invoking the general backup exporter; full-data publications are refused before large export work. A publication change during export is refused. This package is a consumer projection, not a replacement for the existing backup/restore feature.

```bash
python -m taxi_platform.analyst export data/analyst-release-2 --database data/sample-workspace/warehouse.duckdb
python -m taxi_platform.analyst verify data/analyst-release-2
```

New destinations only. The exporter verifies the existing mart contract, compares daily-borough keys/counts/amounts against an independent fact SQL aggregation, reconciles quality to classified/quarantine rows, checks the Parquet round trip, and hashes the complete staged package before renaming it into place. Package manifests retain the source SHA-256 values, model signature and model-file hashes, publication UUID, exporter source hash, runtime versions and artifact hashes. Checksums detect corruption, not authenticity; the repository commit provides the publication context.

| Field | Meaning |
|---|---|
| `pickup_date` | ISO date derived from the source's naive NYC local civil timestamp; no UTC conversion |
| `pickup_borough` | Pickup zone lookup borough, null coalesced to `Unknown`; `EWR` and `Unknown` are retained |
| `trips` | Valid fact rows after within-source-month trip-identity deduplication; integer count |
| `total_amount_cents` | Exact integer sum of recorded `total_amount` in USD cents; includes recorded fare/taxes/tolls/tips/components, not net revenue or driver earnings |

Only cent-representable amounts are accepted by this release projection; the source mart remains unchanged and stores `sum(total_amount)` as `revenue` in floating point. Its existing floating tolerance is checked first; rounded mart cents must then equal the separate sum of individual fact cents exactly. The browser sums safe integers and formats dollars only for display. The package is limited to 10,000 groups, two fixed source files and safe integer bounds.

Missing groups mean **no observed valid sample rows**, not measured zero activity. Comparison percentages are unavailable when either selection has no observations or the comparison denominator is zero. Dates are inclusive, periods can overlap, and totals are not normalized by the number of calendar days; the UI shows each period's length and observed-day count. Current-period CSV includes the release ID, applied dates/borough, daily-borough rows, exact cents and decimal USD. JSON includes both periods and identities. Neither silently exports the unfiltered release.

The complete [metric dictionary](explorer/data/metric-dictionary.json), [quality and coverage report](explorer/data/quality-coverage.json), and [release manifest](explorer/data/manifest.json) describe the exclusions and observation window. Whole-release quarantine counts remain visible and are explicitly independent of explorer filters. Trip-level records and local execution paths are not part of the public analyst package.

## Serve and verify the explorer

```bash
python -m http.server 8000 --directory docs
# Open http://localhost:8000/explorer/
node --test tests/explorer.test.mjs
python -m pytest -q tests/test_analyst.py
python -m ruff check .
```

Use HTTP locally or HTTPS hosting; direct `file://` opening cannot fetch the data. The browser checks the JSON checksum, snapshot/source identity, row grain and totals before displaying results. Invalid filters retain the prior visible result. Share links include both periods, borough and release UUID; a stale-release link is identified rather than silently treated as the same snapshot. All controls have labels and keyboard focus; tables scroll on small screens.

CI runs the starter twice, both recipes, the complete Python suite and the Node model/export tests. The Python tests compare every published aggregate with separately summed fact SQL, exercise input/source corruption, simulate an interrupted manifest write, and assert prior results survive. JS tests use hand-computed cents and counts for filters, URL validation, inclusive dates, no-data/zero distinctions and exports. Browser interaction and visual checks are a separate delivery check; these unit tests do not claim to establish browser usability on their own.

No live service, user adoption, timed user study, new full-data benchmark or AWS execution is claimed. The ten-minute first-use target is a design target, not an observed result. See the [delivery prompt](USABILITY_DELIVERY_PROMPT.md) for the task's acceptance criteria.

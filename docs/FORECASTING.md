# Observed pickup forecasting and capacity replay

This is a reproducible **one-day-ahead historical replay**, added to the existing
data platform. It predicts recorded yellow-taxi pickups in each of 263 named TLC
zones, not unmet passenger demand, available drivers or trip duration.

[Inspect the measured report](forecast/index.html) ·
[Every test prediction](forecast/predictions.csv) ·
[Source and evaluation evidence](forecast/report.json)

## Contract and source coverage

- Source: six complete official NYC TLC yellow-taxi Parquet files, January–June
  2023, totaling **19,493,620 source records**. Every file's URL, byte size and
  SHA-256 are recorded. Raw files are not committed.
- Grain: one named pickup zone (`PULocationID` 1–263) per NYC local calendar day.
  Unknown codes 264/265, missing pickup times and records outside the declared
  file month are excluded with disjoint counts. No fare or duration filters are
  applied to this count target; this is intentionally a different population
  from the platform's validated-trip facts.
- Every source calendar day must have at least one accepted record somewhere
  in the city. Missing zone/day combinations can then be filled with zero. A
  missing monthly file or citywide uncovered day fails; it is not interpreted
  as zero demand. These checks do **not** prove complete vendor submissions.
- Source records are counted without deduplication: the source has no assumed
  unique trip key. Accepted records exactly reconcile to aggregate panel counts.
- Timestamps are TLC local wall time. Daily buckets retain the 23/25-hour days
  around daylight-saving transitions; no unsupported UTC conversion is made.
- The committed aggregate panel is checksummed against its provenance and is
  sufficient to rerun modeling cheaply. Rebuild it from the raw files to audit
  ingestion independently.

## Timing and leakage controls

The forecast for day `d` uses only counts dated **before `d`**: lags 1, 7, 14 and
28; trailing 7/28-day means shifted by one day; and known calendar features.
The first 28 days supply lag history and are not training labels. Zone IDs are
not treated as ordered numerical features: historical counts supply each zone's
scale. No fare, finished-trip distance, duration or future weather is used.

| Interval | Purpose |
|---|---|
| January–February | Initial fit of a fixed histogram-gradient-boosted Poisson count model |
| March | Choose seasonal-naive versus learned model by MAE, retaining baseline on ties |
| January–March | Refit the same fixed learned model after selection |
| April | Calibrate an absolute-residual interval separately for each zone |
| May–June | Final reporting only; one next-day origin per day, no parameter refitting |

The seasonal-naive method predicts the same zone's observed count seven days
earlier. The candidate uses 120 boosting iterations, 15 maximum leaves,
learning rate 0.08, 30 minimum leaf samples, L2 penalty 1 and seed 42. There is
no hyperparameter sweep or early stopping on the final test months.

Features are refreshed from preceding observed days during the replay. That is
valid for a next-day system **only if yesterday's counts are available**. TLC
publishes monthly files with a delay, so these public files cannot supply this
workflow live. This is not an online forecast, deployed dispatch service or
claim of real-time data access.

## Measured results (11 November 2026 run)

March model selection was close: learned MAE 33.319 versus seasonal-naive 33.380.
The predefined rule selected the learned method; no claim of a statistically
established March difference is made.

| Final May–June replay, 16,043 zone-days / 6,746,955 accepted pickups | Seasonal naive | Poisson boosting |
|---|---:|---:|
| MAE, pickups per zone-day | 45.35 | **40.23** |
| RMSE | 158.94 | **140.25** |
| WAPE | 10.78% | **9.57%** |
| Nominal 90% interval: empirical coverage | 91.14% | **88.20%** |
| Mean interval width, pickups | 177.70 | 144.87 |

Observed MAE was **11.3% lower** than the weekly baseline on this holdout. This
is a period-specific result, not a guarantee of future improvement. The learned
interval **missed its nominal coverage target**; tighter intervals are not a
free improvement. Errors by month and every zone are retained in the report.
Temporal dependence and distribution shift invalidate a blanket exchangeable
conformal-coverage guarantee. Six months do not validate annual seasonality.

## A bounded planning scenario

A fixed daily pool equals 95% of mean accepted daily pickups in the last 28
pre-calibration days. Each method distributes this pool in proportion to its
predictions, using largest remainders so nonnegative integer allocations sum
exactly to the budget. Equal fractional shares use zone order to break ties.

The pool is **pickup slots, not vehicles**. An uncovered observed pickup costs
1 illustrative unit and idle capacity costs 0.25. The learned allocation's
held-out scenario cost is 755,779.75 units versus 771,428.50 for the baseline.
This is not money, measured savings, causal impact, a dispatch policy or an
estimate of customer demand that was never served. Demand may itself change
with supply; this static replay cannot identify that response.

## Reproduce

```bash
pip install -r requirements-forecast.txt
# Fast model/evaluation rebuild from the full-source aggregate, not a trip sample:
python -m taxi_forecast --panel data/forecast/daily_pickups_2023_h1.csv --output /tmp/taxi-forecast-reproduction

# Independently download and aggregate all six complete Parquet files:
python -m taxi_forecast --download --raw-dir data/forecast_raw --save-panel /tmp/pickups.csv --output /tmp/taxi-forecast-full-source

pytest tests/test_forecast.py -q
```

Output directories must be new. A complete CSV, JSON report, chart and HTML
report are staged, checked and renamed together; failed generation leaves
previous output directories unchanged. The manifest includes artifact hashes.
The evidence also records package versions, source hashes, split identities,
and the SHA-256 of the forecast module bytes, including any uncommitted source.

The manual **Forecast replay** GitHub workflow repeats the aggregate-based run
on an explicitly chosen code revision and retains the result as an artifact.
It does not automatically replace the published portfolio or refresh raw data.

## Verification and limitations

Regression tests use independent hand-computed examples for aggregation,
exclusions, zero-day semantics, count reconciliation, lag cutoffs, train/test
isolation, metrics and integer allocation. They prove future labels cannot
select the winner or influence their own predictions, detect a corrupted panel,
and inject publication failure. Synthetic weekly signals used in tests are
labelled fixtures; the table above comes from the six real full source files.

The historical replay is single-city and single-period. It has no live traffic,
actual staffing data, independently observed unmet demand or measured business
outcome. Model parameters are frozen for the held-out replay, while lagged
features update daily; this is not a 61-day-ahead forecast from one origin.

Design references: [TLC source and publication caveats](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
[scikit-learn lagged-feature forecasting](https://scikit-learn.org/stable/auto_examples/applications/plot_time_series_lagged_features.html),
and [GitHub's testing instruction example](https://docs.github.com/en/copilot/tutorials/customization-library/custom-instructions/testing-automation).
Implementation and review were AI-assisted; no human peer review is claimed.

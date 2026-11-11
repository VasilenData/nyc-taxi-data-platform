"""Independent fixtures for timing, source completeness and decision boundaries."""
import json

import duckdb
import numpy as np
import pandas as pd
import pytest

from taxi_forecast.data import aggregate_sources, complete_panel, months_between, read_panel_cache, sha256, write_panel_cache
from taxi_forecast.model import FEATURES, Split, allocate_capacity, evaluate, features, interval_radius, metric_values
from taxi_forecast.report import publish, render_html


def regular_panel():
    days = pd.date_range("2023-01-01", "2023-06-30")
    return pd.DataFrame([{"zone": zone, "date": day, "pickups": 20 * zone + 3 * day.dayofweek}
                         for zone in [1, 2, 3] for day in days])


def test_zero_zone_days_are_filled_but_missing_source_days_fail():
    counts = pd.DataFrame({"date": ["2023-01-01", "2023-01-02"], "zone": [1, 2], "pickups": [4, 7]})
    panel = complete_panel(counts, "2023-01-01", "2023-01-03", [1, 2])
    assert panel.pickups.tolist() == [4, 0, 0, 7]
    assert panel.pickups.sum() == 11
    with pytest.raises(ValueError, match="uncovered days"):
        complete_panel(counts.iloc[:1], "2023-01-01", "2023-01-03", [1, 2])
    with pytest.raises(ValueError, match="duplicate"):
        complete_panel(pd.concat([counts, counts]), "2023-01-01", "2023-01-03", [1, 2])


@pytest.mark.parametrize("start,end", [("2023-1", "2023-02"), ("2023-13", "2023-14"), ("2023-02", "2023-01")])
def test_month_contract_rejects_ambiguous_or_reversed_ranges(start, end):
    with pytest.raises(ValueError):
        months_between(start, end)


def test_full_source_fixture_reconciles_disjoint_exclusions_and_hashes(tmp_path):
    rows = pd.DataFrame({"tpep_pickup_datetime": list(pd.date_range("2023-01-01", "2023-01-31")) +
                         [pd.Timestamp("2022-12-31"), pd.Timestamp("2023-01-03"), pd.NaT],
                         "PULocationID": [1] * 31 + [1, 264, 1]})
    path = tmp_path / "yellow_tripdata_2023-01.parquet"
    with duckdb.connect() as con:
        con.register("rows", rows)
        con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    panel, provenance = aggregate_sources(tmp_path, ["2023-01"])
    assert len(panel) == 263 * 31
    assert panel.pickups.sum() == 31
    assert provenance["raw_rows"] == 34
    assert provenance["sources"][0]["disposition"] == {
        "accepted": 31, "outside_source_month": 1, "unnamed_zone": 1, "missing_pickup_time": 1}
    assert provenance["sources"][0]["sha256"] == sha256(path)
    cache = tmp_path / "panel.csv"
    write_panel_cache(panel, provenance, cache)
    restored, _ = read_panel_cache(cache)
    pd.testing.assert_frame_equal(restored, panel, check_dtype=False)
    cache.write_text(cache.read_text().replace(",1\n", ",999\n", 1))
    with pytest.raises(ValueError, match="checksum"):
        read_panel_cache(cache)


def test_contiguous_months_and_missing_files_are_required(tmp_path):
    with pytest.raises(ValueError, match="contiguously"):
        aggregate_sources(tmp_path, ["2023-01", "2023-03"])
    with pytest.raises(FileNotFoundError):
        aggregate_sources(tmp_path, ["2023-01"])


def test_features_use_strictly_past_counts_and_do_not_cross_zones():
    panel = regular_panel()
    result = features(panel)
    row = result[(result.zone == 2) & (result.date == "2023-02-01")].iloc[0]
    prior = panel[(panel.zone == 2) & (panel.date < "2023-02-01")]
    assert row.lag_1 == prior.iloc[-1].pickups
    assert row.lag_7 == prior.iloc[-7].pickups
    assert row.mean_7 == prior.tail(7).pickups.mean()
    assert row.feature_cutoff == pd.Timestamp("2023-01-31")
    changed = panel.copy()
    changed.loc[changed.date >= "2023-02-01", "pickups"] += 100000
    prefix = features(changed)
    pd.testing.assert_frame_equal(result.loc[result.date <= "2023-02-01", FEATURES],
                                  prefix.loc[prefix.date <= "2023-02-01", FEATURES])
    assert (result.feature_cutoff < result.date).all()


def test_missing_panel_dates_and_duplicate_keys_are_not_silently_shifted():
    panel = regular_panel()
    with pytest.raises(ValueError, match="contiguous"):
        features(panel.drop(index=10))
    with pytest.raises(ValueError, match="duplicate"):
        features(pd.concat([panel, panel.iloc[:1]]))


def test_metrics_match_hand_computation_and_zero_total_is_undefined():
    score = metric_values(np.array([0, 10, 20]), np.array([2, 8, 22]))
    assert score["mae"] == 2
    assert score["rmse"] == 2
    assert score["wape"] == 0.2
    assert score["bias"] == pytest.approx(2 / 3)
    assert metric_values(np.array([0, 0]), np.array([1, 0]))["wape"] is None
    with pytest.raises(ValueError):
        metric_values(np.array([1]), np.array([np.nan]))


def test_calibration_uses_order_statistic_and_requires_enough_observations():
    assert interval_radius(np.arange(1, 10), 0.8) == 8
    assert interval_radius(np.arange(1, 10), 0.9) == 9
    with pytest.raises(ValueError, match="not enough"):
        interval_radius(np.arange(3), 0.99)


def test_capacity_is_nonnegative_integer_and_exactly_conserved():
    assert allocate_capacity(np.array([1, 2, 3]), 12).tolist() == [2, 4, 6]
    assert allocate_capacity(np.zeros(3), 5).tolist() == [2, 2, 1]
    rng = np.random.default_rng(7)
    for capacity in [0, 1, 19, 10003]:
        allocated = allocate_capacity(rng.uniform(0, 100, 263), capacity)
        assert allocated.sum() == capacity
        assert (allocated >= 0).all()
    with pytest.raises(ValueError):
        allocate_capacity(np.array([1, -1]), 10)


def test_split_order_and_coverage_are_enforced():
    data = features(regular_panel())
    with pytest.raises(ValueError, match="ordered"):
        Split(calibration_start="2023-02-01").validate(data)
    with pytest.raises(ValueError, match="coverage"):
        Split(test_end="2023-08-01").validate(data)


def test_final_test_labels_do_not_choose_the_winner_or_predict_themselves():
    panel = regular_panel()
    predictions, report = evaluate(panel, Split())
    assert report["selected_model"] == "seasonal_naive"
    assert report["test"]["seasonal_naive"]["mae"] == 0
    assert report["test"]["seasonal_naive"]["interval_coverage"] == 1
    changed = panel.copy()
    changed.loc[changed.date >= "2023-05-01", "pickups"] *= 10
    alternative, changed_report = evaluate(changed, Split())
    assert changed_report["selection"] == report["selection"]
    assert changed_report["selected_model"] == report["selected_model"]
    original_first = predictions[predictions.date == "2023-05-01"].prediction.to_numpy()
    changed_first = alternative[alternative.date == "2023-05-01"].prediction.to_numpy()
    np.testing.assert_array_equal(original_first, changed_first)
    assert (predictions.groupby(["model", "date"]).capacity.sum() ==
            report["capacity_scenario"]["daily_pickup_slots"]).all()


def test_failed_publication_preserves_previous_release_and_leaves_no_partial(tmp_path, monkeypatch):
    predictions, evaluation = evaluate(regular_panel(), Split())
    old = tmp_path / "previous"
    old.mkdir()
    (old / "report.json").write_bytes(b"last good report")
    from taxi_forecast import report as publisher
    monkeypatch.setattr(publisher, "make_chart", lambda *_: (_ for _ in ()).throw(OSError("disk failure")))
    with pytest.raises(OSError, match="disk failure"):
        publish(predictions, {"evaluation": evaluation}, tmp_path / "candidate")
    assert (old / "report.json").read_bytes() == b"last good report"
    assert not (tmp_path / "candidate").exists()
    assert list(tmp_path.glob(".forecast-*")) == []
    with pytest.raises(FileExistsError):
        publish(predictions, {}, old)


def test_published_artifacts_reconcile_to_manifest(tmp_path, monkeypatch):
    predictions, evaluation = evaluate(regular_panel(), Split())
    from taxi_forecast import report as publisher
    monkeypatch.setattr(publisher, "make_chart", lambda _p, path: path.write_bytes(b"test chart"))
    report = {"evaluation": evaluation, "source": {"raw_rows": 543, "sources": []}, "generated_utc": "test-fixture"}
    output = tmp_path / "release"
    publish(predictions, report, output)
    manifest = json.loads((output / "manifest.json").read_text())
    assert set(manifest["files"]) == {"forecast.png", "index.html", "predictions.csv", "report.json"}
    for name, metadata in manifest["files"].items():
        assert metadata["sha256"] == sha256(output / name)
    saved = pd.read_csv(output / "predictions.csv")
    assert saved.pickups.sum() == predictions.pickups.sum()
    assert len(saved) == len(predictions)


def test_zero_observed_total_remains_explicit_in_the_report():
    _, evaluation = evaluate(regular_panel(), Split())
    evaluation["test"]["seasonal_naive"]["wape"] = None
    evaluation["test_by_month"][0]["wape"] = None
    html = render_html({"evaluation": evaluation, "source": {"raw_rows": 543, "sources": []},
                        "generated_utc": "test-fixture"})
    assert html.count("n/a (zero observed total)") == 2

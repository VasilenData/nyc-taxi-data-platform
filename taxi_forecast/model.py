"""A fixed, modest forecast and explicit temporal evaluation boundaries."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

FEATURES = ["lag_1", "lag_7", "lag_14", "lag_28", "mean_7", "mean_28",
            "weekday_sin", "weekday_cos", "month_sin", "month_cos", "weekend"]


@dataclass(frozen=True)
class Split:
    selection_start: str = "2023-03-01"
    calibration_start: str = "2023-04-01"
    test_start: str = "2023-05-01"
    test_end: str = "2023-07-01"

    def validate(self, data: pd.DataFrame) -> None:
        dates = pd.to_datetime([self.selection_start, self.calibration_start, self.test_start, self.test_end])
        if any(dates[i] >= dates[i + 1] for i in range(3)):
            raise ValueError("train, selection, calibration and test intervals must be ordered and disjoint")
        for left, right in [(data.date.min(), dates[0]), (dates[0], dates[1]),
                            (dates[1], dates[2]), (dates[2], dates[3])]:
            if not ((data.date >= left) & (data.date < right)).any():
                raise ValueError("every declared evaluation interval must contain feature rows")
        if data.date.max() < dates[3] - pd.Timedelta(days=1):
            raise ValueError("source coverage ends before the declared test interval")


def features(panel: pd.DataFrame) -> pd.DataFrame:
    data = panel.copy().sort_values(["zone", "date"]).reset_index(drop=True)
    data["date"] = pd.to_datetime(data["date"])
    if data.duplicated(["zone", "date"]).any():
        raise ValueError("duplicate zone/day panel keys")
    if not np.isfinite(data.pickups).all() or (data.pickups < 0).any():
        raise ValueError("nonnegative finite pickup counts required")
    differences = data.groupby("zone")["date"].diff().dropna()
    if not (differences == pd.Timedelta(days=1)).all():
        raise ValueError("contiguous daily panel required before constructing lags")
    grouped = data.groupby("zone")["pickups"]
    for lag in [1, 7, 14, 28]:
        data[f"lag_{lag}"] = grouped.shift(lag)
    for window in [7, 28]:
        data[f"mean_{window}"] = grouped.transform(lambda series: series.shift(1).rolling(window).mean())
    weekday, month = data.date.dt.dayofweek, data.date.dt.month
    data["weekday_sin"], data["weekday_cos"] = np.sin(2 * np.pi * weekday / 7), np.cos(2 * np.pi * weekday / 7)
    data["month_sin"], data["month_cos"] = np.sin(2 * np.pi * (month - 1) / 12), np.cos(2 * np.pi * (month - 1) / 12)
    data["weekend"] = (weekday >= 5).astype(int)
    data["feature_cutoff"] = data.date - pd.Timedelta(days=1)
    return data.dropna(subset=FEATURES).reset_index(drop=True)


def fit_model(data: pd.DataFrame) -> HistGradientBoostingRegressor:
    model = HistGradientBoostingRegressor(loss="poisson", learning_rate=0.08, max_iter=120,
                                         max_leaf_nodes=15, min_samples_leaf=30,
                                         l2_regularization=1.0, early_stopping=False, random_state=42)
    if data.empty or data.pickups.sum() <= 0:
        raise ValueError("training needs nonempty rows and some positive counts")
    model.fit(data[FEATURES], data.pickups)
    return model


def metric_values(actual: np.ndarray, prediction: np.ndarray) -> dict:
    actual, prediction = np.asarray(actual, dtype=float), np.asarray(prediction, dtype=float)
    if actual.shape != prediction.shape or actual.ndim != 1 or len(actual) == 0:
        raise ValueError("aligned nonempty vectors required")
    if not np.isfinite(actual).all() or not np.isfinite(prediction).all() or (actual < 0).any() or (prediction < 0).any():
        raise ValueError("nonnegative finite observations and predictions required")
    error = prediction - actual
    return {"rows": len(actual), "actual_pickups": float(actual.sum()),
            "mae": float(np.mean(np.abs(error))), "rmse": float(np.sqrt(np.mean(error ** 2))),
            "wape": float(np.abs(error).sum() / actual.sum()) if actual.sum() else None,
            "bias": float(error.mean())}


def interval_radius(errors: np.ndarray, coverage: float = 0.9) -> float:
    errors = np.asarray(errors, dtype=float)
    if errors.ndim != 1 or len(errors) == 0 or not np.isfinite(errors).all() or (errors < 0).any():
        raise ValueError("nonempty finite absolute calibration errors required")
    if not 0 < coverage < 1:
        raise ValueError("coverage must be between zero and one")
    rank = int(np.ceil((len(errors) + 1) * coverage))
    if rank > len(errors):
        raise ValueError("not enough calibration observations for requested coverage")
    return float(np.sort(errors)[rank - 1])


def allocate_capacity(prediction: np.ndarray, capacity: int) -> np.ndarray:
    prediction = np.asarray(prediction, dtype=float)
    if prediction.ndim != 1 or len(prediction) == 0 or not np.isfinite(prediction).all() or (prediction < 0).any():
        raise ValueError("finite nonnegative predictions required")
    if not isinstance(capacity, (int, np.integer)) or capacity < 0:
        raise ValueError("capacity must be a nonnegative integer")
    if prediction.sum() == 0:
        weights = np.full(len(prediction), 1 / len(prediction))
    else:
        weights = prediction / prediction.sum()
    shares = weights * capacity
    allocated = np.floor(shares).astype(int)
    # Stable row/zone ordering defines ties; the capacity total is exact.
    order = np.argsort(-(shares - allocated), kind="stable")
    allocated[order[:capacity - int(allocated.sum())]] += 1
    return allocated


def evaluate(panel: pd.DataFrame, split: Split, coverage: float = 0.9) -> tuple[pd.DataFrame, dict]:
    data = features(panel)
    split.validate(data)
    train = data[data.date < split.selection_start]
    selection = data[(data.date >= split.selection_start) & (data.date < split.calibration_start)]
    preliminary = fit_model(train)
    selection_predictions = {"seasonal_naive": selection.lag_7.to_numpy(),
                             "poisson_boosting": preliminary.predict(selection[FEATURES])}
    selection_metrics = {key: metric_values(selection.pickups.to_numpy(), value)
                         for key, value in selection_predictions.items()}
    # Ties retain the simpler baseline. Test labels never enter model selection.
    winner = ("poisson_boosting" if selection_metrics["poisson_boosting"]["mae"] <
              selection_metrics["seasonal_naive"]["mae"] else "seasonal_naive")
    fitted = fit_model(data[data.date < split.calibration_start])
    calibration = data[(data.date >= split.calibration_start) & (data.date < split.test_start)].copy()
    test = data[(data.date >= split.test_start) & (data.date < split.test_end)].copy()
    cal_predictions = {"seasonal_naive": calibration.lag_7.to_numpy(),
                       "poisson_boosting": fitted.predict(calibration[FEATURES])}
    test_predictions = {"seasonal_naive": test.lag_7.to_numpy(), "poisson_boosting": fitted.predict(test[FEATURES])}
    records, scores = [], {}
    daily_training = panel[panel.date < pd.Timestamp(split.calibration_start)].groupby("date").pickups.sum()
    capacity = int(round(float(daily_training.tail(28).mean()) * 0.95))
    for name, prediction in test_predictions.items():
        cal = calibration[["zone"]].copy()
        cal["error"] = np.abs(calibration.pickups.to_numpy() - cal_predictions[name])
        radii = cal.groupby("zone").error.apply(lambda values: interval_radius(values.to_numpy(), coverage))
        result = test[["date", "zone", "pickups", "feature_cutoff"]].copy()
        result["model"], result["prediction"] = name, prediction
        radius = result.zone.map(radii).to_numpy()
        result["lower"] = np.maximum(0, prediction - radius)
        result["upper"] = prediction + radius
        result["covered"] = (result.pickups >= result.lower) & (result.pickups <= result.upper)
        result["capacity"] = 0
        for _, day in result.groupby("date"):
            result.loc[day.index, "capacity"] = allocate_capacity(day.prediction.to_numpy(), capacity)
        result["uncovered_pickups_proxy"] = np.maximum(0, result.pickups - result.capacity)
        result["idle_capacity_proxy"] = np.maximum(0, result.capacity - result.pickups)
        score = metric_values(result.pickups.to_numpy(), prediction)
        score.update({"interval_coverage": float(result.covered.mean()),
                      "mean_interval_width": float((result.upper - result.lower).mean()),
                      "uncovered_pickups_proxy": int(result.uncovered_pickups_proxy.sum()),
                      "idle_capacity_proxy": int(result.idle_capacity_proxy.sum()),
                      "illustrative_cost_units": float(result.uncovered_pickups_proxy.sum() +
                                                       0.25 * result.idle_capacity_proxy.sum())})
        scores[name] = score
        records.append(result)
    output = pd.concat(records, ignore_index=True)
    monthly = []
    for (name, month), group in output.groupby(["model", output.date.dt.strftime("%Y-%m")]):
        monthly.append({"model": name, "month": month, **metric_values(group.pickups.to_numpy(), group.prediction.to_numpy()),
                        "interval_coverage": float(group.covered.mean())})
    zones = []
    for (name, zone), group in output.groupby(["model", "zone"]):
        zones.append({"model": name, "zone": int(zone), **metric_values(group.pickups.to_numpy(), group.prediction.to_numpy())})
    return output, {"selection": selection_metrics, "selected_model": winner, "test": scores,
                    "test_by_month": monthly, "test_by_zone": zones, "nominal_interval_coverage": coverage,
                    "capacity_scenario": {"daily_pickup_slots": capacity, "capacity_fraction": 0.95,
                                          "basis": "mean daily accepted pickups in final 28 pre-calibration days",
                                          "uncovered_unit_cost": 1.0, "idle_unit_cost": 0.25,
                                          "status": "illustrative allocation simulation; not vehicles, dispatch or measured savings"},
                    "split": vars(split), "feature_columns": FEATURES,
                    "training_rows": len(train), "selection_rows": len(selection),
                    "refit_rows": int((data.date < split.calibration_start).sum()), "calibration_rows": len(calibration),
                    "rolling_origin": "one-day-ahead replay; features updated daily, parameters frozen before calibration",
                    "availability_assumption": "previous-day pickup totals assumed available; monthly public TLC publication cannot supply this live",
                    "interval_limit": "per-zone absolute residual quantiles; temporal dependence/shift means no guaranteed future coverage",
                    "selection_rule": "lowest selection-month MAE, baseline on ties; no final-test tuning"}

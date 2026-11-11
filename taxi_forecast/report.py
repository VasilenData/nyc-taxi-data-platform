"""Publish one immutable, checksummed report directory after validation."""
from __future__ import annotations

import html
import json
import os
import shutil
import tempfile
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

from .data import sha256

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def make_chart(predictions: pd.DataFrame, path: Path) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(11, 7), constrained_layout=True)
    for model, group in predictions.groupby("model"):
        daily = group.groupby("date")[["pickups", "prediction"]].sum()
        axes[0].plot(daily.index, daily.prediction, label=model.replace("_", " "), linewidth=1.5)
    actual = predictions[predictions.model == "seasonal_naive"].groupby("date").pickups.sum()
    axes[0].plot(actual.index, actual, label="observed pickups", color="#202c31", linewidth=1.2)
    axes[0].set(ylabel="Observed / forecast pickup records", title="Daily total: held-out replay")
    axes[0].legend(ncol=3, fontsize=8)
    worst = predictions.assign(error=lambda d: (d.prediction - d.pickups).abs()).groupby(["zone", "model"]).error.mean().unstack()
    worst = worst.loc[worst.max(axis=1).nlargest(8).index]
    worst.plot.bar(ax=axes[1], color=["#49a58d", "#52647d"])
    axes[1].set(xlabel="TLC pickup zone ID", ylabel="MAE (pickups / zone-day)", title="Largest absolute-error zones — both methods struggle here")
    for ax in axes:
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", alpha=0.15)
    fig.savefig(path, dpi=150)
    plt.close(fig)


def render_html(report: dict) -> str:
    def percentage(value):
        return "n/a (zero observed total)" if value is None else f"{value:.1%}"

    rows = "".join(
        f"<tr><th>{html.escape(name.replace('_', ' '))}</th><td>{score['mae']:.2f}</td>"
        f"<td>{percentage(score['wape'])}</td><td>{score['interval_coverage']:.1%}</td>"
        f"<td>{score['mean_interval_width']:.1f}</td><td>{score['illustrative_cost_units']:,.0f}</td></tr>"
        for name, score in report["evaluation"]["test"].items()
    )
    monthly = "".join(
        f"<tr><td>{row['month']}</td><td>{row['model'].replace('_', ' ')}</td>"
        f"<td>{row['mae']:.2f}</td><td>{percentage(row['wape'])}</td><td>{row['interval_coverage']:.1%}</td></tr>"
        for row in report["evaluation"]["test_by_month"]
    )
    source = report["source"]
    selected = html.escape(report["evaluation"]["selected_model"].replace("_", " "))
    split = report["evaluation"]["split"]
    return f"""<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NYC pickup forecasting · Vasilen Kanchev</title><style>
:root{{font:17px/1.65 system-ui,sans-serif;color:#dce8e4;background:#111b20}}body{{max-width:1120px;margin:0 auto;padding:44px 24px}}
a{{color:#80dec0}}h1{{font-size:clamp(2rem,5vw,3.5rem);line-height:1.12;max-width:850px}}h2{{margin-top:2.5rem}}.eyebrow{{color:#80dec0;letter-spacing:.12em;text-transform:uppercase;font-size:.8rem}}
.intro{{max-width:850px;color:#adc4ba;font-size:1.1rem}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin:32px 0}}.card{{padding:18px 24px;background:#1d2a31;border:1px solid #34464d;border-radius:12px}}.number{{display:block;font-size:1.8rem;font-weight:650}}
table{{border-collapse:collapse;width:100%;font-size:.92rem}}td,th{{text-align:left;padding:12px;border-bottom:1px solid #34464d}}.table-wrap{{overflow:auto}}img{{width:100%;border-radius:12px;background:white}}.note{{border-left:3px solid #80dec0;padding-left:20px}}footer{{margin-top:60px;color:#adc4ba;font-size:.85rem}}</style>
<p class="eyebrow">Measured forecasting • public data • reproducible replay</p><h1>Where will tomorrow's taxi pickups happen?</h1>
<p class="intro">A next-day forecast of observed yellow-taxi pickup records across 263 named zones. A seasonal baseline competes with a modest boosted count model; held-out errors, intervals and a capacity scenario remain visible.</p>
<div class="cards"><div class="card"><span class="number">{source['raw_rows']:,}</span>source records, {len(source['sources'])} complete monthly files</div><div class="card"><span class="number">263</span>named pickup zones</div><div class="card"><span class="number">{selected}</span>selected before final evaluation</div></div>
<p class="note"><strong>Retrospective replay, not a live dispatch system.</strong> Forecasts assume yesterday's completed pickup totals are available. Public TLC files arrive monthly with a delay, so this source cannot drive the same daily workflow in real time. Recorded pickups are not unmet demand.</p>
<h2>Held-out results</h2><p>Dates: {split['test_start']} through {split['test_end']} (end exclusive). Lower MAE/WAPE is better. Interval target: 90%; empirical coverage may differ. All zones and zero-count days stay in the evaluation.</p>
<div class="table-wrap"><table><thead><tr><th>Method</th><th>MAE</th><th>WAPE</th><th>Coverage</th><th>Mean width</th><th>Scenario cost units</th></tr></thead><tbody>{rows}</tbody></table></div>
<p>The allocation scenario uses a fixed daily pool of pickup slots, not cars. Each uncovered pickup costs 1 hypothetical unit; idle capacity costs 0.25. Costs do not represent money, causal impact or achieved savings.</p>
<img src="forecast.png" alt="Daily observed versus forecast pickup totals and errors in the hardest zones">
<h2>Performance over time</h2><div class="table-wrap"><table><thead><tr><th>Month</th><th>Method</th><th>MAE</th><th>WAPE</th><th>Coverage</th></tr></thead><tbody>{monthly}</tbody></table></div>
<h2>What keeps the comparison honest?</h2><ul><li>Initial fit ends before {split['selection_start']}; model selection ends before {split['calibration_start']}. Refit uses only pre-calibration data; intervals calibrate before {split['test_start']}, when the final holdout begins.</li><li>Every feature uses data dated no later than the prior day. Calendar features are known ahead; no recorded trip distance, fare, duration or future weather enters a forecast.</li><li>Every monthly file is hashed, every source row has a disposition, and accepted counts reconcile to the complete zone/day panel.</li><li>Per-zone residual intervals expose uncertainty, but temporal dependence and distribution shift prevent a coverage guarantee.</li><li>This bounded period supports weekly patterns, not a validated annual seasonality claim. One city and period do not establish generalization.</li></ul>
<h2>Inspect or reproduce</h2><p><a href="predictions.csv">Every held-out prediction (CSV)</a> · <a href="report.json">Metrics, source hashes and splits</a> · <a href="manifest.json">Artifact checksums</a> · <a href="https://github.com/VasilenData/nyc-taxi-data-platform/tree/main/taxi_forecast">Code and tests</a></p>
<footer>Vasilen Kanchev · Public NYC TLC data · Built with AI assistance; results measured by the reproducible pipeline. Generated {html.escape(report['generated_utc'])}.</footer></html>"""


def publish(predictions: pd.DataFrame, report: dict, destination: Path) -> None:
    """Refuse overwrites; rename a completely validated sibling staging directory."""
    if destination.exists():
        raise FileExistsError("use a new output directory; existing evidence is immutable")
    if predictions.empty or not np.isfinite(predictions[["prediction", "lower", "upper"]].to_numpy()).all():
        raise ValueError("cannot publish empty or non-finite forecasts")
    if (predictions.lower > predictions.upper).any() or (predictions.prediction < 0).any():
        raise ValueError("invalid prediction intervals")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".forecast-", dir=destination.parent))
    try:
        predictions.to_csv(staging / "predictions.csv", index=False, lineterminator="\n", float_format="%.9g")
        (staging / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
        make_chart(predictions, staging / "forecast.png")
        (staging / "index.html").write_text(render_html(report), encoding="utf-8")
        manifest = {"schema_version": 1, "files": {path.name: {"bytes": path.stat().st_size, "sha256": sha256(path)}
                    for path in sorted(staging.iterdir()) if path.is_file()}}
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        for name, identity in manifest["files"].items():
            if sha256(staging / name) != identity["sha256"]:
                raise AssertionError("artifact changed during publication")
        os.replace(staging, destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging)

"""Run with python -m taxi_forecast --help."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from .data import aggregate_sources, download_month, months_between, read_panel_cache, write_panel_cache
from .model import Split, evaluate
from .report import publish


def main() -> None:
    parser = argparse.ArgumentParser(description="Next-day pickup forecasting, temporal holdout and capacity replay")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--download", action="store_true", help="download all declared full monthly Parquet files")
    parser.add_argument("--start-month", default="2023-01")
    parser.add_argument("--end-month", default="2023-06")
    parser.add_argument("--panel", type=Path, help="reuse a checksummed aggregate panel and its provenance")
    parser.add_argument("--save-panel", type=Path, help="write aggregate panel for lightweight reproducibility")
    parser.add_argument("--selection-start", default="2023-03-01")
    parser.add_argument("--calibration-start", default="2023-04-01")
    parser.add_argument("--test-start", default="2023-05-01")
    parser.add_argument("--test-end", default="2023-07-01")
    parser.add_argument("--output", type=Path, required=True, help="new directory; previous artifacts are never overwritten")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output must be a new directory")
    if args.panel and (args.download or args.save_panel):
        parser.error("--panel cannot be combined with --download or --save-panel")
    if args.panel:
        panel, provenance = read_panel_cache(args.panel)
    else:
        months = months_between(args.start_month, args.end_month)
        if args.download:
            for month in months:
                print(f"Ensuring full source month {month}", flush=True)
                download_month(month, args.raw_dir)
        panel, provenance = aggregate_sources(args.raw_dir, months)
        if args.save_panel:
            write_panel_cache(panel, provenance, args.save_panel)
    split = Split(args.selection_start, args.calibration_start, args.test_start, args.test_end)
    predictions, evaluation = evaluate(panel, split)
    code = Path(__file__).parent
    source_hash = hashlib.sha256()
    for path in sorted(code.glob("*.py")):
        source_hash.update(path.name.encode())
        source_hash.update(path.read_bytes())
    try:
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = None
    report = {"schema_version": 1, "generated_utc": datetime.now(timezone.utc).isoformat(),
              "source": provenance, "evaluation": evaluation,
              "runtime": {"python": platform.python_version(), "platform": platform.platform(),
                          "packages": {name: importlib.metadata.version(name) for name in
                                       ["numpy", "pandas", "scikit-learn", "duckdb", "matplotlib"]}},
              "code": {"base_git_revision": revision, "taxi_forecast_source_sha256": source_hash.hexdigest(),
                       "note": "module hash includes uncommitted source bytes; base revision alone is not build identity"}}
    publish(predictions, report, args.output)
    print(json.dumps({"output": str(args.output), "source_rows": provenance["raw_rows"],
                      "selection": evaluation["selection"], "selected": evaluation["selected_model"],
                      "test": evaluation["test"]}, indent=2))


if __name__ == "__main__":
    main()

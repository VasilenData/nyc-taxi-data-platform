"""A benchmark must prove what ran and must not silently substitute sample data."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def run_benchmark(*args):
    env = os.environ.copy()
    env["PATH"] = str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", "")
    return subprocess.run([sys.executable, "scripts/benchmark.py", *map(str, args)],
                          cwd=ROOT, env=env, capture_output=True, text=True, timeout=600)


def test_full_benchmark_cannot_fall_back_to_bundled_samples(tmp_path):
    output = tmp_path / "missing.json"
    result = run_benchmark("--dataset", "full", "--data-dir", tmp_path / "empty", "--output", output)
    assert result.returncode != 0
    assert "Missing" in result.stderr
    assert not output.exists()


def test_benchmark_preserves_existing_evidence(tmp_path):
    output = tmp_path / "existing.json"
    previous = '{"previous_run": true}\n'
    output.write_text(previous)
    result = run_benchmark("--output", output)
    assert result.returncode != 0
    assert "already exists" in result.stderr
    assert output.read_text() == previous


def test_benchmark_rejects_output_that_would_overwrite_its_own_log(tmp_path):
    output = tmp_path / "result.log"
    result = run_benchmark("--output", output)
    assert result.returncode != 0
    assert "must have a .json extension" in result.stderr
    assert not output.exists()
    assert not list(tmp_path.iterdir())


def test_sample_benchmark_records_verifiable_pipeline_result(tmp_path):
    output = tmp_path / "sample.json"
    result = run_benchmark("--output", output)
    assert result.returncode == 0, result.stdout + result.stderr
    record = json.loads(output.read_text())
    assert record["success"] and record["exit_code"] == 0
    assert record["input_rows"] == 30000
    assert record["warehouse"]["rows"]["raw"] == 30000
    assert all(record["warehouse"]["reconciliation"].values())
    for name in ("coverage", "reconciliation", "reject_rate"):
        assert record["asset_checks"][name] == "PASS"
    log = output.with_suffix(".log")
    assert hashlib.sha256(log.read_bytes()).hexdigest() == record["log_sha256"]
    assert record["wall_seconds"] > 0
    assert record["conditions"]["source_downloads_in_timing"] is False
    assert record["environment"]["packages"]["duckdb"]
    assert len(record["source"]["source_sha256"]["run_pipeline.py"]) == 64
    assert all(len(source["sha256"]) == 64 for source in record["inputs"])
    assert record["publication"]["tables"] == 8
    assert 0 <= record["publication"]["copy_seconds"] <= record["publication"]["publication_seconds"]
    assert record["resources"]["peak_summed_process_rss_bytes"] > 0
    assert record["resources"]["samples"] > 1
    assert record["resources"]["peak_working_directory_bytes"] >= record["warehouse"]["checkpointed_database_bytes"]
    assert len(record["warehouse"]["publication"]["model_sha256"]) == 64

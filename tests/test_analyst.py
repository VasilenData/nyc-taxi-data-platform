"""Real fresh-user path, immutable failure boundary and independent SQL oracles."""
import csv
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import duckdb
import pytest

from taxi_platform import analyst
from taxi_platform.release_bundle import sha256

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def sample_workspace(tmp_path_factory):
    directory = tmp_path_factory.mktemp("starter") / "sample"
    # The starter must ignore even malformed inherited full-data configuration.
    decoy = tmp_path_factory.mktemp("decoy")
    sentinel = decoy / "warehouse.duckdb"
    sentinel.write_bytes(b"never replace a user warehouse")
    env = {**os.environ, "TAXI_DB": str(sentinel), "TAXI_DATA_DIR": str(decoy),
           "TAXI_DBT_DIR": str(decoy), "TAXI_MAX_REJECT_RATE": "invalid", "TAXI_DUCKDB_MEMORY_LIMIT": "invalid"}
    process = subprocess.run([sys.executable, "-m", "taxi_platform.sample", "--output", str(directory)],
                             cwd=ROOT, env=env, capture_output=True, text=True, timeout=900)
    assert process.returncode == 0, process.stderr
    assert sentinel.read_bytes() == b"never replace a user warehouse"
    return directory


def test_repeat_starter_preserves_one_queryable_snapshot(sample_workspace):
    before = {p.name: sha256(p) for p in sample_workspace.iterdir() if p.is_file()}
    process = subprocess.run([sys.executable, "-m", "taxi_platform.sample", "--output", str(sample_workspace)],
                             cwd=ROOT, capture_output=True, text=True, timeout=90)
    assert process.returncode == 0, process.stderr
    assert '"reused": true' in process.stdout
    assert before == {p.name: sha256(p) for p in sample_workspace.iterdir() if p.is_file()}
    with duckdb.connect(str(sample_workspace / "warehouse.duckdb"), read_only=True) as con:
        assert con.execute("select count(*) from raw.trips").fetchone()[0] == 30000
        assert con.execute("select count(*) from audit.publications").fetchone()[0] == 1


def test_recipe_outputs_are_useful_read_only_csv(sample_workspace):
    for recipe in ("boroughs", "quality"):
        process = subprocess.run([sys.executable, "-m", "taxi_platform.query", recipe, "--workspace", str(sample_workspace)],
                                 cwd=ROOT, capture_output=True, text=True, timeout=30)
        assert process.returncode == 0, process.stderr
        rows = list(csv.DictReader(process.stdout.splitlines()))
        assert rows
        if recipe == "quality":
            assert sum(int(row["input_rows"]) for row in rows) == 30000


def test_all_formats_match_independent_fact_sql(sample_workspace):
    package = sample_workspace / "analyst"
    data = json.loads((package / "data.json").read_text())
    with duckdb.connect(str(sample_workspace / "warehouse.duckdb"), read_only=True) as con:
        # Independent cents oracle sums individual facts, not rounded mart totals.
        expected = con.execute("""select pickup_date::varchar, coalesce(pickup_borough, 'Unknown'),
            count(*), sum(cast(total_amount as decimal(18,2))) * 100
            from main.fct_trips group by 1,2 order by 1,2""").fetchall()
        actual = con.execute("select * from read_parquet(?) order by 1,2", [str(package / "daily-borough.parquet")]).fetchall()
        assert actual == expected
        oracle = con.execute("""select count(*), sum(cast(total_amount as decimal(18,2))) * 100 from main.fct_trips
            where pickup_date between date '2023-01-01' and date '2023-01-31' and pickup_borough = 'Manhattan'""").fetchone()
    rows = [r for r in data["rows"] if "2023-01-01" <= r["pickup_date"] <= "2023-01-31" and r["pickup_borough"] == "Manhattan"]
    assert (sum(r["trips"] for r in rows), sum(r["total_amount_cents"] for r in rows)) == oracle
    with (package / "daily-borough.csv").open(newline="", encoding="utf-8") as stream:
        csv_rows = list(csv.DictReader(stream))
    assert [(r["pickup_date"], r["pickup_borough"], int(r["trips"]), int(r["total_amount_cents"])) for r in csv_rows] == expected
    assert [(r["pickup_date"], r["pickup_borough"], r["trips"], r["total_amount_cents"]) for r in data["rows"]] == expected


def test_exact_source_model_release_identity(sample_workspace):
    manifest = analyst.verify_analyst(sample_workspace / "analyst")
    assert manifest["inputs"] == analyst.check_inputs()
    with duckdb.connect(str(sample_workspace / "warehouse.duckdb"), read_only=True) as con:
        version, model = con.execute("select version, model_sha256 from main.publication").fetchone()
    assert (manifest["release_id"], manifest["model_sha256"]) == (version, model)


def test_missing_and_changed_inputs_identify_offending_file(tmp_path):
    with pytest.raises(ValueError, match="2023-01.*No download"):
        analyst.check_inputs(tmp_path)
    (tmp_path / "yellow_tripdata_2023-01.parquet").write_bytes(b"bad parquet")
    with pytest.raises(ValueError, match="checksum mismatch.*2023-01"):
        analyst.check_inputs(tmp_path)


@pytest.mark.parametrize("damage,match", [
    ("update main.mart_daily_borough set revenue = revenue + 1", "grain/metric mismatch"),
    ("update audit.source_loads set source_sha256 = 'changed'", "bundled.*inputs"),
])
def test_refused_release_preserves_previous_complete_package(sample_workspace, tmp_path, damage, match):
    previous = sample_workspace / "analyst"
    before = {p.name: sha256(p) for p in previous.iterdir()}
    database = tmp_path / "bad.duckdb"
    shutil.copy2(sample_workspace / "warehouse.duckdb", database)
    with duckdb.connect(str(database)) as con:
        con.execute(damage)
    with pytest.raises(ValueError, match=match):
        analyst.export_analyst(database, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()
    assert not list(tmp_path.glob(".analyst-*"))
    assert before == {p.name: sha256(p) for p in previous.iterdir()}


def test_immutable_destination_is_never_overwritten(sample_workspace):
    with pytest.raises(FileExistsError, match="immutable"):
        analyst.export_analyst(sample_workspace / "warehouse.duckdb", sample_workspace / "analyst")


def test_interrupted_export_never_exposes_partial_package(sample_workspace, tmp_path, monkeypatch):
    original = analyst.write_json

    def fail_manifest(path, value):
        if Path(path).name == "manifest.json":
            raise OSError("simulated manifest write interruption")
        original(path, value)

    monkeypatch.setattr(analyst, "write_json", fail_manifest)
    with pytest.raises(OSError, match="interruption"):
        analyst.export_analyst(sample_workspace / "warehouse.duckdb", tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()
    assert not list(tmp_path.glob(".analyst-*"))


def test_corrupt_download_is_detected(sample_workspace, tmp_path):
    package = tmp_path / "bad"
    shutil.copytree(sample_workspace / "analyst", package)
    (package / "daily-borough.csv").write_text("broken")
    with pytest.raises(ValueError, match="checksum mismatch.*csv"):
        analyst.verify_analyst(package)


def test_existing_unrelated_output_and_missing_warehouse_have_actionable_errors(tmp_path):
    result = tmp_path / "existing"
    result.mkdir()
    (result / "important.txt").write_text("keep")
    process = subprocess.run([sys.executable, "-m", "taxi_platform.sample", "--output", str(result)],
                             cwd=ROOT, capture_output=True, text=True, timeout=30)
    assert process.returncode != 0 and "choose a new" in process.stderr
    assert (result / "important.txt").read_text() == "keep"
    process = subprocess.run([sys.executable, "-m", "taxi_platform.query", "quality", "--workspace", str(result)],
                             cwd=ROOT, capture_output=True, text=True, timeout=30)
    assert process.returncode != 0 and "run python -m taxi_platform.sample" in process.stderr


@pytest.mark.parametrize("damage", ["changed_identity", "missing_identity"])
def test_corrupt_starter_receipt_refuses_reuse_without_replacing_outputs(sample_workspace, tmp_path, damage):
    destination = tmp_path / "sample"
    shutil.copytree(sample_workspace, destination)
    path = destination / "sample-receipt.json"
    receipt = json.loads(path.read_text())
    if damage == "changed_identity":
        receipt["release_id"] = "not-the-published-snapshot"
    else:
        del receipt["release_id"]
    path.write_text(json.dumps(receipt))
    before = {p.name: sha256(p) for p in destination.iterdir() if p.is_file()}
    process = subprocess.run([sys.executable, "-m", "taxi_platform.sample", "--output", str(destination)],
                             cwd=ROOT, capture_output=True, text=True, timeout=30)
    assert process.returncode != 0
    assert "identity" in process.stderr or "Incomplete sample receipt" in process.stderr
    assert "Traceback" not in process.stderr
    assert before == {p.name: sha256(p) for p in destination.iterdir() if p.is_file()}


def test_sample_bound_is_checked_before_general_backup_export(sample_workspace, tmp_path, monkeypatch):
    database = tmp_path / "larger.duckdb"
    shutil.copy2(sample_workspace / "warehouse.duckdb", database)
    with duckdb.connect(str(database)) as con:
        counts = json.loads(con.execute("select row_counts from main.publication").fetchone()[0])
        counts["fct_trips"] = 30000000
        con.execute("update main.publication set row_counts = ?", [json.dumps(counts)])

    def never_export(*args):
        raise AssertionError("large backup export must not start")

    monkeypatch.setattr(analyst, "export_release", never_export)
    with pytest.raises(ValueError, match="bounded sample"):
        analyst.export_analyst(database, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()

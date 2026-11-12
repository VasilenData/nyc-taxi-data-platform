"""Review/apply plans must not operate against changed inputs or state."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import duckdb
import pytest

from taxi_platform import config, revision
from taxi_platform.change_plan import create_plan, validate_plan

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def initial(tmp_path_factory):
    work = tmp_path_factory.mktemp("change-plan")
    database = work / "initial.duckdb"
    environment = {**os.environ, "TAXI_DB": str(database), "TAXI_DATA_DIR": str(config.SAMPLE_DIR),
                   "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    result = subprocess.run([sys.executable, "run_pipeline.py", "2023-01", "2023-02"],
                            cwd=ROOT, env=environment, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    return database


def working_copy(initial, tmp_path):
    database = tmp_path / "warehouse.duckdb"
    shutil.copy2(initial, database)
    return database


def test_unchanged_source_is_a_noop_and_plan_does_not_mutate(initial, tmp_path):
    database = working_copy(initial, tmp_path)
    before = database.read_bytes()
    plan = create_plan(database, config.SAMPLE_DIR, ["2023-01"])
    assert plan["reload_months"] == []
    assert plan["full_reclassification"] is False
    validate_plan(plan)
    assert database.read_bytes() == before


def test_reviewed_source_and_warehouse_cannot_change(initial, tmp_path):
    database = working_copy(initial, tmp_path)
    source = tmp_path / "yellow_tripdata_2023-01.parquet"
    shutil.copy2(config.SAMPLE_DIR / source.name, source)
    plan = create_plan(database, tmp_path, ["2023-01"])
    source.write_bytes(source.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="Source changed"):
        validate_plan(plan)
    shutil.copy2(config.SAMPLE_DIR / source.name, source)
    with duckdb.connect(str(database)) as con:
        con.execute("update raw.trips set _loaded_at = timestamp '2000-01-01' where _source_month='2023-02'")
    with pytest.raises(ValueError, match="Warehouse changed"):
        validate_plan(plan)


def test_plan_cannot_be_edited_or_reused_after_model_change(initial, tmp_path, monkeypatch):
    database = working_copy(initial, tmp_path)
    plan = create_plan(database, config.SAMPLE_DIR, [])
    edited = copy.deepcopy(plan)
    edited["data_dir"] = "/different/source"
    with pytest.raises(ValueError, match="checksum"):
        validate_plan(edited)
    import taxi_platform.change_plan as changes
    monkeypatch.setattr(changes, "model_signature", lambda: "new-model")
    with pytest.raises(ValueError, match="Transformation changed"):
        validate_plan(plan)


def test_legacy_or_changed_model_requires_all_months(initial, tmp_path):
    database = working_copy(initial, tmp_path)
    with duckdb.connect(str(database)) as con:
        con.execute("update main.publication set model_sha256 = 'older-model'")
        assert revision.requires_full_refresh(con)
    plan = create_plan(database, config.SAMPLE_DIR, [])
    assert plan["full_reclassification"]
    assert plan["affected_classification_months"] == ["2023-01", "2023-02"]
    assert plan["reload_months"] == []


def test_required_column_removal_is_refused_before_replacement(initial, tmp_path):
    database = working_copy(initial, tmp_path)
    before = database.read_bytes()
    source = tmp_path / "yellow_tripdata_2023-01.parquet"
    with duckdb.connect() as con:
        con.execute("copy (select * exclude(fare_amount) from read_parquet(?)) to ? (format parquet)",
                    [str(config.SAMPLE_DIR / source.name), str(source)])
    with pytest.raises(ValueError, match="missing required columns.*fare_amount"):
        create_plan(database, tmp_path, ["2023-01"])
    assert database.read_bytes() == before


@pytest.mark.parametrize("poisoned", [False, True])
def test_apply_correction_preserves_other_month_or_last_good_publication(initial, tmp_path, poisoned):
    database = working_copy(initial, tmp_path)
    source = tmp_path / "yellow_tripdata_2023-01.parquet"
    with duckdb.connect() as con:
        change = "case when row_number() over () % 5 < 2 then -5 else fare_amount end as fare_amount" if poisoned \
            else "total_amount + 1 as total_amount"
        # An extra source column is a compatible schema change; it is audited and
        # ignored by the loader, while every existing required field is retained.
        con.execute(f"copy (select * replace({change}), 'correction' extra_source_note "
                    "from read_parquet(?)) to ? (format parquet)",
                    [str(config.SAMPLE_DIR / source.name), str(source)])
    plan = create_plan(database, tmp_path, ["2023-01"])
    assert plan["reload_months"] == ["2023-01"] and not plan["full_reclassification"]
    plan_file, result_file = tmp_path / "plan.json", tmp_path / "result.json"
    plan_file.write_text(json.dumps(plan))
    result = subprocess.run([sys.executable, "-m", "taxi_platform.change_plan", "apply",
                             "--plan", str(plan_file), "--output", str(result_file)],
                            cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == (1 if poisoned else 0), result.stdout[-3000:] + result.stderr[-3000:]
    report = json.loads(result_file.read_text())
    assert report["unaffected_classifier_months"] == ["2023-02"]
    assert report["unaffected_classifier_months_unchanged"]
    assert report["consumer_output_preserved"] is poisoned
    assert report["pipeline_success"] is not poisoned


def test_model_change_rebuilds_unchanged_source_history(initial, tmp_path):
    database = working_copy(initial, tmp_path)
    # Simulate a published older classification revision containing wrong state.
    # Source files are unchanged. A normal build must repair all incremental rows.
    with duckdb.connect(str(database)) as con:
        expected = con.execute("select count(*), sum(fare_amount) from main.stg_trips_classified").fetchone()
        con.execute("update build.stg_trips_classified set fare_amount = 0 where _source_month='2023-02'")
        con.execute("update main.publication set model_sha256 = 'older-model'")
    plan = create_plan(database, config.SAMPLE_DIR, [])
    plan_file, result_file = tmp_path / "plan.json", tmp_path / "result.json"
    plan_file.write_text(json.dumps(plan))
    result = subprocess.run([sys.executable, "-m", "taxi_platform.change_plan", "apply",
                             "--plan", str(plan_file), "--output", str(result_file)],
                            cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    with duckdb.connect(str(database), read_only=True) as con:
        actual = con.execute("select count(*), sum(fare_amount) from main.stg_trips_classified").fetchone()
        assert actual[0] == expected[0]
        assert actual[1] == pytest.approx(expected[1])
        assert not revision.requires_full_refresh(con)

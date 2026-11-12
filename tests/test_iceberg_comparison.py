"""Optional real PyIceberg integration; core CI may skip without the extra package."""

import pytest

pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")

from scripts.iceberg_comparison import run_comparison  # noqa: E402


def test_snapshot_correction_schema_abort_and_rollback(tmp_path):
    report = run_comparison(tmp_path / "lab")
    assert report["passed"] and len(report["checks"]) == 8
    assert report["final_row_count"] == 5
    assert sum(row["fare_cents"] for row in report["final_rows"]) == 15200
    assert report["physical_bytes"]["iceberg_aborted_candidate_data"] > 0
    assert report["physical_bytes"]["iceberg_all_data_files_including_history_and_aborted"] > report["physical_bytes"]["iceberg_current_referenced_data"]
    assert report["snapshots"]["initial"] != report["snapshots"]["corrected"]
    with pytest.raises(FileExistsError):
        run_comparison(tmp_path / "lab")

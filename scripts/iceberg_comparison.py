"""Measured local table-format exercise on five synthetic taxi records.

This does not port the full NYC pipeline or provide atomic publication of its eight
tables. Operation timings and physical bytes are descriptive fixture observations.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def digest(rows):
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def run_comparison(work_dir: Path) -> dict:
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyiceberg
    from pyiceberg.catalog import load_catalog
    from pyiceberg.expressions import EqualTo
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    work_dir = work_dir.resolve()
    if work_dir.exists():
        raise FileExistsError("Use a new work directory so previous evidence remains intact")
    work_dir.mkdir(parents=True)
    schema = Schema(NestedField(1, "trip_id", LongType(), required=False),
                    NestedField(2, "fare_cents", LongType(), required=False),
                    NestedField(3, "source_month", StringType(), required=False))
    arrow_schema = pa.schema([("trip_id", pa.int64()), ("fare_cents", pa.int64()), ("source_month", pa.string())])
    initial = [{"trip_id": i, "fare_cents": i * 1000, "source_month": "2023-01" if i < 3 else "2023-02"}
               for i in range(1, 4)]
    appended = [{"trip_id": 4, "fare_cents": 4000, "source_month": "2023-02"},
                {"trip_id": 5, "fare_cents": 5000, "source_month": "2023-03"}]
    corrected_month = [{**initial[0], "fare_cents": 1200}, initial[1]]
    expected_append = initial + appended
    expected_corrected = corrected_month + [initial[2]] + appended
    steps = []
    checks = {}

    def measured(engine, operation, action):
        start = time.perf_counter()
        result = action()
        steps.append({"engine": engine, "operation": operation, "seconds": round(time.perf_counter() - start, 6)})
        return result

    catalog = load_catalog("local_fixture", type="sql", uri="sqlite:///" + str(work_dir / "catalog.sqlite").replace("\\", "/"),
                           warehouse=(work_dir / "warehouse").as_posix(),
                           **{"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"})
    catalog.create_namespace("taxi")
    table = catalog.create_table("taxi.fixture", schema=schema,
                                 partition_spec=PartitionSpec(PartitionField(3, 1000, IdentityTransform(), "source_month")))

    def rows(snapshot_id=None):
        values = table.scan(snapshot_id=snapshot_id, selected_fields=("trip_id", "fare_cents", "source_month")).to_arrow().to_pylist()
        return sorted(values, key=lambda row: row["trip_id"])

    measured("iceberg", "initial_append", lambda: table.append(pa.Table.from_pylist(initial, schema=arrow_schema)))
    first_id = table.current_snapshot().snapshot_id
    assert rows() == initial
    measured("iceberg", "append_second_batch", lambda: table.append(pa.Table.from_pylist(appended, schema=arrow_schema)))
    appended_id = table.current_snapshot().snapshot_id
    assert rows() == expected_append
    before_files = {task.file.file_path for task in table.scan(row_filter=EqualTo("source_month", "2023-02")).plan_files()}
    measured("iceberg", "correct_january_only", lambda: table.overwrite(
        pa.Table.from_pylist(corrected_month, schema=arrow_schema), overwrite_filter=EqualTo("source_month", "2023-01")))
    corrected_id = table.current_snapshot().snapshot_id
    assert rows() == expected_corrected
    after_files = {task.file.file_path for task in table.scan(row_filter=EqualTo("source_month", "2023-02")).plan_files()}
    assert before_files == after_files
    checks["unaffected_february_files_reused"] = True
    ids_before = {field.name: field.field_id for field in table.schema().fields}

    def evolve():
        with table.update_schema() as update:
            update.add_column("quality_note", StringType())

    measured("iceberg", "add_nullable_column", evolve)
    assert all(table.schema().find_field(name).field_id == field_id for name, field_id in ids_before.items())
    assert table.scan(selected_fields=("quality_note",)).to_arrow().column("quality_note").null_count == 5
    checks["schema_add_preserves_field_ids_and_old_rows"] = True
    published_metadata = table.metadata_location
    files_before_abort = {p.relative_to(work_dir).as_posix() for p in work_dir.rglob("*.parquet")}

    def abort():
        try:
            with table.transaction() as transaction:
                transaction.append(pa.Table.from_pylist(
                    [{"trip_id": 999, "fare_cents": 999, "source_month": "2023-04"}], schema=arrow_schema))
                raise RuntimeError("deliberately abort after writing candidate data, before catalog commit")
        except RuntimeError as exc:
            if "deliberately abort" not in str(exc):
                raise

    measured("iceberg", "abort_candidate_before_commit", abort)
    table = catalog.load_table("taxi.fixture")
    assert table.current_snapshot().snapshot_id == corrected_id
    assert table.metadata_location == published_metadata
    assert rows() == expected_corrected
    checks["aborted_write_preserves_catalog_snapshot_and_rows"] = True
    aborted_files = sorted({p.relative_to(work_dir).as_posix() for p in work_dir.rglob("*.parquet")} - files_before_abort)
    assert aborted_files, "The exercise must actually write a candidate file before aborting"
    checks["aborted_candidate_file_observed"] = True
    assert rows(first_id) == initial and rows(appended_id) == expected_append
    checks["time_travel_reads_prior_rows"] = True
    measured("iceberg", "rollback_to_append_snapshot", lambda: table.manage_snapshots().rollback_to_snapshot(appended_id).commit())
    table = catalog.load_table("taxi.fixture")
    assert table.current_snapshot().snapshot_id == appended_id and rows() == expected_append
    # Snapshot rollback changes data, not the current table schema.
    assert table.schema().find_field("quality_note")
    checks["rollback_restores_previous_data_snapshot"] = True
    measured("iceberg", "restore_corrected_snapshot", lambda: table.manage_snapshots().set_current_snapshot(corrected_id).commit())
    table = catalog.load_table("taxi.fixture")
    assert rows() == expected_corrected
    checks["restore_corrected_snapshot"] = True

    db_path = work_dir / "current.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("create table fixture(trip_id bigint primary key, fare_cents bigint, source_month varchar)")

    def duck_insert(values):
        con.execute("begin")
        try:
            con.executemany("insert into fixture values (?, ?, ?)", [[r[k] for k in ["trip_id", "fare_cents", "source_month"]] for r in values])
            con.execute("commit")
        except BaseException:
            con.execute("rollback")
            raise

    measured("duckdb", "initial_insert", lambda: duck_insert(initial))
    measured("duckdb", "append_second_batch", lambda: duck_insert(appended))

    def duck_correct():
        con.execute("begin")
        try:
            con.execute("delete from fixture where source_month='2023-01'")
            con.executemany("insert into fixture values (?, ?, ?)", [[r[k] for k in ["trip_id", "fare_cents", "source_month"]]
                                                                     for r in corrected_month])
            con.execute("commit")
        except BaseException:
            con.execute("rollback")
            raise

    measured("duckdb", "correct_january_only", duck_correct)
    measured("duckdb", "add_nullable_column", lambda: con.execute("alter table fixture add column quality_note varchar"))

    def duck_abort():
        con.execute("begin")
        try:
            con.execute("insert into fixture values (999,999,'2023-04',null)")
            raise RuntimeError("candidate aborted")
        except RuntimeError:
            con.execute("rollback")

    measured("duckdb", "abort_candidate_before_commit", duck_abort)
    observed = [dict(zip(["trip_id", "fare_cents", "source_month"], r, strict=True)) for r in
                con.execute("select trip_id,fare_cents,source_month from fixture order by trip_id").fetchall()]
    assert observed == expected_corrected
    checks["duckdb_current_rows_and_abort_match_oracle"] = True
    con.execute("checkpoint")
    con.close()
    pq.write_table(pa.Table.from_pylist(expected_corrected, schema=arrow_schema).append_column(
        "quality_note", pa.array([None] * 5, type=pa.string())), work_dir / "current.parquet", compression="zstd")

    current_files = {task.file.file_path: task.file.file_size_in_bytes for task in table.scan().plan_files()}
    all_parquet = list((work_dir / "warehouse").rglob("*.parquet"))
    metadata_files = [p for p in (work_dir / "warehouse").rglob("*") if p.is_file() and p.suffix != ".parquet"]
    file_bytes = sum(p.stat().st_size for p in all_parquet)
    metadata_bytes = sum(p.stat().st_size for p in metadata_files)
    root = Path(__file__).resolve().parents[1]
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        commit = None
    return {
        "scope": "Five synthetic rows, local SQLite catalog and Parquet, single-table fixture comparison; no cloud or full TLC run",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "implementation_sha256": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
        "repository_head_at_run": commit,
        "runtime": {"python": platform.python_version(), "platform": platform.platform(), "cpu_count": os.cpu_count(),
                    "pyiceberg": pyiceberg.__version__, "pyarrow": pa.__version__, "duckdb": duckdb.__version__},
        "input_sha256": digest({"initial": initial, "append": appended, "corrected_january": corrected_month}),
        "final_rows": expected_corrected, "final_row_count": 5, "final_fare_cents": 15200,
        "checks": checks, "passed": all(checks.values()), "operation_timings": steps,
        "snapshots": {"initial": first_id, "appended": appended_id, "corrected": corrected_id,
                      "retained_count": len(table.snapshots())},
        "physical_bytes": {
            "iceberg_current_referenced_data": sum(current_files.values()),
            "iceberg_all_data_files_including_history_and_aborted": file_bytes,
            "iceberg_metadata_files": metadata_bytes,
            "iceberg_sqlite_catalog": (work_dir / "catalog.sqlite").stat().st_size,
            "iceberg_aborted_candidate_data": sum((work_dir / p).stat().st_size for p in aborted_files),
            "duckdb_current_table_database": db_path.stat().st_size,
            "standalone_current_parquet": (work_dir / "current.parquet").stat().st_size,
        },
        "limits": ["One observation per operation; no repeated/warm-cache benchmark or throughput claim",
                   "Iceberg bytes retain history and aborted files; DuckDB/standalone Parquet figures retain only current state",
                   "Aborted Iceberg writes can leave unreferenced files; no cleanup executed in this exercise",
                   "Iceberg commit/rollback is per table, not an atomic version across the main pipeline's eight consumer tables",
                   "Schema is current table metadata; rolling back a data snapshot does not undo the added column",
                   "Injected Python exception before catalog commit, not a host power loss or distributed concurrent-writer test"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a new output path; previous evidence is immutable")
    result = run_comparison(args.work_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write("\n")
    print(json.dumps({"passed": result["passed"], "checks": len(result["checks"]), "output": str(args.output)}))

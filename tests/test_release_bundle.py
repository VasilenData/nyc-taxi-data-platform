"""Serving-backup correctness, corruption rejection and actual abrupt-process recovery."""
import json
import os
from pathlib import Path
import subprocess
import sys

import duckdb
import pytest

from taxi_platform.publication import TABLES
from taxi_platform.release_bundle import RELATIONS, export_release, restore_into, verify_release


def warehouse(path, version="12345678-1234-1234-1234-123456789abc", offset=0):
    with duckdb.connect(str(path)) as con:
        for name in TABLES:
            con.execute(f'''create table "{name}" as
                select i + ? id, cast(i * 0.17 as decimal(10,2)) amount,
                    date '2023-01-01' + cast(i as integer) as event_date,
                    case when i=1 then null else 'Borough ' || i end label
                from range(3) t(i)''', [offset])
        con.execute("create table publication(version varchar, row_counts json)")
        con.execute("insert into publication values (?,?)", [version, json.dumps(dict.fromkeys(TABLES, 3))])


def snapshot(path):
    with duckdb.connect(str(path)) as con:
        return {name: {"schema": con.execute(f'describe "{name}"').fetchall(),
                       "rows": con.execute(f'select * from "{name}" order by all').fetchall()}
                for name in RELATIONS}


def test_roundtrip_preserves_exact_values_types_nulls_and_version(tmp_path):
    source, target, bundle = tmp_path / "source.duckdb", tmp_path / "restored.duckdb", tmp_path / "release"
    warehouse(source)
    manifest = export_release(source, bundle)
    assert manifest["total_bytes"] == sum((bundle / item["file"]).stat().st_size for item in manifest["files"])
    with duckdb.connect(str(target)) as con:
        result = restore_into(con, bundle)
        assert result["relations_restored"] == 9
        assert not con.execute("select * from information_schema.schemata where schema_name in ('raw','build','audit')").fetchall()
    assert snapshot(source) == snapshot(target)
    with pytest.raises(FileExistsError):
        export_release(source, bundle)


@pytest.mark.parametrize("damage", ["checksum", "missing", "extra", "path", "release_id", "schema"])
def test_invalid_bundle_never_changes_previous_serving_version(tmp_path, damage):
    source, target, bundle = tmp_path / "source.duckdb", tmp_path / "target.duckdb", tmp_path / "release"
    warehouse(source)
    warehouse(target, offset=900)
    before = snapshot(target)
    export_release(source, bundle)
    manifest = json.loads((bundle / "manifest.json").read_text())
    if damage == "checksum":
        with (bundle / "fct_trips.parquet").open("ab") as output:
            output.write(b"corruption")
    elif damage == "missing":
        (bundle / "fct_trips.parquet").unlink()
    elif damage == "extra":
        (bundle / "unexpected.parquet").write_bytes(b"extra")
    elif damage == "path":
        manifest["files"][0]["file"] = "../outside.parquet"
    elif damage == "release_id":
        manifest["release_id"] = "a different version"
    else:
        manifest["files"][0]["schema"][0][1] = "VARCHAR"
    (bundle / "manifest.json").write_text(json.dumps(manifest))
    with duckdb.connect(str(target)) as con, pytest.raises(ValueError):
        restore_into(con, bundle)
    assert snapshot(target) == before


@pytest.mark.parametrize("failure_after", [1, 5, 9, "commit"])
def test_interrupted_restore_keeps_every_old_relation(tmp_path, failure_after):
    source, target, bundle = tmp_path / "source.duckdb", tmp_path / "target.duckdb", tmp_path / "release"
    warehouse(source)
    warehouse(target, offset=700)
    before = snapshot(target)
    export_release(source, bundle)
    with duckdb.connect(str(target)) as con:
        class FailingConnection:
            copied = 0

            def execute(self, sql):
                if sql == "commit" and failure_after == "commit":
                    raise OSError("injected interruption")
                result = con.execute(sql)
                if sql.startswith("create or replace table main."):
                    self.copied += 1
                    if self.copied == failure_after:
                        raise OSError("injected interruption")
                return result

        with pytest.raises(OSError, match="interruption"):
            restore_into(FailingConnection(), bundle)
    assert snapshot(target) == before


def test_process_death_during_restore_preserves_old_version_then_recovers(tmp_path):
    source, target, bundle = tmp_path / "source.duckdb", tmp_path / "target.duckdb", tmp_path / "release"
    warehouse(source)
    warehouse(target, offset=500)
    before = snapshot(target)
    export_release(source, bundle)
    code = '''
import os, duckdb
from taxi_platform.release_bundle import restore_into
con = duckdb.connect(os.environ['RESTORE_TEST_DB'])
class CrashConnection:
    def execute(self, sql):
        result = con.execute(sql)
        if sql.startswith('create or replace table main."fct_trips"'):
            os._exit(37)
        return result
restore_into(CrashConnection(), os.environ['RESTORE_TEST_BUNDLE'])
'''
    env = os.environ | {"RESTORE_TEST_DB": str(target), "RESTORE_TEST_BUNDLE": str(bundle)}
    result = subprocess.run([sys.executable, "-c", code], env=env, cwd=Path(__file__).resolve().parents[1], timeout=30)
    assert result.returncode == 37  # Confirms the observed abrupt-exit boundary was reached.
    assert snapshot(target) == before
    with duckdb.connect(str(target)) as con:
        restore_into(con, bundle)
    assert snapshot(target) == snapshot(source)
    assert verify_release(bundle)["scope"] == "published_consumer_relations_only"

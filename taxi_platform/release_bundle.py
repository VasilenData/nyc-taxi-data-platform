"""Immutable Parquet serving backups; verified, transactional consumer restoration.

This backs up the nine published relations, not ingestion state or audit history.
The manifest detects accidental corruption; it is not an authenticity signature.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import time

import duckdb

from taxi_platform.publication import TABLES

RELATIONS = (*TABLES, "publication")


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def schema(con, query):
    return [[row[0], row[1]] for row in con.execute("describe " + query).fetchall()]


def export_release(database, destination):
    """Read one consistent published snapshot and expose the bundle only when complete."""
    destination = Path(destination).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError("Release bundles are immutable; choose a new destination")
    started = time.perf_counter()
    staging = Path(tempfile.mkdtemp(prefix=".release-", dir=destination.parent))
    try:
        with duckdb.connect(str(database), read_only=True) as con:
            con.execute("begin transaction")
            publications = con.execute("select version, row_counts from main.publication").fetchall()
            if len(publications) != 1:
                raise ValueError("Exactly one published version is required")
            version, row_counts = publications[0]
            counts = json.loads(row_counts)
            files = []
            for name in RELATIONS:
                target = staging / f"{name}.parquet"
                query = f'select * from main."{name}"'
                rows = con.execute(f'select count(*) from main."{name}"').fetchone()[0]
                if name != "publication" and counts.get(name) != rows:
                    raise ValueError(f"Publication row count does not match {name}")
                source_schema = schema(con, query)
                con.execute(f"copy ({query}) to {literal(target)} (format parquet, compression zstd)")
                files.append({"table": name, "file": target.name, "rows": rows,
                              "schema": source_schema, "bytes": target.stat().st_size,
                              "sha256": sha256(target)})
            con.execute("commit")
        manifest = {"schema_version": 1, "scope": "published_consumer_relations_only",
                    "release_id": version, "created_at_utc": datetime.now(timezone.utc).isoformat(),
                    "files": files, "total_bytes": sum(item["bytes"] for item in files),
                    "export_seconds": round(time.perf_counter() - started, 6)}
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        verify_release(staging)
        staging.rename(destination)
        return manifest
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def verify_release(directory):
    directory = Path(directory).resolve()
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    files = manifest.get("files", [])
    if manifest.get("schema_version") != 1 or [item.get("table") for item in files] != list(RELATIONS):
        raise ValueError("Unsupported or incomplete release manifest")
    expected_files = {"manifest.json", *(f"{name}.parquet" for name in RELATIONS)}
    if {item.name for item in directory.iterdir()} != expected_files:
        raise ValueError("Bundle must contain exactly the declared release files")
    with duckdb.connect() as con:
        for item in files:
            if item["file"] != f'{item["table"]}.parquet':
                raise ValueError("Unsafe or inconsistent file name")
            path = directory / item["file"]
            if path.is_symlink() or not path.is_file():
                raise ValueError("Release files must be regular files")
            if path.stat().st_size != item["bytes"] or sha256(path) != item["sha256"]:
                raise ValueError(f'Checksum mismatch: {item["table"]}')
            query = f"select * from read_parquet({literal(path)})"
            if con.execute(f"select count(*) from ({query})").fetchone()[0] != item["rows"]:
                raise ValueError("Parquet row count mismatch")
            # DuckDB's Parquet writer preserves JSON logical types on supported versions.
            if schema(con, query) != item["schema"]:
                raise ValueError(f'Parquet schema mismatch: {item["table"]}')
        publication = con.execute(f"select version, row_counts from read_parquet({literal(directory / 'publication.parquet')})").fetchall()
        if len(publication) != 1 or publication[0][0] != manifest["release_id"]:
            raise ValueError("Published version does not match manifest")
        counts = json.loads(publication[0][1])
        if any(counts.get(item["table"]) != item["rows"] for item in files[:-1]):
            raise ValueError("Published row counts do not match manifest")
    if sum(item["bytes"] for item in files) != manifest["total_bytes"]:
        raise ValueError("Total byte count mismatch")
    return manifest


def restore_into(con, directory):
    """Replace all serving relations in one transaction. Caller owns the connection."""
    directory = Path(directory).resolve()
    manifest = verify_release(directory)
    started = time.perf_counter()
    con.execute("begin transaction")
    try:
        views = {row[0] for row in con.execute("select table_name from information_schema.views where table_schema='main'").fetchall()}
        for item in manifest["files"]:
            name = item["table"]  # Previously checked against the exact fixed allowlist.
            if name in views:
                con.execute(f'drop view main."{name}"')
            path = directory / item["file"]
            con.execute(f'create or replace table main."{name}" as select * from read_parquet({literal(path)})')
            if sha256(path) != item["sha256"]:
                raise ValueError("Release changed during restoration")
        con.execute("commit")
    except BaseException:
        con.execute("rollback")
        raise
    return {"release_id": manifest["release_id"], "relations_restored": len(RELATIONS),
            "restore_seconds": round(time.perf_counter() - started, 6),
            "scope": manifest["scope"], "cloud_execution": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["export", "verify", "restore"])
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--database", type=Path)
    args = parser.parse_args()
    if args.action != "verify" and args.database is None:
        parser.error("--database is required for export and restore")
    if args.action == "export":
        result = export_release(args.database, args.bundle)
    elif args.action == "verify":
        result = verify_release(args.bundle)
    else:
        with duckdb.connect(str(args.database)) as con:
            result = restore_into(con, args.bundle)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

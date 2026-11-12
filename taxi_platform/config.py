"""Paths and settings, all overridable with environment variables so CI, Docker and laptops share one code path."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DB_PATH = Path(os.environ.get("TAXI_DB", ROOT / "data" / "warehouse.duckdb"))
# Folder that holds yellow_tripdata_YYYY-MM.parquet. Missing files are downloaded from the TLC.
DATA_DIR = Path(os.environ.get("TAXI_DATA_DIR", ROOT / "data" / "raw"))
SAMPLE_DIR = ROOT / "data" / "sample"
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
DBT_DIR = Path(os.environ.get("TAXI_DBT_DIR", ROOT / "dbt"))
# Keep ingestion and dbt on the same optional memory budget. DuckDB's default is 80%.
DUCKDB_MEMORY_LIMIT = os.environ.get("TAXI_DUCKDB_MEMORY_LIMIT", "80%")

# A month where more than this share of rows is rejected fails the quality gate.
MAX_REJECT_RATE = float(os.environ.get("TAXI_MAX_REJECT_RATE", "0.10"))

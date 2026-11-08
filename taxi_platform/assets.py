"""Dagster assets: partitioned raw load, then the whole dbt graph, then data-quality checks."""

from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from . import config, ingest
from .publication import TABLES, publish

MONTHS = dg.MonthlyPartitionsDefinition(start_date="2023-01", end_offset=0, fmt="%Y-%m")

dbt_project = DbtProject(project_dir=config.DBT_DIR, profiles_dir=config.DBT_DIR)
# `dbt parse` writes target/manifest.json (the asset graph is built from it). Re-parse only if it is missing.
if not dbt_project.manifest_path.exists():
    dbt_project.preparer.prepare(dbt_project)


class Translator(DagsterDbtTranslator):
    """Make dbt's source('raw','trips') the same node as the Dagster asset `raw_trips`, so lineage is one graph."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return dg.AssetKey("raw_trips")
        return super().get_asset_key(dbt_resource_props)


@dg.asset(
    partitions_def=MONTHS,
    group_name="ingestion",
    kinds={"python", "duckdb"},
    description="One TLC parquet file per month, loaded untouched into raw.trips (idempotent per month).",
)
def raw_trips(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = ingest.load_month(context.partition_key)
    return dg.MaterializeResult(metadata={k: dg.MetadataValue.text(str(v)) for k, v in result.items()}
                                | {"rows": dg.MetadataValue.int(result["rows"])})


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=Translator())
def taxi_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    # `build` = run + test + seed in dependency order; a failing test stops downstream models.
    yield from dbt.cli(["build"], context=context).stream()


@dg.asset(deps=[dg.AssetKey(name) for name in TABLES], group_name="publication",
          description="Validate the build snapshot and atomically replace all main consumer tables.")
def published_warehouse() -> dg.MaterializeResult:
    return dg.MaterializeResult(metadata=publish())

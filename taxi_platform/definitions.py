import dagster as dg
from dagster_dbt import DbtCliResource

from .assets import dbt_project, published_warehouse, raw_trips, taxi_dbt_assets
from .checks import coverage, reconciliation, reject_rate

# Two jobs because one asset is partitioned (raw load, per month) and the dbt graph is not.
load_job = dg.define_asset_job("load_latest_month", selection=[raw_trips], partitions_def=raw_trips.partitions_def)
transform_job = dg.define_asset_job(
    "transform_and_check",
    selection=dg.AssetSelection.assets(taxi_dbt_assets, published_warehouse)
    | dg.AssetSelection.checks_for_assets(dg.AssetKey("fct_trips")),
)

# TLC publishes with a lag: on the 2nd load the finished month, an hour later rebuild the models and run the checks.
load_schedule = dg.build_schedule_from_partitioned_job(load_job, hour_of_day=6, day_of_month=2)
transform_schedule = dg.ScheduleDefinition(
    job=transform_job, cron_schedule="0 7 2 * *", execution_timezone="UTC", name="transform_schedule"
)

defs = dg.Definitions(
    assets=[raw_trips, taxi_dbt_assets, published_warehouse],
    asset_checks=[reconciliation, reject_rate, coverage],
    jobs=[load_job, transform_job],
    schedules=[load_schedule, transform_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)

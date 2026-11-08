"""Run the whole platform without the UI: load months, build candidates, check and atomically publish.

    python run_pipeline.py 2023-01 2023-02

Same assets and checks as `dagster dev`. Exits non-zero if a load, a dbt model/test or a check fails (CI uses this).
"""
import sys


from taxi_platform.definitions import defs


def run(months: list[str]) -> bool:
    load = defs.resolve_job_def("load_latest_month")
    for month in months:
        if not load.execute_in_process(partition_key=month, raise_on_error=False).success:
            return False
    transform = defs.resolve_job_def("transform_and_check")
    result = transform.execute_in_process(raise_on_error=False)
    for ev in result.get_asset_check_evaluations():
        print(f"check {ev.check_name[:48]:48s} {'PASS' if ev.passed else 'FAIL'}")
    checks_ok = all(ev.passed for ev in result.get_asset_check_evaluations())
    return result.success and checks_ok


if __name__ == "__main__":
    sys.exit(0 if run(sys.argv[1:] or ["2023-01"]) else 1)

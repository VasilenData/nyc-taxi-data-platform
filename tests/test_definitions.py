from dagster import AssetKey

from taxi_platform.definitions import defs


def test_graph_is_one_connected_lineage():
    graph = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert {"raw_trips", "stg_trips_classified", "stg_trips", "fct_trips", "mart_data_quality"} <= keys
    # raw_trips must be the upstream of the dbt staging model (translator wiring)
    parents = {p.to_user_string() for p in graph.get(AssetKey("stg_trips_classified")).parent_keys}
    assert "raw_trips" in parents


def test_checks_and_schedules_registered():
    assert {s.name for c in defs.asset_checks for s in c.check_specs} >= {"reconciliation", "reject_rate", "coverage"}
    names = {s.name for s in defs.schedules}
    assert len(names) == 2

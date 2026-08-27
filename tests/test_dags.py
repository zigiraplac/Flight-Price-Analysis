"""DAG integrity tests.

Requires apache-airflow, which isn't installed on the local dev machine (Airflow
doesn't support native Windows). Run these inside the container instead:
    docker-compose exec airflow-webserver pytest tests/test_dags.py
Outside that environment, `pytest.importorskip` skips this file cleanly.
"""

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_dagbag_has_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_flight_price_pipeline_is_loaded(dagbag):
    assert dagbag.get_dag("flight_price_pipeline") is not None


LOAD_TASK_IDS = {
    "load_fct_flights",
    "load_kpi_avg_fare_by_airline",
    "load_kpi_seasonal_fare_variation",
    "load_kpi_booking_count_by_airline",
    "load_kpi_top_routes",
}


def test_flight_price_pipeline_task_graph(dagbag):
    dag = dagbag.get_dag("flight_price_pipeline")
    assert set(dag.task_ids) == {
        "check_source_file",
        "extract_to_staging",
        "validate_and_reject",
        "transform_and_stage_kpis",
        "pipeline_summary",
    } | LOAD_TASK_IDS
    assert dag.get_task("extract_to_staging").upstream_task_ids == {"check_source_file"}
    assert dag.get_task("validate_and_reject").upstream_task_ids == {"extract_to_staging"}
    assert dag.get_task("transform_and_stage_kpis").upstream_task_ids == {"validate_and_reject"}

    # Each load task hangs directly off transform_and_stage_kpis, independent
    # of its siblings, so one failing doesn't fail or block the others.
    for task_id in LOAD_TASK_IDS:
        assert dag.get_task(task_id).upstream_task_ids == {"transform_and_stage_kpis"}
        assert dag.get_task(task_id).downstream_task_ids == {"pipeline_summary"}

    # pipeline_summary fans in from every load task and runs even if some of
    # them failed or were skipped, so a run's outcome is visible from one task.
    summary_task = dag.get_task("pipeline_summary")
    assert summary_task.upstream_task_ids == LOAD_TASK_IDS
    assert summary_task.trigger_rule == "all_done"

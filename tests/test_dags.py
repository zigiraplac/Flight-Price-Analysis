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


def test_flight_price_pipeline_task_graph(dagbag):
    dag = dagbag.get_dag("flight_price_pipeline")
    assert set(dag.task_ids) == {
        "extract_to_staging",
        "validate_and_reject",
        "transform_and_stage_kpis",
        "load_to_analytics",
    }
    assert dag.get_task("validate_and_reject").upstream_task_ids == {"extract_to_staging"}
    assert dag.get_task("transform_and_stage_kpis").upstream_task_ids == {"validate_and_reject"}
    assert dag.get_task("load_to_analytics").upstream_task_ids == {"transform_and_stage_kpis"}

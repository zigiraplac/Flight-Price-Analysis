"""Airflow DAG: Flight Price Analysis Pipeline.

CSV -> MySQL staging -> validation/reject -> Total Fare transform -> PostgreSQL analytics.

Each task re-reads its input from the database rather than passing DataFrames
through XCom (which would be capped by the metadata DB's XCom size limit and
hide progress in the DAG graph). Every task also does a full truncate-and-reload
of its own output, so retries or manual re-runs are safe: the source is one
static historical CSV per run, not an incremental daily feed.

`fct_flights` and each of the 4 KPI tables load in their own independent task,
fanned out after `transform_and_stage_kpis`, so a failure loading one (e.g. a
bad KPI groupby, a transient Postgres error on one table) doesn't fail or
block the others -- each one still completes and its Postgres table still
gets refreshed. `check_source_file` fails fast, before anything touches a
database, if the CSV is missing or empty. `pipeline_summary` runs no matter
what happened upstream (`trigger_rule="all_done"`) and reports which of the
five load tasks actually produced a result, so a run's outcome is visible
from one task instead of five.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.exceptions import AirflowFailException
from airflow.sdk import TriggerRule, dag, get_current_context, task

from plugins.common.db import get_mysql_engine, get_postgres_engine
from plugins.ingestion import (
    MissingRequiredColumnsError,
    MissingSourceFileError,
    assert_source_file_exists,
    load_to_staging,
    read_source_csv,
)
from plugins.load import load_fct_flights_table, load_kpi_table
from plugins.transform import transform_and_stage
from plugins.validation import validate_and_stage

log = logging.getLogger(__name__)

SOURCE_CSV_PATH = "/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv"


@dag(
    dag_id="flight_price_pipeline",
    description="Ingest, validate, and compute KPIs for the Bangladesh flight price dataset.",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["flight-price", "kpi"],
)
def flight_price_pipeline():
    @task
    def check_source_file() -> None:
        try:
            assert_source_file_exists(SOURCE_CSV_PATH)
        except MissingSourceFileError as exc:
            raise AirflowFailException(str(exc)) from exc

    @task
    def extract_to_staging() -> dict:
        run_id = get_current_context()["dag_run"].run_id
        try:
            df = read_source_csv(SOURCE_CSV_PATH)
        except MissingRequiredColumnsError as exc:
            raise AirflowFailException(str(exc)) from exc

        engine = get_mysql_engine()
        return load_to_staging(df, engine, run_id)

    @task
    def validate_and_reject(extract_metrics: dict) -> dict:
        engine = get_mysql_engine()
        metrics = validate_and_stage(engine)
        return {**extract_metrics, **metrics}

    @task
    def transform_and_stage_kpis(validate_metrics: dict) -> dict:
        engine = get_mysql_engine()
        metrics = transform_and_stage(engine)
        return {**validate_metrics, **metrics}

    @task
    def load_fct_flights(transform_metrics: dict) -> dict:
        mysql_engine = get_mysql_engine()
        postgres_engine = get_postgres_engine()
        row_count = load_fct_flights_table(mysql_engine, postgres_engine)
        return {**transform_metrics, "fct_flights_rows": row_count}

    @task
    def load_kpi_avg_fare_by_airline(transform_metrics: dict) -> dict:
        mysql_engine = get_mysql_engine()
        postgres_engine = get_postgres_engine()
        row_count = load_kpi_table(mysql_engine, postgres_engine, "kpi_avg_fare_by_airline")
        return {**transform_metrics, "kpi_avg_fare_by_airline_rows": row_count}

    @task
    def load_kpi_seasonal_fare_variation(transform_metrics: dict) -> dict:
        mysql_engine = get_mysql_engine()
        postgres_engine = get_postgres_engine()
        row_count = load_kpi_table(mysql_engine, postgres_engine, "kpi_seasonal_fare_variation")
        return {**transform_metrics, "kpi_seasonal_fare_variation_rows": row_count}

    @task
    def load_kpi_booking_count_by_airline(transform_metrics: dict) -> dict:
        mysql_engine = get_mysql_engine()
        postgres_engine = get_postgres_engine()
        row_count = load_kpi_table(mysql_engine, postgres_engine, "kpi_booking_count_by_airline")
        return {**transform_metrics, "kpi_booking_count_by_airline_rows": row_count}

    @task
    def load_kpi_top_routes(transform_metrics: dict) -> dict:
        mysql_engine = get_mysql_engine()
        postgres_engine = get_postgres_engine()
        row_count = load_kpi_table(mysql_engine, postgres_engine, "kpi_top_routes")
        return {**transform_metrics, "kpi_top_routes_rows": row_count}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def pipeline_summary() -> dict:
        """Runs regardless of upstream outcome. Pulls each load task's result
        by hand (rather than taking them as normal TaskFlow arguments) so a
        failed/skipped load task -- which never pushes a return value --
        doesn't also fail this reporting task; it just shows up as missing."""
        ti = get_current_context()["ti"]
        load_results = {
            task_id: ti.xcom_pull(task_ids=task_id)
            for task_id in [
                "load_fct_flights",
                "load_kpi_avg_fare_by_airline",
                "load_kpi_seasonal_fare_variation",
                "load_kpi_booking_count_by_airline",
                "load_kpi_top_routes",
            ]
        }
        failed_or_skipped = [task_id for task_id, result in load_results.items() if result is None]
        if failed_or_skipped:
            log.warning("Load tasks with no result (failed or skipped): %s", failed_or_skipped)

        return {"load_results": load_results, "failed_or_skipped_load_tasks": failed_or_skipped}

    extracted = extract_to_staging()
    check_source_file() >> extracted

    transform_metrics = transform_and_stage_kpis(validate_and_reject(extracted))
    load_results = [
        load_fct_flights(transform_metrics),
        load_kpi_avg_fare_by_airline(transform_metrics),
        load_kpi_seasonal_fare_variation(transform_metrics),
        load_kpi_booking_count_by_airline(transform_metrics),
        load_kpi_top_routes(transform_metrics),
    ]
    load_results >> pipeline_summary()


flight_price_pipeline()

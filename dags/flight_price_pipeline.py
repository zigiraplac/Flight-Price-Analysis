"""Airflow DAG: Flight Price Analysis Pipeline.

CSV -> MySQL staging -> validation/reject -> KPI transform -> PostgreSQL analytics.

Each task re-reads its input from the database rather than passing DataFrames
through XCom (which would be capped by the metadata DB's XCom size limit and
hide progress in the DAG graph). Every task also does a full truncate-and-reload
of its own output, so retries or manual re-runs are safe: the source is one
static historical CSV per run, not an incremental daily feed.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from sqlalchemy import text

from plugins.common.db import get_mysql_engine, get_postgres_engine
from plugins.extract.csv_loader import MissingRequiredColumnsError, read_source_csv
from plugins.load.postgres_loader import load_analytics
from plugins.transform.kpis import (
    compute_kpis,
    recompute_total_fare,
    with_peak_season_flag,
)
from plugins.transform.staging_writer import update_total_fare, write_kpi_tables
from plugins.validation.reject_writer import write_rejected_rows
from plugins.validation.rules import validate_dataframe

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
    def extract_to_staging() -> dict:
        run_id = get_current_context()["dag_run"].run_id
        try:
            df = read_source_csv(SOURCE_CSV_PATH)
        except MissingRequiredColumnsError as exc:
            raise AirflowFailException(str(exc)) from exc

        df["dag_run_id"] = run_id
        df["loaded_at"] = datetime.utcnow()

        engine = get_mysql_engine()
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE flights"))
            conn.execute(text("TRUNCATE TABLE rejected_rows"))
        df.to_sql("flights", engine, if_exists="append", index=False)
        return {"rows_ingested": len(df)}

    @task
    def validate_and_reject(extract_metrics: dict) -> dict:
        engine = get_mysql_engine()
        staged_df = pd.read_sql_table("flights", engine)
        valid_df, rejected_df = validate_dataframe(staged_df)

        rejected_count = write_rejected_rows(rejected_df, engine)
        if rejected_count:
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM flights WHERE id = :id"), rejected_df[["id"]].to_dict("records")
                )

        return {
            "rows_ingested": extract_metrics["rows_ingested"],
            "rows_valid": len(valid_df),
            "rows_rejected": rejected_count,
        }

    @task
    def transform_and_stage_kpis(validate_metrics: dict) -> dict:
        engine = get_mysql_engine()
        valid_df = pd.read_sql_table("flights", engine)

        enriched_df, fare_metrics = recompute_total_fare(valid_df)
        enriched_df = with_peak_season_flag(enriched_df)
        kpi_frames = compute_kpis(enriched_df)

        update_total_fare(engine, enriched_df)
        kpi_counts = write_kpi_tables(engine, kpi_frames)

        return {**validate_metrics, **fare_metrics, "kpi_row_counts": kpi_counts}

    @task
    def load_to_analytics(transform_metrics: dict) -> dict:
        mysql_engine = get_mysql_engine()
        postgres_engine = get_postgres_engine()
        load_counts = load_analytics(mysql_engine, postgres_engine)
        return {**transform_metrics, "analytics_row_counts": load_counts}

    load_to_analytics(transform_and_stage_kpis(validate_and_reject(extract_to_staging())))


flight_price_pipeline()

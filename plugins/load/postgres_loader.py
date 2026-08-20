"""Reads transformed staging data from MySQL and loads it into the PostgreSQL analytics warehouse.

Re-reads MySQL rather than receiving DataFrames from the transform task, so the
load task can be retried on its own (e.g. after a transient Postgres outage)
without recomputing anything upstream.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from plugins.transform.kpis import with_peak_season_flag

FLIGHTS_COLUMNS = [
    "dag_run_id",
    "loaded_at",
    "airline",
    "source_code",
    "source_name",
    "destination_code",
    "destination_name",
    "departure_datetime",
    "arrival_datetime",
    "duration_hours",
    "stopovers",
    "aircraft_type",
    "travel_class",
    "booking_source",
    "base_fare",
    "tax_surcharge",
    "total_fare_source",
    "total_fare_recomputed",
    "seasonality",
    "days_before_departure",
]

KPI_TABLES = [
    "kpi_avg_fare_by_airline",
    "kpi_seasonal_fare_variation",
    "kpi_booking_count_by_airline",
    "kpi_top_routes",
]


def load_analytics(mysql_engine: Engine, postgres_engine: Engine) -> dict:
    """Full-refresh load: read validated+transformed staging output from MySQL,
    replace the contents of every Postgres analytics table in one transaction."""
    flights_df = pd.read_sql_table("flights", mysql_engine, columns=FLIGHTS_COLUMNS)
    flights_df = flights_df.rename(columns={"total_fare_recomputed": "total_fare"})
    flights_df = with_peak_season_flag(flights_df)

    kpi_frames = {table: pd.read_sql_table(table, mysql_engine) for table in KPI_TABLES}

    counts = {}
    with postgres_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE analytics.fct_flights"))
        for table in KPI_TABLES:
            conn.execute(text(f"TRUNCATE TABLE analytics.{table}"))

        flights_df.to_sql("fct_flights", conn, schema="analytics", if_exists="append", index=False)
        counts["fct_flights"] = len(flights_df)

        for table in KPI_TABLES:
            kpi_frames[table].to_sql(table, conn, schema="analytics", if_exists="append", index=False)
            counts[table] = len(kpi_frames[table])

    return counts

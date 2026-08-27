"""Deliverable 4: Data Loading into PostgreSQL.

Reads transformed staging data (`flights`, already valid + Total-Fare-recomputed)
from MySQL and loads it into the PostgreSQL analytics warehouse. Re-reads MySQL
rather than receiving a DataFrame from the transform step, so this step can be
retried on its own (e.g. after a transient Postgres outage) without recomputing
anything upstream. The 4 KPI aggregates aren't staged anywhere -- they're pure,
cheap-to-derive functions of `flights`, so they're computed fresh here, right
before being written to Postgres, instead of round-tripping through a MySQL
KPI table first.

`fct_flights` and each of the 4 `kpi_*` tables are loaded independently --
their own function, own transaction, own DAG task -- so a failure loading one
(a bad KPI groupby, a transient Postgres error on one table) doesn't roll
back or block the others.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from plugins.transform import (
    kpi_avg_fare_by_airline,
    kpi_booking_count_by_airline,
    kpi_seasonal_fare_variation,
    kpi_top_routes,
    with_peak_season_flag,
)

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

KPI_FUNCTIONS = {
    "kpi_avg_fare_by_airline": kpi_avg_fare_by_airline,
    "kpi_seasonal_fare_variation": kpi_seasonal_fare_variation,
    "kpi_booking_count_by_airline": kpi_booking_count_by_airline,
    "kpi_top_routes": kpi_top_routes,
}


def _read_flights(mysql_engine: Engine) -> pd.DataFrame:
    """Re-reads the validated+transformed `flights` table, renamed/flagged
    exactly as every downstream loader (fct_flights or any KPI) needs it."""
    flights_df = pd.read_sql_table("flights", mysql_engine, columns=FLIGHTS_COLUMNS)
    flights_df = flights_df.rename(columns={"total_fare_recomputed": "total_fare"})
    return with_peak_season_flag(flights_df)


def load_fct_flights_table(mysql_engine: Engine, postgres_engine: Engine) -> int:
    """Truncates and reloads `analytics.fct_flights` only."""
    flights_df = _read_flights(mysql_engine)
    with postgres_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE analytics.fct_flights"))
        flights_df.to_sql("fct_flights", conn, schema="analytics", if_exists="append", index=False)
    return len(flights_df)


def load_kpi_table(mysql_engine: Engine, postgres_engine: Engine, table: str) -> int:
    """Computes the one named KPI fresh from `flights` and truncates+reloads
    only its `analytics.<table>` -- independent of every other KPI table."""
    flights_df = _read_flights(mysql_engine)
    kpi_df = KPI_FUNCTIONS[table](flights_df)
    with postgres_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE analytics.{table}"))
        kpi_df.to_sql(table, conn, schema="analytics", if_exists="append", index=False)
    return len(kpi_df)

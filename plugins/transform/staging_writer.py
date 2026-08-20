"""Persists transform-stage outputs (recomputed fares, KPI aggregates) back to MySQL staging.

Kept separate from kpis.py's pure logic so that module stays unit-testable
without a database, mirroring the rules.py / reject_writer.py split in
plugins/validation.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

KPI_TABLES = [
    "kpi_avg_fare_by_airline",
    "kpi_seasonal_fare_variation",
    "kpi_booking_count_by_airline",
    "kpi_top_routes",
]


def update_total_fare(engine: Engine, flights_df: pd.DataFrame) -> int:
    """Bulk-updates flights.total_fare_recomputed for each row, matched by id."""
    records = (
        flights_df[["id", "total_fare"]]
        .rename(columns={"total_fare": "total_fare_recomputed"})
        .to_dict("records")
    )
    if not records:
        return 0
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE flights SET total_fare_recomputed = :total_fare_recomputed WHERE id = :id"),
            records,
        )
    return len(records)


def write_kpi_tables(engine: Engine, kpi_frames: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Full-refresh: truncate each KPI staging table, then insert this run's aggregates."""
    counts = {}
    with engine.begin() as conn:
        for table in KPI_TABLES:
            conn.execute(text(f"TRUNCATE TABLE {table}"))
        for table in KPI_TABLES:
            frame = kpi_frames[table]
            frame.to_sql(table, conn, if_exists="append", index=False)
            counts[table] = len(frame)
    return counts

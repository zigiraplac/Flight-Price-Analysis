"""Deliverable 3: Data Transformation & KPI Computation.

Pure pandas logic (Total Fare recompute, peak-season flag, the four KPIs) is
unit-testable without a database. `transform_and_stage` is the DB-facing
entry point the `transform_and_stage_kpis` DAG task calls: it reads the
validated `flights` table and persists the recomputed Total Fare back to
MySQL staging. The KPI functions here are also reused directly by
`plugins/load.py`, which computes them fresh at load time rather than
reading them back from a MySQL-side copy -- see load.py's docstring.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

NON_PEAK_SEASONALITY = "Regular"


def recompute_total_fare(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Recompute total_fare from base_fare + tax_surcharge, ignoring the source value.

    The source CSV's own Total Fare column is not trusted as-is: profiling
    found it disagrees with base_fare + tax_surcharge for ~4% of rows.
    Recomputing deterministically keeps downstream KPIs internally consistent;
    the mismatch rate is reported as a metric rather than used to reject rows,
    since base_fare/tax_surcharge are themselves valid numbers.
    """
    out = df.copy()
    out["total_fare"] = out["base_fare"] + out["tax_surcharge"]
    mismatch_mask = (out["total_fare"] - out["total_fare_source"]).abs() > 0.01
    metrics = {
        "rows_transformed": len(out),
        "total_fare_mismatches": int(mismatch_mask.sum()),
        "total_fare_mismatch_pct": round(100 * mismatch_mask.mean(), 4) if len(out) else 0.0,
    }
    return out, metrics


def with_peak_season_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Peak = any Seasonality value other than 'Regular' (Winter Holidays, Eid, Hajj, ...).

    Dataset-driven rather than an external date-range calendar: the source CSV
    already encodes this dimension directly via its `Seasonality` column.
    """
    out = df.copy()
    out["is_peak_season"] = out["seasonality"] != NON_PEAK_SEASONALITY
    return out


def kpi_avg_fare_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("airline")["total_fare"]
        .agg(avg_total_fare="mean", booking_count="count")
        .reset_index()
        .sort_values("avg_total_fare", ascending=False)
        .reset_index(drop=True)
    )


def kpi_seasonal_fare_variation(df: pd.DataFrame) -> pd.DataFrame:
    """df must already have `is_peak_season` (see with_peak_season_flag)."""
    return (
        df.groupby(["seasonality", "is_peak_season"])["total_fare"]
        .agg(avg_total_fare="mean", booking_count="count")
        .reset_index()
        .sort_values("avg_total_fare", ascending=False)
        .reset_index(drop=True)
    )


def kpi_booking_count_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("airline")
        .size()
        .reset_index(name="booking_count")
        .sort_values("booking_count", ascending=False)
        .reset_index(drop=True)
    )


def kpi_top_routes(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    routes = (
        df.groupby(["source_code", "destination_code"])
        .size()
        .reset_index(name="booking_count")
        .sort_values("booking_count", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    routes["route_label"] = routes["source_code"] + " -> " + routes["destination_code"]
    routes["route_rank"] = routes.index + 1
    return routes


def compute_kpis(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """df must already have `total_fare` and `is_peak_season` populated
    (see recompute_total_fare / with_peak_season_flag)."""
    return {
        "kpi_avg_fare_by_airline": kpi_avg_fare_by_airline(df),
        "kpi_seasonal_fare_variation": kpi_seasonal_fare_variation(df),
        "kpi_booking_count_by_airline": kpi_booking_count_by_airline(df),
        "kpi_top_routes": kpi_top_routes(df),
    }


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


def transform_and_stage(engine: Engine) -> dict:
    """Reads the now-valid-only `flights`, recomputes Total Fare, and persists
    it back to MySQL staging (`flights.total_fare_recomputed`).

    KPI aggregates are pure/derived from `flights` and aren't staged in MySQL
    -- the load step recomputes them fresh (via `compute_kpis`) directly from
    the already-persisted `total_fare_recomputed` before writing to Postgres,
    so MySQL staging only ever holds raw-ish rows (`flights`/`rejected_rows`),
    never a second copy of the KPI warehouse.
    """
    valid_df = pd.read_sql_table("flights", engine)
    enriched_df, fare_metrics = recompute_total_fare(valid_df)
    update_total_fare(engine, enriched_df)
    return fare_metrics

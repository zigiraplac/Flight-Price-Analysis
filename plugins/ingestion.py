"""Deliverable 1: Data Ingestion -- CSV -> MySQL staging `flights` table.

Raw load only: columns are renamed and typed to match the staging schema
(dates parsed, per the spec's "appropriate column types matching the original
structure"), but no business-rule filtering or value changes happen here.
That's `validation.py`'s job.
"""

from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

REQUIRED_COLUMNS = [
    "Airline",
    "Source",
    "Destination",
    "Base Fare (BDT)",
    "Tax & Surcharge (BDT)",
    "Total Fare (BDT)",
]

# Source CSV header -> staging table column name.
COLUMN_RENAME_MAP = {
    "Airline": "airline",
    "Source": "source_code",
    "Source Name": "source_name",
    "Destination": "destination_code",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hours",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "travel_class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare",
    "Tax & Surcharge (BDT)": "tax_surcharge",
    "Total Fare (BDT)": "total_fare_source",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}

DATE_COLUMNS = ["Departure Date & Time", "Arrival Date & Time"]


class MissingRequiredColumnsError(ValueError):
    """Raised when the source CSV is missing one or more required columns."""


class MissingSourceFileError(OSError):
    """Raised when the source CSV doesn't exist or is empty."""


def assert_source_file_exists(path: str) -> None:
    """Fail-fast check that the source CSV is present and non-empty.

    Run as its own DAG task ahead of `extract_to_staging`, so a missing or
    empty file reads as one clear, distinct failure instead of surfacing as
    an opaque pandas parse error inside the ingest task.
    """
    if not os.path.isfile(path):
        raise MissingSourceFileError(f"Source CSV not found at {path}")
    if os.path.getsize(path) == 0:
        raise MissingSourceFileError(f"Source CSV at {path} is empty")


def assert_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise MissingRequiredColumnsError(f"Source CSV is missing required columns: {missing}")


def read_source_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=DATE_COLUMNS)
    assert_required_columns(df)
    return df.rename(columns=COLUMN_RENAME_MAP)


def load_to_staging(df: pd.DataFrame, engine: Engine, run_id: str) -> dict:
    """Truncate-and-reload `flights` (+ `rejected_rows`, so a rerun's staging
    tables agree), then bulk-insert this run's rows tagged with dag_run_id/loaded_at."""
    out = df.copy()
    out["dag_run_id"] = run_id
    out["loaded_at"] = datetime.utcnow()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE flights"))
        conn.execute(text("TRUNCATE TABLE rejected_rows"))
    out.to_sql("flights", engine, if_exists="append", index=False)
    return {"rows_ingested": len(out)}

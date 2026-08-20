"""CSV ingestion helpers for the flight price pipeline.

Pure pandas -- no Airflow or database imports -- so schema/parsing logic is
unit-testable without any infrastructure.
"""

from __future__ import annotations

import pandas as pd

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


def assert_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise MissingRequiredColumnsError(f"Source CSV is missing required columns: {missing}")


def read_source_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=DATE_COLUMNS)
    assert_required_columns(df)
    return df.rename(columns=COLUMN_RENAME_MAP)

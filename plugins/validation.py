"""Deliverable 2: Data Validation -- row-level data-quality rules + reject handling.

`validate_dataframe` is pure pandas (no DB/Airflow imports), so it's
unit-testable on its own; `validate_and_stage` is the DB-facing entry point
the DAG calls, which reads `flights`, splits valid/rejected, writes rejected
rows to `rejected_rows`, and removes them from `flights` so that table always
ends the task containing only valid rows.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

NUMERIC_NON_NEGATIVE_COLUMNS = ["base_fare", "tax_surcharge"]
REQUIRED_NON_NULL_COLUMNS = [
    "airline",
    "source_code",
    "destination_code",
    "base_fare",
    "tax_surcharge",
    "departure_datetime",
]
CATEGORICAL_NON_EMPTY_COLUMNS = ["travel_class", "stopovers", "booking_source", "seasonality"]

REJECTED_TABLE = "rejected_rows"


def _build_code_reference(df: pd.DataFrame, code_col: str, name_col: str) -> dict:
    """Derive the dominant code -> name mapping observed in this batch.

    There's no external IATA reference list to validate against, so "invalid
    city name" is defined relative to the dataset itself: a code is expected
    to map to the name most other rows agree on. A row whose name disagrees
    with that majority is flagged as inconsistent.
    """
    pairs = df[[code_col, name_col]].dropna()
    if pairs.empty:
        return {}
    return pairs.groupby(code_col)[name_col].agg(lambda names: names.value_counts().idxmax()).to_dict()


def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split staged rows into (valid_rows, rejected_rows_with_reason).

    Each row gets at most one reason -- the first rule it fails -- since a
    single actionable reason is more useful for an audit trail than a list.
    """
    reasons = pd.Series([None] * len(df), index=df.index, dtype="object")

    def flag(mask: pd.Series, reason: str) -> None:
        newly_flagged = mask.fillna(False) & reasons.isna()
        reasons.loc[newly_flagged] = reason

    for col in REQUIRED_NON_NULL_COLUMNS:
        flag(df[col].isna() | (df[col].astype(str).str.strip() == ""), f"missing required field: {col}")

    for col in NUMERIC_NON_NEGATIVE_COLUMNS:
        numeric = pd.to_numeric(df[col], errors="coerce")
        flag(numeric.isna(), f"non-numeric value in {col}")
        flag(numeric < 0, f"negative value in {col}")

    if "duration_hours" in df.columns:
        duration = pd.to_numeric(df["duration_hours"], errors="coerce")
        flag(duration.isna() | (duration <= 0), "invalid duration_hours")

    if "days_before_departure" in df.columns:
        days = pd.to_numeric(df["days_before_departure"], errors="coerce")
        flag(days.isna() | (days < 0), "invalid days_before_departure")

    if {"source_code", "source_name"}.issubset(df.columns):
        source_ref = _build_code_reference(df, "source_code", "source_name")
        flag(df["source_code"].map(source_ref) != df["source_name"], "unrecognized source code/name pair")

    if {"destination_code", "destination_name"}.issubset(df.columns):
        dest_ref = _build_code_reference(df, "destination_code", "destination_name")
        flag(
            df["destination_code"].map(dest_ref) != df["destination_name"],
            "unrecognized destination code/name pair",
        )

    for col in CATEGORICAL_NON_EMPTY_COLUMNS:
        if col in df.columns:
            flag(
                df[col].isna() | (df[col].astype(str).str.strip() == ""), f"missing categorical field: {col}"
            )

    is_rejected = reasons.notna()
    valid_df = df.loc[~is_rejected].copy()
    rejected_df = df.loc[is_rejected].copy()
    rejected_df["reason"] = reasons.loc[is_rejected]
    return valid_df, rejected_df


def write_rejected_rows(rejected_df: pd.DataFrame, engine: Engine) -> int:
    """Appends rejected rows to `rejected_rows`, renaming their origin `id` to
    `source_row_id` so it doesn't collide with rejected_rows' own primary key."""
    if rejected_df.empty:
        return 0
    out = rejected_df.rename(columns={"id": "source_row_id"})
    out.to_sql(REJECTED_TABLE, engine, if_exists="append", index=False)
    return len(out)


def validate_and_stage(engine: Engine) -> dict:
    """Reads `flights`, splits valid/rejected, writes rejects, and deletes
    rejected rows from `flights` so it ends up containing only valid rows."""
    staged_df = pd.read_sql_table("flights", engine)
    valid_df, rejected_df = validate_dataframe(staged_df)

    rejected_count = write_rejected_rows(rejected_df, engine)
    if rejected_count:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM flights WHERE id = :id"), rejected_df[["id"]].to_dict("records")
            )

    return {"rows_valid": len(valid_df), "rows_rejected": rejected_count}

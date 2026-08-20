"""Row-level data-quality rules for staged flight records.

Pure pandas -- no Airflow or database imports -- so it's unit-testable without
any infrastructure. `validate_dataframe` is the only entry point the DAG uses.
"""

from __future__ import annotations

import pandas as pd

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

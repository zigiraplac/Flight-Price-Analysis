"""Unit tests for plugins/validation/rules.py -- pure pandas logic, no DB/Airflow needed."""

import pandas as pd

from plugins.validation.rules import validate_dataframe

BASE_ROW = {
    "id": 0,
    "airline": "Emirates",
    "source_code": "DAC",
    "source_name": "Hazrat Shahjalal International Airport, Dhaka",
    "destination_code": "DXB",
    "destination_name": "Dubai International Airport",
    "departure_datetime": pd.Timestamp("2025-06-01 10:00:00"),
    "arrival_datetime": pd.Timestamp("2025-06-01 14:00:00"),
    "duration_hours": 4.0,
    "stopovers": "Direct",
    "aircraft_type": "Boeing 777",
    "travel_class": "Economy",
    "booking_source": "Online Website",
    "base_fare": 30000.0,
    "tax_surcharge": 5000.0,
    "total_fare_source": 35000.0,
    "seasonality": "Regular",
    "days_before_departure": 20,
}


def make_df(overrides_list):
    rows = []
    for i, overrides in enumerate(overrides_list):
        row = dict(BASE_ROW)
        row["id"] = i
        row.update(overrides)
        rows.append(row)
    return pd.DataFrame(rows)


def test_valid_rows_pass_through_untouched():
    df = make_df([{}, {}])
    valid_df, rejected_df = validate_dataframe(df)
    assert len(valid_df) == 2
    assert rejected_df.empty


def test_negative_base_fare_is_rejected():
    df = make_df([{}, {"base_fare": -100.0}])
    valid_df, rejected_df = validate_dataframe(df)
    assert len(valid_df) == 1
    assert len(rejected_df) == 1
    assert "negative value in base_fare" in rejected_df.iloc[0]["reason"]


def test_missing_required_field_is_rejected():
    df = make_df([{}, {"airline": None}])
    _, rejected_df = validate_dataframe(df)
    assert len(rejected_df) == 1
    assert "missing required field: airline" in rejected_df.iloc[0]["reason"]


def test_non_numeric_fare_is_rejected():
    df = make_df([{}, {"base_fare": "not-a-number"}])
    _, rejected_df = validate_dataframe(df)
    assert len(rejected_df) == 1
    assert "non-numeric value in base_fare" in rejected_df.iloc[0]["reason"]


def test_unrecognized_source_code_name_pair_is_rejected():
    # Two rows agree that DAC -> Dhaka; a third row has a typo'd name for the same code.
    df = make_df([{}, {}, {"source_name": "Dhaka Airport (typo)"}])
    _, rejected_df = validate_dataframe(df)
    assert len(rejected_df) == 1
    assert "unrecognized source code/name pair" in rejected_df.iloc[0]["reason"]


def test_invalid_days_before_departure_is_rejected():
    df = make_df([{}, {"days_before_departure": -5}])
    _, rejected_df = validate_dataframe(df)
    assert len(rejected_df) == 1
    assert "invalid days_before_departure" in rejected_df.iloc[0]["reason"]


def test_missing_categorical_field_is_rejected():
    df = make_df([{}, {"travel_class": ""}])
    _, rejected_df = validate_dataframe(df)
    assert len(rejected_df) == 1
    assert "missing categorical field: travel_class" in rejected_df.iloc[0]["reason"]


def test_each_rejected_row_has_exactly_one_reason():
    # A row that fails multiple rules still gets exactly one reason (the first rule it hit).
    df = make_df([{"airline": None, "base_fare": -100.0}])
    _, rejected_df = validate_dataframe(df)
    assert len(rejected_df) == 1
    assert rejected_df.iloc[0]["reason"] == "missing required field: airline"

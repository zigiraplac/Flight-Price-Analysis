"""Unit tests for plugins/transform.py -- pure pandas logic, no DB/Airflow needed."""

import pandas as pd

from plugins.transform import (
    compute_kpis,
    recompute_total_fare,
    with_peak_season_flag,
)


def sample_df():
    return pd.DataFrame(
        [
            {
                "airline": "Emirates",
                "source_code": "DAC",
                "destination_code": "DXB",
                "base_fare": 30000.0,
                "tax_surcharge": 5000.0,
                "total_fare_source": 99999.0,  # deliberately wrong, to test recomputation
                "seasonality": "Regular",
            },
            {
                "airline": "Emirates",
                "source_code": "DAC",
                "destination_code": "DXB",
                "base_fare": 32000.0,
                "tax_surcharge": 5000.0,
                "total_fare_source": 37000.0,
                "seasonality": "Eid",
            },
            {
                "airline": "Qatar Airways",
                "source_code": "DAC",
                "destination_code": "DOH",
                "base_fare": 28000.0,
                "tax_surcharge": 4000.0,
                "total_fare_source": 32000.0,
                "seasonality": "Winter Holidays",
            },
        ]
    )


def test_recompute_total_fare_ignores_source_value():
    df, metrics = recompute_total_fare(sample_df())
    assert list(df["total_fare"]) == [35000.0, 37000.0, 32000.0]
    assert metrics["total_fare_mismatches"] == 1
    assert metrics["rows_transformed"] == 3


def test_peak_season_flag():
    df = with_peak_season_flag(sample_df())
    assert not df.loc[df["seasonality"] == "Regular", "is_peak_season"].iloc[0]
    assert df.loc[df["seasonality"] == "Eid", "is_peak_season"].iloc[0]
    assert df.loc[df["seasonality"] == "Winter Holidays", "is_peak_season"].iloc[0]


def test_compute_kpis_shapes_and_values():
    df, _ = recompute_total_fare(sample_df())
    df = with_peak_season_flag(df)
    kpis = compute_kpis(df)

    avg_fare = kpis["kpi_avg_fare_by_airline"].set_index("airline")
    assert avg_fare.loc["Emirates", "avg_total_fare"] == 36000.0
    assert avg_fare.loc["Emirates", "booking_count"] == 2
    assert avg_fare.loc["Qatar Airways", "booking_count"] == 1

    booking_counts = kpis["kpi_booking_count_by_airline"].set_index("airline")
    assert booking_counts.loc["Emirates", "booking_count"] == 2

    routes = kpis["kpi_top_routes"].set_index(["source_code", "destination_code"])
    assert routes.loc[("DAC", "DXB"), "booking_count"] == 2

    seasonal = kpis["kpi_seasonal_fare_variation"]
    assert set(seasonal["seasonality"]) == {"Regular", "Eid", "Winter Holidays"}
    assert not seasonal.set_index("seasonality").loc["Regular", "is_peak_season"]
    assert seasonal.set_index("seasonality").loc["Eid", "is_peak_season"]

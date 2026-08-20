# KPI Methodology

All four KPIs are computed in
[`plugins/transform/kpis.py`](../plugins/transform/kpis.py) — pure pandas
functions, unit-tested in
[`tests/test_transformations.py`](../tests/test_transformations.py) — from the
validated `staging.flights` table, then copied into both MySQL staging
(`staging.kpi_*`) and Postgres (`analytics.kpi_*`).

## Total Fare (prerequisite for every KPI below)

```
total_fare = base_fare + tax_surcharge
```

Recomputed unconditionally, **not** read from the CSV's own `Total Fare`
column. Profiling the actual dataset found the source column disagrees with
`base_fare + tax_surcharge` in 2,522 of 57,000 rows (4.4%), by up to ~93,000
BDT. Base Fare and Tax & Surcharge are individually valid numbers in those
rows — only the source's derived column is arithmetically stale — so this is
handled as a recompute-and-log data-quality finding, not a rejected row. See
[challenges.md](challenges.md).

## 1. Average Fare by Airline — `kpi_avg_fare_by_airline`

Mean `total_fare` and booking count, grouped by `airline`.

```python
df.groupby("airline")["total_fare"].agg(avg_total_fare="mean", booking_count="count")
```

| Column | Meaning |
|---|---|
| `airline` | Carrier name |
| `avg_total_fare` | Mean total fare across all bookings for that airline |
| `booking_count` | Number of bookings backing the average |

## 2. Seasonal Fare Variation — `kpi_seasonal_fare_variation`

Peak vs. non-peak is **dataset-driven**, not an external Eid/Winter calendar:
the CSV already ships a `Seasonality` column (`Regular`, `Winter Holidays`,
`Eid`, `Hajj`). `is_peak_season = (seasonality != "Regular")`. Inventing a
second, external date-range source of truth for the same fact would only
risk disagreeing with the data itself.

```python
df.groupby(["seasonality", "is_peak_season"])["total_fare"].agg(
    avg_total_fare="mean", booking_count="count"
)
```

Grouping by the raw `seasonality` label (rather than collapsing straight to a
peak/non-peak boolean) preserves the breakdown between Eid, Hajj, and Winter
Holidays individually, while `is_peak_season` still gives the two-bucket
comparison the lab asks for in a single `WHERE`/`GROUP BY`.

## 3. Booking Count by Airline — `kpi_booking_count_by_airline`

Row count per `airline`. Kept as its own table (rather than only the count
already present in `kpi_avg_fare_by_airline`) so each of the lab's four listed
KPIs maps to one clearly named, independently queryable table.

```python
df.groupby("airline").size()
```

## 4. Most Popular Routes — `kpi_top_routes`

Booking count per `(source_code, destination_code)` pair, ranked descending,
top 20.

```python
df.groupby(["source_code", "destination_code"]).size()
    .sort_values(ascending=False).head(20)
```

`route_label` (`"DAC -> DXB"`) and `route_rank` (1..20) are added for
direct display without further joins.

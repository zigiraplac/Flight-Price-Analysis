# Architecture

## Overview

The pipeline moves the Bangladesh flight price dataset through three storage
layers, orchestrated by a single Airflow DAG:

```
CSV (data/raw/)
   |
   v
MySQL "staging" database  --  raw ingest, validation/reject split, KPI aggregates
   |
   v
PostgreSQL "analytics" schema  --  fact table + KPI tables for downstream analysis
```

PostgreSQL hosts two schemas in one instance: `airflow` (Airflow's own
scheduler/metadata state) and `analytics` (this pipeline's warehouse), rather
than two separate containers — lighter footprint for a local/lab stack, and
the boundary between the two roles is still explicit via schema rather than
implicit via table-name convention.

## DAG: `flight_price_pipeline`

Manually/CLI-triggered (`schedule=None`), `max_active_runs=1`, four sequential
tasks. Every task re-reads its input from the database rather than receiving
it via XCom, and fully truncates-and-reloads its own output — so any task can
be retried in isolation without recomputing upstream work, and a rerun always
produces the same result (the source is one static CSV, not an incremental
feed).

```
extract_to_staging -> validate_and_reject -> transform_and_stage_kpis -> load_to_analytics
```

### 1. `extract_to_staging`

- Reads `data/raw/Flight_Price_Dataset_of_Bangladesh.csv` with pandas.
- Fails the task immediately (no retries — `AirflowFailException`) if any of
  the six lab-mandated required columns (`Airline`, `Source`, `Destination`,
  `Base Fare`, `Tax & Surcharge`, `Total Fare`) is missing from the source
  file entirely — this is a schema-level failure, not a row-level one.
- Truncates `staging.flights` / `staging.rejected_rows`, then bulk-inserts
  every row into `flights`, tagged with the current `dag_run_id` and
  `loaded_at`.
- Code: [`plugins/extract/csv_loader.py`](../plugins/extract/csv_loader.py).

### 2. `validate_and_reject`

- Reads `staging.flights` back, runs it through
  [`plugins/validation/rules.py`](../plugins/validation/rules.py) (pure
  pandas, unit-tested independently of any database).
- Rows failing a rule are appended to `staging.rejected_rows` with a `reason`
  (via [`reject_writer.py`](../plugins/validation/reject_writer.py)) and
  deleted from `flights`, so `flights` always ends the task containing only
  valid rows.
- Rules: required fields present, fares numeric and non-negative, positive
  duration, non-negative days-before-departure, non-empty categorical fields,
  and a **dataset-derived** source/destination code-to-name consistency check
  (no external IATA list is used — see [challenges.md](challenges.md)).

### 3. `transform_and_stage_kpis`

- Reads the now-valid-only `staging.flights`.
- Recomputes `total_fare = base_fare + tax_surcharge` unconditionally (see
  [kpi-methodology.md](kpi-methodology.md) for why) and computes the four
  KPIs — pure logic in
  [`plugins/transform/kpis.py`](../plugins/transform/kpis.py).
- Persists results back to MySQL via
  [`staging_writer.py`](../plugins/transform/staging_writer.py): bulk-updates
  `flights.total_fare_recomputed`, and truncates+reloads the four
  `staging.kpi_*` tables.

### 4. `load_to_analytics`

- Independently re-reads `staging.flights` (now fully validated and
  transformed) and the four `staging.kpi_*` tables from MySQL.
- Truncates and reloads `analytics.fct_flights` and the four
  `analytics.kpi_*` tables in Postgres inside one transaction.
- Code: [`plugins/load/postgres_loader.py`](../plugins/load/postgres_loader.py).

## Data model

**MySQL `staging`**: `flights`, `rejected_rows`, `kpi_avg_fare_by_airline`,
`kpi_seasonal_fare_variation`, `kpi_booking_count_by_airline`,
`kpi_top_routes`. DDL: [`sql/staging_schema.sql`](../sql/staging_schema.sql).

**PostgreSQL `analytics` schema**: `fct_flights` plus the same four
`kpi_*` tables. DDL:
[`sql/analytics_schema.sql`](../sql/analytics_schema.sql).

Both schema files are mounted directly into their container's
`docker-entrypoint-initdb.d/`, so `docker-compose up` alone produces a fully
migrated stack with no manual `psql`/`mysql` step.

## Why four tasks instead of one script

Splitting extract/validate/transform/load into separate Airflow tasks (rather
than one Python function) buys per-phase retries, per-phase logs, and
per-phase status in the DAG graph — e.g. if `load_to_analytics` fails because
Postgres was briefly unavailable, only that task reruns; extraction,
validation, and KPI computation are not repeated. That operational
visibility is the actual value Airflow adds over a plain script for this
lab.

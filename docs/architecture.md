# Architecture

## Overview

The pipeline moves the Bangladesh flight price dataset through three storage
layers, orchestrated by a single Airflow DAG:

```
CSV (data/raw/)
   |
   v
MySQL "staging" database  --  raw ingest, validation/reject split, Total Fare recompute
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

Manually/CLI-triggered (`schedule=None`), `max_active_runs=1`, ten tasks: a
pre-flight check, three sequential staging/transform tasks, a fan-out of five
independent load tasks, and a summary task that fans back in. Every task
re-reads its input from the database rather than receiving it via XCom, and
fully truncates-and-reloads its own output — so any task can be retried in
isolation without recomputing upstream work, and a rerun always produces the
same result (the source is one static CSV, not an incremental feed).

```
                                                                                  -> load_fct_flights                    ->
                                                                                  -> load_kpi_avg_fare_by_airline        ->
check_source_file -> extract_to_staging -> validate_and_reject -> transform_and_stage_kpis -> load_kpi_seasonal_fare_variation -> pipeline_summary
                                                                                  -> load_kpi_booking_count_by_airline   ->
                                                                                  -> load_kpi_top_routes                 ->
```

Each of the five load tasks depends only on `transform_and_stage_kpis`, not
on each other — so if, say, `load_kpi_top_routes` fails, the other four load
tasks still run and their Postgres tables still get refreshed. Splitting
these out (rather than one `load_to_analytics` task loading everything in a
single Postgres transaction) trades a bit of duplicated work — every load
task re-reads and re-transforms `flights` independently, which is cheap
pandas — for that failure isolation.

### 0. `check_source_file`

- Checks `data/raw/Flight_Price_Dataset_of_Bangladesh.csv` exists and is
  non-empty, before anything touches a database.
- Fails fast with a clear `AirflowFailException` message if not — so "the
  file isn't there" reads as one distinct, obvious failure instead of an
  opaque pandas parse error surfacing from inside `extract_to_staging`.
- Code: `assert_source_file_exists` in
  [`plugins/ingestion.py`](../plugins/ingestion.py).

### 1. `extract_to_staging`

- Reads `data/raw/Flight_Price_Dataset_of_Bangladesh.csv` with pandas.
- Fails the task immediately (no retries — `AirflowFailException`) if any of
  the six lab-mandated required columns (`Airline`, `Source`, `Destination`,
  `Base Fare`, `Tax & Surcharge`, `Total Fare`) is missing from the source
  file entirely — this is a schema-level failure, not a row-level one.
- Truncates `staging.flights` / `staging.rejected_rows`, then bulk-inserts
  every row into `flights`, tagged with the current `dag_run_id` and
  `loaded_at`.
- Code: [`plugins/ingestion.py`](../plugins/ingestion.py).

### 2. `validate_and_reject`

- Reads `staging.flights` back, runs it through `validate_dataframe` in
  [`plugins/validation.py`](../plugins/validation.py) (pure pandas,
  unit-tested independently of any database).
- Rows failing a rule are appended to `staging.rejected_rows` with a `reason`
  and deleted from `flights`, so `flights` always ends the task containing
  only valid rows.
- Rules: required fields present, fares numeric and non-negative, positive
  duration, non-negative days-before-departure, non-empty categorical fields,
  and a **dataset-derived** source/destination code-to-name consistency check
  (no external IATA list is used — see [challenges.md](challenges.md)).

### 3. `transform_and_stage_kpis`

- Reads the now-valid-only `staging.flights`.
- Recomputes `total_fare = base_fare + tax_surcharge` unconditionally (see
  [kpi-methodology.md](kpi-methodology.md) for why) — pure logic in
  [`plugins/transform.py`](../plugins/transform.py).
- Persists the result back to MySQL: bulk-updates `flights.total_fare_recomputed`.
  The four KPIs are *not* computed or staged here — see the next task.

### 4. `load_fct_flights`, `load_kpi_avg_fare_by_airline`, `load_kpi_seasonal_fare_variation`, `load_kpi_booking_count_by_airline`, `load_kpi_top_routes`

- Each independently re-reads `staging.flights` (now fully validated, with
  Total Fare recomputed) from MySQL.
- `load_fct_flights` truncates+reloads `analytics.fct_flights` as-is; each
  `load_kpi_*` task computes its one KPI fresh from `flights` (the matching
  pure function in `plugins/transform.py`, e.g. `kpi_top_routes`) and
  truncates+reloads only its own `analytics.kpi_*` table.
- Code: [`plugins/load.py`](../plugins/load.py) — `load_fct_flights_table`
  and `load_kpi_table`.

### 5. `pipeline_summary`

- Runs after all five load tasks, with `trigger_rule="all_done"` — so unlike
  every other task here, it still runs even if one or more load tasks failed
  or were skipped upstream.
- Pulls each load task's result directly via `ti.xcom_pull(...)` rather than
  taking them as ordinary TaskFlow arguments: a failed/skipped task never
  pushed a return value, and TaskFlow argument resolution would raise on a
  missing one, which would fail this task too and defeat the point.
- Reports which load tasks produced a result and which didn't, so a run's
  overall outcome (all five loaded? partial? none?) is visible from one task
  instead of having to open five separate task logs.

## Data model

**MySQL `staging`**: just `flights` and `rejected_rows` — no KPI tables live
here; KPIs are computed on the fly at load time and only ever persisted in
Postgres. DDL: [`sql/staging_schema.sql`](../sql/staging_schema.sql).

**PostgreSQL `analytics` schema**: `fct_flights` plus the same four
`kpi_*` tables. DDL:
[`sql/analytics_schema.sql`](../sql/analytics_schema.sql).

Both schema files are mounted directly into their container's
`docker-entrypoint-initdb.d/`, so `docker-compose up` alone produces a fully
migrated stack with no manual `psql`/`mysql` step.

## Why separate tasks instead of one script

Splitting extract/validate/transform into separate Airflow tasks (rather than
one Python function) buys per-phase retries, per-phase logs, and per-phase
status in the DAG graph — e.g. if `validate_and_reject` fails, only that task
reruns; extraction is not repeated. That operational visibility is the actual
value Airflow adds over a plain script for this lab.

The five load tasks are split further, for a different reason: fault
isolation between independent outputs. If they were one `load_to_analytics`
task loading `fct_flights` + all four KPI tables in a single Postgres
transaction, a failure anywhere (a bad groupby, a transient Postgres error on
one table) would roll back and fail all five — including the ones that had
nothing wrong with them. As five separate tasks, each with its own
transaction, a failure in `load_kpi_top_routes` doesn't touch
`load_fct_flights` or the other three KPI tasks; they still run, still
commit, and their Postgres tables still get refreshed on this run.

That isolation has a cost, though: confirming "did the whole run succeed"
now means checking five task logs instead of one. `pipeline_summary` is the
fix — a fan-in task that always runs and reports which of the five load
tasks actually produced a result, so partial failure is visible from a
single place without giving up the isolation itself.

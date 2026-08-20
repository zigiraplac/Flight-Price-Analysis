-- PostgreSQL bootstrap for the Flight Price Analysis warehouse.
-- Mounted at /docker-entrypoint-initdb.d/ (see docker-compose.yaml) so it runs
-- automatically against the database named by POSTGRES_DB the first time the
-- postgres container initializes its data volume.
--
-- One Postgres instance hosts two schemas:
--   airflow   -- Airflow's own metadata tables (AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
--               points here via ?options=-csearch_path%3Dairflow)
--   analytics -- this pipeline's KPI warehouse, everything below

CREATE SCHEMA IF NOT EXISTS airflow;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.fct_flights (
    id                     BIGSERIAL PRIMARY KEY,
    dag_run_id             VARCHAR(255) NOT NULL,
    loaded_at              TIMESTAMP NOT NULL,
    airline                VARCHAR(100),
    source_code            VARCHAR(10),
    source_name            VARCHAR(255),
    destination_code       VARCHAR(10),
    destination_name       VARCHAR(255),
    departure_datetime     TIMESTAMP,
    arrival_datetime       TIMESTAMP,
    duration_hours         DOUBLE PRECISION,
    stopovers              VARCHAR(20),
    aircraft_type          VARCHAR(100),
    travel_class           VARCHAR(20),
    booking_source         VARCHAR(50),
    base_fare              NUMERIC(14,4),
    tax_surcharge          NUMERIC(14,4),
    total_fare_source      NUMERIC(14,4),
    total_fare             NUMERIC(14,4),
    seasonality            VARCHAR(30),
    is_peak_season         BOOLEAN,
    days_before_departure  INT
);
CREATE INDEX IF NOT EXISTS idx_fct_flights_airline ON analytics.fct_flights (airline);
CREATE INDEX IF NOT EXISTS idx_fct_flights_route ON analytics.fct_flights (source_code, destination_code);

CREATE TABLE IF NOT EXISTS analytics.kpi_avg_fare_by_airline (
    airline         VARCHAR(100) PRIMARY KEY,
    avg_total_fare  NUMERIC(14,4) NOT NULL,
    booking_count   INT NOT NULL,
    computed_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.kpi_seasonal_fare_variation (
    seasonality     VARCHAR(30) PRIMARY KEY,
    is_peak_season  BOOLEAN NOT NULL,
    avg_total_fare  NUMERIC(14,4) NOT NULL,
    booking_count   INT NOT NULL,
    computed_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.kpi_booking_count_by_airline (
    airline        VARCHAR(100) PRIMARY KEY,
    booking_count  INT NOT NULL,
    computed_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.kpi_top_routes (
    route_rank        INT PRIMARY KEY,
    source_code       VARCHAR(10) NOT NULL,
    destination_code  VARCHAR(10) NOT NULL,
    route_label       VARCHAR(50) NOT NULL,
    booking_count     INT NOT NULL,
    computed_at       TIMESTAMP NOT NULL DEFAULT now()
);

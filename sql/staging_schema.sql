-- MySQL staging schema for the Flight Price Analysis pipeline.
-- Mounted at /docker-entrypoint-initdb.d/ (see docker-compose.yaml) so it runs
-- automatically the first time the mysql container initializes its data volume.

CREATE TABLE IF NOT EXISTS flights (
    id                     BIGINT AUTO_INCREMENT PRIMARY KEY,
    dag_run_id             VARCHAR(255)  NOT NULL,
    loaded_at              DATETIME      NOT NULL,
    airline                VARCHAR(100),
    source_code            VARCHAR(10),
    source_name            VARCHAR(255),
    destination_code       VARCHAR(10),
    destination_name       VARCHAR(255),
    departure_datetime     DATETIME,
    arrival_datetime       DATETIME,
    duration_hours         DOUBLE,
    stopovers              VARCHAR(20),
    aircraft_type          VARCHAR(100),
    travel_class           VARCHAR(20),
    booking_source         VARCHAR(50),
    base_fare              DECIMAL(14,4),
    tax_surcharge          DECIMAL(14,4),
    total_fare_source      DECIMAL(14,4),
    total_fare_recomputed  DECIMAL(14,4),
    seasonality            VARCHAR(30),
    days_before_departure  INT,
    INDEX idx_flights_airline (airline),
    INDEX idx_flights_route (source_code, destination_code)
) ENGINE=InnoDB;

-- Rows that failed a validation rule. Same shape as `flights` (minus total_fare_recomputed,
-- which is never computed for rejected rows) plus a `reason` and a pointer back to the
-- original flights.id the row briefly held, for traceability.
CREATE TABLE IF NOT EXISTS rejected_rows (
    id                     BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_row_id          BIGINT,
    dag_run_id             VARCHAR(255)  NOT NULL,
    loaded_at              DATETIME      NOT NULL,
    airline                VARCHAR(100),
    source_code            VARCHAR(10),
    source_name            VARCHAR(255),
    destination_code       VARCHAR(10),
    destination_name       VARCHAR(255),
    departure_datetime     DATETIME,
    arrival_datetime       DATETIME,
    duration_hours         DOUBLE,
    stopovers              VARCHAR(20),
    aircraft_type          VARCHAR(100),
    travel_class           VARCHAR(20),
    booking_source         VARCHAR(50),
    base_fare              DECIMAL(14,4),
    tax_surcharge          DECIMAL(14,4),
    total_fare_source      DECIMAL(14,4),
    seasonality            VARCHAR(30),
    days_before_departure  INT,
    reason                 VARCHAR(500)  NOT NULL,
    INDEX idx_rejected_reason (reason(100))
) ENGINE=InnoDB;

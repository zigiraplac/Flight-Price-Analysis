# Flight Price Analysis Pipeline

An Apache Airflow pipeline that ingests the Flight Price Dataset of
Bangladesh (57,000 bookings, sourced from Kaggle), validates it, computes
four business KPIs, and loads the results into a PostgreSQL analytics
warehouse.

```text
CSV  --extract-->  MySQL staging  --validate/transform-->  KPI aggregates  --load-->  PostgreSQL analytics
```

## What it computes

| KPI | Answers |
| --- | --- |
| Average Fare by Airline | Which carriers are cheapest/most expensive on average? |
| Seasonal Fare Variation | How much more do Eid/Hajj/Winter Holiday bookings cost vs. regular season? |
| Booking Count by Airline | Which airlines have the most bookings? |
| Most Popular Routes | Which source-to-destination pairs are booked most? |

See [`docs/kpi-methodology.md`](docs/kpi-methodology.md) for exact definitions and [`docs/architecture.md`](docs/architecture.md) for how the pipeline is put together.

## Stack

Apache Airflow (orchestration) - MySQL (staging) - PostgreSQL (analytics warehouse) - Python/Pandas (processing) - Pytest (tests)

## Repository layout

```text
dags/          Airflow DAG definition
plugins/       Pipeline logic (extract, validation, transform, load)
sql/           Database schema (MySQL staging + PostgreSQL analytics)
tests/         Unit tests for the pipeline logic + DAG integrity
docs/          Architecture, KPI methodology, challenges & resolutions
data/raw/      Where the source CSV goes (not committed)
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- The dataset CSV: `Flight_Price_Dataset_of_Bangladesh.csv`, placed at `data/raw/Flight_Price_Dataset_of_Bangladesh.csv`

## Setup

1. Copy the environment template and fill in real values:

   ```bash
   cp .env.example .env
   ```

   Generate a Fernet key for `AIRFLOW_FERNET_KEY`:

   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

2. Start the stack:

   ```bash
   docker-compose up -d
   ```

   This brings up MySQL, PostgreSQL, and Airflow (webserver + scheduler), and automatically creates all database schemas/tables on first boot — no manual SQL step needed.

3. Open the Airflow UI at [localhost:8080](http://localhost:8080) (login: the `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` you set in `.env`) and trigger the `flight_price_pipeline` DAG.

   Or trigger it from the CLI:

   ```bash
   docker-compose exec airflow-webserver airflow dags trigger flight_price_pipeline
   ```

4. Check the results, e.g.:

   ```sql
   SELECT * FROM analytics.kpi_avg_fare_by_airline ORDER BY avg_total_fare DESC;
   SELECT * FROM analytics.kpi_seasonal_fare_variation;
   ```

## Running the tests

The pipeline's logic (validation rules, KPI calculations) is pure Python/Pandas and can be tested without Docker:

```bash
pip install -r requirements.txt
pytest tests/
```

DAG-integrity tests require Airflow and are skipped automatically outside the container; run them with:

```bash
docker-compose exec airflow-webserver pytest tests/test_dags.py
```

## Code quality

```bash
flake8 . && black --check .   # lint & format check
black . && isort .            # auto-format
```

## Stopping the stack

```bash
docker-compose down
```

Add `-v` to also remove the MySQL/Postgres data volumes (full reset).

## Troubleshooting

**`ports are not available: ... bind: ... 3306`** — something on your machine
(often a local MySQL install) is already using port 3306. This project's
MySQL container publishes to host port `3307` instead (see
`docker-compose.yaml`'s `mysql.ports`) specifically to avoid that collision;
containers still talk to each other over the internal Docker network on
`mysql:3306` regardless of the host-side port. If you still hit a conflict,
change the host-side port (`"3307:3306"`) to any other free port.

## Further reading

- [`docs/architecture.md`](docs/architecture.md) — pipeline design, DAG task breakdown, data model
- [`docs/kpi-methodology.md`](docs/kpi-methodology.md) — exact KPI definitions and computation logic
- [`docs/challenges.md`](docs/challenges.md) — data-quality findings and the trade-offs made to handle them

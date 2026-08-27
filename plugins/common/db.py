"""Builds plain SQLAlchemy engines from Airflow connection metadata.

Deliberately bypasses `MySqlHook`/`PostgresHook` (which would pull in the
apache-airflow-providers-mysql/postgres packages) in favor of
`BaseHook.get_connection()` + `sqlalchemy.create_engine()`. `BaseHook` is core
Airflow and works with any conn_type, so this keeps the runtime dependency
surface to `pymysql` and `psycopg2-binary` -- both pure-Python/binary-wheel
installs with no native compiler needed inside the Airflow image.
"""

from __future__ import annotations

from airflow.sdk import BaseHook
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

MYSQL_CONN_ID = "mysql_staging"
POSTGRES_CONN_ID = "postgres_analytics"


def _engine_from_connection(conn_id: str, drivername: str) -> Engine:
    conn = BaseHook.get_connection(conn_id)
    uri = f"{drivername}://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(uri)


def get_mysql_engine() -> Engine:
    return _engine_from_connection(MYSQL_CONN_ID, "mysql+pymysql")


def get_postgres_engine() -> Engine:
    return _engine_from_connection(POSTGRES_CONN_ID, "postgresql+psycopg2")

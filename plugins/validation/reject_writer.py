"""Writes rows that failed validation to the `rejected_rows` staging table."""

from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine

REJECTED_TABLE = "rejected_rows"


def write_rejected_rows(rejected_df: pd.DataFrame, engine: Engine) -> int:
    """Appends rejected rows to `rejected_rows`, renaming their origin `id` to
    `source_row_id` so it doesn't collide with rejected_rows' own primary key."""
    if rejected_df.empty:
        return 0
    out = rejected_df.rename(columns={"id": "source_row_id"})
    out.to_sql(REJECTED_TABLE, engine, if_exists="append", index=False)
    return len(out)

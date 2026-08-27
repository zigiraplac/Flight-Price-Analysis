# Challenges Encountered and Resolutions

## 1. The source `Total Fare` column doesn't always add up

Before writing any transformation code, the actual CSV was profiled (57,000
rows). Base Fare and Tax & Surcharge summed to something other than the
provided `Total Fare` in 2,522 rows (4.4%), with discrepancies up to ~93,000
BDT. This wasn't anticipated by the lab spec, which only says "if not already
present, calculate" — but here it *was* present, just wrong ~4% of the time.

**Resolution:** always recompute `total_fare = base_fare + tax_surcharge`
and treat the source column as an untrusted, informational field
(`total_fare_source`, kept in staging for audit). The recompute-vs-source
mismatch rate is logged as a pipeline metric rather than triggering
row rejection, since the two inputs feeding the recomputation are themselves
valid numbers.

## 2. No external "peak season" calendar existed — and didn't need to

The lab describes peak season only by example ("e.g. Eid, Winter holidays").
Profiling the CSV found it already ships a `Seasonality` column with four
values (`Regular`: 44,525 rows, `Winter Holidays`: 10,930, `Hajj`: 942,
`Eid`: 603).

**Resolution:** define peak as `Seasonality != "Regular"` directly from the
data, instead of building and maintaining a separate external date-range
lookup that could disagree with what the source data already encodes.

## 3. "Invalid city names" has no ground truth to check against

The lab requires flagging "invalid city names," but there's no bundled IATA
reference table, and hardcoding one raises its own maintenance burden and
still wouldn't be guaranteed to match this dataset's exact code set.

**Resolution:** validate source/destination codes against a reference built
from the batch itself — the dominant (majority) code→name pairing observed
across all rows. A row whose code/name pair disagrees with that majority is
flagged as `unrecognized source/destination code/name pair`. On the actual
CSV this reference table is fully consistent (8 origin codes, 20 destination
codes, exactly one name each), so the check is expected to reject 0 rows on
real data — it exists and is unit-tested with a synthetic mismatch fixture
([`tests/test_validation.py`](../tests/test_validation.py)) to prove the
mechanism works even though it never fires on the actual dataset.

## 4. Keeping tasks independently retryable without bloating XCom

Passing full DataFrames between Airflow tasks via XCom is capped by the
metadata database's storage and is a well-known anti-pattern at this row
count (57k rows). The alternative — combining extract/validate/transform/load
into one task — would lose per-phase retries and per-phase visibility in the
DAG graph, which is most of the reason to use Airflow here at all.

**Resolution:** every task re-reads its input from MySQL/Postgres and
XCom only carries small metric dictionaries (row counts, mismatch counts).
Each task fully truncates-and-reloads its own output tables, so a retry of
any single task is safe and doesn't require re-running earlier tasks.

## 5. Avoiding native compilation inside the Airflow image

The default MySQL driver Airflow documentation points to (`mysqlclient`)
needs system-level MySQL dev headers to compile, which the stock
`apache/airflow` image doesn't ship, and this project installs extra
dependencies via `_PIP_ADDITIONAL_REQUIREMENTS` (no custom Dockerfile/apt step
available).

**Resolution:** use `pymysql` and `psycopg2-binary` instead — both install
from prebuilt/pure-Python wheels with no compiler needed. Rather than routing
through `MySqlHook`/`PostgresHook` (which assume `mysqlclient`/`psycopg2`
unless separately configured), [`plugins/common/db.py`](../plugins/common/db.py)
builds a plain SQLAlchemy engine directly from `BaseHook.get_connection()`
(core Airflow, no provider package needed), specifying the driver explicitly.
Trade-off: gives up provider-hook conveniences (like MySqlHook's bulk-load
helpers) in exchange for a smaller, simpler dependency set — acceptable since
this pipeline's DB I/O is a handful of pandas `to_sql`/`read_sql_table` calls
that don't need those helpers.

## 6. Naming consistency: "reject", not "quarantine"

An early draft named the code path that isolates bad rows "quarantine"
(`quarantine.py`) while the actual database table was `rejected_rows` — two
different words for the same concept. Caught during design review and
standardized on "reject" everywhere (`reject_writer.py`, docstrings, this
document) so the vocabulary in code, schema, and docs all match.

## 7. MySQL has no real boolean type, and it broke the Postgres load

Running the full pipeline end-to-end against live containers (not just unit
tests) surfaced a bug the pure-logic tests couldn't catch: `load_to_analytics`
failed with `column "is_peak_season" is of type boolean but expression is of
type integer`.

`is_peak_season` is computed in pandas as a real boolean, and MySQL stores it
in `staging.kpi_seasonal_fare_variation` as `TINYINT(1)` (MySQL has no native
boolean type — `TINYINT(1)` is the conventional stand-in). When
`load_to_analytics` reads that table back out of MySQL, pandas returns the
column as a plain integer (`0`/`1`), not a Python bool. Postgres's real
`BOOLEAN` column then rejects the integer parameter outright rather than
implicitly casting it.

Only `kpi_seasonal_fare_variation` hit this — `fct_flights`' own
`is_peak_season` is computed fresh from `seasonality` in Python at load time,
never round-tripped through MySQL, so it was never at risk.

**Resolution (original):** explicitly `.astype(bool)` the column right after
reading it back from MySQL, before writing to Postgres. The broader lesson:
a value's *type* isn't always preserved by a round trip through a database
that models it differently — this only surfaces by actually running the
pipeline against both real databases, which is why it wasn't caught by the
pure-pandas unit tests (which never touch either database) or by testing
MySQL and Postgres in isolation.

**Superseded:** the KPI aggregates (including `kpi_seasonal_fare_variation`)
were later moved out of MySQL staging entirely — `plugins/load.py` now
computes them fresh from `staging.flights` in pandas, right before writing
to Postgres, instead of staging them in MySQL first. That removes the round
trip this bug depended on, so the `.astype(bool)` workaround is gone too.
Kept here as a record of the failure mode, since the same class of bug would
reappear if any boolean-typed column is ever round-tripped through MySQL
again.

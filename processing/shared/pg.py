"""Writes into the serving layer: plain SQL over plain Python values.

Both Lambda layers write here -- the speed layer's `zone_metrics` and `alerts`,
the batch layer's `daily_bill` -- so the idempotency rules live in one file
instead of being re-derived per job.

**Why psycopg rather than Spark's JDBC writer.** Every write in this project has
to be idempotent, because both layers deliver at-least-once: the streaming query
runs in update mode and re-emits an open window on every micro-batch, and the
billing DAG must be safe to re-run for a simulated day. Idempotency means
`INSERT ... ON CONFLICT DO UPDATE`, and Spark's JDBC writer cannot express it --
`mode("append")` would hit the primary key and fail, `mode("overwrite")` would
drop the table. The usual workaround is to stage each batch in a temporary table
and merge it with SQL, which needs a SQL client anyway.

So the sink collects each micro-batch to the driver and upserts it directly. The
honest limit: this puts every row through the driver, and it is only reasonable
because the rows are few -- four zones times a couple of open windows per batch,
and forty households per billing day. A production volume would need the
staging-table merge instead. The scale is a property of the simulation, not an
assumption hidden in the code, so the sink logs the row count of every batch.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Sequence

import psycopg

# Column order is shared by the INSERT and the row-tuple builder below, so the
# two cannot drift apart silently.
ZONE_METRIC_COLUMNS: Sequence[str] = (
    "window_start", "window_end", "grid_zone",
    "total_consumption_kwh", "total_solar_kwh", "renewable_pct", "active_meters",
)

# The PK is (window_start, grid_zone), so a re-emitted window updates in place.
# `updated_at` is deliberately refreshed: the API's freshness check and the
# Phase 6 health check both read it as "when did the pipeline last touch this
# zone", which is real time, unlike everything else in the row.
_UPSERT_ZONE_METRICS = """
INSERT INTO zone_metrics (
    window_start, window_end, grid_zone,
    total_consumption_kwh, total_solar_kwh, renewable_pct, active_meters, updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (window_start, grid_zone) DO UPDATE SET
    window_end            = EXCLUDED.window_end,
    total_consumption_kwh = EXCLUDED.total_consumption_kwh,
    total_solar_kwh       = EXCLUDED.total_solar_kwh,
    renewable_pct         = EXCLUDED.renewable_pct,
    active_meters         = EXCLUDED.active_meters,
    updated_at            = now()
"""

# The batch layer's output. PK (sim_day, household_id) is what makes the billing
# DAG safe to re-run: a retry, or a second run of the same simulated day, rewrites
# that day's rows instead of double-billing the household. `run_id` is overwritten
# too, so the row always names the run whose numbers it currently holds.
DAILY_BILL_COLUMNS: Sequence[str] = (
    "sim_day", "household_id", "grid_zone",
    "total_consumption_kwh", "total_solar_kwh", "net_kwh",
    "tariff_rate", "billing_tier", "subsidy_flag",
    "gross_amount", "subsidy_amount", "net_amount", "run_id",
)

_UPSERT_DAILY_BILL = """
INSERT INTO daily_bill ({columns}, generated_at)
VALUES ({placeholders}, now())
ON CONFLICT (sim_day, household_id) DO UPDATE SET
    grid_zone             = EXCLUDED.grid_zone,
    total_consumption_kwh = EXCLUDED.total_consumption_kwh,
    total_solar_kwh       = EXCLUDED.total_solar_kwh,
    net_kwh               = EXCLUDED.net_kwh,
    tariff_rate           = EXCLUDED.tariff_rate,
    billing_tier          = EXCLUDED.billing_tier,
    subsidy_flag          = EXCLUDED.subsidy_flag,
    gross_amount          = EXCLUDED.gross_amount,
    subsidy_amount        = EXCLUDED.subsidy_amount,
    net_amount            = EXCLUDED.net_amount,
    run_id                = EXCLUDED.run_id,
    generated_at          = now()
""".format(
    columns=", ".join(DAILY_BILL_COLUMNS),
    placeholders=", ".join(["%s"] * len(DAILY_BILL_COLUMNS)),
)

# Alerts are an append-only log of events that happened, so there is no upsert
# here. Repeats are suppressed before the insert, by the caller's dedupe keys.
_INSERT_ALERT = """
INSERT INTO alerts (level, grid_zone, rule, message, run_id)
VALUES (%s, %s, %s, %s, %s)
"""

_REGISTER_RUN = """
INSERT INTO pipeline_runs (run_id, component, status, sim_day, notes)
VALUES (%s, %s, 'RUNNING', %s, %s)
ON CONFLICT (run_id) DO UPDATE SET
    status = 'RUNNING', sim_day = EXCLUDED.sim_day, notes = EXCLUDED.notes
"""

# A new run of a component proves any earlier RUNNING row for it is finished,
# however it ended. This matters because a container stop cannot close its own
# row: `docker compose stop` signals PID 1, which for a PySpark job is
# spark-submit's JVM, and the JVM kills the Python driver rather than passing the
# signal on -- so the graceful path in speed_layer.py only runs for Ctrl+C.
# Without this, pipeline_runs -- the one table whose job is to say what ran --
# accumulates rows claiming to be RUNNING weeks later.
_SUPERSEDE_RUNS = """
UPDATE pipeline_runs
   SET status = 'FAILED', finished_at = now(),
       notes = coalesce(notes || ' | ', '') ||
               'did not shut down cleanly; superseded by ' || %s
 WHERE component = %s AND status = 'RUNNING' AND run_id <> %s
"""

_FINISH_RUN = """
UPDATE pipeline_runs
   SET status = %s, finished_at = now(), rows_written = %s
 WHERE run_id = %s
"""


@contextmanager
def connection(dsn: str):
    """A committed-on-exit connection.

    Opened per micro-batch rather than held for the lifetime of the query. A
    long-lived connection is faster, but it also has to survive every Postgres
    restart and idle timeout for hours, and the reconnect logic that needs would
    be more code than the ~2 ms a fresh local connection costs every 5 seconds.
    """
    conn = psycopg.connect(dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_zone_metrics(conn, rows: Iterable[Dict[str, Any]]) -> int:
    """Insert-or-update one micro-batch of zone windows. Returns rows written."""
    params: List[tuple] = [
        tuple(row[column] for column in ZONE_METRIC_COLUMNS) for row in rows
    ]
    if not params:
        return 0
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_ZONE_METRICS, params)
    return len(params)


def upsert_daily_bills(conn, bills: Iterable[Dict[str, Any]], run_id: str) -> int:
    """Insert-or-update one simulated day's bills. Returns rows written.

    Every row is stamped with the run that produced it, which is what makes a
    bill traceable: `daily_bill.run_id` joins to `pipeline_runs` and to the JSON
    log lines of that execution.
    """
    params: List[tuple] = [
        tuple(run_id if column == "run_id" else bill[column]
              for column in DAILY_BILL_COLUMNS)
        for bill in bills
    ]
    if not params:
        return 0
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_DAILY_BILL, params)
    return len(params)


def insert_alerts(conn, alerts: Iterable[Any], run_id: str) -> int:
    """Append raised alerts. `alerts` holds alert_rules.Alert instances."""
    params = [(a.level, a.grid_zone, a.rule, a.message, run_id) for a in alerts]
    if not params:
        return 0
    with conn.cursor() as cur:
        cur.executemany(_INSERT_ALERT, params)
    return len(params)


def register_run(dsn: str, run_id: str, component: str, notes: str = "",
                 sim_day: Any = None) -> int:
    """Record this execution, and close out any earlier run of the same component.

    `sim_day` is the simulated day a batch run is billing; the streaming jobs
    leave it null because they are not scoped to a day.

    Returns how many stale rows were superseded, which the caller logs: a
    non-zero count is how you find out the previous run was killed rather than
    stopped.
    """
    with connection(dsn) as conn, conn.cursor() as cur:
        cur.execute(_REGISTER_RUN, (run_id, component, sim_day, notes))
        cur.execute(_SUPERSEDE_RUNS, (run_id, component, run_id))
        return cur.rowcount


def finish_run(dsn: str, run_id: str, status: str, rows_written: int) -> None:
    """Close out the run. status is SUCCESS or FAILED, per the table's CHECK."""
    with connection(dsn) as conn, conn.cursor() as cur:
        cur.execute(_FINISH_RUN, (status, rows_written, run_id))

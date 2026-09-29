"""Read-side queries for the serving layer.

Separate from `processing/shared/pg.py` on purpose: that module writes and this
one only reads. Keeping them apart means the API cannot accidentally mutate a
table the Spark jobs own, and neither file has to carry the other's SQL.

A connection is opened per request rather than pooled. At four zones and a
dashboard refreshing every few seconds this costs ~2 ms against a local Postgres,
and it removes a pool's failure modes (stale connections after a Postgres
restart) from a component whose job is to answer "is the pipeline alive?".
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

# Average power over the window, derived rather than stored: consumption is
# energy across the window, so dividing by the window's own length turns it into
# the kW figure an operator actually reads. Doing it in SQL means the API, the
# Prometheus gauges and Grafana cannot each divide by a slightly different number.
#
# Rounded through `numeric` because Postgres has no round(double, int), then cast
# straight back to a float: left as numeric it arrives as a Python Decimal, and
# Pydantic serialises Decimal as a JSON *string*, so `avg_load_kw` would reach
# Grafana quoted and sort as text.
_AVG_LOAD_KW = (
    "total_consumption_kwh / (EXTRACT(EPOCH FROM (window_end - window_start)) / 3600.0)"
)

_METRIC_FIELDS = """
    window_start, window_end, grid_zone,
    total_consumption_kwh, total_solar_kwh, renewable_pct, active_meters,
    round(({avg_load})::numeric, 3)::float8 AS avg_load_kw,
    updated_at
""".format(avg_load=_AVG_LOAD_KW)

# The newest window in the table is still being filled -- the speed layer runs in
# update mode and rewrites it on every micro-batch -- so its totals are a partial
# sum and would make the grid look like it had lost load. `only_complete` excludes
# it by comparing against the newest window boundary in the table as a whole,
# which every reporting zone shares.
_LATEST_PER_ZONE = """
WITH horizon AS (SELECT max(window_start) AS open_window FROM zone_metrics)
SELECT DISTINCT ON (grid_zone) {fields}
  FROM zone_metrics, horizon
 WHERE (NOT %(only_complete)s) OR window_start < horizon.open_window
 ORDER BY grid_zone, window_start DESC
""".format(fields=_METRIC_FIELDS)

_ZONE_HISTORY = """
SELECT {fields}
  FROM zone_metrics
 WHERE grid_zone = %(grid_zone)s
 ORDER BY window_start DESC
 LIMIT %(limit)s
""".format(fields=_METRIC_FIELDS)

# The ::text casts are not decoration. An optional filter passed as NULL reaches
# Postgres as an untyped parameter, and `$1 IS NULL` alone gives it nothing to
# infer a type from -- the query is rejected before it runs, with "could not
# determine data type of parameter $1". Naming the type resolves it.
_RECENT_ALERTS = """
SELECT alert_id, ts, level, grid_zone, rule, message, run_id
  FROM alerts
 WHERE (%(rule)s::text IS NULL OR rule = %(rule)s::text)
   AND (%(grid_zone)s::text IS NULL OR grid_zone = %(grid_zone)s::text)
 ORDER BY ts DESC
 LIMIT %(limit)s
"""

_ALERT_COUNTS = "SELECT rule, level, count(*) AS total FROM alerts GROUP BY rule, level"

# `updated_at` is the only real-world clock in the table; every other timestamp is
# simulated. It is therefore the only column that can answer "has the pipeline
# stalled?", which is what the health check and the Phase 6 alert both ask.
_FRESHNESS = """
SELECT count(*)                                            AS windows,
       count(DISTINCT grid_zone)                           AS zones_reporting,
       max(window_start)                                   AS newest_window_start,
       max(updated_at)                                     AS last_write,
       EXTRACT(EPOCH FROM (now() - max(updated_at)))::float AS seconds_since_last_write
  FROM zone_metrics
"""


# --- batch layer: daily_bill ----------------------------------------------
# Rounded through `numeric` and cast back to float8 for the same reason as
# avg_load_kw above: Postgres has no round(double, int), and a Decimal would be
# serialised as a JSON string.
_BILLED_DAYS = """
SELECT sim_day,
       count(*)                                    AS households,
       round(sum(net_kwh)::numeric, 3)::float8     AS net_kwh,
       round(sum(net_amount)::numeric, 2)::float8  AS net_amount,
       count(DISTINCT run_id)                      AS runs,
       max(generated_at)                           AS generated_at
  FROM daily_bill
 GROUP BY sim_day
 ORDER BY sim_day DESC
 LIMIT %(limit)s
"""

_BILLS_FOR_DAY = """
SELECT sim_day, household_id, grid_zone,
       total_consumption_kwh, total_solar_kwh, net_kwh,
       tariff_rate, billing_tier, subsidy_flag,
       gross_amount, subsidy_amount, net_amount,
       run_id, generated_at
  FROM daily_bill
 WHERE sim_day = %(sim_day)s::date
 ORDER BY household_id
"""


def _query(dsn: str, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()


def latest_zone_metrics(dsn: str, only_complete: bool = True) -> list[dict[str, Any]]:
    """The most recent window per zone, newest complete one by default."""
    return _query(dsn, _LATEST_PER_ZONE, {"only_complete": only_complete})


def zone_history(dsn: str, grid_zone: str, limit: int) -> list[dict[str, Any]]:
    """One zone's recent windows, newest first."""
    return _query(dsn, _ZONE_HISTORY, {"grid_zone": grid_zone, "limit": limit})


def recent_alerts(dsn: str, limit: int, rule: str | None = None,
                  grid_zone: str | None = None) -> list[dict[str, Any]]:
    return _query(dsn, _RECENT_ALERTS,
                  {"limit": limit, "rule": rule, "grid_zone": grid_zone})


def alert_counts(dsn: str) -> list[dict[str, Any]]:
    return _query(dsn, _ALERT_COUNTS)


def billed_days(dsn: str, limit: int) -> list[dict[str, Any]]:
    """Which simulated days the batch layer has billed, newest first."""
    return _query(dsn, _BILLED_DAYS, {"limit": limit})


def bills_for_day(dsn: str, sim_day: str) -> list[dict[str, Any]]:
    """Every household's bill for one simulated day."""
    return _query(dsn, _BILLS_FOR_DAY, {"sim_day": sim_day})


def freshness(dsn: str) -> dict[str, Any]:
    """One row, always: the aggregates are null on an empty table, not missing."""
    return _query(dsn, _FRESHNESS)[0]

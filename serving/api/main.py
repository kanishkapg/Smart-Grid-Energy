"""Phase 3: the real-time metrics API over the serving layer.

    uvicorn serving.api.main:app --reload        # from the project root
    http://localhost:8000/docs                   # generated OpenAPI docs

Answers the "right now" half of the use case -- grid load and renewable
contribution by zone -- by reading what the speed layer has written to Postgres.
It runs no analytics of its own: aggregation belongs to the layer that owns the
stream, and an API that recomputed it would be a second, disagreeing
implementation.

Endpoints:
    GET /zones                  latest window per zone
    GET /zones/{zone}/load      one zone's recent windows
    GET /alerts                 recent threshold breaches
    GET /health                 liveness + whether the pipeline is still feeding us
    GET /metrics                Prometheus exposition (scraped in Phase 6)
"""

from __future__ import annotations

from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException, Query, Response
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

from common.config import load_config
from common.logging_setup import new_run_id, setup_logging
from serving.api import db

COMPONENT = "api"

cfg = load_config()
run_id = new_run_id(COMPONENT)
log = setup_logging(COMPONENT, run_id=run_id, level=cfg.logging["level"])
DSN = cfg.postgres_dsn

app = FastAPI(
    title="Smart Grid Energy Monitoring API",
    description=__doc__,
    version="0.3.0",
)

# Gauges rather than counters: each one is a current reading of the grid, and is
# re-read from Postgres on every scrape. Prometheus owns the history, so the API
# keeps no state of its own -- a restart loses nothing.
ZONE_GAUGES = {
    "total_consumption_kwh": Gauge(
        "smartgrid_zone_consumption_kwh", "Consumption in the latest window", ["grid_zone"]),
    "total_solar_kwh": Gauge(
        "smartgrid_zone_solar_kwh", "Solar generation in the latest window", ["grid_zone"]),
    "renewable_pct": Gauge(
        "smartgrid_zone_renewable_pct", "Solar as a share of consumption", ["grid_zone"]),
    "active_meters": Gauge(
        "smartgrid_zone_active_meters", "Meters that reported in the window", ["grid_zone"]),
    "avg_load_kw": Gauge(
        "smartgrid_zone_avg_load_kw", "Mean zone power over the window", ["grid_zone"]),
}
LAG_GAUGE = Gauge(
    "smartgrid_serving_lag_seconds",
    "Real seconds since the speed layer last wrote a zone window")
ALERT_GAUGE = Gauge(
    "smartgrid_alerts_total", "Alerts stored, by rule and level", ["rule", "level"])


def read(query, *args, **kwargs):
    """Run one read-side query, mapping database failures onto honest statuses.

    The two cases are kept apart because they mean opposite things to whoever is
    on call. An unreachable database is 503: the dependency is down and the API
    is fine, so restarting the API would achieve nothing. A query that Postgres
    refuses is 500: the API is at fault and its own code needs fixing. Collapsing
    both into 503 is how a broken query gets mistaken for a broken database --
    which happened here once, during Phase 3, over a malformed numeric cast.
    """
    try:
        return query(*args, **kwargs)
    except psycopg.OperationalError as exc:
        log.error("database_unavailable", error=str(exc))
        raise HTTPException(status_code=503,
                            detail="serving database unavailable") from exc
    except psycopg.Error as exc:
        log.error("query_failed", error=str(exc), query=getattr(query, "__name__", "?"))
        raise HTTPException(status_code=500,
                            detail="serving query failed") from exc


@app.get("/zones", summary="Latest metrics for every grid zone")
def list_zones(
    include_open: bool = Query(
        False,
        description="include the window currently being filled; its totals are "
                    "a partial sum and will keep rising",
    ),
) -> dict[str, Any]:
    """Grid load and renewable contribution per zone, from the newest window.

    By default this is the newest *complete* window. The speed layer rewrites the
    window in progress on every micro-batch, so serving it as-is would show
    consumption climbing from zero each time a window opened. One window of
    latency is 12.5 real seconds at the simulation's compression.
    """
    rows = read(db.latest_zone_metrics, DSN, only_complete=not include_open)

    if not rows:
        # Distinguished from "the grid is idle": an empty table means the speed
        # layer has not closed its first window yet, and the fix is to wait, not
        # to debug the query.
        return {
            "zones": [],
            "note": "no completed window yet -- is the speed layer running? "
                    "the first window closes ~12 real seconds after it starts",
        }

    return {
        "window_complete": not include_open,
        "zone_count": len(rows),
        "grid_total_consumption_kwh": round(
            sum(r["total_consumption_kwh"] for r in rows), 4),
        "grid_total_solar_kwh": round(sum(r["total_solar_kwh"] for r in rows), 4),
        "grid_renewable_pct": _renewable_pct(rows),
        "zones": rows,
    }


@app.get("/zones/{grid_zone}/load", summary="One zone's recent windows")
def zone_load(
    grid_zone: str,
    windows: int = Query(24, ge=1, le=500,
                         description="how many windows back to return; 24 is one "
                                     "simulated day at the default window size"),
) -> dict[str, Any]:
    """A zone's load and renewable share over its recent windows, newest first."""
    if grid_zone not in cfg.zone_names:
        # Checked against config rather than against the table: an unknown zone is
        # a client error, whereas a configured zone with no rows yet is not.
        raise HTTPException(
            status_code=404,
            detail="unknown grid zone {!r}; configured zones are {}".format(
                grid_zone, cfg.zone_names),
        )
    rows = read(db.zone_history, DSN, grid_zone, windows)

    return {"grid_zone": grid_zone, "window_count": len(rows), "windows": rows}


@app.get("/alerts", summary="Recent threshold breaches")
def list_alerts(
    limit: int = Query(50, ge=1, le=500),
    rule: str | None = Query(None, description="filter to one rule name"),
    grid_zone: str | None = Query(None, description="filter to one zone"),
) -> dict[str, Any]:
    """The alert log, newest first, as written by the speed layer's rules."""
    rows = read(db.recent_alerts, DSN, limit=limit, rule=rule, grid_zone=grid_zone)

    return {"alert_count": len(rows), "alerts": rows}


@app.get("/health", summary="Liveness and pipeline freshness")
def health() -> dict[str, Any]:
    """Is the API up, and is the speed layer still feeding it?

    Two separate questions, deliberately not collapsed into one status code. A
    stale pipeline still returns 200: the API is healthy and correctly reporting
    that its upstream has stopped. Only an unreachable database is a 503. Phase 6
    turns the `data_fresh` flag into the firing health-check alert.
    """
    state = read(db.freshness, DSN)

    threshold = float(cfg.alerts["no_data_real_seconds"])
    age = state["seconds_since_last_write"]
    fresh = age is not None and age <= threshold

    return {
        "status": "ok" if fresh else "degraded",
        "database": "reachable",
        "data_fresh": fresh,
        "seconds_since_last_write": None if age is None else round(age, 1),
        "freshness_threshold_seconds": threshold,
        "windows_stored": state["windows"],
        "zones_reporting": state["zones_reporting"],
        "newest_window_start": state["newest_window_start"],
        "zones_configured": len(cfg.zone_names),
        "run_id": run_id,
    }


@app.get("/metrics", summary="Prometheus exposition")
def metrics() -> Response:
    """Re-read the serving layer and expose it in Prometheus text format.

    Scraping pulls, so the gauges are refreshed here rather than on a timer: the
    values are never staler than the scrape that returned them, and the API does
    no work when nothing is watching.
    """
    rows = read(db.latest_zone_metrics, DSN, only_complete=True)
    state = read(db.freshness, DSN)
    counts = read(db.alert_counts, DSN)

    for row in rows:
        for field, gauge in ZONE_GAUGES.items():
            gauge.labels(grid_zone=row["grid_zone"]).set(float(row[field]))
    LAG_GAUGE.set(float(state["seconds_since_last_write"] or 0.0))
    for row in counts:
        ALERT_GAUGE.labels(rule=row["rule"], level=row["level"]).set(row["total"])

    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def _renewable_pct(rows: list[dict[str, Any]]) -> float:
    """Grid-wide renewable share.

    Recomputed from the totals rather than averaged across zones: a mean of four
    percentages would weight a 9-meter zone the same as a 12-meter one.
    """
    consumption = sum(r["total_consumption_kwh"] for r in rows)
    if consumption <= 0:
        return 0.0
    return round(100 * sum(r["total_solar_kwh"] for r in rows) / consumption, 2)

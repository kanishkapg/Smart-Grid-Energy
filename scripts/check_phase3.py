#!/usr/bin/env python3
"""Phase 3 checkpoint: is the speed layer live, and does the API serve it?

    python scripts/check_phase3.py
    python scripts/check_phase3.py --expect-alert low_renewable_contribution

The plan's checkpoint is "API returns live per-zone metrics; forcing low solar in
the producer makes a low-renewable alert appear". This script checks the first
half unconditionally and the second on demand, so the same command verifies the
normal state of the pipeline and the alert demo.

Two independent readings are compared throughout: what is in Postgres, and what
the API says. A discrepancy between them is the failure this is really looking
for -- a live-looking dashboard served from a stale or wrongly-joined query.

HTTP goes through urllib rather than a client library, so the script needs no
dependency the API itself does not already have.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from common.config import load_config  # noqa: E402

# The speed layer commits every `trigger_seconds`, and a window only appears once
# it has received data, so "fresh" has to allow for one trigger plus slack. Judged
# against the same threshold the health endpoint and the Phase 6 alert use.
FRESHNESS_SLACK_SECONDS = 15


def endpoint(cfg) -> str:
    """host:port/database out of the DSN, with the password and options dropped."""
    return cfg.postgres_dsn.split("@")[-1].split("?")[0]


def api_base_url() -> str:
    return "http://localhost:{}".format(os.environ.get("API_PORT", "8000"))


def api_get(path: str) -> dict:
    """GET a JSON endpoint. Raises with a readable message if the API is down."""
    url = api_base_url() + path
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError("{} returned HTTP {}".format(url, exc.code)) from exc
    except Exception as exc:  # URLError, timeout, JSON errors
        raise RuntimeError("{} unreachable: {}".format(url, exc)) from exc


def query(cfg, sql: str, params: dict | None = None) -> list[dict]:
    with psycopg.connect(cfg.postgres_dsn, row_factory=dict_row, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------

def check_serving_tables(cfg) -> tuple[bool, list[str]]:
    """Has the speed layer written anything, and did it register its run?"""
    try:
        counts = query(cfg, """
            SELECT (SELECT count(*) FROM zone_metrics)                       AS windows,
                   (SELECT count(*) FROM alerts)                             AS alerts,
                   (SELECT count(*) FROM pipeline_runs
                     WHERE component = 'speed_layer')                        AS speed_runs
        """)[0]
    except psycopg.Error as exc:
        return False, ["- cannot reach Postgres at {}: {}".format(
            endpoint(cfg), exc)]

    lines = [
        "+ zone_metrics holds {:,} window row(s)".format(counts["windows"]),
        "+ alerts holds {:,} row(s)".format(counts["alerts"]),
        "{} pipeline_runs has {} speed_layer run(s) registered".format(
            "+" if counts["speed_runs"] else "-", counts["speed_runs"]),
    ]
    if not counts["windows"]:
        lines.append("- no windows yet. Is the speed layer running, and the producer?")
        lines.append("  docker compose logs --tail 30 speed-layer")
    return bool(counts["windows"] and counts["speed_runs"]), lines


def check_zone_coverage(cfg) -> tuple[bool, list[str]]:
    """Is every configured zone reporting, and recently?"""
    rows = query(cfg, """
        SELECT DISTINCT ON (grid_zone)
               grid_zone, window_start, renewable_pct, active_meters,
               total_consumption_kwh,
               EXTRACT(EPOCH FROM (now() - updated_at))::float AS age_seconds
          FROM zone_metrics
         ORDER BY grid_zone, window_start DESC
    """)
    by_zone = {row["grid_zone"]: row for row in rows}
    budget = float(cfg.alerts["no_data_real_seconds"]) + FRESHNESS_SLACK_SECONDS

    lines = ["", "      {:<16}{:>12}{:>10}{:>9}  {}".format(
        "grid_zone", "renewable%", "meters", "age(s)", "newest window (simulated)")]
    ok = True
    for zone in cfg.zone_names:
        row = by_zone.get(zone)
        if row is None:
            lines.append("      - {:<14}{}".format(zone, "never reported"))
            ok = False
            continue
        fresh = row["age_seconds"] <= budget
        ok &= fresh
        lines.append("      {} {:<14}{:>12.1f}{:>10}{:>9.1f}  {}".format(
            "+" if fresh else "-", zone, row["renewable_pct"], row["active_meters"],
            row["age_seconds"], row["window_start"].strftime("%Y-%m-%d %H:%M")))

    lines.append("")
    lines.append("{} all {} configured zone(s) present".format(
        "+" if len(by_zone) >= len(cfg.zone_names) else "-", len(cfg.zone_names)))
    lines.append("{} rows written within {:.0f}s (no_data_real_seconds + {}s slack)".format(
        "+" if ok else "-", budget, FRESHNESS_SLACK_SECONDS))
    if not ok:
        lines.append("  A stale row means the speed layer or the producer has stopped.")
    return ok and len(by_zone) >= len(cfg.zone_names), lines


def check_window_arithmetic(cfg) -> tuple[bool, list[str]]:
    """Are the stored windows the right length, and is renewable_pct consistent?

    This is the check that would catch a genuinely wrong aggregation: the window
    length proves the event-time windowing ran on simulated time rather than
    processing time, and recomputing the percentage proves the stored value came
    from the stored totals.
    """
    expected_minutes = int(cfg.speed_layer["window_minutes"])
    rows = query(cfg, """
        SELECT grid_zone, window_start,
               EXTRACT(EPOCH FROM (window_end - window_start)) / 60 AS minutes,
               total_consumption_kwh, total_solar_kwh, renewable_pct, active_meters
          FROM zone_metrics
         ORDER BY window_start DESC
         LIMIT 200
    """)

    wrong_length = [r for r in rows if abs(r["minutes"] - expected_minutes) > 1e-6]
    mismatched = [
        r for r in rows
        if r["total_consumption_kwh"] > 0
        and abs(100 * r["total_solar_kwh"] / r["total_consumption_kwh"]
                - r["renewable_pct"]) > 0.02
    ]
    over_capacity = [r for r in rows if r["active_meters"] > cfg.total_meters]

    lines = [
        "{} {} window(s) checked, all {} simulated minutes long".format(
            "+" if not wrong_length else "-", len(rows), expected_minutes),
        "{} renewable_pct matches solar/consumption in every row".format(
            "+" if not mismatched else "-"),
        "{} active_meters never exceeds the {} configured meters".format(
            "+" if not over_capacity else "-", cfg.total_meters),
    ]
    for label, bad in (("wrong window length", wrong_length),
                       ("renewable_pct mismatch", mismatched),
                       ("impossible meter count", over_capacity)):
        if bad:
            lines.append("  {}: e.g. {} at {}".format(
                label, bad[0]["grid_zone"], bad[0]["window_start"]))

    # Informational: a full simulated day is 1440 / window_minutes windows per
    # zone, which is what makes the row count interpretable during a demo.
    lines.append("+ a full simulated day is {} window(s) per zone".format(
        1440 // expected_minutes))
    return not (wrong_length or mismatched or over_capacity), lines


def check_api(cfg) -> tuple[bool, list[str]]:
    """Does the API serve what the database holds?"""
    lines = []
    try:
        health = api_get("/health")
        zones = api_get("/zones")
    except RuntimeError as exc:
        return False, ["- {}".format(exc),
                       "  docker compose logs --tail 30 api"]

    lines.append("+ /health   status={} data_fresh={} last_write={}s ago".format(
        health["status"], health["data_fresh"], health["seconds_since_last_write"]))
    ok = health["database"] == "reachable" and bool(health["data_fresh"])
    if not health["data_fresh"]:
        lines.append("- the API can reach Postgres but nothing is writing to it")

    served = zones.get("zones", [])
    if not served:
        return False, lines + ["- /zones returned no zones: {}".format(
            zones.get("note", "")), "  the first window closes ~12 real seconds "
            "after the speed layer starts"]

    lines.append("+ /zones    {} zone(s), grid load {:.2f} kWh/window, "
                 "renewable {:.1f}%".format(
                     zones["zone_count"], zones["grid_total_consumption_kwh"],
                     zones["grid_renewable_pct"]))
    for row in served:
        lines.append("      {:<16} {:>8.2f} kWh  {:>6.2f} kW  {:>6.1f}%  {} meters".format(
            row["grid_zone"], row["total_consumption_kwh"],
            float(row["avg_load_kw"]), row["renewable_pct"], row["active_meters"]))

    # The comparison that matters: the API must be reading the same rows, not a
    # cached or differently-ordered view of them.
    db_latest = {
        r["grid_zone"]: r["renewable_pct"] for r in query(cfg, """
            WITH horizon AS (SELECT max(window_start) AS open_window FROM zone_metrics)
            SELECT DISTINCT ON (grid_zone) grid_zone, renewable_pct
              FROM zone_metrics, horizon
             WHERE window_start < horizon.open_window
             ORDER BY grid_zone, window_start DESC
        """)
    }
    disagreements = [
        row["grid_zone"] for row in served
        if abs(db_latest.get(row["grid_zone"], -1) - row["renewable_pct"]) > 0.01
    ]
    lines.append("{} /zones agrees with the newest complete window in Postgres".format(
        "+" if not disagreements else "-"))
    ok &= not disagreements

    zone = served[0]["grid_zone"]
    history = api_get("/zones/{}/load?windows=5".format(urllib.parse.quote(zone)))
    descending = all(
        history["windows"][i]["window_start"] >= history["windows"][i + 1]["window_start"]
        for i in range(len(history["windows"]) - 1)
    )
    lines.append("{} /zones/{}/load returned {} window(s), newest first".format(
        "+" if descending else "-", zone, history["window_count"]))
    ok &= descending

    unknown_status = "404 as expected"
    try:
        api_get("/zones/Atlantis/load")
        unknown_status = "200 -- an unknown zone should be rejected"
        ok = False
    except RuntimeError as exc:
        if "HTTP 404" not in str(exc):
            unknown_status = str(exc)
            ok = False
    lines.append("{} unknown zone: {}".format("+" if ok else "-", unknown_status))
    return ok, lines


def check_alerts(cfg, expected_rule: str | None) -> tuple[bool, list[str]]:
    """Summarise the alert log, and optionally require a specific rule to have fired."""
    rows = query(cfg, """
        SELECT rule, level, count(*) AS total, max(ts) AS newest,
               (array_agg(message ORDER BY ts DESC))[1] AS latest_message
          FROM alerts GROUP BY rule, level ORDER BY 1, 2
    """)
    served = api_get("/alerts?limit=5") if rows else {"alert_count": 0}

    lines = []
    if not rows:
        lines.append("+ no alerts raised: every zone window is within its thresholds")
        lines.append("  To force the low-renewable rule, restart the producer with")
        lines.append("    python -m sources.stream_producer --cloud-cover 0.95")
        lines.append("  and wait for a window inside {}:00-{}:00 simulated.".format(
            cfg.alerts["renewable_check_start_hour"],
            cfg.alerts["renewable_check_end_hour"]))
    for row in rows:
        lines.append("+ {:<28} {:<5} {:>4} raised, newest {}".format(
            row["rule"], row["level"], row["total"],
            row["newest"].strftime("%H:%M:%S")))
        lines.append('    "{}"'.format(row["latest_message"]))

    if rows:
        lines.append("+ /alerts served {} of them".format(served["alert_count"]))

    if expected_rule is None:
        return True, lines

    fired = any(row["rule"] == expected_rule for row in rows)
    lines.append("{} expected rule {!r} has fired".format("+" if fired else "-",
                                                          expected_rule))
    if not fired:
        lines.append("  Rules are only evaluated on windows that breach them. Check the")
        lines.append("  simulated hour: the renewable rule is inactive outside "
                     "{}:00-{}:00.".format(cfg.alerts["renewable_check_start_hour"],
                                            cfg.alerts["renewable_check_end_hour"]))
    return fired, lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expect-alert", metavar="RULE", default=None,
                        help="fail unless this rule has fired "
                             "(e.g. low_renewable_contribution)")
    args = parser.parse_args()
    cfg = load_config()

    print("\n  Smart Grid Lambda Platform -- Phase 3 speed layer + API check")
    print("  " + "-" * 58)
    # Only the endpoint: the rest of the DSN is a password and connection options.
    print("\n  Serving DB   {}".format(endpoint(cfg)))
    print("  API          {}".format(api_base_url()))

    checks = [
        ("Serving tables", lambda: check_serving_tables(cfg)),
        ("Zone coverage", lambda: check_zone_coverage(cfg)),
        ("Window arithmetic", lambda: check_window_arithmetic(cfg)),
        ("API endpoints", lambda: check_api(cfg)),
        ("Alert rules", lambda: check_alerts(cfg, args.expect_alert)),
    ]

    failures = []
    for label, check in checks:
        try:
            ok, lines = check()
        except Exception as exc:  # noqa: BLE001 - a broken check is a failed check
            ok, lines = False, ["- {}: {}".format(type(exc).__name__, exc)]
        if not ok:
            failures.append(label)
        print("\n  {:<20} [{}]".format(label, "PASS" if ok else "FAIL"))
        for line in lines:
            print("      {}".format(line) if line and not line.startswith("      ") else line)
        # A dead database or a dead API makes every later check meaningless.
        if not ok and label == "Serving tables":
            break

    print("\n  " + "-" * 58)
    if failures:
        print("  PHASE 3 CHECKPOINT: FAILED  ({})".format(", ".join(failures)))
        print("  Logs: docker compose logs --tail 40 speed-layer api\n")
        return 1
    print("  PHASE 3 CHECKPOINT: PASSED")
    print("  Live API docs: {}/docs\n".format(api_base_url()))
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Phase 5 checkpoint: do both Grafana dashboards render live data?

    python scripts/check_phase5.py

"Grafana is up" proves little: a dashboard with a broken query still loads and
simply shows "No data". So this script asks Grafana for each provisioned
dashboard and sends every panel's SQL back through Grafana's own query API --
the same path the browser uses -- and counts the rows that come back. That
exercises the datasource, its credentials, Grafana's macros ($__timeFilter)
and the SQL itself, in one go.

Standard library only: HTTP basic auth against the admin user in .env.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.request

DATASOURCE_UID = "sg-postgres"
DASHBOARDS = {"sg-live": "Live Zone Monitoring", "sg-billing": "Daily Billing"}
# Alerts legitimately stay empty while nothing breaches, so an empty result on
# these panels is reported but does not fail the checkpoint.
MAY_BE_EMPTY = {"Alerts in selected time range", "Recent alerts"}


def base_url() -> str:
    return "http://localhost:{}".format(os.environ.get("GRAFANA_PORT", "3000"))


def grafana(path: str, body: dict | None = None) -> dict:
    """GET (or POST, with a body) a Grafana API path as the admin user."""
    user = os.environ.get("GRAFANA_ADMIN_USER", "admin")
    password = os.environ.get("GRAFANA_ADMIN_PASSWORD", "admin")
    token = base64.b64encode("{}:{}".format(user, password).encode()).decode()
    request = urllib.request.Request(
        base_url() + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Basic " + token, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:300]
        raise RuntimeError("{} returned HTTP {}: {}".format(path, exc.code, detail)) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError("{} unreachable: {}".format(base_url(), exc.reason)) from exc


def run_sql(sql: str, fmt: str = "table") -> int:
    """Run one query through Grafana's datasource proxy; return the row count."""
    result = grafana("/api/ds/query", {
        "from": "now-30m", "to": "now",
        "queries": [{"refId": "A", "datasource": {"uid": DATASOURCE_UID},
                     "rawSql": sql, "format": fmt, "rawQuery": True}],
    })["results"]["A"]
    if result.get("error"):
        raise RuntimeError(result["error"])
    rows = 0
    for frame in result.get("frames", []):
        values = frame.get("data", {}).get("values") or []
        rows += len(values[0]) if values else 0
    return rows


def check_grafana() -> tuple[bool, list[str]]:
    health = grafana("/api/health")
    ds = grafana("/api/datasources/uid/{}/health".format(DATASOURCE_UID))
    ok = health.get("database") == "ok" and ds.get("status") == "OK"
    return ok, [
        "{} Grafana {} (internal database: {})".format(
            "+" if health.get("database") == "ok" else "-",
            health.get("version"), health.get("database")),
        "{} datasource {}: {}".format("+" if ds.get("status") == "OK" else "-",
                                      DATASOURCE_UID, ds.get("message")),
    ]


def latest_billed_day() -> str | None:
    """What the billing dashboard's $sim_day variable defaults to."""
    result = grafana("/api/ds/query", {
        "from": "now-30m", "to": "now",
        "queries": [{"refId": "A", "datasource": {"uid": DATASOURCE_UID}, "format": "table",
                     "rawQuery": True, "rawSql": "SELECT max(sim_day)::text AS d FROM daily_bill"}],
    })["results"]["A"]
    values = result["frames"][0]["data"]["values"]
    return values[0][0] if values and values[0] else None


def check_dashboard(uid: str, sim_day: str | None) -> tuple[bool, list[str]]:
    dashboard = grafana("/api/dashboards/uid/{}".format(uid))["dashboard"]
    ok, lines = True, ["+ provisioned as \"{}\" ({} panels)".format(
        dashboard["title"], len(dashboard["panels"]))]
    for panel in dashboard["panels"]:
        target = panel["targets"][0]
        sql = target["rawSql"]
        if "$sim_day" in sql:
            if sim_day is None:
                ok = False
                lines.append("- {}: nothing billed yet, so no day to show".format(panel["title"]))
                continue
            sql = sql.replace("$sim_day", sim_day)
        title = panel["title"].replace("$sim_day", sim_day or "?")
        try:
            rows = run_sql(sql, target.get("format", "table"))
        except RuntimeError as exc:
            ok = False
            lines.append("- {}: query failed: {}".format(title, exc))
            continue
        if rows == 0 and panel["title"] not in MAY_BE_EMPTY:
            ok = False
            lines.append("- {}: no rows".format(title))
        else:
            lines.append("{} {}: {} row(s)".format("+" if rows else "~", title, rows))
    return ok, lines


def main() -> int:
    print("\n  Smart Grid Lambda Platform -- Phase 5 dashboard check")
    print("  " + "-" * 58)
    print("\n  Grafana      {}".format(base_url()))

    try:
        sim_day = latest_billed_day()
    except Exception:  # noqa: BLE001 - reported by the Grafana check below
        sim_day = None
    print("  Billed day   {}".format(sim_day or "(none yet)"))

    checks = [("Grafana + datasource", check_grafana)]
    checks += [(name, lambda uid=uid: check_dashboard(uid, sim_day))
               for uid, name in DASHBOARDS.items()]

    failures = []
    for label, check in checks:
        try:
            ok, lines = check()
        except Exception as exc:  # noqa: BLE001 - a broken check is a failed check
            ok, lines = False, ["- {}: {}".format(type(exc).__name__, exc)]
        if not ok:
            failures.append(label)
        print("\n  {:<22} [{}]".format(label, "PASS" if ok else "FAIL"))
        for line in lines:
            print("      " + line)
        if not ok and label == "Grafana + datasource":
            break

    print("\n  " + "-" * 58)
    if failures:
        print("  PHASE 5 CHECKPOINT: FAILED  ({})".format(", ".join(failures)))
        print("  Logs: docker compose logs --tail 40 grafana\n")
        return 1
    print("  PHASE 5 CHECKPOINT: PASSED")
    print("  Dashboards: {0}/d/sg-live  and  {0}/d/sg-billing\n".format(base_url()))
    return 0


if __name__ == "__main__":
    sys.exit(main())

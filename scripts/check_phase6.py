#!/usr/bin/env python3
"""Phase 6 checkpoint: is the pipeline observable, and does the health check fire?

    python scripts/check_phase6.py                              # healthy state
    python scripts/check_phase6.py --expect-firing NoMeterData  # after stopping the producer

The plan's checkpoint is "kill the producer -> the no-data health-check alert
fires and is visible". The first form proves the monitoring works while all is
well: every scrape target up, the alert rules loaded, events flowing, nothing
firing. The second waits for a named alert to reach `firing` in Prometheus --
the same state the Grafana Pipeline Health dashboard shows.

Standard library only.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

EXPECTED_JOBS = ("api", "producer", "prometheus")
EXPECTED_RULES = ("NoMeterData", "ProducerDown", "ProducerSendErrors", "ApiDown", "BillingRunFailed")
LAG_THRESHOLD_SECONDS = 30   # config.yaml alerts.no_data_real_seconds


def prometheus_url() -> str:
    return "http://localhost:{}".format(os.environ.get("PROMETHEUS_PORT", "9090"))


def grafana_url() -> str:
    return "http://localhost:{}".format(os.environ.get("GRAFANA_PORT", "3000"))


def get_json(url: str, auth: bool = False) -> dict:
    headers = {}
    if auth:
        creds = "{}:{}".format(os.environ.get("GRAFANA_ADMIN_USER", "admin"),
                               os.environ.get("GRAFANA_ADMIN_PASSWORD", "admin"))
        headers["Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=15) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError("{} returned HTTP {}".format(url, exc.code)) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError("{} unreachable: {}".format(url, exc.reason)) from exc


def promql(expr: str) -> float | None:
    """Instant query; the first sample's value, or None if the result is empty."""
    url = prometheus_url() + "/api/v1/query?query=" + urllib.parse.quote(expr)
    result = get_json(url)["data"]["result"]
    return float(result[0]["value"][1]) if result else None


def alert_states() -> dict[str, str]:
    """alertname -> pending/firing, for every alert that is not inactive."""
    alerts = get_json(prometheus_url() + "/api/v1/alerts")["data"]["alerts"]
    return {a["labels"]["alertname"]: a["state"] for a in alerts}


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------

def check_targets(expect_healthy: bool) -> tuple[bool, list[str]]:
    targets = get_json(prometheus_url() + "/api/v1/targets")["data"]["activeTargets"]
    health = {t["labels"]["job"]: (t["health"], t.get("lastError", "")) for t in targets}
    ok, lines = True, []
    for job in EXPECTED_JOBS:
        state, error = health.get(job, ("missing", "not in prometheus.yml"))
        # With the producer deliberately stopped, its target being down is the point.
        required = expect_healthy or job != "producer"
        if state != "up" and required:
            ok = False
        mark = "+" if state == "up" else ("-" if required else "~")
        lines.append("{} {:<11} {}{}".format(mark, job, state, "  ({})".format(error) if error else ""))
    return ok, lines


def check_rules() -> tuple[bool, list[str]]:
    groups = get_json(prometheus_url() + "/api/v1/rules")["data"]["groups"]
    loaded = {rule["name"] for group in groups for rule in group["rules"]}
    missing = [name for name in EXPECTED_RULES if name not in loaded]
    lines = ["+ {} alert rule(s) loaded: {}".format(len(loaded), ", ".join(sorted(loaded)))]
    if missing:
        lines.append("- missing: {}".format(", ".join(missing)))
    return not missing, lines


def check_metrics(expect_healthy: bool) -> tuple[bool, list[str]]:
    rate = promql("rate(smartgrid_producer_events_total[1m])")
    lag = promql("smartgrid_serving_lag_seconds")
    duration = promql("smartgrid_batch_last_duration_seconds")
    ok = True
    lines = []
    if expect_healthy and not rate:
        ok = False
    lines.append("{} producer throughput: {} events/s (expected ~20)".format(
        "+" if rate else ("-" if expect_healthy else "~"),
        "none" if rate is None else round(rate, 1)))
    lag_ok = lag is not None and lag <= LAG_THRESHOLD_SECONDS
    if expect_healthy and not lag_ok:
        ok = False
    lines.append("{} end-to-end lag: {} s (health-check threshold {} s)".format(
        "+" if lag_ok else ("-" if expect_healthy else "~"),
        "none" if lag is None else round(lag, 1), LAG_THRESHOLD_SECONDS))
    lines.append("{} last billing run: {}".format(
        "+" if duration else "~",
        "{:.1f} s".format(duration) if duration else "none recorded yet"))
    return ok, lines


def check_grafana() -> tuple[bool, list[str]]:
    ds = get_json(grafana_url() + "/api/datasources/uid/sg-prometheus/health", auth=True)
    dash = get_json(grafana_url() + "/api/dashboards/uid/sg-ops", auth=True)["dashboard"]
    ok = ds.get("status") == "OK"
    return ok, [
        "{} Prometheus datasource: {}".format("+" if ok else "-", ds.get("message")),
        "+ dashboard \"{}\" provisioned ({} panels)".format(dash["title"], len(dash["panels"])),
    ]


def check_no_alerts_firing() -> tuple[bool, list[str]]:
    states = alert_states()
    firing = sorted(name for name, state in states.items() if state == "firing")
    if firing:
        return False, ["- firing: {}".format(", ".join(firing))]
    pending = sorted(name for name, state in states.items() if state == "pending")
    return True, ["+ nothing firing" + ("  (pending: {})".format(", ".join(pending)) if pending else "")]


def wait_for_firing(name: str, timeout: int) -> tuple[bool, list[str]]:
    deadline = time.time() + timeout
    state = "inactive"
    while time.time() < deadline:
        state = alert_states().get(name, "inactive")
        if state == "firing":
            break
        print("      ... {} is {}; waiting".format(name, state), flush=True)
        time.sleep(10)
    lines = ["{} {} is {}".format("+" if state == "firing" else "-", name, state)]
    others = sorted(n for n, s in alert_states().items() if s == "firing" and n != name)
    if others:
        lines.append("~ also firing: {}".format(", ".join(others)))
    return state == "firing", lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expect-firing", metavar="ALERT",
                        help="wait until this alert fires (e.g. NoMeterData)")
    parser.add_argument("--timeout", type=int, default=120,
                        help="seconds to wait with --expect-firing (default 120)")
    args = parser.parse_args()
    healthy = args.expect_firing is None

    print("\n  Smart Grid Lambda Platform -- Phase 6 observability check")
    print("  " + "-" * 58)
    print("\n  Prometheus   {}".format(prometheus_url()))
    print("  Grafana      {}".format(grafana_url()))
    print("  Mode         {}".format("healthy pipeline" if healthy
                                     else "waiting for {} to fire".format(args.expect_firing)))

    checks = [
        ("Scrape targets", lambda: check_targets(healthy)),
        ("Alert rules", check_rules),
        ("Pipeline metrics", lambda: check_metrics(healthy)),
        ("Grafana ops view", check_grafana),
    ]
    if healthy:
        checks.append(("Alert state", check_no_alerts_firing))
    else:
        checks.append(("Health-check alert", lambda: wait_for_firing(args.expect_firing, args.timeout)))

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
            print("      " + line)

    print("\n  " + "-" * 58)
    if failures:
        print("  PHASE 6 CHECKPOINT: FAILED  ({})".format(", ".join(failures)))
        print("  Logs: docker compose logs --tail 40 prometheus api\n")
        return 1
    print("  PHASE 6 CHECKPOINT: PASSED")
    print("  Alerts: {}/alerts   Dashboard: {}/d/sg-ops\n".format(prometheus_url(), grafana_url()))
    return 0


if __name__ == "__main__":
    sys.exit(main())

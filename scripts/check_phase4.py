#!/usr/bin/env python3
"""Phase 4 checkpoint: did the batch layer bill a day, and is the bill right?

    python scripts/check_phase4.py
    python scripts/check_phase4.py --sim-day 20260127 --household H-101

The plan's checkpoint is "a hand-calculated bill matches `daily_bill`". This
script does that calculation for you and shows its working: it re-reads one
household's readings straight from the Parquet master store with pyarrow, sums
them in Python, applies the same billing rules, and compares every column
against the row Spark wrote.

That comparison is worth something only because the two paths share nothing but
the rules: Spark aggregated the store over a cluster-shaped code path, this
sums the same files in a loop. Agreement then means the numbers are right rather
than that one implementation was run twice.

Needs no JVM: pyarrow reads the Parquet, psycopg reads the serving tables, boto3
reads the tariff and the published report.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from common.config import load_config  # noqa: E402
from common.storage import get_text, s3_client  # noqa: E402
from processing.shared.billing import (  # noqa: E402
    BillingTerms,
    bill_household,
    parse_tariff_csv,
    tariff_object_key,
)

# Two float computations over the same values in a different order, so the last
# bits may differ: 0.1 Wh on energy and a cent on money.
KWH_TOLERANCE = 1e-4
MONEY_TOLERANCE = 0.01


def endpoint(cfg) -> str:
    return cfg.postgres_dsn.split("@")[-1].split("?")[0]


def api_base_url() -> str:
    return "http://localhost:{}".format(os.environ.get("API_PORT", "8000"))


def api_get(path: str) -> dict:
    url = api_base_url() + path
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError("{} returned HTTP {}".format(url, exc.code)) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError("{} is unreachable ({}). Is the api service up?".format(
            url, exc.reason)) from exc


def query(cfg, sql: str, params: tuple | None = None) -> list[dict]:
    with psycopg.connect(cfg.postgres_dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()


def resolve_day(cfg, requested: str | None) -> date | None:
    """The day to check: the one asked for, or the most recently billed."""
    if requested:
        digits = requested.replace("-", "")
        return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
    rows = query(cfg, "SELECT max(sim_day) AS day FROM daily_bill")
    return rows[0]["day"]


def recompute_from_master_store(cfg, sim_day: date, household_id: str) -> dict:
    """Sum one household's day out of the Parquet files, in plain Python.

    Deduplicated on (meter_id, timestamp) exactly as `read_raw_readings` does:
    the raw sink is at-least-once, so a replayed micro-batch is in the files and
    must not be counted twice here either -- otherwise this "independent" check
    would report a disagreement that is really its own bug.
    """
    root = Path(cfg.raw_store_path) / "sim_day={}".format(sim_day.isoformat())
    if not root.exists():
        raise RuntimeError("no partition for {} under {}".format(sim_day, root))

    seen: set = set()
    consumption = solar = 0.0
    for parquet_file in sorted(root.rglob("*.parquet")):
        if any(part.startswith(("_", ".")) for part in parquet_file.relative_to(root).parts):
            continue
        table = pq.read_table(parquet_file, columns=[
            "meter_id", "household_id", "timestamp",
            "power_consumption_kwh", "solar_generation_kwh",
        ])
        for row in table.to_pylist():
            if row["household_id"] != household_id:
                continue
            key = (row["meter_id"], row["timestamp"])
            if key in seen:
                continue
            seen.add(key)
            consumption += row["power_consumption_kwh"]
            solar += row["solar_generation_kwh"]

    if not seen:
        raise RuntimeError("{} has no readings in the {} partition".format(
            household_id, sim_day))
    return {"readings": len(seen),
            "total_consumption_kwh": round(consumption, 4),
            "total_solar_kwh": round(solar, 4)}


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------

def check_batch_run(cfg, sim_day: date) -> tuple[bool, list[str]]:
    """Was this day billed by a run that finished, and does it own every row?"""
    runs = query(cfg, """
        SELECT run_id, status, rows_written, started_at, finished_at, notes
          FROM pipeline_runs
         WHERE component = 'billing_batch' AND sim_day = %s
         ORDER BY started_at DESC
    """, (sim_day,))
    if not runs:
        return False, [
            "- no billing_batch run recorded for {}".format(sim_day),
            "  Trigger one:  docker compose exec airflow \\",
            "      airflow dags trigger daily_billing --conf '{{\"sim_day\": \"{}\"}}'"
            .format(sim_day.strftime("%Y%m%d")),
        ]

    newest = runs[0]
    lines = ["{} newest run {} finished {}".format(
        "+" if newest["status"] == "SUCCESS" else "-",
        newest["run_id"], newest["status"])]
    if newest["finished_at"]:
        lines.append("  wrote {} row(s) in {:.1f}s".format(
            newest["rows_written"],
            (newest["finished_at"] - newest["started_at"]).total_seconds()))
    if len(runs) > 1:
        # Re-runnability is a claim this makes visible rather than asserts: the
        # day has been billed more than once and still holds one row per household.
        lines.append("+ billed {} times in total; the upsert kept one row per "
                     "household".format(len(runs)))

    owners = query(cfg, "SELECT DISTINCT run_id FROM daily_bill WHERE sim_day = %s",
                   (sim_day,))
    one_owner = len(owners) == 1
    lines.append("{} every row for the day carries one run_id{}".format(
        "+" if one_owner else "-",
        "" if one_owner else ": {}".format(sorted(r["run_id"] for r in owners))))
    traced = one_owner and owners[0]["run_id"] == newest["run_id"]
    lines.append("{} daily_bill.run_id matches the newest run, so the rows trace "
                 "back to its logs".format("+" if traced else "-"))
    return newest["status"] == "SUCCESS" and traced, lines


def check_coverage(cfg, sim_day: date) -> tuple[bool, list[str]]:
    """One bill per household, every zone represented, nothing negative."""
    rows = query(cfg, """
        SELECT grid_zone,
               count(*) AS households,
               round(sum(total_consumption_kwh)::numeric, 3) AS consumption_kwh,
               round(sum(total_solar_kwh)::numeric, 3)       AS solar_kwh,
               round(sum(net_kwh)::numeric, 3)               AS net_kwh,
               round(sum(net_amount)::numeric, 2)            AS net_amount,
               count(*) FILTER (WHERE subsidy_flag)          AS subsidised
          FROM daily_bill WHERE sim_day = %s
         GROUP BY grid_zone ORDER BY grid_zone
    """, (sim_day,))
    if not rows:
        return False, ["- daily_bill holds nothing for {}".format(sim_day)]

    currency = cfg.billing["currency"]
    lines = ["", "      {:<16}{:>7}{:>13}{:>11}{:>14}".format(
        "grid_zone", "bills", "net kWh", "subsidised", "payable " + currency)]
    for row in rows:
        lines.append("      {:<16}{:>7}{:>13}{:>11}{:>14}".format(
            row["grid_zone"], row["households"], row["net_kwh"],
            row["subsidised"], row["net_amount"]))

    billed = sum(row["households"] for row in rows)
    complete = billed == cfg.total_meters
    lines.append("")
    lines.append("{} {} of {} configured households billed".format(
        "+" if complete else "-", billed, cfg.total_meters))
    if not complete:
        lines.append("  A household with no readings on this simulated day is not")
        lines.append("  billed at all. That is honest, but it means the producer was")
        lines.append("  not running for the whole day -- check scripts/check_phase2.py.")

    missing_zones = set(cfg.zone_names) - {row["grid_zone"] for row in rows}
    lines.append("{} every configured zone appears{}".format(
        "+" if not missing_zones else "-",
        "" if not missing_zones else ": missing {}".format(sorted(missing_zones))))

    bad = query(cfg, """
        SELECT count(*) AS n FROM daily_bill
         WHERE sim_day = %s AND (net_kwh < 0 OR net_amount < 0
                                 OR net_kwh > total_consumption_kwh)
    """, (sim_day,))[0]["n"]
    lines.append("{} no bill is negative or charges more units than were used"
                 .format("+" if bad == 0 else "-"))
    return complete and not missing_zones and bad == 0, lines


def check_hand_calculation(cfg, sim_day: date, household: str | None) -> tuple[bool, list[str]]:
    """The checkpoint itself: recompute one bill from the raw files and compare."""
    stored = query(cfg, """
        SELECT * FROM daily_bill
         WHERE sim_day = %s AND (%s::text IS NULL OR household_id = %s::text)
         ORDER BY (subsidy_flag AND total_solar_kwh > 0) DESC, household_id
         LIMIT 1
    """, (sim_day, household, household))
    if not stored:
        return False, ["- no stored bill for {} on {}".format(household or "any household",
                                                              sim_day)]
    bill = stored[0]
    terms = BillingTerms.from_config(cfg.billing)

    measured = recompute_from_master_store(cfg, sim_day, bill["household_id"])
    tariff_rows = parse_tariff_csv(get_text(
        s3_client(cfg), cfg.bucket("tariff"),
        tariff_object_key(cfg.storage["tariff_filename_pattern"], sim_day)))
    tariff = next(r for r in tariff_rows if r["household_id"] == bill["household_id"])

    recomputed = bill_household(
        dict(measured, household_id=bill["household_id"], grid_zone=bill["grid_zone"]),
        tariff, terms)

    currency = terms.currency
    lines = [
        "",
        "      {} on {} -- recomputed from {} reading(s) in the master store:".format(
            bill["household_id"], sim_day, measured["readings"]),
        "",
        "      {:<34}{:>14}{:>14}".format("", "recomputed", "stored"),
    ]
    comparisons = [
        ("consumption                   kWh", "total_consumption_kwh", KWH_TOLERANCE, 4),
        ("solar generation              kWh", "total_solar_kwh", KWH_TOLERANCE, 4),
        ("net = max(used - solar, 0)    kWh", "net_kwh", KWH_TOLERANCE, 4),
        ("gross = net x {:<6.2f}          {}".format(tariff["tariff_rate"], currency),
         "gross_amount", MONEY_TOLERANCE, 2),
        ("subsidy @ {:>4.0%}{:<15}  {}".format(
            terms.subsidy_pct, "" if tariff["subsidy_flag"] else " (not subsidised)",
            currency), "subsidy_amount", MONEY_TOLERANCE, 2),
        ("payable                       {}".format(currency),
         "net_amount", MONEY_TOLERANCE, 2),
    ]
    ok = True
    for label, field, tolerance, places in comparisons:
        mine, theirs = float(recomputed[field]), float(bill[field])
        agrees = abs(mine - theirs) <= tolerance
        ok &= agrees
        lines.append("      {} {:<32}{:>14}{:>14}".format(
            "+" if agrees else "-", label,
            round(mine, places), round(theirs, places)))

    lines.append("")
    lines.append("{} tier {} on the {} tariff file{}".format(
        "+" if tariff["billing_tier"] == bill["billing_tier"] else "-",
        bill["billing_tier"], sim_day,
        ", subsidised" if bill["subsidy_flag"] else ""))
    ok &= tariff["billing_tier"] == bill["billing_tier"]
    if not ok:
        lines.append("  A disagreement here means the batch layer and this script read")
        lines.append("  different data or different rules -- not a rounding artefact.")
    return ok, lines


def check_report(cfg, sim_day: date) -> tuple[bool, list[str]]:
    """The published artefact: one CSV per billed day in the reports bucket."""
    bucket = cfg.bucket("reports")
    key = cfg.storage["report_filename_pattern"].format(sim_day=sim_day.strftime("%Y%m%d"))
    try:
        text = get_text(s3_client(cfg), bucket, key)
    except Exception as exc:  # noqa: BLE001 - absence is the finding
        return False, ["- s3://{}/{} is not there ({})".format(bucket, key,
                                                               type(exc).__name__)]

    data_rows = [line for line in text.strip().splitlines()[1:] if line]
    expected = query(cfg, "SELECT count(*) AS n FROM daily_bill WHERE sim_day = %s",
                     (sim_day,))[0]["n"]
    matches = len(data_rows) == expected
    return matches, [
        "+ s3://{}/{} published".format(bucket, key),
        "{} {} data row(s), matching daily_bill's {}".format(
            "+" if matches else "-", len(data_rows), expected),
        "  first row: {}".format(data_rows[0] if data_rows else "(empty)"),
    ]


def check_api(cfg, sim_day: date) -> tuple[bool, list[str]]:
    """Does the serving API expose the batch layer's output, and agree with it?"""
    days = api_get("/bills?limit=5")
    listed = {row["sim_day"] for row in days["days"]}
    present = sim_day.isoformat() in listed
    lines = ["{} /bills lists {} billed day(s){}".format(
        "+" if present else "-", days["day_count"],
        "" if present else "; {} is not among {}".format(sim_day, sorted(listed)))]

    served = api_get("/bills/{}".format(sim_day.isoformat()))
    stored = query(cfg, """
        SELECT count(*) AS households,
               round(sum(net_amount)::numeric, 2)::float8 AS net_amount
          FROM daily_bill WHERE sim_day = %s
    """, (sim_day,))[0]
    agrees = (served["households"] == stored["households"]
              and abs(served["total_net_amount"] - float(stored["net_amount"]))
              <= MONEY_TOLERANCE)
    lines.append("{} /bills/{} serves {} bills totalling {} {}, agreeing with Postgres"
                 .format("+" if agrees else "-", sim_day, served["households"],
                         served["total_net_amount"], served["currency"]))

    unknown = "404 as expected"
    ok_unknown = True
    try:
        api_get("/bills/1999-01-01")
        unknown, ok_unknown = "200 -- an unbilled day should be a 404", False
    except RuntimeError as exc:
        if "HTTP 404" not in str(exc):
            unknown, ok_unknown = str(exc), False
    lines.append("{} an unbilled day: {}".format("+" if ok_unknown else "-", unknown))
    return present and agrees and ok_unknown, lines


def report_lambda_agreement(cfg, sim_day: date) -> list[str]:
    """Informational: the two layers' totals for the same simulated day.

    Not a pass/fail check. The speed layer only holds windows for the period it
    was actually running, and it counts meters approximately, so a gap here
    usually means the streaming job was started later than the producer -- which
    is precisely why the batch layer exists and why it, not the stream, is what
    anyone is billed from.
    """
    rows = query(cfg, """
        SELECT b.grid_zone,
               round(b.consumption::numeric, 2)  AS batch_kwh,
               round(s.consumption::numeric, 2)  AS speed_kwh,
               s.windows
          FROM (SELECT grid_zone, sum(total_consumption_kwh) AS consumption
                  FROM daily_bill WHERE sim_day = %s GROUP BY grid_zone) b
          LEFT JOIN (SELECT grid_zone, sum(total_consumption_kwh) AS consumption,
                            count(*) AS windows
                       FROM zone_metrics
                      WHERE window_start >= %s::date
                        AND window_start <  %s::date + 1
                      GROUP BY grid_zone) s USING (grid_zone)
         ORDER BY 1
    """, (sim_day, sim_day, sim_day))

    lines = ["", "      {:<16}{:>14}{:>14}{:>10}{:>12}".format(
        "grid_zone", "batch kWh", "speed kWh", "windows", "difference")]
    for row in rows:
        if row["speed_kwh"] is None:
            lines.append("      {:<16}{:>14}{:>14}{:>10}{:>12}".format(
                row["grid_zone"], row["batch_kwh"], "-", 0, "no windows"))
            continue
        delta = float(row["batch_kwh"]) - float(row["speed_kwh"])
        lines.append("      {:<16}{:>14}{:>14}{:>10}{:>11.1f}%".format(
            row["grid_zone"], row["batch_kwh"], row["speed_kwh"], row["windows"],
            100 * delta / float(row["batch_kwh"]) if row["batch_kwh"] else 0.0))
    lines.append("")
    lines.append("      The batch figure is the authoritative one. The speed layer")
    lines.append("      only covers the windows it was running for, so a difference")
    lines.append("      here is coverage, not disagreement about arithmetic.")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sim-day", metavar="YYYYMMDD",
                        help="which simulated day to check (default: the most "
                             "recently billed one)")
    parser.add_argument("--household", metavar="H-101",
                        help="which household to hand-calculate (default: a "
                             "subsidised one with solar)")
    args = parser.parse_args()
    cfg = load_config()

    print("\n  Smart Grid Lambda Platform -- Phase 4 batch billing check")
    print("  " + "-" * 58)
    print("\n  Serving DB   {}".format(endpoint(cfg)))
    print("  Master store {}".format(cfg.raw_store_path))

    try:
        sim_day = resolve_day(cfg, args.sim_day)
    except psycopg.Error as exc:
        print("\n  - cannot reach the serving database: {}".format(exc))
        print("\n  PHASE 4 CHECKPOINT: FAILED\n")
        return 1

    if sim_day is None:
        print("\n  daily_bill is empty: no simulated day has been billed yet.")
        print("\n  Run the DAG for a day the producer covered:")
        print("    docker compose exec airflow \\")
        print("      airflow dags trigger daily_billing --conf '{\"sim_day\": \"YYYYMMDD\"}'")
        print("\n  PHASE 4 CHECKPOINT: FAILED\n")
        return 1

    print("  Billed day   {}   ({} to {}, simulated)".format(
        sim_day, sim_day, sim_day + timedelta(days=1)))

    checks = [
        ("Batch run", lambda: check_batch_run(cfg, sim_day)),
        ("Bill coverage", lambda: check_coverage(cfg, sim_day)),
        ("Hand calculation", lambda: check_hand_calculation(cfg, sim_day, args.household)),
        ("Published report", lambda: check_report(cfg, sim_day)),
        ("Serving API", lambda: check_api(cfg, sim_day)),
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

    print("\n  {:<20} [{}]".format("Lambda cross-check", "INFO"))
    for line in report_lambda_agreement(cfg, sim_day):
        print(line)

    print("\n  " + "-" * 58)
    if failures:
        print("  PHASE 4 CHECKPOINT: FAILED  ({})".format(", ".join(failures)))
        print("  Logs: docker compose logs --tail 60 airflow\n")
        return 1
    print("  PHASE 4 CHECKPOINT: PASSED")
    print("  Airflow UI: http://localhost:{}  (admin / admin)\n".format(
        os.environ.get("AIRFLOW_PORT", "8080")))
    return 0


if __name__ == "__main__":
    sys.exit(main())

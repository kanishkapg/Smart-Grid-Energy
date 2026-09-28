"""Daily tariff file: one CSV per simulated day, dropped into object storage.

    python -m sources.tariff_generator --once        # write today's file, exit
    python -m sources.tariff_generator               # write, then one per sim-day
    python -m sources.tariff_generator --sim-day 20260103

This is the batch-layer input, and the reason the architecture is Lambda rather
than Kappa: the tariff genuinely arrives as a file on a daily boundary, so it
stays a file instead of being forced onto a Kafka topic.

Two properties matter for billing to be defensible:

  * A household's **tier and subsidy status are stable** across days -- they are
    derived by hashing the household id, not by rolling dice, so H-101 is on the
    same tier in every file and in every process.
  * The **rate itself varies daily** within a small band, because that is what a
    published daily tariff does. It is seeded by (household, day), so
    regenerating any past day reproduces that day's file exactly.
"""

from __future__ import annotations

import argparse
import csv
import io
import time
from datetime import date

from common.config import load_config
from common.logging_setup import log_startup, new_run_id, setup_logging
from common.sim_clock import SimClock
from common.storage import put_text, s3_client
from sources.meter_model import build_meters, stable_fraction

CSV_COLUMNS = ("household_id", "tariff_rate", "billing_tier", "subsidy_flag")


def assign_tier(household_id: str, tier_weights: dict[str, float]) -> str:
    """Pick a billing tier deterministically from the household id.

    Weights need not sum to exactly 1; they are normalised here so editing
    config.yaml cannot silently leave a household without a tier.
    """
    total = sum(tier_weights.values())
    position = stable_fraction("tier", household_id) * total
    cumulative = 0.0
    for tier, weight in sorted(tier_weights.items()):
        cumulative += weight
        if position < cumulative:
            return tier
    return sorted(tier_weights)[-1]      # float rounding at the very top edge


def has_subsidy(household_id: str, probability: float) -> bool:
    return stable_fraction("subsidy", household_id) < probability


def daily_rate(household_id: str, sim_day: date, base_rate: float, jitter_pct: float) -> float:
    """The tier's rate nudged by up to +/- jitter_pct, stable for a given day."""
    offset = (stable_fraction("rate", household_id, sim_day.isoformat()) * 2) - 1
    return round(base_rate * (1 + offset * jitter_pct), 2)


def build_rows(cfg, sim_day: date) -> list[dict]:
    """One tariff row per household, for the given simulated day."""
    billing = cfg.billing
    tiers, weights = billing["tiers"], billing["tier_weights"]
    subsidy_probability = float(billing["subsidy_probability"])
    jitter_pct = float(billing["rate_jitter_pct"])

    rows = []
    for meter in build_meters(cfg):
        tier = assign_tier(meter.household_id, weights)
        rows.append({
            "household_id": meter.household_id,
            "tariff_rate": daily_rate(
                meter.household_id, sim_day, float(tiers[tier]["rate"]), jitter_pct
            ),
            "billing_tier": tier,
            "subsidy_flag": str(has_subsidy(meter.household_id, subsidy_probability)).lower(),
        })
    return rows


def rows_to_csv(rows: list[dict]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def tariff_key(cfg, sim_day: date) -> str:
    return cfg.storage["tariff_filename_pattern"].format(sim_day=sim_day.strftime("%Y%m%d"))


def write_tariff(cfg, client, sim_day: date, log) -> str:
    """Write one day's tariff file and return its object key."""
    rows = build_rows(cfg, sim_day)
    key = tariff_key(cfg, sim_day)
    bucket = cfg.bucket("tariff")
    put_text(client, bucket, key, rows_to_csv(rows))
    log.info("tariff_file_written", bucket=bucket, key=key, households=len(rows),
             sim_day=sim_day.isoformat(),
             subsidised=sum(r["subsidy_flag"] == "true" for r in rows))
    return key


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true",
                        help="write the current sim-day's file and exit")
    parser.add_argument("--sim-day", metavar="YYYYMMDD",
                        help="write this specific simulated day, then exit")
    parser.add_argument("--log-format", choices=("json", "console"), default="json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config()
    run_id = new_run_id("tariff_generator")
    log = setup_logging("tariff_generator", run_id=run_id,
                        level=cfg.logging["level"], fmt=args.log_format)
    log_startup(log, cfg, "tariff_generator")

    clock = SimClock.from_config(cfg)
    client = s3_client(cfg)

    if args.sim_day:
        write_tariff(cfg, client, date(
            int(args.sim_day[:4]), int(args.sim_day[4:6]), int(args.sim_day[6:8])
        ), log)
        return 0

    # Always write the current day first, so the batch layer is never waiting on
    # a boundary that is minutes away.
    write_tariff(cfg, client, clock.sim_day(), log)
    if args.once:
        return 0

    log.info("watching_for_sim_day_boundaries",
             next_boundary_in_real_seconds=round(clock.real_seconds_until_next_sim_day(), 1))
    try:
        while True:
            time.sleep(clock.real_seconds_until_next_sim_day() + 0.5)
            write_tariff(cfg, client, clock.sim_day(), log)
    except KeyboardInterrupt:
        log.info("tariff_generator_stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Phase 4: the batch layer -- authoritative daily billing.

    spark-submit /app/processing/billing_batch.py                  # the day that just closed
    spark-submit /app/processing/billing_batch.py --sim-day 20260127

This is the half of Lambda that is allowed to be slow and has to be right. It
recomputes one simulated day's bills from scratch:

    Parquet master store (sim_day partition)  ->  dedupe on (meter_id, timestamp)
      ->  aggregate per household (exact meter count)
      ->  join that day's tariff file
      ->  apply the billing rules
      ->  upsert into daily_bill   +   CSV report into object storage

Nothing is read from the speed layer, and nothing here trusts it. The two layers
share their arithmetic (`processing/shared/transforms.py`,
`processing/shared/billing.py`) and nothing else, which is what makes agreement
between them evidence rather than tautology.

Re-runnable by design: the aggregation is a pure function of the day's raw data,
the tariff file for a past day regenerates identically, and the write is an
upsert keyed on (sim_day, household_id). Running it twice for the same day is a
no-op, which is what lets Airflow retry a task without double-billing anyone.
"""

from __future__ import annotations

import argparse
import csv
import io
from datetime import date, timedelta

from pyspark.sql import functions as F

from common.config import load_config
from common.logging_setup import log_startup, new_run_id, setup_logging
from common.sim_clock import SimClock
from common.storage import get_text, put_text, s3_client
from processing.shared import pg
from processing.shared.billing import (
    BillingTerms,
    TARIFF_FIELDS,
    bill_household,
    parse_tariff_csv,
    summarise_bills,
    tariff_object_key,
)
from processing.shared.schemas import read_raw_readings
from processing.shared.spark_session import build_spark_session
from processing.shared.transforms import aggregate_energy

COMPONENT = "billing_batch"

# Report column order: usage first, then the terms applied to it, then money.
REPORT_COLUMNS = (
    "sim_day", "household_id", "grid_zone",
    "total_consumption_kwh", "total_solar_kwh", "net_kwh",
    "billing_tier", "tariff_rate", "subsidy_flag",
    "gross_amount", "subsidy_amount", "net_amount",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sim-day", metavar="YYYYMMDD",
                        help="the simulated day to bill (default: the one that "
                             "just closed, per the shared simulated clock)")
    parser.add_argument("--tariff-file", metavar="PATH",
                        help="read the tariff from a local file instead of from "
                             "object storage (offline debugging)")
    parser.add_argument("--allow-open-day", action="store_true",
                        help="bill a simulated day that has not finished yet; "
                             "the totals will be partial")
    parser.add_argument("--log-format", choices=("json", "console"), default="json")
    return parser.parse_args()


def parse_sim_day(value: str) -> date:
    """Accept YYYYMMDD, as the tariff filenames and partition labels use."""
    cleaned = value.replace("-", "")
    if len(cleaned) != 8 or not cleaned.isdigit():
        raise ValueError("--sim-day must look like 20260127, not {!r}".format(value))
    return date(int(cleaned[:4]), int(cleaned[4:6]), int(cleaned[6:8]))


def resolve_sim_day(args, clock: SimClock) -> date:
    """Which simulated day to bill, and a refusal to bill one still in progress.

    A day still being written would produce a bill that is simply too low --
    with no error, and with a primary key that stops the correct figure from
    being inserted later without an explicit re-run. So the default is the day
    that has just closed, and billing today needs `--allow-open-day`.
    """
    if args.sim_day:
        requested = parse_sim_day(args.sim_day)
        if requested >= clock.sim_day() and not args.allow_open_day:
            raise SystemExit(
                "refusing to bill {}: the simulated day is still in progress "
                "(it is now {} simulated). Bill the previous day, or pass "
                "--allow-open-day.".format(requested, clock.now().isoformat(" ", "seconds"))
            )
        return requested
    if clock.sim_day_index() < 1:
        raise SystemExit(
            "no simulated day has closed yet: the clock started "
            "{:.0f}s ago and a simulated day is {:.0f}s long."
            .format((clock.sim_day_progress() * clock.sim_day_real_seconds),
                    clock.sim_day_real_seconds)
        )
    return clock.sim_day() - timedelta(days=1)


def load_tariff(cfg, sim_day: date, tariff_file: str | None, log):
    """That day's tariff rows, from object storage or from a local file.

    Read with boto3 rather than by Spark over S3A. The file is forty rows: the
    parsing that matters is turning `"true"` into a bool and `"45.0"` into a
    float, which `parse_tariff_csv` does once for both this job and the
    verification script. Spark's part is the join, below.
    """
    if tariff_file:
        with open(tariff_file, encoding="utf-8") as handle:
            text = handle.read()
        log.info("tariff_loaded", source=tariff_file)
    else:
        bucket = cfg.bucket("tariff")
        key = tariff_object_key(cfg.storage["tariff_filename_pattern"], sim_day)
        try:
            text = get_text(s3_client(cfg), bucket, key)
        except Exception as exc:  # noqa: BLE001 - the message matters more than the type
            raise SystemExit(
                "no tariff file for {} at s3://{}/{} ({}). The generator can "
                "reproduce any past day exactly:\n"
                "    python -m sources.tariff_generator --sim-day {}"
                .format(sim_day, bucket, key, type(exc).__name__,
                        sim_day.strftime("%Y%m%d"))
            )
        log.info("tariff_loaded", source="s3://{}/{}".format(bucket, key))
    return parse_tariff_csv(text)


def daily_usage(spark, cfg, sim_day: date, log):
    """One row per household for the day: consumption, solar, meters.

    `read_raw_readings` deduplicates on (meter_id, timestamp) first -- the raw
    sink is at-least-once, and a replayed micro-batch would otherwise be billed
    twice. The meter count is *exact* here, unlike the speed layer's HyperLogLog
    estimate: the batch layer's job is to be right, and it is also a data-quality
    check, since one household is metered once.
    """
    readings = read_raw_readings(spark, cfg.raw_store_path, sim_day.isoformat()).cache()
    stored = readings.count()
    if stored == 0:
        raise SystemExit(
            "the master store holds no readings for {} under {}. Available days:\n"
            "    ls {}".format(sim_day, cfg.raw_store_path, cfg.raw_store_path)
        )

    expected = cfg.total_meters * cfg.readings_per_meter_per_sim_day
    log.info("day_read", sim_day=sim_day.isoformat(), readings=stored,
             expected_readings=expected,
             completeness_pct=round(100 * stored / expected, 1))

    usage = aggregate_energy(readings, ["household_id", "grid_zone"],
                             exact_meter_count=True)
    return usage, stored, expected


def join_tariff(spark, usage, tariff_rows):
    """Left-join the tariff onto the day's usage.

    Left, not inner: an inner join would silently drop a household whose tariff
    row is missing, and a household that vanishes from a bill run is exactly the
    failure that must be loud. The nulls are checked by the caller.
    """
    tariff = spark.createDataFrame(
        [tuple(row[field] for field in TARIFF_FIELDS) for row in tariff_rows],
        schema=list(TARIFF_FIELDS),
    )
    # Broadcast because the tariff is one small file per day while the usage side
    # comes from the master store; without it Spark would shuffle both.
    return usage.join(F.broadcast(tariff), on="household_id", how="left")


def bills_to_csv(bills: list[dict]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=REPORT_COLUMNS,
                            extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(bills)
    return buffer.getvalue()


def publish_report(cfg, sim_day: date, bills: list[dict], log) -> str:
    """Drop the day's bills into the reports bucket as a CSV.

    The serving tables answer queries; this is the artefact a billing department
    would actually be sent, and it is the visible output of the batch run in
    object storage.
    """
    bucket = cfg.bucket("reports")
    key = cfg.storage["report_filename_pattern"].format(sim_day=sim_day.strftime("%Y%m%d"))
    put_text(s3_client(cfg), bucket, key, bills_to_csv(bills))
    log.info("report_published", bucket=bucket, key=key, rows=len(bills))
    return "s3://{}/{}".format(bucket, key)


def main() -> int:
    args = parse_args()
    cfg = load_config()
    run_id = new_run_id(COMPONENT)
    log = setup_logging(COMPONENT, run_id=run_id, level=cfg.logging["level"],
                        fmt=args.log_format)
    log_startup(log, cfg, COMPONENT)

    clock = SimClock.from_config(cfg)
    sim_day = resolve_sim_day(args, clock)
    terms = BillingTerms.from_config(cfg.billing)
    log.info("billing_day", sim_day=sim_day.isoformat(), sim_now=clock.now().isoformat(),
             subsidy_pct=terms.subsidy_pct, net_metering=terms.net_metering,
             currency=terms.currency)

    superseded = pg.register_run(cfg.postgres_dsn, run_id, COMPONENT,
                                 notes="billing {}".format(sim_day.isoformat()),
                                 sim_day=sim_day)
    if superseded:
        log.warning("superseded_stale_runs", count=superseded,
                    detail="a previous billing run was killed rather than stopped")

    status, written = "SUCCESS", 0
    spark = None
    try:
        # The tariff is fetched before the Spark session is built, so a day whose
        # file has not landed fails in a second with a readable message rather
        # than after twenty seconds of JVM startup. It is also the cheaper check:
        # the other input is on local disk, this one is over the network.
        tariff_rows = load_tariff(cfg, sim_day, args.tariff_file, log)
        spark = build_spark_session(cfg, app_name="smartgrid-billing-batch")
        usage, readings, expected = daily_usage(spark, cfg, sim_day, log)
        joined = join_tariff(spark, usage, tariff_rows)

        # Collected deliberately: one row per household, and the billing rules are
        # plain Python so that money is testable without a JVM. See
        # processing/shared/billing.py for the reasoning and the scale limit.
        rows = [row.asDict() for row in joined.collect()]

        unpriced = sorted(r["household_id"] for r in rows if r["tariff_rate"] is None)
        if unpriced:
            raise SystemExit(
                "{} household(s) have no row in the {} tariff file: {}. Billing "
                "part of a zone would produce a plausible, wrong total, so this "
                "run writes nothing.".format(len(unpriced), sim_day, unpriced)
            )
        overmetered = sorted(r["household_id"] for r in rows if r["active_meters"] != 1)
        if overmetered:
            log.warning("unexpected_meter_count", households=overmetered,
                        detail="a household should be metered exactly once")

        bills = [
            dict(bill_household(row, row, terms), sim_day=sim_day)
            for row in sorted(rows, key=lambda r: r["household_id"])
        ]
        summary = summarise_bills(bills)

        with pg.connection(cfg.postgres_dsn) as conn:
            written = pg.upsert_daily_bills(conn, bills, run_id)
        report = publish_report(cfg, sim_day, bills, log)

        log.info("billing_complete", sim_day=sim_day.isoformat(), rows_written=written,
                 households_expected=cfg.total_meters, readings=readings,
                 readings_expected=expected, report=report,
                 currency=terms.currency, **summary)
        if written != cfg.total_meters:
            # Not fatal: a day the producer only partly covered is still billable,
            # and refusing to bill it would lose the data. But it must be said.
            log.warning("partial_day", billed=written, configured=cfg.total_meters,
                        detail="some households did not report on this simulated day")
    # SystemExit as well as Exception: the guards above refuse a bad run with a
    # one-line message rather than a traceback, and SystemExit is not an Exception,
    # so without naming it here the `finally` below would close the run as SUCCESS.
    except (Exception, SystemExit) as exc:  # noqa: BLE001 - the run must be recorded
        status = "FAILED"
        log.error("billing_failed", sim_day=sim_day.isoformat(), error=str(exc))
        raise
    finally:
        pg.finish_run(cfg.postgres_dsn, run_id, status, written)
        if spark is not None:
            spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

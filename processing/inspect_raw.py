"""Read the Parquet master store back with Spark and query a partition.

    spark-submit /app/processing/inspect_raw.py
    spark-submit /app/processing/inspect_raw.py --sim-day 2026-01-02

This is the Phase 2 checkpoint's second half -- "you can spark.read.parquet(...)
and query a partition" -- and it is also a rehearsal for Phase 4: the batch
billing job reads this same dataset the same way. If this prints sensible
per-zone totals, the batch layer has something authoritative to recompute from.
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from common.config import load_config
from processing.shared.schemas import dedupe_readings
from processing.shared.spark_session import build_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sim-day", metavar="YYYY-MM-DD",
                        help="query one simulated day (default: the most recent)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config()
    spark = build_spark_session(cfg, app_name="smartgrid-inspect-raw")
    path = cfg.raw_store_path

    # Cached deliberately: the raw sink is appending while this runs, so without
    # a fixed snapshot the "stored" and "deduplicated" counts would be taken at
    # different moments and their difference would be meaningless (it came out
    # negative the first time this was run against a live stream).
    stored = spark.read.parquet(path).cache()
    print(f"\n=== master store: {path} ===\n")
    stored.printSchema()

    total = stored.count()
    if total == 0:
        print("nothing stored yet -- is the raw-sink job running, and the producer?")
        return 1

    # The gap between the two counts is the at-least-once duplication the sink
    # can produce, which every reader of the store removes this same way.
    raw = dedupe_readings(stored)
    unique = raw.count()
    print(f"rows as stored:     {total:,}")
    print(f"rows after dedup:   {unique:,}   (natural key: meter_id + timestamp)")
    print(f"duplicate rows:     {total - unique:,}\n")

    print("=== rows per partition (sim_day / grid_zone) ===")
    (raw.groupBy("sim_day", "grid_zone")
        .agg(F.count("*").alias("rows"),
             F.countDistinct("meter_id").alias("meters"))
        .orderBy("sim_day", "grid_zone")
        .show(40, truncate=False))

    sim_day = args.sim_day or raw.selectExpr("max(sim_day) as d").first()["d"]
    print(f"=== one partition queried: sim_day = {sim_day} ===")
    print("Partition pruning means Spark reads only this day's directories.\n")

    # The same shape of aggregation the speed layer computes per window and the
    # batch layer computes per household -- proof the stored data is usable.
    (raw.filter(F.col("sim_day") == F.lit(sim_day))
        .groupBy("grid_zone")
        .agg(F.round(F.sum("power_consumption_kwh"), 3).alias("consumption_kwh"),
             F.round(F.sum("solar_generation_kwh"), 3).alias("solar_kwh"),
             F.countDistinct("meter_id").alias("active_meters"),
             F.min("timestamp").alias("first_event"),
             F.max("timestamp").alias("last_event"))
        .withColumn(
            "renewable_pct",
            F.round(100 * F.col("solar_kwh") / F.col("consumption_kwh"), 1),
        )
        .orderBy("grid_zone")
        .show(truncate=False))

    print("=== three sample rows ===")
    raw.filter(F.col("sim_day") == F.lit(sim_day)).show(3, truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

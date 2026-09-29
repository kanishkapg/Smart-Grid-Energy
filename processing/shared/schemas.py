"""The meter-reading contract as Spark sees it, plus the sim-day derivation.

Imported by the raw sink (Phase 2), the speed layer (Phase 3) and the batch
billing job (Phase 4), so all three agree on field names, types and -- crucially
-- on which simulated day a reading belongs to.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Declared explicitly, never inferred. Structured Streaming cannot infer a schema
# from an unbounded source, and an explicit schema also means a producer change
# fails loudly here instead of silently nulling a column.
READING_SCHEMA = StructType([
    StructField("meter_id", StringType(), nullable=False),
    StructField("household_id", StringType(), nullable=False),
    StructField("grid_zone", StringType(), nullable=False),
    StructField("power_consumption_kwh", DoubleType(), nullable=False),
    StructField("solar_generation_kwh", DoubleType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
])


def parse_reading_events(kafka_df: DataFrame) -> DataFrame:
    """Turn raw Kafka records into typed readings with a sim_day partition key.

    Takes the DataFrame that `spark.read/readStream.format("kafka")` produces --
    with its binary `key`/`value` and Kafka metadata -- and returns the reading
    columns plus:

        sim_day      the simulated calendar date, the raw store's partition key
        ingested_at  processing time, kept only so the raw store records when an
                     event landed as well as when it happened

    The event `timestamp` carries an offset (+05:30). `to_date` resolves it using
    the session time zone, which `build_spark_session` pins to the simulation's
    zone -- otherwise a reading at 02:00 Colombo time would be filed under the
    previous simulated day, and the billing totals for both days would be wrong.
    """
    return (
        kafka_df
        .select(F.from_json(F.col("value").cast("string"), READING_SCHEMA).alias("r"))
        # A record that is not valid JSON, or is missing a required field, parses
        # to a null struct. Dropping those here is the pipeline's cleaning step:
        # the master dataset must not contain rows the batch layer cannot bill.
        .filter(F.col("r").isNotNull() & F.col("r.meter_id").isNotNull())
        .select("r.*")
        .withColumn("sim_day", F.to_date(F.col("timestamp")))
        .withColumn("ingested_at", F.current_timestamp())
    )


# A meter emits at most one reading per instant, so this pair identifies a
# reading uniquely and is what makes duplicates detectable.
READING_NATURAL_KEY = ["meter_id", "timestamp"]


def dedupe_readings(df: DataFrame) -> DataFrame:
    """Collapse at-least-once duplicates. Separated so a caller that already
    holds a DataFrame -- e.g. one it cached to get a stable snapshot of a store
    still being appended to -- can dedupe it without re-reading."""
    return df.dropDuplicates(READING_NATURAL_KEY)


def read_raw_readings(spark, path: str, sim_day: str | None = None) -> DataFrame:
    """Read the Parquet master store, with at-least-once duplicates removed.

    The raw sink writes each micro-batch with `foreachBatch`, which is
    at-least-once: a batch whose files landed before the driver died is written
    again on retry. Deduplicating here, rather than at write time, is the
    standard data-lake answer -- the store keeps everything it ever received and
    readers agree on one interpretation of it.

    This matters directly for money: without it the Phase 4 billing job would
    double-count a replayed batch and overcharge those households.

    Passing `sim_day` filters on the partition column, so Spark reads only that
    day's directories instead of the whole dataset.
    """
    df = spark.read.parquet(path)
    if sim_day is not None:
        df = df.filter(F.col("sim_day") == F.lit(sim_day).cast("date"))
    return dedupe_readings(df)

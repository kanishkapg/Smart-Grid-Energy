"""The energy aggregation, written once and called by both Lambda layers.

This module is the project's answer to the standard objection to Lambda -- that
it forces you to maintain two implementations of the same logic. The speed layer
groups by (event-time window, grid_zone); the batch layer groups by
(sim_day, household_id). Those are different questions, but "sum what was
consumed, sum what was generated, count the meters, express solar as a share of
consumption" is one piece of arithmetic, and it lives here.

The one place the layers genuinely differ is the distinct-meter count, and that
difference is forced rather than chosen -- see `aggregate_energy`.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, functions as F

from common.sim_clock import window_alignment_minutes

# Rounded at the aggregation, not at display time, so the number stored in
# Postgres is the number every consumer sees. A kWh figure to 4 decimals is
# 0.1 Wh -- far below anything a meter would resolve.
KWH_PRECISION = 4


def aggregate_energy(
    df: DataFrame,
    group_cols: list[Column | str],
    exact_meter_count: bool = True,
) -> DataFrame:
    """Sum consumption and generation over `group_cols`, and count the meters.

    `exact_meter_count=False` switches to HyperLogLog. That is not a performance
    tweak: Spark rejects an exact `count(distinct ...)` inside a streaming
    aggregation, because exactness would mean retaining every meter id ever seen
    in each open window's state. So the speed layer counts approximately and the
    batch layer counts exactly -- which is the accuracy split the architecture
    decision already argues for, showing up in the code.

    With 9-12 meters per zone the approximation is in fact exact; HyperLogLog
    only starts to err at cardinalities far above this.
    """
    count_meters = F.count_distinct if exact_meter_count else F.approx_count_distinct
    return df.groupBy(*group_cols).agg(
        F.round(F.sum("power_consumption_kwh"), KWH_PRECISION).alias("total_consumption_kwh"),
        F.round(F.sum("solar_generation_kwh"), KWH_PRECISION).alias("total_solar_kwh"),
        count_meters("meter_id").alias("active_meters"),
    )


def with_renewable_pct(df: DataFrame) -> DataFrame:
    """Add solar generation as a percentage of consumption.

    Guarded against a zero denominator. It is not a theoretical case: a group
    with no consumption divides to null in Spark rather than raising, and a null
    would then violate the NOT NULL column in `zone_metrics` -- the write would
    fail for a reason that looks nothing like its cause.

    The value can exceed 100: a zone generating more than it consumes at midday
    is a real state of a solar grid, not an error, so it is not clamped.
    """
    return df.withColumn(
        "renewable_pct",
        F.when(
            F.col("total_consumption_kwh") > 0,
            F.round(100 * F.col("total_solar_kwh") / F.col("total_consumption_kwh"), 2),
        ).otherwise(F.lit(0.0)),
    )


def zone_window_metrics(
    readings: DataFrame,
    window_minutes: int,
    watermark_minutes: int,
    timezone: str,
) -> DataFrame:
    """Speed layer: per-zone metrics over tumbling event-time windows.

    Both interval arguments are in *simulated* minutes, because the event
    `timestamp` is a simulated instant. At 288x compression a 60-minute window
    closes every 12.5 real seconds.

    The watermark is what makes this job finite: it tells Spark how far
    out-of-order events may arrive, so state for older windows can be dropped
    instead of accumulating for the lifetime of the query. It is also what the
    producer's deliberately backdated events exercise -- they arrive late, land
    in the window they belong to, and are counted.
    """
    watermarked = readings.withWatermark(
        "timestamp", "{} minutes".format(watermark_minutes)
    )
    length = "{} minutes".format(window_minutes)
    aggregated = aggregate_energy(
        watermarked,
        [
            # Tumbling, so the slide equals the length. The slide has to be given
            # explicitly before the alignment offset can be.
            F.window("timestamp", length, length,
                     "{} minutes".format(window_alignment_minutes(timezone, window_minutes))),
            F.col("grid_zone"),
        ],
        exact_meter_count=False,
    )
    return with_renewable_pct(aggregated).select(
        # Flattened out of Spark's window struct here so the column names match
        # the `zone_metrics` table exactly and the sink needs no mapping.
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("grid_zone"),
        F.col("total_consumption_kwh"),
        F.col("total_solar_kwh"),
        F.col("renewable_pct"),
        F.col("active_meters"),
    )


# Java DateTimeFormatter pattern; XXX renders the offset as "+05:30".
_ISO_WITH_OFFSET = "yyyy-MM-dd HH:mm:ssXXX"


def collect_local_rows(df: DataFrame, timestamp_cols: list[str]) -> list[dict]:
    """Collect a small DataFrame to the driver, timestamps kept unambiguous.

    Only for results already reduced to a handful of rows -- an aggregated
    micro-batch or one day's bills.

    The conversion is the point. PySpark hands a TimestampType column back as a
    *naive* `datetime` built with `datetime.fromtimestamp`, which silently uses
    the driver container's local time zone -- UTC in this image -- not the
    session time zone. Two bugs follow from that, and neither raises: the alert
    rules would read Sri Lankan solar noon as 06:30 and never fire, and psycopg
    would hand a naive datetime to a TIMESTAMPTZ column for Postgres to reinterpret
    in its own zone, shifting every stored window by 5.5 hours.

    Formatting with `date_format` first puts the conversion in Spark, which does
    honour `spark.sql.session.timeZone`, and the offset in the string makes the
    datetime that comes back aware.
    """
    formatted = df
    for column in timestamp_cols:
        formatted = formatted.withColumn(column, F.date_format(column, _ISO_WITH_OFFSET))

    rows = []
    for row in formatted.collect():
        values = row.asDict()
        for column in timestamp_cols:
            if values.get(column) is not None:
                values[column] = datetime.fromisoformat(values[column])
        rows.append(values)
    return rows

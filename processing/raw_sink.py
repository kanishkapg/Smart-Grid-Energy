"""Phase 2: land every meter reading in the immutable Parquet master store.

    spark-submit /app/processing/raw_sink.py
    spark-submit /app/processing/raw_sink.py --trigger-seconds 5

This is the foundation the whole Lambda architecture rests on. The batch layer
is *defined* as recomputing billing from immutable raw data, so if this job is
wrong or lossy there is nothing to recompute from and the batch layer's claim to
be authoritative is empty.

Deliberately the dullest job in the project: read, parse, append. No
aggregation, no windowing, no joins. The master dataset must record what the
meters actually said, so that a future bug fix in the aggregation logic can be
replayed over history rather than being lost.

Output layout:

    s3a://raw/meter_readings/sim_day=2026-01-01/grid_zone=Kandy/part-*.parquet

Partitioning by `sim_day` first is what lets the Phase 4 billing job read exactly
one simulated day -- Spark prunes the other directories instead of scanning them.
"""

from __future__ import annotations

import argparse

from common.config import load_config
from common.logging_setup import log_startup, new_run_id, setup_logging
from processing.shared.schemas import parse_reading_events
from processing.shared.spark_session import build_spark_session, kafka_source_options


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trigger-seconds", type=int, default=None,
                        help="micro-batch interval (default: from config.yaml)")
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=("earliest", "latest"),
                        help="'earliest' replays the topic; the checkpoint wins on restart")
    parser.add_argument("--once", action="store_true",
                        help="process what is currently available, then exit")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config()
    run_id = new_run_id("raw_sink")
    log = setup_logging("raw_sink", run_id=run_id, level=cfg.logging["level"])
    log_startup(log, cfg, "raw_sink")

    spark_cfg = cfg.raw["spark"]
    trigger_seconds = args.trigger_seconds or int(spark_cfg["raw_sink_trigger_seconds"])
    output_path = cfg.raw_store_path
    checkpoint_path = f"{spark_cfg['checkpoint_root']}/raw_sink"
    # Declared in config.yaml so the writer here and the verification script
    # cannot disagree about the layout.
    partition_cols = list(cfg.storage["raw_partition_cols"])

    spark = build_spark_session(cfg, app_name="smartgrid-raw-sink")

    readings = parse_reading_events(
        spark.readStream.format("kafka")
        .options(**kafka_source_options(cfg, args.starting_offsets))
        .load()
    )

    log.info("starting_query", source_topic=cfg.kafka_topic,
             bootstrap=cfg.kafka_bootstrap, output_path=output_path,
             checkpoint_path=checkpoint_path,
             partition_by=partition_cols,
             trigger_seconds=trigger_seconds, once=args.once)

    def write_batch(batch_df, batch_id: int) -> None:
        """Append one micro-batch to the master store as a normal batch write."""
        (batch_df
            # Without this each of the topic's 3 partitions writes its own file
            # per zone, so one batch produces ~12 files instead of ~4. Grouping
            # by the partition columns first collapses that, which matters
            # because every extra small file is another object to open on a read.
            .repartition(*[batch_df[c] for c in partition_cols])
            .write
            .mode("append")
            .partitionBy(*partition_cols)
            .parquet(output_path))
        log.info("batch_written", batch_id=batch_id)

    writer = (
        readings.writeStream
        # Append is the only honest mode for a master dataset: a log of what was
        # measured, never updated or overwritten.
        .outputMode("append")
        # The checkpoint holds the Kafka offsets already committed, which is what
        # makes a restart resume rather than re-read from the beginning.
        .option("checkpointLocation", checkpoint_path)
        # foreachBatch instead of .format("parquet"):
        #
        # The built-in file sink maintains a commit log at <path>/_spark_metadata
        # to give exactly-once file visibility. That log cannot be written to
        # SeaweedFS -- S3A fails with "parent is not a dir", because the S3
        # gateway does not present the marker object as a directory. (Verified:
        # fs.s3a.directory.marker.retention=keep does not help.)
        #
        # foreachBatch does an ordinary partitioned write per micro-batch, so no
        # commit log is involved. The cost is that delivery becomes
        # **at-least-once**: if a batch's files land and the driver dies before
        # the offsets are committed, the retry writes those rows again. That is
        # handled where it matters rather than ignored -- read_raw_readings()
        # deduplicates on the natural key (meter_id, timestamp), and every reader
        # of the master store goes through it.
        .foreachBatch(write_batch)
    )

    # A trigger interval, rather than Spark's default of "as fast as possible",
    # is what keeps the file count sane: one file per partition per micro-batch,
    # so a tight trigger would bury the store in tiny files.
    if args.once:
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime=f"{trigger_seconds} seconds").start()

    try:
        _log_progress(query, log)
    except KeyboardInterrupt:
        log.info("stopping_query")
        query.stop()

    log.info("query_finished", rows_written=_rows_written(query))
    spark.stop()
    return 0


def _rows_written(query) -> int:
    return sum(int(p["numInputRows"]) for p in query.recentProgress)


def _log_progress(query, log) -> None:
    """Emit one structured log line per micro-batch that actually did work.

    Spark's own progress goes to the driver log as JSON blobs; this keeps the
    project's single structured-logging format across every stage instead.
    """
    seen = set()
    while query.isActive:
        query.awaitTermination(timeout=5)
        for progress in query.recentProgress:
            batch_id = progress["batchId"]
            rows = int(progress["numInputRows"])
            if batch_id in seen or rows == 0:
                seen.add(batch_id)
                continue
            seen.add(batch_id)
            log.info(
                "micro_batch",
                batch_id=batch_id,
                rows=rows,
                rows_per_second=round(progress.get("processedRowsPerSecond") or 0.0, 1),
                batch_duration_ms=progress.get("batchDuration"),
            )


if __name__ == "__main__":
    raise SystemExit(main())

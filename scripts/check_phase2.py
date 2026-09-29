#!/usr/bin/env python3
"""Phase 2 checkpoint: is the Parquet master store complete and queryable?

    python scripts/check_phase2.py

The plan's checkpoint is "row count in Parquet ~= events produced". This script
makes that precise by reconciling two independent sources of truth:

    events in Kafka  = sum(end offset - beginning offset) over all partitions
    rows in Parquet  = read from the object store

Parquet is expected to trail Kafka slightly -- by up to one micro-batch trigger,
plus whatever the producer has emitted since. It must not fall far behind.

Uses pyarrow, not Spark, so verifying Phase 2 needs no JVM on the host. Files are
walked and read explicitly rather than through pyarrow's dataset auto-discovery,
which also means the partition path structure is checked directly instead of
being trusted.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pyarrow.parquet as pq  # noqa: E402
from kafka import KafkaConsumer, TopicPartition  # noqa: E402

from common.config import load_config  # noqa: E402

# Nothing from processing/ is imported: those modules need pyspark, which lives
# only inside the Spark container. The expected layout comes from config.yaml.
DATA_COLUMNS = {
    "meter_id", "household_id", "power_consumption_kwh",
    "solar_generation_kwh", "timestamp", "ingested_at",
}

# How many trigger intervals the sink may trail Kafka by before we call it
# stalled. The sink commits every `raw_sink_trigger_seconds`, so it is always
# behind by roughly one interval's worth of events; three is slack for a slow
# batch. Judged in events rather than as a percentage on purpose: the lag is
# absolute, so a fixed percentage would fail a freshly started sink on a small
# topic and pass a badly stalled one on a large topic.
LAG_BUDGET_INTERVALS = 3


def kafka_message_count(cfg) -> int:
    """How many messages the topic currently holds, across all partitions."""
    consumer = KafkaConsumer(bootstrap_servers=cfg.kafka_bootstrap, group_id=None)
    try:
        partition_ids = consumer.partitions_for_topic(cfg.kafka_topic)
        if not partition_ids:
            return 0
        tps = [TopicPartition(cfg.kafka_topic, p) for p in partition_ids]
        beginning, end = consumer.beginning_offsets(tps), consumer.end_offsets(tps)
        return sum(end[tp] - beginning[tp] for tp in tps)
    finally:
        consumer.close()


def parse_partition_path(path: Path, root: Path, partition_cols: list[str]) -> dict | None:
    """Pull the partition values out of a file's path.

    Expects <root>/<col>=<value>/<col>=<value>/<file>.parquet, which is Spark's
    Hive-style layout. Returning None marks a file that does not match, so a
    stray path is reported rather than silently counted.
    """
    segments = path.relative_to(root).parts[:-1]        # drop the file name
    if len(segments) != len(partition_cols):
        return None
    values = {}
    for segment, expected_col in zip(segments, partition_cols):
        col, sep, value = segment.partition("=")
        if not sep or col != expected_col:
            return None
        values[col] = value
    return values


def load_store(cfg):
    """Return (rows, unique_readings, partition_counts, files, columns, sample).

    Reads every Parquet file under the master store. Fine at this scale, and it
    keeps the layout check explicit.
    """
    root = Path(cfg.raw_store_path)
    partition_cols = list(cfg.storage["raw_partition_cols"])

    if not root.exists():
        print(f"      - {root} does not exist yet")
        print("        Is raw-sink running?  docker compose logs --tail 30 raw-sink")
        return None

    # Spark leaves _SUCCESS markers and, mid-write, _temporary directories.
    files = [p for p in sorted(root.rglob("*.parquet"))
             if not any(part.startswith(("_", ".")) for part in p.parts)]
    if not files:
        print(f"      - no Parquet files under {root}")
        print("        Is raw-sink running?  docker compose logs --tail 30 raw-sink")
        return None

    rows = 0
    unique: set[tuple] = set()
    partition_counts: Counter = Counter()
    bad_paths: list[str] = []
    columns: set[str] = set()
    sample: list[dict] = []

    for file_path in files:
        values = parse_partition_path(file_path, root, partition_cols)
        if values is None:
            bad_paths.append(str(file_path))
            continue
        table = pq.read_table(file_path)
        columns |= set(table.schema.names)
        rows += table.num_rows
        partition_counts[tuple(values[c] for c in partition_cols)] += table.num_rows
        # (meter_id, timestamp) identifies a reading, so this detects the
        # at-least-once duplication the sink can produce.
        unique.update(zip(table.column("meter_id").to_pylist(),
                          table.column("timestamp").to_pylist()))
        if len(sample) < 3:
            for row in table.to_pylist()[: 3 - len(sample)]:
                sample.append({**row, **values})

    return {
        "rows": rows, "unique": len(unique), "partitions": partition_counts,
        "files": len(files), "columns": columns, "sample": sample,
        "bad_paths": bad_paths, "partition_cols": partition_cols, "root": root,
    }


def report_reconciliation(cfg, store, kafka_events: int) -> bool:
    rows, unique = store["rows"], store["unique"]
    print(f"      + Kafka holds       {kafka_events:,} events")
    print(f"      + Parquet stores    {rows:,} rows in {store['files']} file(s)")
    print(f"      + distinct readings {unique:,}  (meter_id + timestamp)")
    if rows != unique:
        print(f"      + {rows - unique:,} duplicate row(s) from at-least-once "
              "delivery, removed on read by dedupe_readings()")

    if kafka_events == 0:
        print("      - Kafka is empty, so there is nothing to reconcile against")
        return False

    if unique > kafka_events:
        print(f"      + coverage {unique / kafka_events:.1%}; Parquet exceeds Kafka "
              "because retention has expired segments the master store already "
              "holds -- exactly why it exists")
        return True

    # Expected production rate comes from config, not measurement: every meter
    # emits once per interval, so the sink should never trail by more than a few
    # intervals' worth of that.
    events_per_second = cfg.total_meters / float(cfg.simulation["meter_interval_seconds"])
    trigger_seconds = int(cfg.raw["spark"]["raw_sink_trigger_seconds"])
    budget = trigger_seconds * events_per_second * LAG_BUDGET_INTERVALS
    lag = kafka_events - unique
    ok = lag <= budget

    print(f"      {'+' if ok else '-'} coverage {unique / kafka_events:.1%}; "
          f"lag {lag:,.0f} events "
          f"(~{lag / events_per_second:.0f}s of production at {events_per_second:.0f}/s)")
    print(f"        budget is {budget:,.0f} events = {LAG_BUDGET_INTERVALS} x the "
          f"{trigger_seconds}s trigger interval")
    if not ok:
        print("        The sink is stalled or falling behind. Check:"
              " docker compose logs --tail 40 raw-sink")
    return ok


def report_schema(store) -> bool:
    missing = DATA_COLUMNS - store["columns"]
    if missing:
        print(f"      - Parquet files are missing columns {sorted(missing)}")
        return False
    print(f"      + all {len(DATA_COLUMNS)} data columns present in the files")
    print(f"      + partition columns encoded in the path: {store['partition_cols']}")
    if store["bad_paths"]:
        print(f"      - {len(store['bad_paths'])} file(s) not in the expected "
              f"<col>=<value> layout, e.g. {store['bad_paths'][0]}")
        return False
    return True


def report_partitions(cfg, store) -> bool:
    print(f"\n      {'sim_day':<14}{'grid_zone':<16}{'rows':>8}")
    for key, count in sorted(store["partitions"].items()):
        sim_day, zone = key
        print(f"      {sim_day:<14}{zone:<16}{count:>8}")

    zones = {zone for _, zone in store["partitions"]}
    unexpected = zones - set(cfg.zone_names)
    if unexpected:
        print(f"      - unexpected zones in the store: {sorted(unexpected)}")
        return False
    print(f"\n      + {len(store['partitions'])} partition(s); every zone is a "
          "configured zone")
    # A complete simulated day holds meters * readings_per_day rows per zone.
    print(f"      + a full sim-day zone partition should hold "
          f"meters x {cfg.readings_per_meter_per_sim_day} rows "
          f"(e.g. Colombo-North: {12 * cfg.readings_per_meter_per_sim_day})")
    return True


def report_sample(cfg, store) -> None:
    # Parquet stores instants in UTC, so a reading at 00:02 Colombo time comes
    # back as 18:32 the previous day. Printed raw that looks as though sim_day
    # disagrees with the timestamp, so convert into the simulation's zone -- the
    # same zone Spark used to derive sim_day in the first place.
    zone = ZoneInfo(cfg.simulation["timezone"])
    print(f"\n      three sample rows (times in {zone}):")
    for row in store["sample"]:
        local = row["timestamp"].replace(tzinfo=timezone.utc).astimezone(zone)
        print(f"        sim_day={row['sim_day']} {row['grid_zone']:<15} "
              f"{row['meter_id']:<7} used={row['power_consumption_kwh']:.4f} "
              f"solar={row['solar_generation_kwh']:.4f} "
              f"at={local.strftime('%Y-%m-%d %H:%M:%S')}")


def main() -> int:
    argparse.ArgumentParser(description=__doc__).parse_args()
    cfg = load_config()

    print("\n  Smart Grid Lambda Platform -- Phase 2 master store check")
    print("  " + "-" * 58)
    print(f"\n  Raw master store   {cfg.raw_store_path}")

    kafka_events = kafka_message_count(cfg)
    store = load_store(cfg)
    if store is None:
        print("\n  " + "-" * 58)
        print("  PHASE 2 CHECKPOINT: FAILED  (master store unreadable)\n")
        return 1

    checks = [
        report_reconciliation(cfg, store, kafka_events),
        report_schema(store),
        report_partitions(cfg, store),
    ]
    report_sample(cfg, store)

    print("\n  " + "-" * 58)
    if all(checks):
        print("  PHASE 2 CHECKPOINT: PASSED")
        print("  Spark-side query:  docker compose run --rm --no-deps raw-sink \\")
        print("    /opt/spark/bin/spark-submit /app/processing/inspect_raw.py\n")
        return 0
    print("  PHASE 2 CHECKPOINT: FAILED\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())

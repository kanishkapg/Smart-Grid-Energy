#!/usr/bin/env python3
"""Phase 1 checkpoint: are the two simulated sources producing usable data?

    python scripts/check_phase1.py
    python scripts/check_phase1.py --events 200

Consumes a sample of live events off the topic, validates every one against the
event contract, summarises them by zone, then checks the tariff drop in object
storage. Exits 0 only when both sources pass, so it doubles as a gate.

Run it while `python -m sources.stream_producer` is running in another terminal.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Running a file inside scripts/ puts scripts/ on sys.path, not the project
# root, so `import common` would fail. Add the root explicitly, which keeps
# `python scripts/check_phase1.py` working without installing the project.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka import KafkaConsumer  # noqa: E402 - must follow the sys.path fix

from common.config import load_config  # noqa: E402
from common.sim_clock import SimClock  # noqa: E402
from common.storage import get_text, list_keys, s3_client  # noqa: E402
from sources.meter_model import EVENT_FIELDS  # noqa: E402

NUMERIC_FIELDS = ("power_consumption_kwh", "solar_generation_kwh")


def validate(event: dict) -> str | None:
    """Return a human-readable reason the event is invalid, or None if it is fine."""
    missing = [f for f in EVENT_FIELDS if f not in event]
    if missing:
        return f"missing fields: {missing}"
    for field in NUMERIC_FIELDS:
        value = event[field]
        if not isinstance(value, (int, float)):
            return f"{field} is {type(value).__name__}, expected a number"
        if value < 0:
            return f"{field} is negative ({value})"
    try:
        datetime.fromisoformat(event["timestamp"])
    except (TypeError, ValueError):
        return f"timestamp is not ISO-8601: {event['timestamp']!r}"
    return None


def sample_stream(cfg, wanted: int, timeout_seconds: int) -> tuple[list[dict], list[str]]:
    """Read up to `wanted` events from the tail of the topic."""
    consumer = KafkaConsumer(
        cfg.kafka_topic,
        bootstrap_servers=cfg.kafka_bootstrap,
        # latest + no group: we want whatever is flowing right now, and we must
        # not commit offsets that a real consumer group would later depend on.
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=timeout_seconds * 1000,
    )
    events, problems = [], []
    for message in consumer:
        try:
            event = json.loads(message.value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            problems.append(f"payload is not JSON: {exc}")
            continue
        reason = validate(event)
        if reason:
            problems.append(reason)
        else:
            events.append(event)
        if len(events) >= wanted:
            break
    consumer.close()
    return events, problems


def report_stream(cfg, clock, events: list[dict], problems: list[str]) -> bool:
    if not events:
        print("      - no events arrived")
        print("        Is the producer running?  python -m sources.stream_producer")
        return False

    zones: dict[str, dict] = defaultdict(
        lambda: {"events": 0, "kwh": 0.0, "solar": 0.0, "meters": set()}
    )
    for event in events:
        zone = zones[event["grid_zone"]]
        zone["events"] += 1
        zone["kwh"] += event["power_consumption_kwh"]
        zone["solar"] += event["solar_generation_kwh"]
        zone["meters"].add(event["meter_id"])

    print(f"      + {len(events)} events validated against the event contract")
    if problems:
        print(f"      - {len(problems)} malformed: {problems[:3]}")

    print(f"\n      {'zone':<16}{'events':>7}{'meters':>8}{'kWh used':>10}"
          f"{'kWh solar':>11}{'renew %':>9}")
    for name in sorted(zones):
        z = zones[name]
        pct = (100 * z["solar"] / z["kwh"]) if z["kwh"] else 0.0
        print(f"      {name:<16}{z['events']:>7}{len(z['meters']):>8}"
              f"{z['kwh']:>10.3f}{z['solar']:>11.3f}{pct:>8.1f}%")

    # Solar is exactly 0 outside daylight by design, so say which it is -- a
    # night-time 0% is correct, not a fault.
    total_solar = sum(z["solar"] for z in zones.values())
    if clock.is_daytime(start_hour=cfg.alerts["daytime_start_hour"],
                        end_hour=cfg.alerts["daytime_end_hour"]):
        print("\n      + daylight in simulated time, so solar generation is expected")
        if total_solar == 0:
            print("      - but no solar was generated at all, which is wrong")
            return False
    else:
        print("\n      + night in simulated time: 0.000 kWh solar is the correct"
              " reading, not a fault")

    arrival = [datetime.fromisoformat(e["timestamp"]) for e in events]
    out_of_order = sum(1 for a, b in zip(arrival, arrival[1:]) if b < a)
    times = sorted(arrival)
    span = (times[-1] - times[0]).total_seconds() / 60
    print(f"      + event-time range spans {span:.1f} simulated minutes "
          f"({times[0].isoformat(timespec='seconds')} .. "
          f"{times[-1].isoformat(timespec='seconds')})")
    print(f"      + {out_of_order} of {len(events)} events arrived out of event-time "
          f"order -- this is what the speed layer's "
          f"{cfg.speed_layer['watermark_minutes']}-simulated-minute watermark absorbs")

    expected_zones = set(cfg.zone_names)
    if set(zones) - expected_zones:
        print(f"      - unexpected zones: {set(zones) - expected_zones}")
        return False
    return not problems


def report_tariff(cfg) -> bool:
    client = s3_client(cfg)
    bucket = cfg.bucket("tariff")
    try:
        keys = list_keys(client, bucket)
    except Exception as exc:                       # noqa: BLE001 - report, don't crash
        print(f"      - could not reach object storage: {exc}")
        return False

    if not keys:
        print(f"      - bucket '{bucket}' is empty")
        print("        Run:  python -m sources.tariff_generator --once")
        return False

    print(f"      + {len(keys)} tariff file(s) in '{bucket}': {', '.join(keys[-3:])}")

    lines = get_text(client, bucket, keys[-1]).strip().splitlines()
    header, rows = lines[0], lines[1:]
    expected_header = "household_id,tariff_rate,billing_tier,subsidy_flag"
    if header != expected_header:
        print(f"      - header is {header!r}, expected {expected_header!r}")
        return False

    print(f"      + {keys[-1]}: header correct, {len(rows)} household rows")
    if len(rows) != cfg.total_meters:
        print(f"      - expected {cfg.total_meters} rows, one per household")
        return False
    for line in rows[:3]:
        print(f"        {line}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=120,
                        help="how many events to sample (default 120 = 3 ticks)")
    parser.add_argument("--timeout", type=int, default=25,
                        help="seconds to wait for events before giving up")
    args = parser.parse_args()

    cfg = load_config()
    clock = SimClock.from_config(cfg)

    print("\n  Smart Grid Lambda Platform -- Phase 1 source check")
    print("  " + "-" * 58)
    print(f"  sim day {clock.sim_day().isoformat()} "
          f"({clock.sim_day_progress() * 100:.0f}% elapsed), "
          f"sim time {clock.now().isoformat(timespec='seconds')}")

    print(f"\n  Meter stream         (sampling up to {args.events} events)")
    events, problems = sample_stream(cfg, args.events, args.timeout)
    stream_ok = report_stream(cfg, clock, events, problems)

    print("\n  Daily tariff file")
    tariff_ok = report_tariff(cfg)

    print("\n  " + "-" * 58)
    if stream_ok and tariff_ok:
        print("  PHASE 1 CHECKPOINT: PASSED\n")
        return 0
    failed = [n for n, ok in (("stream", stream_ok), ("tariff", tariff_ok)) if not ok]
    print(f"  PHASE 1 CHECKPOINT: FAILED  ({', '.join(failed)})\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())

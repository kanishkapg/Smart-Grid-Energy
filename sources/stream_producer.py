"""Simulated smart-meter stream: one reading per meter, every 2 real seconds.

    python -m sources.stream_producer                    # run until Ctrl+C
    python -m sources.stream_producer --sim-days 2       # stop after 2 sim-days
    python -m sources.stream_producer --cloud-cover 0.9  # force low renewable %
    python -m sources.stream_producer --log-format console

Pacing is driven by SimClock, so every event carries a *simulated* event time
while the loop sleeps in real seconds. Messages are keyed by `household_id`,
which spreads them evenly across the topic's three partitions; keying by
grid_zone would put 12 of 40 meters on one partition.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import time
from datetime import timedelta

from kafka import KafkaProducer

from common.config import load_config
from common.logging_setup import log_startup, new_run_id, setup_logging
from common.sim_clock import SimClock
from sources.meter_model import build_meters, make_reading

_running = True


def _stop(signum, frame):  # noqa: ARG001 - signal handler signature
    global _running
    _running = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sim-days", type=float, default=None,
                        help="stop after this many simulated days (default: run forever)")
    parser.add_argument("--cloud-cover", type=float, default=0.0,
                        help="0.0-1.0 reduction in solar output, for the alert demo")
    parser.add_argument("--log-format", choices=("json", "console"), default="json")
    parser.add_argument("--summary-every", type=int, default=10,
                        help="log a summary every N ticks (1 tick = 2 real seconds)")
    return parser.parse_args()


def build_producer(cfg) -> KafkaProducer:
    # Keys and values are encoded at the call site rather than through
    # key_serializer/value_serializer callbacks: kafka-python 3.x deprecates
    # plain callables there, and one json.dumps at the send is clearer anyway.
    return KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap,
        acks=cfg.raw["kafka"]["producer_acks"],
        linger_ms=50,        # small batching window: 40 events per tick travel together
    )


def main() -> int:
    args = parse_args()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    cfg = load_config()
    run_id = new_run_id("stream_producer")
    log = setup_logging("stream_producer", run_id=run_id,
                        level=cfg.logging["level"], fmt=args.log_format)
    log_startup(log, cfg, "stream_producer")

    clock = SimClock.from_config(cfg)
    meters = build_meters(cfg)
    rng = random.Random()

    sim = cfg.simulation
    tick_seconds = float(sim["meter_interval_seconds"])
    interval_hours = 24.0 / cfg.readings_per_meter_per_sim_day
    late_probability = float(sim["late_event_probability"])
    late_max_minutes = float(sim["late_event_max_sim_minutes"])
    jitter = float(cfg.grid["load_jitter_pct"])
    day_start, day_end = cfg.alerts["daytime_start_hour"], cfg.alerts["daytime_end_hour"]

    producer = build_producer(cfg)
    topic = cfg.kafka_topic
    # run_id travels in a header, not the payload: it is metadata about the
    # pipeline run, not part of the meter reading contract.
    headers = [("run_id", run_id.encode("utf-8"))]

    log.info("producer_ready", bootstrap=cfg.kafka_bootstrap, topic=topic,
             meters=len(meters), solar_meters=sum(m.has_solar for m in meters),
             cloud_cover=args.cloud_cover, sim_day=clock.sim_day().isoformat())

    sent = late_sent = ticks = 0
    current_day = clock.sim_day()
    started = time.time()
    next_tick = started

    while _running:
        now_real = time.time()
        sim_now = clock.sim_time_at(now_real).astimezone(clock.tz)

        for meter in meters:
            # A late event was generated earlier and is only arriving now, so its
            # event time AND its physics both belong to that earlier moment.
            event_time = sim_now
            is_late = rng.random() < late_probability
            if is_late:
                event_time -= timedelta(minutes=rng.uniform(1.0, late_max_minutes))
                late_sent += 1

            reading = make_reading(
                meter, event_time, interval_hours,
                clock.solar_factor(event_time, day_start, day_end),
                rng, jitter, args.cloud_cover,
            )
            producer.send(
                topic,
                key=meter.household_id.encode("utf-8"),
                value=json.dumps(reading).encode("utf-8"),
                headers=headers,
            )
            sent += 1

        ticks += 1
        producer.flush()

        if clock.sim_day(now_real) != current_day:
            current_day = clock.sim_day(now_real)
            log.info("sim_day_rollover", sim_day=current_day.isoformat(),
                     events_sent=sent)

        if ticks % args.summary_every == 0:
            elapsed = time.time() - started
            log.info(
                "progress",
                sim_time=sim_now.isoformat(timespec="seconds"),
                sim_day=current_day.isoformat(),
                sim_day_progress_pct=round(clock.sim_day_progress() * 100, 1),
                events_sent=sent,
                late_events=late_sent,
                events_per_sec=round(sent / elapsed, 1) if elapsed else 0.0,
            )

        if args.sim_days is not None:
            sim_days_done = (time.time() - started) / float(sim["sim_day_real_seconds"])
            if sim_days_done >= args.sim_days:
                log.info("sim_days_reached", sim_days=args.sim_days)
                break

        next_tick += tick_seconds
        time.sleep(max(0.0, next_tick - time.time()))

    producer.flush()
    producer.close()
    log.info("producer_stopped", events_sent=sent, late_events=late_sent,
             real_seconds=round(time.time() - started, 1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

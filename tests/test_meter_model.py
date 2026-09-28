"""Tests for the meter population and the reading physics.

No Kafka and no Docker: the model is pure, which is the point of separating it
from the producer.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

import pytest

from common.config import load_config
from common.sim_clock import SimClock
from sources.meter_model import (
    EVENT_FIELDS,
    build_meters,
    load_multiplier,
    make_reading,
    stable_fraction,
)

MIDNIGHT = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
NOON = MIDNIGHT + timedelta(hours=12)
INTERVAL_HOURS = 0.16          # 9.6 simulated minutes, the real reading interval


@pytest.fixture(scope="module")
def cfg():
    return load_config()


@pytest.fixture(scope="module")
def meters(cfg):
    return build_meters(cfg)


# --- deterministic hashing, the mechanism that keeps components agreeing ----
def test_stable_fraction_is_in_range_and_repeatable() -> None:
    first = stable_fraction("tier", "H-101")
    assert 0.0 <= first < 1.0
    assert stable_fraction("tier", "H-101") == first


def test_stable_fraction_separates_namespaces() -> None:
    # The same household must not get the same answer for two different
    # questions, or solar ownership and tariff tier would be correlated.
    assert stable_fraction("solar", "H-101") != stable_fraction("tier", "H-101")


# --- the meter population --------------------------------------------------
def test_population_size_matches_config(cfg, meters) -> None:
    assert len(meters) == cfg.total_meters


def test_ids_are_unique_and_paired(meters) -> None:
    assert len({m.meter_id for m in meters}) == len(meters)
    assert len({m.household_id for m in meters}) == len(meters)
    # One meter per household: M-101 meters H-101.
    for meter in meters:
        assert meter.meter_id[2:] == meter.household_id[2:]


def test_numbering_starts_at_101(meters) -> None:
    # The project plan's worked example is household H-101, so it must exist.
    assert meters[0].household_id == "H-101"
    assert "H-101" in {m.household_id for m in meters}


def test_zones_match_config(cfg, meters) -> None:
    assert {m.grid_zone for m in meters} == set(cfg.zone_names)
    for zone in cfg.zones:
        in_zone = [m for m in meters if m.grid_zone == zone["name"]]
        assert len(in_zone) == zone["meters"]


def test_population_is_identical_across_calls(cfg) -> None:
    # The producer and the tariff generator build this independently; if the two
    # disagreed about who has solar, billing would not reconcile.
    assert build_meters(cfg) == build_meters(cfg)


def test_solar_ownership_matches_the_quota_exactly(cfg, meters) -> None:
    # Ownership is a quota, not a per-household coin flip, so this is exact
    # rather than approximate.
    for zone in cfg.zones:
        in_zone = [m for m in meters if m.grid_zone == zone["name"]]
        expected = round(len(in_zone) * float(zone["solar_penetration"]))
        assert sum(m.has_solar for m in in_zone) == expected


def test_no_zone_with_solar_configured_ends_up_with_none(cfg, meters) -> None:
    # Regression guard: independent coin flips gave Kandy 0 of 9 panels against a
    # 0.30 target, which would pin its renewable share at 0% and make the
    # low-renewable alert fire for the entire run.
    for zone in cfg.zones:
        if float(zone["solar_penetration"]) <= 0:
            continue
        in_zone = [m for m in meters if m.grid_zone == zone["name"]]
        assert any(m.has_solar for m in in_zone), f"{zone['name']} has no solar at all"


# --- the daily load curve --------------------------------------------------
def test_load_multiplier_peaks_in_the_evening() -> None:
    assert load_multiplier(MIDNIGHT + timedelta(hours=19)) > load_multiplier(NOON)
    assert load_multiplier(NOON) > load_multiplier(MIDNIGHT + timedelta(hours=3))


def test_load_multiplier_interpolates_between_hours() -> None:
    half_past = load_multiplier(MIDNIGHT + timedelta(hours=6, minutes=30))
    six, seven = (load_multiplier(MIDNIGHT + timedelta(hours=h)) for h in (6, 7))
    assert min(six, seven) < half_past < max(six, seven)


def test_load_multiplier_is_always_positive() -> None:
    for hour in range(24):
        assert load_multiplier(MIDNIGHT + timedelta(hours=hour)) > 0


# --- one reading -----------------------------------------------------------
def _solar_meter(meters):
    return next(m for m in meters if m.has_solar)


def test_reading_has_exactly_the_contract_fields(meters) -> None:
    reading = make_reading(meters[0], NOON, INTERVAL_HOURS, 1.0, random.Random(1))
    assert tuple(reading) == EVENT_FIELDS


def test_solar_is_exactly_zero_at_night(meters) -> None:
    # solar_factor is 0 overnight; a 0% renewable reading at 02:00 is correct,
    # not an anomaly, so it must be a hard zero rather than a rounding smear.
    reading = make_reading(_solar_meter(meters), MIDNIGHT, INTERVAL_HOURS, 0.0,
                           random.Random(1))
    assert reading["solar_generation_kwh"] == 0.0


def test_solar_is_exactly_zero_without_panels(meters) -> None:
    no_panels = next(m for m in meters if not m.has_solar)
    reading = make_reading(no_panels, NOON, INTERVAL_HOURS, 1.0, random.Random(1))
    assert reading["solar_generation_kwh"] == 0.0


def test_solar_is_generated_at_midday_with_panels(meters) -> None:
    reading = make_reading(_solar_meter(meters), NOON, INTERVAL_HOURS, 1.0,
                           random.Random(1))
    assert reading["solar_generation_kwh"] > 0


def test_cloud_cover_suppresses_solar(meters) -> None:
    # This is what --cloud-cover drives to force the low-renewable alert.
    meter = _solar_meter(meters)
    clear = make_reading(meter, NOON, INTERVAL_HOURS, 1.0, random.Random(7))
    covered = make_reading(meter, NOON, INTERVAL_HOURS, 1.0, random.Random(7),
                           cloud_cover=0.9)
    assert covered["solar_generation_kwh"] < clear["solar_generation_kwh"]


def test_consumption_is_positive_and_scales_with_interval(meters) -> None:
    short = make_reading(meters[0], NOON, 0.16, 0.0, random.Random(3))
    long = make_reading(meters[0], NOON, 1.60, 0.0, random.Random(3))
    assert short["power_consumption_kwh"] > 0
    # Ten times the interval is ten times the energy: kWh, not kW.
    assert long["power_consumption_kwh"] == pytest.approx(
        short["power_consumption_kwh"] * 10, rel=0.01
    )


def test_timestamp_round_trips(meters) -> None:
    reading = make_reading(meters[0], NOON, INTERVAL_HOURS, 1.0, random.Random(1))
    assert datetime.fromisoformat(reading["timestamp"]) == NOON


# --- whole-day energy balance ----------------------------------------------
def _simulate_one_sim_day(cfg, meter) -> tuple[float, float]:
    """Sum a meter's consumption and generation over a full simulated day."""
    clock = SimClock(MIDNIGHT, 300.0, real_anchor=0.0, tz=timezone.utc)
    readings = cfg.readings_per_meter_per_sim_day
    interval = 24.0 / readings
    rng = random.Random(42)

    consumption = solar = 0.0
    for step in range(readings):
        moment = MIDNIGHT + timedelta(hours=step * interval)
        reading = make_reading(
            meter, moment, interval, clock.solar_factor(moment, 6, 18), rng
        )
        consumption += reading["power_consumption_kwh"]
        solar += reading["solar_generation_kwh"]
    return consumption, solar


def test_daily_totals_are_in_the_plans_ballpark(cfg, meters) -> None:
    # The plan's worked example is H-101 with 18 kWh consumed and 6 kWh solar
    # over one simulated day. The panel sizing in config.yaml is chosen to land
    # near that, so this test is what catches someone changing solar_panel_kw
    # without realising it moves every figure in the report.
    consumption, solar = _simulate_one_sim_day(cfg, _solar_meter(meters))
    assert 12.0 < consumption < 30.0, f"daily consumption {consumption:.1f} kWh"
    assert 0.20 < solar / consumption < 0.45, f"solar share {solar / consumption:.2f}"


def test_a_zones_solar_never_exceeds_its_consumption_at_solar_noon(cfg, meters) -> None:
    # Guards the readability of renewable_pct: above 100% the metric stops
    # meaning "share of demand met by solar" and the alert threshold is unusable.
    rng = random.Random(11)
    for zone_name in cfg.zone_names:
        in_zone = [m for m in meters if m.grid_zone == zone_name]
        readings = [
            make_reading(m, NOON, INTERVAL_HOURS, 1.0, rng, load_jitter_pct=0.0)
            for m in in_zone
        ]
        consumption = sum(r["power_consumption_kwh"] for r in readings)
        solar = sum(r["solar_generation_kwh"] for r in readings)
        assert solar < consumption, (
            f"{zone_name}: solar {solar:.3f} >= consumption {consumption:.3f} kWh "
            "at solar noon, so renewable_pct would exceed 100%"
        )

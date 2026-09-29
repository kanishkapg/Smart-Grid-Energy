"""Tests for the speed layer's threshold rules.

The rules are the part of Phase 3 that decides whether an operator is woken up,
and the part most likely to be wrong in a way that looks fine: a rule that never
fires and a rule that always fires both produce a dashboard nobody questions.
They are pure functions precisely so they can be tested here, without Kafka,
Spark or a database.

The thresholds come from config.yaml rather than being hard-coded, so these tests
also fail if someone retunes a threshold into a state where the rule cannot fire.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from common.config import load_config
from sources.meter_model import load_multiplier
from processing.shared.alert_rules import (
    LOW_RENEWABLE,
    ZONE_OVERLOAD,
    evaluate_zone_window,
    mean_kw_per_meter,
)


# The overload threshold is only meaningful relative to the load the simulator
# actually produces, so it is measured against the producer's own demand curve
# rather than against a number repeated in the test.
PEAK_HOUR = max(range(24), key=lambda h: load_multiplier(datetime(2026, 1, 1, h)))
PEAK_LOAD_MULTIPLIER = load_multiplier(datetime(2026, 1, 1, PEAK_HOUR))


@pytest.fixture(scope="module")
def cfg():
    return load_config()


@pytest.fixture(scope="module")
def thresholds(cfg):
    return cfg.alerts


@pytest.fixture(scope="module")
def window_minutes(cfg):
    return int(cfg.speed_layer["window_minutes"])


def window(cfg, hour: int, renewable_pct: float, consumption_kwh: float = 12.0,
           meters: int = 12, zone: str = "Colombo-North") -> dict:
    """One row of `zone_metrics`, as the sink hands it to the rules.

    `window_start` is timezone-aware in the simulation's zone, which is the whole
    reason `collect_local_rows` exists: the hour of day decides whether the solar
    rule applies at all.
    """
    tz = ZoneInfo(cfg.simulation["timezone"])
    return {
        "window_start": datetime(2026, 1, 1, hour, 0, tzinfo=tz),
        "window_end": datetime(2026, 1, 1, hour, 0, tzinfo=tz) + timedelta(hours=1),
        "grid_zone": zone,
        "total_consumption_kwh": consumption_kwh,
        "total_solar_kwh": consumption_kwh * renewable_pct / 100,
        "renewable_pct": renewable_pct,
        "active_meters": meters,
    }


def rules_fired(alerts) -> set:
    return {alert.rule for alert in alerts}


# ---------------------------------------------------------------------------
# low renewable contribution
# ---------------------------------------------------------------------------

def test_low_renewable_fires_around_solar_noon(cfg, thresholds, window_minutes) -> None:
    breach = float(thresholds["low_renewable_pct"]) - 5
    alerts = evaluate_zone_window(window(cfg, 12, breach), thresholds, window_minutes)
    assert LOW_RENEWABLE in rules_fired(alerts)
    # The message is what reaches the dashboard and the logs, so it has to name
    # the zone and the number -- "threshold breached" is not an alert.
    message = next(a for a in alerts if a.rule == LOW_RENEWABLE).message
    assert "Colombo-North" in message
    assert "{:.1f}".format(breach) in message


def test_healthy_renewable_share_is_silent(cfg, thresholds, window_minutes) -> None:
    healthy = float(thresholds["low_renewable_pct"]) + 10
    alerts = evaluate_zone_window(window(cfg, 12, healthy), thresholds, window_minutes)
    assert LOW_RENEWABLE not in rules_fired(alerts)


def test_night_is_not_an_incident(cfg, thresholds, window_minutes) -> None:
    # 0% renewable at 02:00 is darkness. A rule that fired here would raise four
    # alerts per zone per simulated day, every day, and mean nothing.
    alerts = evaluate_zone_window(window(cfg, 2, 0.0), thresholds, window_minutes)
    assert LOW_RENEWABLE not in rules_fired(alerts)


def test_dawn_and_dusk_are_outside_the_check_window(cfg, thresholds, window_minutes) -> None:
    # The sun is up but barely, so a low share is expected rather than reportable.
    # This is why the rule uses its own narrower hours instead of daytime_*.
    for hour in (int(cfg.alerts["daytime_start_hour"]),
                 int(cfg.alerts["daytime_end_hour"]) - 1):
        alerts = evaluate_zone_window(window(cfg, hour, 1.0), thresholds, window_minutes)
        assert LOW_RENEWABLE not in rules_fired(alerts), "fired at hour {}".format(hour)


def test_check_hours_lie_inside_daylight(thresholds) -> None:
    # A renewable check outside daylight could never pass, which would be a
    # silent misconfiguration rather than an error.
    assert (thresholds["daytime_start_hour"]
            <= thresholds["renewable_check_start_hour"]
            < thresholds["renewable_check_end_hour"]
            <= thresholds["daytime_end_hour"])


def test_the_boundary_hours_behave_as_documented(cfg, thresholds, window_minutes) -> None:
    start = int(thresholds["renewable_check_start_hour"])
    end = int(thresholds["renewable_check_end_hour"])
    inside = evaluate_zone_window(window(cfg, start, 1.0), thresholds, window_minutes)
    outside = evaluate_zone_window(window(cfg, end, 1.0), thresholds, window_minutes)
    assert LOW_RENEWABLE in rules_fired(inside), "start hour is inclusive"
    assert LOW_RENEWABLE not in rules_fired(outside), "end hour is exclusive"


# ---------------------------------------------------------------------------
# zone overload
# ---------------------------------------------------------------------------

def test_mean_kw_per_meter_converts_energy_to_power() -> None:
    # 12 kWh over a 60-minute window across 12 meters is 1 kW each.
    assert mean_kw_per_meter(12.0, 60, 12) == pytest.approx(1.0)
    # Same energy over half the window is twice the power.
    assert mean_kw_per_meter(12.0, 30, 12) == pytest.approx(2.0)


def test_empty_window_does_not_divide_by_zero() -> None:
    # An empty window means no data arrived, which is the health check's problem
    # (Phase 6), not the overload rule's.
    assert mean_kw_per_meter(0.0, 60, 0) == 0.0


def test_overload_fires_above_the_ceiling(cfg, thresholds, window_minutes) -> None:
    ceiling = float(thresholds["zone_overload_kw_per_meter"])
    meters = 12
    surging = window(cfg, 2, 0.0, consumption_kwh=(ceiling + 0.5) * meters, meters=meters)
    assert ZONE_OVERLOAD in rules_fired(
        evaluate_zone_window(surging, thresholds, window_minutes))


def test_the_evening_peak_is_not_an_overload(cfg, thresholds, window_minutes) -> None:
    """The heaviest zone at its daily peak must stay silent.

    This is the test that caught the first calibration. A threshold derived from
    base_load_kw alone ignored the simulator's demand curve, and the rule then
    fired for the two largest zones every simulated evening -- which is a
    calendar, not an alert.
    """
    heaviest = max(cfg.zones, key=lambda z: float(z["base_load_kw"]))
    meters = int(heaviest["meters"])
    peak_kw = float(heaviest["base_load_kw"]) * PEAK_LOAD_MULTIPLIER
    peak = window(cfg, PEAK_HOUR, 0.0, consumption_kwh=peak_kw * meters,
                  meters=meters, zone=heaviest["name"])
    assert ZONE_OVERLOAD not in rules_fired(
        evaluate_zone_window(peak, thresholds, window_minutes))


def test_the_ceiling_leaves_room_for_jitter(cfg, thresholds) -> None:
    # Readings vary by +/-load_jitter_pct and only ~6 of them land in a window, so
    # the mean does not fully converge. Measured against a live run the residual
    # is about 10%, and the ceiling has to clear the peak by at least that or the
    # rule reports noise as stress.
    peak_kw = max(float(z["base_load_kw"]) for z in cfg.zones) * PEAK_LOAD_MULTIPLIER
    assert float(thresholds["zone_overload_kw_per_meter"]) > peak_kw * 1.1


# ---------------------------------------------------------------------------
# dedupe key
# ---------------------------------------------------------------------------

def test_dedupe_key_identifies_the_condition_not_the_evaluation(
        cfg, thresholds, window_minutes) -> None:
    # Update mode re-emits an open window on every micro-batch, so the same
    # breach is evaluated repeatedly and must yield one alert, not a dozen.
    row = window(cfg, 12, 1.0)
    first = evaluate_zone_window(row, thresholds, window_minutes)
    again = evaluate_zone_window(dict(row), thresholds, window_minutes)
    assert [a.dedupe_key for a in first] == [a.dedupe_key for a in again]

    later = window(cfg, 13, 1.0)
    assert (evaluate_zone_window(later, thresholds, window_minutes)[0].dedupe_key
            != first[0].dedupe_key), "a different window is a different alert"

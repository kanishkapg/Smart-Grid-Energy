"""Tests for the simulated clock.

No Docker and no services needed, so this is the fastest way to show the
foundation is sound. Every test pins `real_anchor` explicitly -- the whole point
is that the real-to-simulated mapping is deterministic.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common.sim_clock import (
    ANCHOR_ENV_VAR,
    SimClock,
    resolve_real_anchor,
    window_alignment_minutes,
)

ANCHOR = 1_700_000_000.0          # an arbitrary but fixed real instant
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)
SIM_DAY_REAL_SECONDS = 300.0      # 5 real minutes per simulated day


@pytest.fixture
def clock() -> SimClock:
    return SimClock(EPOCH, SIM_DAY_REAL_SECONDS, ANCHOR, timezone.utc)


# --- the compression factor the report and demo both quote -----------------
def test_compression_factor_is_288x(clock: SimClock) -> None:
    assert clock.compression_factor == pytest.approx(288.0)


def test_one_real_second_is_288_sim_seconds(clock: SimClock) -> None:
    delta = clock.sim_time_at(ANCHOR + 1) - clock.sim_time_at(ANCHOR)
    assert delta == timedelta(seconds=288)


# --- real -> simulated mapping ---------------------------------------------
def test_sim_time_at_anchor_equals_epoch(clock: SimClock) -> None:
    assert clock.sim_time_at(ANCHOR) == EPOCH


def test_half_a_real_sim_day_is_simulated_noon(clock: SimClock) -> None:
    assert clock.sim_time_at(ANCHOR + 150) == EPOCH + timedelta(hours=12)


def test_full_sim_day_of_real_time_advances_exactly_one_day(clock: SimClock) -> None:
    assert clock.sim_time_at(ANCHOR + SIM_DAY_REAL_SECONDS) == EPOCH + timedelta(days=1)


@pytest.mark.parametrize(
    "real_offset, expected_index",
    [(0, 0), (299, 0), (300, 1), (400, 1), (900, 3)],
)
def test_sim_day_index_increments_on_boundaries(
    clock: SimClock, real_offset: float, expected_index: int
) -> None:
    assert clock.sim_day_index(ANCHOR + real_offset) == expected_index


def test_sim_day_label_is_the_partition_format(clock: SimClock) -> None:
    # Used in both tariff_YYYYMMDD.csv and sim_day=... partition values.
    assert clock.sim_day_label(EPOCH.date()) == "20260101"


# --- simulated -> real: what the tariff generator sleeps on ----------------
def test_real_seconds_until_next_sim_day(clock: SimClock) -> None:
    # 100 real seconds into a 300-real-second sim day -> 200 left.
    assert clock.real_seconds_until_next_sim_day(ANCHOR + 100) == pytest.approx(200.0)


def test_sim_day_progress_resets_each_day(clock: SimClock) -> None:
    assert clock.sim_day_progress(ANCHOR + 75) == pytest.approx(0.25)
    assert clock.sim_day_progress(ANCHOR + 450) == pytest.approx(0.5)


# --- daylight, which gates the low-renewable alert rule --------------------
@pytest.mark.parametrize(
    "hour, expected", [(5, False), (6, True), (12, True), (17, True), (18, False)]
)
def test_is_daytime_window_is_half_open(clock: SimClock, hour: int, expected: bool) -> None:
    assert clock.is_daytime(EPOCH + timedelta(hours=hour)) is expected


def test_solar_factor_is_zero_at_night(clock: SimClock) -> None:
    # A 0% renewable reading at 02:00 must not be treated as an anomaly.
    assert clock.solar_factor(EPOCH + timedelta(hours=2)) == 0.0


def test_solar_factor_peaks_at_solar_noon(clock: SimClock) -> None:
    assert clock.solar_factor(EPOCH + timedelta(hours=12)) == pytest.approx(1.0)


# --- the anchor sharing that keeps components in step ----------------------
def test_anchor_file_is_created_then_reused(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv(ANCHOR_ENV_VAR, raising=False)
    anchor_path = tmp_path / "data" / ".sim_anchor"

    first = resolve_real_anchor(anchor_path)
    assert anchor_path.exists(), "first caller should persist the anchor"
    assert resolve_real_anchor(anchor_path) == first, "later processes must adopt it"


def test_env_var_overrides_the_anchor_file(tmp_path, monkeypatch) -> None:
    anchor_path = tmp_path / ".sim_anchor"
    anchor_path.write_text("1234.0", encoding="utf-8")
    monkeypatch.setenv(ANCHOR_ENV_VAR, "9999.0")

    # This is how a container inherits the host's clock.
    assert resolve_real_anchor(anchor_path) == 9999.0


# ---------------------------------------------------------------------------
# window alignment (Phase 3)
# ---------------------------------------------------------------------------

def test_window_alignment_shifts_half_hour_zones() -> None:
    # Spark aligns event-time windows to midnight UTC. Asia/Colombo is +05:30,
    # so without a shift an "hourly" window runs 23:30-00:30 local, and an alert
    # rule written as "09:00 to 16:00" silently tests 09:30 to 16:30.
    assert window_alignment_minutes("Asia/Colombo", 60) == 30


def test_whole_hour_zones_need_no_shift() -> None:
    assert window_alignment_minutes("UTC", 60) == 0
    assert window_alignment_minutes("Europe/Berlin", 60) == 0


def test_alignment_is_relative_to_the_window_length() -> None:
    # A 30-minute window already divides the +05:30 offset exactly, so the grid
    # is aligned and shifting it would move every boundary off the clock.
    assert window_alignment_minutes("Asia/Colombo", 30) == 0
    assert window_alignment_minutes("Asia/Colombo", 15) == 0


def test_alignment_is_always_inside_one_window() -> None:
    # A shift of a whole window or more would be indistinguishable from no shift
    # at all, so the remainder is the only meaningful value.
    for minutes in (15, 30, 60, 120, 1440):
        offset = window_alignment_minutes("Asia/Colombo", minutes)
        assert 0 <= offset < minutes

"""Tests for the daily tariff file.

The two properties that make billing defensible are tested here: a household's
tier is stable across days, while its rate varies per day and reproduces exactly
when a past day is regenerated.
"""

from __future__ import annotations

import csv
import io
from datetime import date

import pytest

from common.config import load_config
from sources.tariff_generator import (
    CSV_COLUMNS,
    assign_tier,
    build_rows,
    daily_rate,
    has_subsidy,
    rows_to_csv,
    tariff_key,
)

DAY_1 = date(2026, 1, 1)
DAY_2 = date(2026, 1, 2)


@pytest.fixture(scope="module")
def cfg():
    return load_config()


# --- tier assignment -------------------------------------------------------
def test_every_household_gets_a_defined_tier(cfg) -> None:
    weights, tiers = cfg.billing["tier_weights"], cfg.billing["tiers"]
    for row in build_rows(cfg, DAY_1):
        assert row["billing_tier"] in tiers
        assert assign_tier(row["household_id"], weights) == row["billing_tier"]


def test_tier_is_stable_across_days(cfg) -> None:
    # A household must not drift between tiers day to day; that would make the
    # billing history incoherent.
    day1 = {r["household_id"]: r["billing_tier"] for r in build_rows(cfg, DAY_1)}
    day2 = {r["household_id"]: r["billing_tier"] for r in build_rows(cfg, DAY_2)}
    assert day1 == day2


def test_tier_assignment_uses_all_configured_tiers(cfg) -> None:
    assigned = {r["billing_tier"] for r in build_rows(cfg, DAY_1)}
    assert assigned == set(cfg.billing["tiers"]), (
        "with 40 households every configured tier should appear at least once"
    )


def test_weights_need_not_sum_to_one() -> None:
    # Normalisation means a hand-edited config.yaml cannot leave a household
    # without a tier.
    weights = {"a": 2.0, "b": 2.0}
    assert assign_tier("H-101", weights) in weights
    assert assign_tier("H-140", weights) in weights


# --- subsidy ---------------------------------------------------------------
def test_subsidy_is_deterministic(cfg) -> None:
    probability = cfg.billing["subsidy_probability"]
    assert has_subsidy("H-101", probability) == has_subsidy("H-101", probability)


def test_subsidy_probability_bounds_are_absolute() -> None:
    assert has_subsidy("H-101", 0.0) is False
    assert has_subsidy("H-101", 1.0) is True


# --- daily rate ------------------------------------------------------------
def test_rate_stays_within_the_jitter_band() -> None:
    for household in ("H-101", "H-120", "H-140"):
        rate = daily_rate(household, DAY_1, base_rate=45.0, jitter_pct=0.05)
        assert 45.0 * 0.95 <= rate <= 45.0 * 1.05


def test_same_day_reproduces_exactly_but_days_differ() -> None:
    # Reproducibility is what lets the batch layer recompute a past day and get
    # the same answer; daily variation is what makes it a *daily* tariff.
    assert daily_rate("H-101", DAY_1, 45.0, 0.05) == daily_rate("H-101", DAY_1, 45.0, 0.05)
    rates = {daily_rate("H-101", date(2026, 1, d), 45.0, 0.05) for d in range(1, 15)}
    assert len(rates) > 1


# --- the file itself -------------------------------------------------------
def test_one_row_per_household(cfg) -> None:
    rows = build_rows(cfg, DAY_1)
    assert len(rows) == cfg.total_meters
    assert len({r["household_id"] for r in rows}) == len(rows)


def test_csv_matches_the_documented_schema(cfg) -> None:
    text = rows_to_csv(build_rows(cfg, DAY_1))
    parsed = list(csv.DictReader(io.StringIO(text)))
    assert text.splitlines()[0] == ",".join(CSV_COLUMNS)
    assert len(parsed) == cfg.total_meters
    # subsidy_flag must be parseable by Spark's boolean cast.
    assert {r["subsidy_flag"] for r in parsed} <= {"true", "false"}


def test_filename_follows_the_tariff_pattern(cfg) -> None:
    assert tariff_key(cfg, DAY_1) == "tariff_20260101.csv"

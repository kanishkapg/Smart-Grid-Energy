"""Tests for the billing arithmetic -- the part of the project that is money.

Everything here runs without Spark, Kafka or a database, which is the reason
`processing/shared/billing.py` is written in plain Python: an examiner can
follow these numbers by hand, and so can a hand calculator during a viva.

Two of the tests below check nothing about arithmetic at all. They assert that
the tariff file's columns and its object key are the same on the writing side
(`sources/tariff_generator.py`) and the reading side. Those two modules never
talk to each other, so a rename on one side would otherwise surface as forty
households silently missing a rate.
"""

from __future__ import annotations

from datetime import date

import pytest

from common.config import load_config
from processing.shared.billing import (
    TARIFF_FIELDS,
    BillingTerms,
    bill_household,
    net_billable_kwh,
    parse_tariff_csv,
    summarise_bills,
    tariff_object_key,
)
from processing.shared.pg import DAILY_BILL_COLUMNS
from sources.tariff_generator import CSV_COLUMNS, build_rows, rows_to_csv, tariff_key


@pytest.fixture(scope="module")
def cfg():
    return load_config()


@pytest.fixture(scope="module")
def terms(cfg):
    return BillingTerms.from_config(cfg.billing)


def usage(consumption: float, solar: float, household: str = "H-101") -> dict:
    return {
        "household_id": household,
        "grid_zone": "Colombo-North",
        "total_consumption_kwh": consumption,
        "total_solar_kwh": solar,
    }


def tariff(rate: float = 45.0, subsidy: bool = True, tier: str = "domestic-2") -> dict:
    return {"tariff_rate": rate, "billing_tier": tier, "subsidy_flag": subsidy}


# ---------------------------------------------------------------------------
# the worked example
# ---------------------------------------------------------------------------

def test_the_project_plans_worked_example(terms) -> None:
    """H-101: 18 kWh used, 6 kWh generated, rate 45, subsidised.

        net   = max(18 - 6, 0) = 12 kWh
        gross = 12 x 45        = 540
        subsidy (10%)          = 54
        payable                = 486
    """
    bill = bill_household(usage(18.0, 6.0), tariff(45.0, subsidy=True), terms)
    assert bill["net_kwh"] == 12.0
    assert bill["gross_amount"] == 540.0
    assert bill["subsidy_amount"] == 54.0
    assert bill["net_amount"] == 486.0


def test_an_unsubsidised_household_pays_the_gross(terms) -> None:
    bill = bill_household(usage(18.0, 6.0), tariff(45.0, subsidy=False), terms)
    assert bill["subsidy_amount"] == 0.0
    assert bill["net_amount"] == bill["gross_amount"] == 540.0


# ---------------------------------------------------------------------------
# net metering
# ---------------------------------------------------------------------------

def test_a_net_exporter_owes_nothing_and_is_not_paid(terms) -> None:
    # Import-only metering: generating 20 kWh against 8 kWh of demand zeroes the
    # bill, it does not produce a credit. Without the clamp this would be -540.
    bill = bill_household(usage(8.0, 20.0), tariff(45.0), terms)
    assert bill["net_kwh"] == 0.0
    assert bill["net_amount"] == 0.0
    # The usage columns still record what happened -- the clamp is a billing
    # rule, not a rewriting of the meter.
    assert bill["total_solar_kwh"] == 20.0


def test_a_household_without_solar_pays_for_everything_it_used(terms) -> None:
    assert net_billable_kwh(18.0, 0.0) == 18.0


def test_an_unimplemented_metering_scheme_is_refused() -> None:
    # Silently falling back to import-only would under- or over-charge every
    # exporting household, so a config typo has to raise.
    with pytest.raises(ValueError, match="net_metering"):
        net_billable_kwh(8.0, 20.0, net_metering="feed_in_tariff")


# ---------------------------------------------------------------------------
# rounding
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("consumption,solar,rate", [
    (17.3333, 5.1111, 41.37),
    (23.9999, 0.0, 62.53),
    (9.1234, 9.1233, 30.21),
])
def test_the_three_money_columns_always_add_up(terms, consumption, solar, rate) -> None:
    # A bill whose parts do not reconcile is indefensible however small the gap,
    # and rounding each column independently is exactly how that happens.
    bill = bill_household(usage(consumption, solar), tariff(rate), terms)
    assert bill["gross_amount"] - bill["subsidy_amount"] == pytest.approx(
        bill["net_amount"], abs=1e-9)
    assert bill["gross_amount"] == pytest.approx(
        bill["net_kwh"] * bill["tariff_rate"], abs=0.005)


def test_money_is_stored_to_the_cent(terms) -> None:
    bill = bill_household(usage(17.3333, 5.1111), tariff(41.37), terms)
    for field in ("gross_amount", "subsidy_amount", "net_amount"):
        assert round(bill[field], 2) == bill[field], field


# ---------------------------------------------------------------------------
# the contract with the tariff file and the serving table
# ---------------------------------------------------------------------------

def test_the_reader_expects_the_columns_the_generator_writes() -> None:
    assert tuple(TARIFF_FIELDS) == tuple(CSV_COLUMNS)


def test_the_reader_looks_for_the_key_the_generator_wrote(cfg) -> None:
    day = date(2026, 1, 27)
    assert tariff_object_key(cfg.storage["tariff_filename_pattern"], day) == \
        tariff_key(cfg, day)


def test_a_bill_carries_every_column_the_serving_table_needs(terms) -> None:
    # sim_day and run_id are added by the caller, which is what knows them.
    produced = set(bill_household(usage(18.0, 6.0), tariff(), terms)) | {"sim_day", "run_id"}
    assert produced == set(DAILY_BILL_COLUMNS)


def test_a_generated_tariff_file_parses_back_to_its_own_values(cfg) -> None:
    """Round-trip through the actual CSV the generator writes.

    The types are the point: CSV has none, so `"true"` has to come back as a
    bool. `bool("false")` is True, and that mistake would subsidise the whole
    grid by 10%.
    """
    day = date(2026, 1, 27)
    written = build_rows(cfg, day)
    parsed = parse_tariff_csv(rows_to_csv(written))

    assert len(parsed) == cfg.total_meters
    assert {row["household_id"] for row in parsed} == {r["household_id"] for r in written}
    assert all(isinstance(row["subsidy_flag"], bool) for row in parsed)
    assert all(isinstance(row["tariff_rate"], float) for row in parsed)
    assert [row["subsidy_flag"] for row in parsed] == \
        [r["subsidy_flag"] == "true" for r in written]


def test_a_tariff_file_with_no_rows_is_an_error() -> None:
    with pytest.raises(ValueError):
        parse_tariff_csv("household_id,tariff_rate,billing_tier,subsidy_flag\n")


# ---------------------------------------------------------------------------
# day totals
# ---------------------------------------------------------------------------

def test_the_day_total_is_the_sum_of_its_bills(terms) -> None:
    bills = [
        bill_household(usage(18.0, 6.0, "H-101"), tariff(45.0, subsidy=True), terms),
        bill_household(usage(20.0, 0.0, "H-102"), tariff(30.0, subsidy=False), terms),
    ]
    summary = summarise_bills(bills)

    assert summary["households"] == 2
    assert summary["subsidised_households"] == 1
    assert summary["net_kwh"] == 32.0                    # 12 + 20
    assert summary["gross_amount"] == 1140.0             # 540 + 600
    assert summary["net_amount"] == 1086.0               # 486 + 600


def test_every_configured_tier_produces_a_usable_rate(cfg, terms) -> None:
    # Guards against a tier defined in tier_weights but priced at zero, which
    # would bill a third of the grid nothing at all and look like a data gap.
    for tier, settings in cfg.billing["tiers"].items():
        bill = bill_household(usage(18.0, 6.0), tariff(float(settings["rate"]), tier=tier),
                              terms)
        assert bill["net_amount"] > 0, tier

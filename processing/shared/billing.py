"""The billing arithmetic: the one place money is calculated.

Pure functions over plain values -- no pyspark, no psycopg, no config loader --
for three reasons:

  * money has to be verifiable. The project plan's worked example (H-101) is a
    unit test here, not a number nobody re-checked.
  * `scripts/check_phase4.py` recomputes a bill on the host, where there is no
    JVM, and compares it against what the Spark job stored. An independent
    recomputation is only independent if it can run outside Spark.
  * a rule expressed as a Spark `when` chain can only be exercised by running the
    whole batch job; expressed as a function it can be read and tested.

The batch job therefore does the *big-data* work in Spark -- scan the Parquet
master store, deduplicate, aggregate per household, join the tariff -- and
applies these functions to the one row per household that comes back. At forty
households a day that is the right split; the honest limit is the same one
documented in `processing/shared/pg.py`.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, Sequence

# kWh to 4 decimals (0.1 Wh) and money to 2. Declared here rather than imported
# from `transforms.KWH_PRECISION` because that module needs pyspark and this one
# must stay importable without a JVM.
KWH_PRECISION = 4
MONEY_PRECISION = 2

IMPORT_ONLY = "import_only"

# The tariff file's contract, as the batch layer reads it. Written by
# sources/tariff_generator.py; `tests/test_billing.py` asserts the two agree, so
# a column renamed on the producing side fails a test instead of silently
# nulling a rate.
TARIFF_FIELDS: Sequence[str] = ("household_id", "tariff_rate", "billing_tier", "subsidy_flag")


@dataclass(frozen=True)
class BillingTerms:
    """The commercial terms, read once from config.yaml."""

    subsidy_pct: float
    net_metering: str = IMPORT_ONLY
    currency: str = "LKR"

    @classmethod
    def from_config(cls, billing: Mapping[str, Any]) -> "BillingTerms":
        """Built from `cfg.billing`. Takes the section, not the whole Config, so
        this module stays independent of how configuration is loaded."""
        return cls(
            subsidy_pct=float(billing["subsidy_pct"]),
            net_metering=str(billing.get("net_metering", IMPORT_ONLY)),
            currency=str(billing.get("currency", "LKR")),
        )


def net_billable_kwh(consumption_kwh: float, solar_kwh: float,
                     net_metering: str = IMPORT_ONLY) -> float:
    """The units the household actually pays for.

    Under import-only net metering a household that generates more than it
    consumes is not paid for the surplus -- it simply owes nothing. Clamping at
    zero is what encodes that, and it is not a defensive `max()`: a solar
    household at midday genuinely generates more than it draws, so without the
    clamp a day's bill could come out negative and the household would be
    credited for electricity nobody bought.
    """
    if net_metering != IMPORT_ONLY:
        raise ValueError(
            "unsupported billing.net_metering {!r}; this model only implements "
            "{!r}, and paying for exports would need an export rate as well"
            .format(net_metering, IMPORT_ONLY)
        )
    return round(max(float(consumption_kwh) - float(solar_kwh), 0.0), KWH_PRECISION)


def bill_household(usage: Mapping[str, Any], tariff: Mapping[str, Any],
                   terms: BillingTerms) -> Dict[str, Any]:
    """One household's bill for one simulated day: a `daily_bill` row.

    `usage` is the day's aggregate for the household (the Spark side);
    `tariff` is its row from that day's tariff file. `sim_day` and `run_id` are
    added by the caller, which is what knows them.
    """
    net_kwh = net_billable_kwh(
        usage["total_consumption_kwh"], usage["total_solar_kwh"], terms.net_metering
    )
    rate = float(tariff["tariff_rate"])
    subsidised = bool(tariff["subsidy_flag"])

    gross = round(net_kwh * rate, MONEY_PRECISION)
    # Rounded before the subtraction, so gross - subsidy is exactly the stored
    # net_amount. Carrying full precision into net_amount and rounding it
    # separately lets the three columns disagree by a cent, and a bill whose
    # parts do not add up is indefensible however small the gap.
    subsidy = round(gross * terms.subsidy_pct, MONEY_PRECISION) if subsidised else 0.0

    return {
        "household_id": usage["household_id"],
        "grid_zone": usage["grid_zone"],
        "total_consumption_kwh": round(float(usage["total_consumption_kwh"]), KWH_PRECISION),
        "total_solar_kwh": round(float(usage["total_solar_kwh"]), KWH_PRECISION),
        "net_kwh": net_kwh,
        "tariff_rate": rate,
        "billing_tier": tariff["billing_tier"],
        "subsidy_flag": subsidised,
        "gross_amount": gross,
        "subsidy_amount": subsidy,
        "net_amount": round(gross - subsidy, MONEY_PRECISION),
    }


def summarise_bills(bills: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """Day totals, for the job's log line, the report header and the DAG's check."""
    bills = list(bills)
    total = lambda field: round(sum(float(b[field]) for b in bills), KWH_PRECISION)  # noqa: E731
    return {
        "households": len(bills),
        "total_consumption_kwh": total("total_consumption_kwh"),
        "total_solar_kwh": total("total_solar_kwh"),
        "net_kwh": total("net_kwh"),
        "gross_amount": round(sum(float(b["gross_amount"]) for b in bills), MONEY_PRECISION),
        "subsidy_amount": round(sum(float(b["subsidy_amount"]) for b in bills), MONEY_PRECISION),
        "net_amount": round(sum(float(b["net_amount"]) for b in bills), MONEY_PRECISION),
        "subsidised_households": sum(1 for b in bills if b["subsidy_flag"]),
    }


# ---------------------------------------------------------------------------
# the tariff file
# ---------------------------------------------------------------------------

def tariff_object_key(pattern: str, sim_day: date) -> str:
    """Object key of a simulated day's tariff file, from the configured pattern."""
    return pattern.format(sim_day=sim_day.strftime("%Y%m%d"))


def parse_tariff_csv(text: str) -> List[Dict[str, Any]]:
    """Parse a tariff CSV into typed rows.

    CSV carries no types, so this is where `"45.0"` becomes a float and
    `"true"` becomes a bool. Doing it once, here, is what stops the Spark job
    and the verification script from disagreeing about what the file said --
    and a truthy `bool("false")` would quietly subsidise every household.
    """
    rows: List[Dict[str, Any]] = []
    for row in csv.DictReader(io.StringIO(text)):
        missing = [field for field in TARIFF_FIELDS if row.get(field) in (None, "")]
        if missing:
            raise ValueError("tariff row {} is missing {}".format(row, missing))
        rows.append({
            "household_id": row["household_id"],
            "tariff_rate": float(row["tariff_rate"]),
            "billing_tier": row["billing_tier"],
            "subsidy_flag": row["subsidy_flag"].strip().lower() == "true",
        })
    if not rows:
        raise ValueError("tariff file holds a header but no rows")
    return rows

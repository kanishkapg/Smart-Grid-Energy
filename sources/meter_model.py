"""The simulated meter population and the physics of a single reading.

Pure functions and a small registry: no Kafka, no object storage, no reading of
the wall clock. Keeping the interesting logic free of I/O is what makes it
unit-testable, and this is the part of the pipeline the viva will ask about.

The one thing to understand here: a reading reports **energy consumed during an
interval**, not instantaneous power. Each reading covers 9.6 simulated minutes
(0.16 simulated hours), so kW is multiplied by that interval to get kWh.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Meter:
    meter_id: str
    household_id: str
    grid_zone: str
    base_load_kw: float
    panel_kw: float          # 0.0 for a household with no rooftop solar

    @property
    def has_solar(self) -> bool:
        return self.panel_kw > 0


def stable_fraction(*parts: str) -> float:
    """A deterministic float in [0, 1) derived from the given strings.

    Used instead of `random.random()` wherever a decision must come out the same
    on every run and in every process: which households have solar, which tariff
    tier a household sits on. The producer and the tariff generator never talk to
    each other, yet both must agree about H-101 -- hashing the id gives that for
    free, where a seeded RNG would depend on call order.
    """
    digest = hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / 0x1_0000_0000


def build_meters(cfg) -> list[Meter]:
    """The meter population, identical on every run and in every component.

    Solar ownership is assigned by **quota**, not by an independent coin flip per
    household: each zone gets exactly `round(meters * solar_penetration)` panels,
    and *which* households receive them is settled by ranking the hash.

    The quota matters because the zones only hold 9-12 meters each, where
    independent draws vary wildly. Flipping a coin per household gave Kandy zero
    solar homes against a 0.30 target, which would have pinned its renewable
    share at 0% for the whole run and made the low-renewable alert fire
    permanently. Ranking a hash keeps the assignment fully deterministic, so the
    producer and the tariff generator still agree without talking to each other.
    """
    grid = cfg.grid
    meters: list[Meter] = []
    next_number = 101                       # H-101 onward, matching the plan
    for zone in cfg.zones:
        count = int(zone["meters"])
        household_ids = [
            f"{grid['household_id_prefix']}{n}"
            for n in range(next_number, next_number + count)
        ]

        quota = round(count * float(zone["solar_penetration"]))
        with_solar = set(
            sorted(household_ids, key=lambda h: stable_fraction("solar", h))[:quota]
        )

        for offset, household_id in enumerate(household_ids):
            meters.append(
                Meter(
                    meter_id=f"{grid['meter_id_prefix']}{next_number + offset}",
                    household_id=household_id,
                    grid_zone=zone["name"],
                    base_load_kw=float(zone["base_load_kw"]),
                    panel_kw=(
                        float(grid["solar_panel_kw"]) if household_id in with_solar else 0.0
                    ),
                )
            )
        next_number += count
    return meters


# Household demand across the day as a multiple of base_load_kw: an overnight
# trough, a morning peak around 07:00 and a larger evening peak around 19:00.
# A flat load would make the renewable-percentage metric far less interesting,
# because solar peaks at midday when household demand is actually at its lowest.
_HOURLY_LOAD = [
    0.45, 0.40, 0.38, 0.38, 0.42, 0.60,   # 00-05  overnight trough
    0.95, 1.25, 1.10, 0.85, 0.75, 0.70,   # 06-11  morning peak at 07:00
    0.72, 0.70, 0.68, 0.72, 0.85, 1.15,   # 12-17  afternoon climb
    1.45, 1.50, 1.30, 1.00, 0.70, 0.52,   # 18-23  evening peak at 19:00
]


def load_multiplier(when: datetime) -> float:
    """Interpolate the hourly demand curve so load moves smoothly, not in steps."""
    hour = when.hour + when.minute / 60 + when.second / 3600
    low = int(hour) % 24
    high = (low + 1) % 24
    fraction = hour - int(hour)
    return _HOURLY_LOAD[low] * (1 - fraction) + _HOURLY_LOAD[high] * fraction


def make_reading(
    meter: Meter,
    event_time: datetime,
    interval_hours: float,
    solar_factor: float,
    rng,
    load_jitter_pct: float = 0.25,
    cloud_cover: float = 0.0,
) -> dict:
    """One meter reading, in the exact shape published to Kafka.

    `solar_factor` is the 0.0-1.0 irradiance curve from SimClock, which is
    exactly 0 outside daylight. `cloud_cover` scales generation down and is what
    the producer's --cloud-cover flag drives, to force the low-renewable alert.
    """
    consumption = meter.base_load_kw * load_multiplier(event_time) * interval_hours
    consumption *= 1 + rng.uniform(-load_jitter_pct, load_jitter_pct)

    # Stays exactly 0.0 at night and for a household without panels, rather than
    # a small floating-point smear that would look like phantom generation.
    solar = 0.0
    if meter.panel_kw and solar_factor > 0:
        solar = meter.panel_kw * solar_factor * interval_hours * (1.0 - cloud_cover)
        solar *= 1 + rng.uniform(-0.10, 0.10)

    return {
        "meter_id": meter.meter_id,
        "household_id": meter.household_id,
        "grid_zone": meter.grid_zone,
        "power_consumption_kwh": round(max(consumption, 0.0), 4),
        "solar_generation_kwh": round(max(solar, 0.0), 4),
        "timestamp": event_time.isoformat(),
    }


# The contract the rest of the pipeline relies on. Phase 2 builds its Spark
# schema from this list, and scripts/check_phase1.py validates against it.
EVENT_FIELDS = (
    "meter_id",
    "household_id",
    "grid_zone",
    "power_consumption_kwh",
    "solar_generation_kwh",
    "timestamp",
)

"""Threshold rules, as pure functions over one aggregated zone window.

Kept out of the Spark job on purpose. Expressed in Spark these would be `when`
chains that can only be exercised by running the whole streaming pipeline;
expressed as plain functions over plain values they are unit-testable
(`tests/test_alert_rules.py`) and readable to an examiner who does not know
Spark. The speed layer already collects each micro-batch's handful of window
rows to the driver in order to upsert them, so applying the rules there costs
nothing.

No imports from pyspark, psycopg or the project's config loader: a rule takes
values in and returns alerts out.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

LOW_RENEWABLE = "low_renewable_contribution"
ZONE_OVERLOAD = "zone_overload"


@dataclass(frozen=True)
class Alert:
    """One raised alert, ready to be logged and inserted into `alerts`."""

    level: str          # INFO | WARN | CRITICAL, matching the table's CHECK
    grid_zone: str
    rule: str
    message: str
    window_start: datetime

    @property
    def dedupe_key(self) -> tuple:
        """Identity of the *condition*, not of this evaluation of it.

        The speed layer runs in update mode, so an open window is re-emitted on
        every micro-batch and each rule would otherwise raise the same alert a
        dozen times before the window closes. The sink keeps the keys it has
        already written and skips repeats.
        """
        return (self.rule, self.grid_zone, self.window_start)


def evaluate_zone_window(row: Dict[str, Any], thresholds: Dict[str, Any],
                         window_minutes: int) -> List[Alert]:
    """Every rule that the given zone window breaches.

    `row` is one row of `zone_metrics` as a dict. `window_start` must be
    timezone-aware in the simulation's zone -- the hour of day is what decides
    whether the solar rules apply, and a UTC timestamp would put Sri Lankan
    solar noon at 06:30.
    """
    alerts: List[Alert] = []
    zone = row["grid_zone"]
    window_start = row["window_start"]

    # -- low renewable contribution ----------------------------------------
    # Only around solar noon. At 06:00 a renewable share of 0% is sunrise, not
    # an incident, and an alert that fires every dawn tells an operator nothing.
    if _hour_in(window_start, thresholds["renewable_check_start_hour"],
                thresholds["renewable_check_end_hour"]):
        renewable_pct = float(row["renewable_pct"])
        floor_pct = float(thresholds["low_renewable_pct"])
        if renewable_pct < floor_pct:
            alerts.append(Alert(
                level="WARN", grid_zone=zone, rule=LOW_RENEWABLE,
                window_start=window_start,
                message=(
                    "Low renewable contribution, {}: {:.1f}% "
                    "(floor {:.0f}%) at {} simulated".format(
                        zone, renewable_pct, floor_pct,
                        window_start.strftime("%Y-%m-%d %H:%M"))
                ),
            ))

    # -- zone overload ------------------------------------------------------
    # Consumption is energy over the window, so dividing by the window length
    # gives mean power, and dividing again by the meter count normalises zones
    # of different sizes onto one threshold.
    mean_kw = mean_kw_per_meter(
        row["total_consumption_kwh"], window_minutes, row["active_meters"]
    )
    ceiling_kw = float(thresholds["zone_overload_kw_per_meter"])
    if mean_kw > ceiling_kw:
        alerts.append(Alert(
            level="WARN", grid_zone=zone, rule=ZONE_OVERLOAD,
            window_start=window_start,
            message=(
                "Zone load high, {}: {:.2f} kW/meter across {} meters "
                "(ceiling {:.2f})".format(
                    zone, mean_kw, row["active_meters"], ceiling_kw)
            ),
        ))

    return alerts


def mean_kw_per_meter(consumption_kwh: float, window_minutes: int,
                      active_meters: int) -> float:
    """Mean draw per household over the window, in kW.

    Returns 0.0 for a window with no meters rather than dividing by zero: an
    empty window means no data arrived, which is the health check's business
    (Phase 6), not the overload rule's.
    """
    if active_meters <= 0 or window_minutes <= 0:
        return 0.0
    window_hours = window_minutes / 60.0
    return float(consumption_kwh) / window_hours / active_meters


def _hour_in(moment: datetime, start_hour: int, end_hour: int) -> bool:
    """Is the local hour of `moment` within [start_hour, end_hour)?"""
    return int(start_hour) <= moment.hour < int(end_hour)

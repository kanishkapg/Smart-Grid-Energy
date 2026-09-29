"""The simulated clock: one simulated day compressed into five real minutes.

Every component must agree on what "now" is in simulated time, or the producer
emits readings for sim-day 3 while the tariff generator drops a file for
sim-day 4 and the billing join finds nothing.

Agreement comes from a shared **real-time anchor**: the real instant at which
simulated time equalled `simulation.sim_epoch_start`. Resolution order:

    1. the SG_REAL_ANCHOR environment variable (unix seconds) -- how a container
       or an Airflow task inherits the host's clock;
    2. data/.sim_anchor -- how separately started host processes find each other;
    3. otherwise: now, written to that file so whoever starts next agrees.

Delete data/.sim_anchor to restart the simulation at sim-day 0.

Naming convention: `real_*` values are wall-clock seconds on your machine,
`sim_*` values are inside the simulation.
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - only on the Spark image's Python 3.8
    # `zoneinfo` arrived in 3.9 and the apache/spark:3.5.3 image ships 3.8. The
    # backport is API-identical, and it is installed explicitly in
    # infra/spark/Dockerfile rather than left to arrive as psycopg's dependency.
    from backports.zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ANCHOR_FILE = PROJECT_ROOT / "data" / ".sim_anchor"
ANCHOR_ENV_VAR = "SG_REAL_ANCHOR"

SECONDS_PER_DAY = 86400.0


def window_alignment_minutes(timezone: str, window_minutes: int) -> int:
    """How far to shift the windows so they land on local clock boundaries.

    Spark aligns event-time windows to the Unix epoch, which is midnight UTC. In
    a zone offset by a whole number of hours that is invisible, but Asia/Colombo
    is +05:30, so hourly windows come out running 23:30-00:30 rather than
    00:00-01:00. Two things suffer: every window_start in the report and on the
    dashboard is half an hour off the hour, and the alert rules -- which are
    written in local hours, "check renewable between 09:00 and 16:00" -- end up
    testing 09:30 to 16:30 instead.

    Shifting the grid by the remainder of the zone's offset fixes both. Sri Lanka
    has no daylight saving, so the offset is constant and one number is enough.
    """
    offset = datetime.now(ZoneInfo(timezone)).utcoffset()
    if offset is None:
        return 0
    return int((offset.total_seconds() // 60) % window_minutes)


def resolve_real_anchor(anchor_file: Path = ANCHOR_FILE) -> float:
    """Return the shared real-time anchor as unix seconds, creating it if absent."""
    from_env = os.environ.get(ANCHOR_ENV_VAR)
    if from_env:
        return float(from_env)

    if anchor_file.exists():
        try:
            return float(anchor_file.read_text(encoding="utf-8").strip())
        except ValueError:
            pass  # corrupt anchor: fall through and rewrite it

    # Round before both writing and returning, so the process that creates the
    # anchor holds exactly the value later processes will read back. Without
    # this the creator is out by ~1e-7 s and no two components share a clock.
    now = round(time.time(), 6)
    anchor_file.parent.mkdir(parents=True, exist_ok=True)
    # Write then rename, so two processes racing here cannot leave a
    # half-written anchor behind.
    tmp = anchor_file.with_suffix(".tmp")
    tmp.write_text(f"{now:.6f}", encoding="utf-8")
    os.replace(tmp, anchor_file)
    return now


@dataclass
class SimClock:
    """Maps real time onto simulated time.

    >>> clock = SimClock(datetime(2026, 1, 1, tzinfo=timezone.utc), 300, 1000.0)
    >>> clock.sim_time_at(1000.0).isoformat()      # at the anchor
    '2026-01-01T00:00:00+00:00'
    >>> clock.sim_time_at(1150.0).isoformat()      # half a sim-day later
    '2026-01-01T12:00:00+00:00'
    """

    sim_epoch: datetime
    sim_day_real_seconds: float
    real_anchor: float
    tz: Any = timezone.utc

    @classmethod
    def from_config(cls, cfg, real_anchor: float | None = None) -> "SimClock":
        sim = cfg.simulation
        tz = ZoneInfo(sim.get("timezone", "UTC"))
        epoch = datetime.fromisoformat(str(sim["sim_epoch_start"]))
        if epoch.tzinfo is None:
            epoch = epoch.replace(tzinfo=tz)
        return cls(
            sim_epoch=epoch,
            sim_day_real_seconds=float(sim["sim_day_real_seconds"]),
            real_anchor=real_anchor if real_anchor is not None else resolve_real_anchor(),
            tz=tz,
        )

    @property
    def compression_factor(self) -> float:
        """Simulated seconds elapsed per real second (288.0 by default)."""
        return SECONDS_PER_DAY / self.sim_day_real_seconds

    # -- real -> simulated --------------------------------------------------
    def sim_time_at(self, real_ts: float) -> datetime:
        """Simulated datetime for a real unix timestamp."""
        elapsed = real_ts - self.real_anchor
        return self.sim_epoch + timedelta(seconds=elapsed * self.compression_factor)

    def now(self) -> datetime:
        """Simulated datetime right now, in the configured timezone."""
        return self.sim_time_at(time.time()).astimezone(self.tz)

    def sim_day_index(self, real_ts: float | None = None) -> int:
        """Which simulated day we are in, counting from 0 at the anchor."""
        real_ts = time.time() if real_ts is None else real_ts
        return int((real_ts - self.real_anchor) // self.sim_day_real_seconds)

    def sim_day(self, real_ts: float | None = None) -> date:
        """The simulated date -- the partition key for raw data and billing."""
        real_ts = time.time() if real_ts is None else real_ts
        return self.sim_time_at(real_ts).astimezone(self.tz).date()

    def sim_day_label(self, day: date | None = None) -> str:
        """YYYYMMDD, as used in tariff filenames and Parquet partition values."""
        return (day or self.sim_day()).strftime("%Y%m%d")

    # -- simulated -> real: what the tariff generator sleeps on --------------
    def real_seconds_until_next_sim_day(self, real_ts: float | None = None) -> float:
        """Real seconds to wait for the next sim-day boundary.

        The boundary is where the day closes, the tariff file lands, and the
        billing DAG becomes runnable.
        """
        real_ts = time.time() if real_ts is None else real_ts
        next_boundary = self.real_anchor + (
            self.sim_day_index(real_ts) + 1
        ) * self.sim_day_real_seconds
        return max(0.0, next_boundary - real_ts)

    def sim_day_progress(self, real_ts: float | None = None) -> float:
        """Fraction 0.0-1.0 through the current simulated day."""
        real_ts = time.time() if real_ts is None else real_ts
        elapsed = (real_ts - self.real_anchor) % self.sim_day_real_seconds
        return elapsed / self.sim_day_real_seconds

    # -- domain helpers -----------------------------------------------------
    def is_daytime(self, when: datetime | None = None,
                   start_hour: int = 6, end_hour: int = 18) -> bool:
        """Is it daylight in simulated local time?

        The low-renewable alert only makes sense during the day: 0% solar at
        02:00 is correct behaviour, not an incident.
        """
        moment = (when or self.now()).astimezone(self.tz)
        return start_hour <= moment.hour < end_hour

    def solar_factor(self, when: datetime | None = None,
                     start_hour: int = 6, end_hour: int = 18) -> float:
        """Solar irradiance as 0.0-1.0 across the simulated day.

        A sine curve over the daylight span: 0 at sunrise and sunset, 1 at solar
        noon, and exactly 0 at night rather than a floating-point smear.
        """
        moment = (when or self.now()).astimezone(self.tz)
        hour = moment.hour + moment.minute / 60 + moment.second / 3600
        if not (start_hour <= hour < end_hour):
            return 0.0
        return math.sin(math.pi * (hour - start_hour) / (end_hour - start_hour))

    def summary(self) -> dict:
        """Loggable snapshot for a component's startup banner."""
        return {
            "sim_now": self.now().isoformat(),
            "sim_day": self.sim_day().isoformat(),
            "sim_day_index": self.sim_day_index(),
            "compression_factor": round(self.compression_factor, 1),
        }

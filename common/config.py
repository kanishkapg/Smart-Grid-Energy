"""Configuration loading.

Two sources, deliberately split:

    config/config.yaml  domain settings -- zones, thresholds, window sizes,
                        tariff tiers. Checked into git. This is the file to
                        open when asking "where does that number come from?"

    .env                deployment settings -- hostnames, ports, credentials.
                        Not checked in; differs between host and container.

`.env` is copied into `os.environ` once, with `setdefault` so a real environment
variable always wins. After that every consumer just reads `os.environ`, which
keeps the precedence rule in one place.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
ENV_PATH = PROJECT_ROOT / ".env"


def load_dotenv(path: Path = ENV_PATH) -> None:
    """Copy `.env` into os.environ. Real environment variables take precedence."""
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


@dataclass
class Config:
    """Domain settings from YAML, deployment endpoints from the environment."""

    raw: dict[str, Any]

    # -- domain sections ----------------------------------------------------
    @property
    def simulation(self) -> dict[str, Any]:
        return self.raw["simulation"]

    @property
    def grid(self) -> dict[str, Any]:
        return self.raw["grid"]

    @property
    def zones(self) -> list[dict[str, Any]]:
        return self.raw["grid"]["zones"]

    @property
    def zone_names(self) -> list[str]:
        return [z["name"] for z in self.zones]

    @property
    def total_meters(self) -> int:
        return sum(int(z["meters"]) for z in self.zones)

    @property
    def storage(self) -> dict[str, Any]:
        return self.raw["storage"]

    @property
    def speed_layer(self) -> dict[str, Any]:
        return self.raw["speed_layer"]

    @property
    def alerts(self) -> dict[str, Any]:
        return self.raw["alerts"]

    @property
    def billing(self) -> dict[str, Any]:
        return self.raw["billing"]

    @property
    def logging(self) -> dict[str, Any]:
        return self.raw["logging"]

    # -- derived simulated-clock facts, quoted in the README and the report --
    @property
    def compression_factor(self) -> float:
        """Simulated seconds that pass per real second (288.0 by default)."""
        return 86400.0 / float(self.simulation["sim_day_real_seconds"])

    @property
    def readings_per_meter_per_sim_day(self) -> int:
        return int(
            float(self.simulation["sim_day_real_seconds"])
            / float(self.simulation["meter_interval_seconds"])
        )

    @property
    def sim_minutes_per_reading(self) -> float:
        """Gap between one meter's consecutive readings, in simulated minutes."""
        return 1440.0 / self.readings_per_meter_per_sim_day

    def clock_summary(self) -> dict[str, Any]:
        """Logged by every component on startup so the clock is never implicit."""
        return {
            "sim_day_real_seconds": self.simulation["sim_day_real_seconds"],
            "meter_interval_seconds": self.simulation["meter_interval_seconds"],
            "compression_factor": round(self.compression_factor, 1),
            "readings_per_meter_per_sim_day": self.readings_per_meter_per_sim_day,
            "sim_minutes_per_reading": round(self.sim_minutes_per_reading, 1),
        }

    # -- deployment endpoints -----------------------------------------------
    # Host processes reach the services through published ports; containers use
    # the compose service names. SG_IN_DOCKER=1 selects the latter.
    @property
    def in_docker(self) -> bool:
        return os.environ.get("SG_IN_DOCKER") == "1"

    @property
    def kafka_bootstrap(self) -> str:
        if self.in_docker:
            return os.environ.get("KAFKA_INTERNAL_BOOTSTRAP", "kafka:9092")
        return os.environ.get("KAFKA_EXTERNAL_BOOTSTRAP", "localhost:29092")

    @property
    def kafka_topic(self) -> str:
        return os.environ.get("KAFKA_TOPIC_READINGS", self.raw["kafka"]["topic_readings"])

    @property
    def s3_endpoint(self) -> str:
        if self.in_docker:
            return os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
        return os.environ.get("S3_EXTERNAL_ENDPOINT", "http://localhost:8333")

    @property
    def s3_credentials(self) -> tuple[str, str]:
        return (
            os.environ.get("S3_ACCESS_KEY", "smartgrid"),
            os.environ.get("S3_SECRET_KEY", "smartgridsecret"),
        )

    @property
    def s3_region(self) -> str:
        return os.environ.get("S3_REGION", "us-east-1")

    def bucket(self, kind: str) -> str:
        """kind is one of: tariff, reports."""
        return os.environ.get(
            f"S3_BUCKET_{kind.upper()}", self.storage[f"bucket_{kind}"]
        )

    @property
    def raw_store_path(self) -> str:
        """Filesystem path of the Parquet master store.

        A mounted volume, not object storage -- see the note in config.yaml. The
        container and the host see the same directory at different paths, so the
        right one is chosen the same way the service endpoints are.
        """
        root = (
            self.storage["raw_root_container"] if self.in_docker
            else self.storage["raw_root_host"]
        )
        return f"{root}/{self.storage['raw_prefix']}"

    @property
    def postgres_dsn(self) -> str:
        host = os.environ.get("POSTGRES_HOST", "postgres" if self.in_docker else "localhost")
        return (
            f"postgresql://{os.environ.get('POSTGRES_USER', 'grid')}"
            f":{os.environ.get('POSTGRES_PASSWORD', 'gridpass')}"
            f"@{host}:{os.environ.get('POSTGRES_PORT', '5432')}"
            f"/{os.environ.get('POSTGRES_DB', 'smartgrid')}"
        )


def validate(cfg: Config) -> None:
    """Fail at startup rather than mysteriously three phases later.

    Each rule below corresponds to a mistake that is otherwise *silent*.
    """
    for section in ("simulation", "grid", "kafka", "storage", "speed_layer", "alerts", "billing"):
        if section not in cfg.raw:
            raise ValueError(f"config.yaml is missing the '{section}' section")

    if not cfg.zones or cfg.total_meters <= 0:
        raise ValueError("config.yaml: grid.zones must define at least one meter")

    if float(cfg.simulation["sim_day_real_seconds"]) <= 0 or float(
        cfg.simulation["meter_interval_seconds"]
    ) <= 0:
        raise ValueError("config.yaml: simulation intervals must be greater than zero")

    # A tier a household can be assigned but which has no rate produces a
    # null join and a zero bill -- no error, just a wrong number.
    undefined = set(cfg.billing["tier_weights"]) - set(cfg.billing["tiers"])
    if undefined:
        raise ValueError(f"config.yaml: billing.tier_weights has no rate for {sorted(undefined)}")

    # A watermark shorter than the gap between readings classifies perfectly
    # ordinary events as late and drops them.
    watermark = float(cfg.speed_layer["watermark_minutes"])
    if watermark < cfg.sim_minutes_per_reading:
        raise ValueError(
            "config.yaml: speed_layer.watermark_minutes is shorter than the "
            f"{cfg.sim_minutes_per_reading:.1f} simulated-minute gap between readings; "
            "valid events would be dropped as late"
        )

    # The producer deliberately backdates a few events. If it can backdate them
    # further than the watermark tolerates, the speed layer silently drops data
    # that the batch layer still counts, and the two layers disagree for reasons
    # that look like a bug rather than a setting.
    if float(cfg.simulation["late_event_max_sim_minutes"]) > watermark:
        raise ValueError(
            "config.yaml: simulation.late_event_max_sim_minutes exceeds "
            "speed_layer.watermark_minutes; the producer would emit events the "
            "speed layer must discard"
        )


def load_config(path: Path | str = CONFIG_PATH) -> Config:
    """Read .env and config.yaml, validate, return."""
    load_dotenv()
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    cfg = Config(yaml.safe_load(path.read_text(encoding="utf-8")))
    validate(cfg)
    return cfg

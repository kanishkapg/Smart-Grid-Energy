"""Tests for config loading and its validation rules.

Each validation rule exists because the mistake it catches is otherwise
*silent*: a watermark shorter than the reading interval quietly drops valid
events, and a tariff tier with no rate quietly produces a zero bill.
"""

from __future__ import annotations

import copy

import pytest

from common.config import Config, load_config, load_dotenv, validate


@pytest.fixture(scope="module")
def cfg() -> Config:
    return load_config()


def _mutated(cfg: Config, mutate) -> Config:
    raw = copy.deepcopy(cfg.raw)
    mutate(raw)
    return Config(raw)


# --- the real project config must describe the documented simulation -------
def test_zone_topology_loads(cfg: Config) -> None:
    assert "Colombo-North" in cfg.zone_names
    assert cfg.total_meters == sum(z["meters"] for z in cfg.zones)


def test_documented_sim_clock_numbers(cfg: Config) -> None:
    # The figures quoted in the README and the report. If config.yaml changes
    # without the docs being updated, this test says so.
    summary = cfg.clock_summary()
    assert summary["compression_factor"] == pytest.approx(288.0)
    assert summary["readings_per_meter_per_sim_day"] == 150
    assert summary["sim_minutes_per_reading"] == pytest.approx(9.6)


def test_host_and_container_endpoints_differ(cfg: Config, monkeypatch) -> None:
    # From the host we must use the published external listener...
    monkeypatch.delenv("SG_IN_DOCKER", raising=False)
    assert "29092" in cfg.kafka_bootstrap
    # ...and inside the compose network, the internal one.
    monkeypatch.setenv("SG_IN_DOCKER", "1")
    assert cfg.kafka_bootstrap.startswith("kafka:")


def test_bucket_names_resolve(cfg: Config) -> None:
    # Only two buckets: the Parquet master store is a mounted volume, not S3.
    assert cfg.bucket("tariff") and cfg.bucket("reports")


# --- validation: each rule rejects the mistake it was written for ----------
def test_current_config_is_valid(cfg: Config) -> None:
    validate(cfg)  # must not raise


def test_missing_section_is_rejected(cfg: Config) -> None:
    with pytest.raises(ValueError, match="missing the 'billing' section"):
        validate(_mutated(cfg, lambda raw: raw.pop("billing")))


def test_empty_zone_list_is_rejected(cfg: Config) -> None:
    with pytest.raises(ValueError, match="at least one meter"):
        validate(_mutated(cfg, lambda raw: raw["grid"].__setitem__("zones", [])))


def test_tier_weight_without_a_rate_is_rejected(cfg: Config) -> None:
    with pytest.raises(ValueError, match="no rate for"):
        validate(_mutated(
            cfg, lambda raw: raw["billing"]["tier_weights"].__setitem__("domestic-99", 0.1)
        ))


def test_watermark_shorter_than_reading_gap_is_rejected(cfg: Config) -> None:
    # One reading covers 9.6 simulated minutes; a 5-minute watermark would
    # classify perfectly ordinary events as late and drop them.
    with pytest.raises(ValueError, match="shorter than"):
        validate(_mutated(
            cfg, lambda raw: raw["speed_layer"].__setitem__("watermark_minutes", 5)
        ))


# --- .env handling ---------------------------------------------------------
def test_dotenv_loads_and_real_env_wins(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        '# comment\n\nSG_TEST_NEW=from_file\nSG_TEST_EXISTING="ignored"\n', encoding="utf-8"
    )
    monkeypatch.delenv("SG_TEST_NEW", raising=False)
    monkeypatch.setenv("SG_TEST_EXISTING", "from_environment")

    load_dotenv(env_file)

    import os

    assert os.environ["SG_TEST_NEW"] == "from_file"
    assert os.environ["SG_TEST_EXISTING"] == "from_environment"


def test_missing_env_file_is_not_an_error(tmp_path) -> None:
    load_dotenv(tmp_path / "nope.env")  # must not raise

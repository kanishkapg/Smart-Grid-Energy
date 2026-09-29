"""Tests for the raw master store's declared layout.

Deliberately not Spark tests. pyspark lives only inside the Spark container, and
requiring a JVM to run `pytest` would make the suite useless as a quick check.
What is tested here is the contract the layout depends on: config.yaml, the
verification script and the Spark job must all agree, and the Phase 2 checkpoint
is what exercises the Spark code itself end to end.
"""

from __future__ import annotations

import pytest

from common.config import load_config


@pytest.fixture(scope="module")
def cfg():
    return load_config()


def test_partition_order_puts_sim_day_first(cfg) -> None:
    # Order is not cosmetic: the batch billing job filters by sim_day, so it must
    # be the outermost directory level for Spark to prune whole days rather than
    # opening every zone's files.
    assert list(cfg.storage["raw_partition_cols"]) == ["sim_day", "grid_zone"]


def test_raw_prefix_is_set(cfg) -> None:
    assert cfg.storage["raw_prefix"], "the master store needs a prefix of its own"


def test_master_store_is_a_filesystem_path_not_object_storage(cfg, monkeypatch) -> None:
    # Spark could not commit a write to SeaweedFS by any of three routes
    # (docs/02-tech-stack.md), so the master store is a mounted volume. If
    # someone points it back at s3a:// the raw sink will fail at the first batch.
    monkeypatch.delenv("SG_IN_DOCKER", raising=False)
    assert not cfg.raw_store_path.startswith(("s3a://", "s3://"))
    assert cfg.raw_store_path.endswith(cfg.storage["raw_prefix"])


def test_container_and_host_see_the_same_store(cfg, monkeypatch) -> None:
    # The Spark container writes to /data/raw; the host reads ./data/raw. Both
    # must end in the same prefix or the verification script checks a store
    # nothing is writing to.
    monkeypatch.delenv("SG_IN_DOCKER", raising=False)
    host_path = cfg.raw_store_path
    monkeypatch.setenv("SG_IN_DOCKER", "1")
    container_path = cfg.raw_store_path
    assert host_path != container_path
    assert container_path.startswith("/")
    suffix = f"{cfg.storage['raw_prefix']}"
    assert host_path.endswith(suffix) and container_path.endswith(suffix)


def test_checkpoints_are_not_on_object_storage(cfg) -> None:
    # Structured Streaming checkpoints need atomic rename. S3 only emulates it by
    # copying, which is both slow and corruptible on an interrupted write -- and
    # SeaweedFS rejects the commit-log writes outright. Data goes to S3; offsets
    # stay on a real filesystem.
    checkpoint_root = cfg.raw["spark"]["checkpoint_root"]
    assert not checkpoint_root.startswith(("s3a://", "s3://")), (
        f"checkpoint_root is {checkpoint_root!r}; it must be a real filesystem path"
    )
    assert checkpoint_root.startswith("/")


def test_trigger_is_slow_enough_to_avoid_a_tiny_file_storm(cfg) -> None:
    # One Parquet file is written per partition per micro-batch. With 4 zones a
    # 15s trigger yields roughly 16 files a minute; a 1s trigger would yield 240
    # and the store would drown in files that cost more to open than to read.
    trigger = int(cfg.raw["spark"]["raw_sink_trigger_seconds"])
    assert trigger >= 5, "a sub-5s trigger creates far too many small files"


def test_every_sim_day_should_hold_the_documented_reading_count(cfg) -> None:
    # A full simulated day of one meter is 150 readings, so a zone partition
    # should hold meters * 150 rows. This is the arithmetic the Phase 2
    # checkpoint's per-partition table is read against.
    for zone in cfg.zones:
        expected = int(zone["meters"]) * cfg.readings_per_meter_per_sim_day
        assert expected > 0
        if zone["name"] == "Colombo-North":
            assert expected == 1800

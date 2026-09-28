#!/usr/bin/env python3
"""Phase 0 checkpoint: prove the infrastructure skeleton is actually alive.

Standard library only, so it runs before any project dependency is installed.
Exits 0 only when every check passes, so it also works as a gate.

    python scripts/stack_status.py

Checks, in the order the Phase 0 checkpoint asks for:
  1. containers are running / the one-shot init jobs exited cleanly
  2. the Kafka topic exists
  3. the Postgres serving tables exist
  4. the S3 buckets exist
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPECTED_TABLES = ["alerts", "daily_bill", "pipeline_runs", "zone_metrics"]
EXPECTED_BUCKETS = ["raw", "reports", "tariff"]
INIT_SERVICES = ("kafka-init", "storage-init")


def compose(*args: str) -> tuple[int, str]:
    """Run a docker compose command, returning (exit code, stdout+stderr)."""
    proc = subprocess.run(
        ["docker", "compose", *args],
        cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=180,
    )
    return proc.returncode, (proc.stdout + proc.stderr).strip()


def ps_rows() -> list[dict]:
    """Parse `docker compose ps`, which emits either a JSON array or JSON lines
    depending on the Compose version."""
    code, out = compose("ps", "--all", "--format", "json")
    if code != 0 or not out:
        return []
    if out.startswith("["):
        return json.loads(out)
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def wait_for_init(timeout: int = 150) -> None:
    """Block while the one-shot provisioning jobs are still running.

    SeaweedFS brings up master, volume, filer and only then the S3 endpoint, a
    sequence that takes ~10s, so storage-init sits in its retry loop for a while
    after `docker compose up -d` has already returned. Checking before it
    finishes reports failures that are really just a cold start.
    """
    deadline = time.time() + timeout
    announced = False
    while time.time() < deadline:
        busy = [
            r.get("Service") for r in ps_rows()
            if r.get("Service") in INIT_SERVICES and r.get("State") == "running"
        ]
        if not busy:
            return
        if not announced:
            print(f"  waiting for provisioning jobs: {', '.join(busy)} ...")
            announced = True
        time.sleep(3)


def check_containers() -> tuple[bool, list[str]]:
    rows = ps_rows()
    if not rows:
        return False, ["- no containers -- run: docker compose up -d "
                       "(or Docker Desktop is not running)"]

    ok, lines = True, []
    for row in sorted(rows, key=lambda r: r.get("Service", "")):
        service, state = row.get("Service", "?"), row.get("State", "?")
        health, exit_code = row.get("Health") or "", row.get("ExitCode", 0)

        if service in INIT_SERVICES:
            # One-shot provisioning jobs: exiting 0 is success, not a fault.
            good = state == "exited" and exit_code == 0
            shown = f"{state}({exit_code})" if state == "exited" else state
        else:
            good = state == "running" and health in ("healthy", "")
            shown = state + (f"/{health}" if health else "")

        ok &= good
        lines.append(f"{'+' if good else '-'} {service:<13} {shown}")
    return ok, lines


def check_kafka_topic() -> tuple[bool, list[str]]:
    code, out = compose(
        "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "kafka:9092", "--describe",
    )
    if code != 0:
        return False, [f"- could not reach the broker: {out}"]
    if "meter.readings" not in out:
        return False, ["- topic 'meter.readings' is missing"]
    return True, [f"+ {line.strip()}" for line in out.splitlines() if line.strip()]


def check_postgres_tables() -> tuple[bool, list[str]]:
    # Credentials come from the container's own environment, so this keeps
    # working if you change them in .env.
    code, out = compose(
        "exec", "-T", "postgres", "sh", "-c",
        'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc '
        '"SELECT tablename FROM pg_tables WHERE schemaname = \'public\' ORDER BY 1"',
    )
    if code != 0:
        return False, [f"- psql failed: {out}"]

    found = out.split()
    lines = [f"{'+' if t in found else '-'} table {t}" for t in EXPECTED_TABLES]
    return all(t in found for t in EXPECTED_TABLES), lines


def check_s3_buckets() -> tuple[bool, list[str]]:
    # storage-init has already exited, so we cannot exec into it. `compose run`
    # on the same service reuses its network and credentials; --no-deps stops
    # it restarting the storage service as a side effect.
    code, out = compose(
        "run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "storage-init", "-c",
        'aws s3 ls --endpoint-url "$S3_ENDPOINT"',
    )
    if code != 0:
        return False, [f"- could not list buckets: {out}"]

    # `aws s3 ls` prints one "2026-09-28 15:29:50 raw" line per bucket.
    found = out.split()
    lines = [f"{'+' if b in found else '-'} bucket {b}" for b in EXPECTED_BUCKETS]
    return all(b in found for b in EXPECTED_BUCKETS), lines


CHECKS = [
    ("Containers", check_containers),
    ("Kafka topic", check_kafka_topic),
    ("Postgres tables", check_postgres_tables),
    ("S3 buckets", check_s3_buckets),
]


def main() -> int:
    print("\n  Smart Grid Lambda Platform -- Phase 0 infrastructure check")
    print("  " + "-" * 58)
    wait_for_init()

    failures = []
    for label, check in CHECKS:
        ok, lines = check()
        if not ok:
            failures.append(label)
        print(f"\n  {label:<20} [{'PASS' if ok else 'FAIL'}]")
        for line in lines:
            print(f"      {line}")

    print("\n  " + "-" * 58)
    if failures:
        print(f"  PHASE 0 CHECKPOINT: FAILED  ({', '.join(failures)})")
        print("  Try: docker compose up -d   then re-run this script.\n")
        return 1

    print("  PHASE 0 CHECKPOINT: PASSED")
    print("  Object storage UI: http://localhost:8888  (browse into buckets/)\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

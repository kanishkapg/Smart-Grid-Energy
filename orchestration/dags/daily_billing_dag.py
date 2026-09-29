"""Phase 4: the daily billing DAG -- the batch layer's scheduler.

One run bills one simulated day:

    resolve_sim_day  ->  wait_for_tariff  ->  bill_day  ->  verify_bills

Airflow is here for the four things a `while True:` loop does not give you: a
schedule, retries with a record of them, a dependency between "the input arrived"
and "compute the money", and a visible history of which days have been billed.

**Schedule.** Every five real minutes, because one simulated day *is* five real
minutes at this project's 288x compression. So this is the compressed equivalent
of a nightly billing run, and the DAG's run history reads as one row per
simulated day.

**Which day.** Not Airflow's logical date -- that is real time, and the
simulation is somewhere in January 2026 regardless of today's date. The day to
bill comes from the shared simulated clock (`data/.sim_anchor`, mounted into this
container), and it is always a day that has *closed*. Trigger with
`--conf '{"sim_day": "20260127"}'` to bill a specific past day instead, which is
what a real backfill would do and what the Phase 4 checkpoint uses.

**Retries are safe.** `bill_day` recomputes from immutable Parquet and upserts on
(sim_day, household_id), so a retry -- or a second run of the same day -- rewrites
that day's rows rather than billing anyone twice.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
import psycopg
from psycopg.rows import dict_row

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from common.config import load_config
from common.sim_clock import SimClock
from common.storage import s3_client
from processing.shared.billing import (
    BillingTerms,
    bill_household,
    parse_tariff_csv,
    summarise_bills,
    tariff_object_key,
)

log = logging.getLogger(__name__)

# Money is small enough to compare exactly at 2 decimals, but the comparison is
# between two float computations, so it gets a cent of tolerance rather than ==.
MONEY_TOLERANCE = 0.01


def _sim_day_label(value: str) -> str:
    """YYYYMMDD -> YYYY-MM-DD, the form Postgres and the partitions use."""
    return "{}-{}-{}".format(value[:4], value[4:6], value[6:8])


def resolve_sim_day(**context) -> str:
    """Decide which simulated day this run bills, as YYYYMMDD.

    Pushed to XCom so that every downstream task -- the sensor, the Spark job and
    the verification -- works on the same day. Recomputing the clock in each task
    would let a run that straddles a boundary bill one day and verify another.
    """
    requested = (context["params"] or {}).get("sim_day")
    if requested:
        log.info("billing the requested simulated day %s", requested)
        return str(requested)

    clock = SimClock.from_config(load_config())
    if clock.sim_day_index() < 1:
        raise AirflowFailException(
            "no simulated day has closed since the clock anchor, so there is "
            "nothing to bill yet. Wait one simulated day (5 real minutes) or "
            "trigger this DAG with --conf '{\"sim_day\": \"20260127\"}'."
        )
    closed = clock.sim_day() - timedelta(days=1)
    log.info("simulated clock is at %s; billing the day that closed: %s",
             clock.now().isoformat(), closed)
    return closed.strftime("%Y%m%d")


def tariff_available(sim_day: str, **_) -> bool:
    """Has that day's tariff file landed in object storage?

    A sensor rather than a check inside the billing job: "the daily input has
    arrived" is a scheduling fact, and separating it means a missing tariff shows
    up in the DAG as a task still waiting, not as a Spark job that crashed.
    """
    cfg = load_config()
    bucket = cfg.bucket("tariff")
    key = tariff_object_key(cfg.storage["tariff_filename_pattern"],
                            pendulum.parse(_sim_day_label(sim_day)).date())
    try:
        s3_client(cfg).head_object(Bucket=bucket, Key=key)
    except Exception as exc:  # noqa: BLE001 - a sensor waits rather than failing
        log.info("waiting for s3://%s/%s (%s)", bucket, key, type(exc).__name__)
        return False
    log.info("found s3://%s/%s", bucket, key)
    return True


def verify_bills(sim_day: str, **_) -> dict:
    """The data-quality gate: read back what the job wrote and re-derive it.

    Three questions, all of which a green Spark task can still get wrong:
    did every household get a bill, do the stored amounts follow from the stored
    usage and rate, and does one run own the whole day? The last one catches a
    day half-written by one run and half by another.
    """
    cfg = load_config()
    terms = BillingTerms.from_config(cfg.billing)
    day = _sim_day_label(sim_day)

    with psycopg.connect(cfg.postgres_dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM daily_bill WHERE sim_day = %s::date", (day,))
            rows = cur.fetchall()

    if not rows:
        raise AirflowFailException("daily_bill holds no rows for {}".format(day))
    if len(rows) != cfg.total_meters:
        raise AirflowFailException(
            "billed {} household(s) for {}, but the grid has {}"
            .format(len(rows), day, cfg.total_meters))

    for row in rows:
        expected = bill_household(row, row, terms)
        for field in ("net_kwh", "gross_amount", "subsidy_amount", "net_amount"):
            if abs(float(row[field]) - float(expected[field])) > MONEY_TOLERANCE:
                raise AirflowFailException(
                    "{} on {}: stored {} is {}, recomputed {}"
                    .format(row["household_id"], day, field,
                            row[field], expected[field]))

    run_ids = {row["run_id"] for row in rows}
    if len(run_ids) != 1:
        raise AirflowFailException(
            "{} was written by {} different runs {}; one day's bills must come "
            "from one run".format(day, len(run_ids), sorted(run_ids)))

    summary = summarise_bills(rows)
    log.info("verified %s: %s households, %s %.2f net",
             day, summary["households"], terms.currency, summary["net_amount"])
    return summary


with DAG(
    dag_id="daily_billing",
    description="Recompute one simulated day's household bills from the Parquet "
                "master store (Lambda batch layer)",
    doc_md=__doc__,
    # One simulated day = five real minutes, so this is "nightly".
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # The day to bill comes from the simulated clock, not from the interval being
    # scheduled, so replaying missed intervals would bill the same day repeatedly.
    catchup=False,
    # The job upserts, so two concurrent runs could not corrupt a day -- but they
    # would race on the same rows and make the logs unreadable.
    max_active_runs=1,
    params={"sim_day": None},          # --conf '{"sim_day": "20260127"}'
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["phase4", "batch-layer", "billing"],
) as dag:

    resolve = PythonOperator(
        task_id="resolve_sim_day",
        python_callable=resolve_sim_day,
    )

    wait = PythonSensor(
        task_id="wait_for_tariff",
        python_callable=tariff_available,
        op_kwargs={"sim_day": "{{ ti.xcom_pull(task_ids='resolve_sim_day') }}"},
        poke_interval=15,
        # Deliberately shorter than a simulated day (5 real minutes): a run whose
        # input never arrives has to fail before the next run is scheduled, or
        # `max_active_runs=1` leaves later runs -- including a manual one for a
        # specific day -- queued behind a sensor that is going to fail anyway.
        timeout=180,
        # Frees the worker slot between pokes instead of holding it for ten minutes.
        mode="reschedule",
        retries=0,
    )

    bill = BashOperator(
        task_id="bill_day",
        # spark-submit rather than importing the job: the batch layer runs as its
        # own process with its own driver memory, and it is the same command a
        # human runs by hand (see the Phase 4 section of the README).
        # 512m and no Spark UI: this JVM starts inside a container that is
        # already holding a scheduler, a webserver and a triggerer. With 1g the
        # Docker VM (3.7 GB here) ran out at exactly the wrong moment -- the next
        # task could not fork and failed with "Cannot allocate memory" before it
        # had a log file. One simulated day is 6,000 rows; 512m is ample.
        bash_command=(
            "cd /app && spark-submit --master local[2] --driver-memory 512m "
            "--conf spark.ui.enabled=false "
            "/app/processing/billing_batch.py "
            "--sim-day {{ ti.xcom_pull(task_ids='resolve_sim_day') }}"
        ),
        # The job adopts this instead of minting its own id, so the rows in
        # daily_bill, the pipeline_runs entry and the JSON log lines of this task
        # all carry one correlation id that names the Airflow run that produced them.
        env={"SG_RUN_ID": "sg-billing-{{ ts_nodash }}-{{ ti.try_number }}"},
        append_env=True,
    )

    verify = PythonOperator(
        task_id="verify_bills",
        python_callable=verify_bills,
        op_kwargs={"sim_day": "{{ ti.xcom_pull(task_ids='resolve_sim_day') }}"},
        retries=0,          # a failed check is a real finding, not a flake
    )

    resolve >> wait >> bill >> verify

# Smart Grid Energy Monitoring & Billing

A Lambda-architecture big-data platform for **UC3 — Smart Grid Energy Monitoring & Billing**.

Simulated smart meters stream readings through Kafka. A **speed layer** turns that
stream into live per-zone grid load and renewable-contribution metrics plus
threshold alerts. A **batch layer**, orchestrated by Airflow, recomputes each
simulated day's household billing from an immutable Parquet master store joined
to that day's published tariff file. Both layers land in PostgreSQL, which is
served by a FastAPI real-time API and Grafana dashboards.

> **Architecture decision (Lambda vs Kappa):** [docs/00-architecture-decision.md](docs/00-architecture-decision.md)
> · **Diagrams:** [docs/01-architecture-diagram.md](docs/01-architecture-diagram.md)
> · **Tech stack justification:** [docs/02-tech-stack.md](docs/02-tech-stack.md)

---

## The two clocks

**Real time** is your wall clock. **Simulated time** is the clock inside the data.
One simulated day is compressed into 5 real minutes, so 86,400 simulated seconds
elapse per 300 real seconds — a factor of **288×**.

| Simulated duration | Real duration |
|---|---|
| 1 simulated minute | 0.21 real seconds |
| 1 simulated hour | 12.5 real seconds |
| 1 simulated day | 5 real minutes |

Each meter emits every **2 real seconds**, which is 2 × 288 = 576 simulated
seconds = **9.6 simulated minutes**. So one meter's readings carry timestamps
`00:00:00 → 00:09:36 → 00:19:12 → …`, and after 150 readings the simulated day
is over.

### Why the speed-layer window is 60 simulated minutes

A **window** is the time bucket a streaming aggregation groups readings into,
measured on the reading's own timestamp (event time). Window size = 60 simulated
minutes means: take every reading timestamped inside the same simulated hour,
group by zone, emit one row.

```
Window [14:00:00 → 15:00:00), zone Colombo-North:
  sum(consumption) = 1200 kWh    renewable_pct = 25 %
  sum(solar)       =  300 kWh    active_meters = 12
        →  one row in zone_metrics
```

The project plan suggests a 1-minute window, which breaks under this clock:
readings are 9.6 simulated minutes apart, so a 1-simulated-minute window would
be narrower than the gap between two readings — most windows empty, the rest
holding a single sample. At 60 simulated minutes each window holds
60 / 9.6 ≈ **6 readings per meter**, a genuine aggregate, and since a simulated
hour is 12.5 real seconds a new row still appears roughly every 12 seconds.

The related setting is the **watermark** (30 simulated minutes): how long Spark
holds a window open for late or out-of-order readings before finalising it —
about 3 readings' worth of slack. `common/config.py` refuses to start if the
watermark is shorter than the reading gap, because that silently drops valid
events.

---

## Build status by phase

| Phase | Delivers | Visible outcome | State |
|---|---|---|---|
| **0** | Infra: Kafka (KRaft), MinIO, Postgres + serving schema | `scripts/stack_status.py` prints a green checkpoint | ✅ |
| **0+** | Shared foundation: config, JSON logging, simulated clock | `python -m pytest` → 65 passing | ✅ |
| **1** | Simulated sources: meter stream → Kafka, tariff CSV → object storage | `scripts/check_phase1.py` prints a green checkpoint | ✅ |
| 2 | Raw master store: streaming sink → partitioned Parquet | Parquet row count ≈ events produced | ⏳ |
| 3 | Speed layer + FastAPI real-time API | `/zones` returns live figures; forced low solar fires an alert | ⏳ |
| 4 | Batch billing + Airflow DAG | green DAG run; hand-calculated bill matches `daily_bill` | ⏳ |
| 5 | Grafana dashboards | two rendering dashboards | ⏳ |
| 6 | Observability hardening | kill the producer → "no data" alert fires | ⏳ |
| 7 | Report, README, demo | clean-clone reproduce | ⏳ |

---

## Verifying Phase 0, step by step

**Prerequisites:** Docker Desktop **running**, ~4 GB free RAM, Python 3.10+.

### Step 1 — dependencies (no Docker needed)

```powershell
Copy-Item .env.example .env
python -m pip install -r requirements.txt
```

### Step 2 — run the foundation tests (no Docker needed)

```powershell
python -m pytest
```

**Expect:** `33 passed`. These pin the numbers this README quotes (288×
compression, 150 readings per meter per sim-day, 9.6 simulated minutes per
reading) and assert that the config validator rejects the two silent mistakes:
a too-short watermark, and a tariff tier with no rate.

### Step 3 — start the infrastructure

```powershell
docker compose up -d
```

**Expect:** five containers created. `kafka`, `seaweedfs` and `postgres` stay
running; `kafka-init` and `storage-init` are one-shot provisioning jobs that
exit 0. The first run pulls ~1 GB of images, so allow several minutes.

### Step 4 — run the checkpoint script

```powershell
python scripts/stack_status.py
```

**Expect** four `[PASS]` blocks and:

```
  PHASE 0 CHECKPOINT: PASSED
  Object storage UI: http://localhost:8888
```

It exits non-zero on failure, so it also works as a gate. If something fails,
the block that failed names it.

### Step 5 — see it yourself, without the script

Each of these is one of the plan's Phase 0 checkpoint items:

```powershell
# containers and their health
docker compose ps

# the Kafka topic exists with 3 partitions
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe

# the serving tables exist (expect: alerts, daily_bill, pipeline_runs, zone_metrics)
docker compose exec postgres psql -U grid -d smartgrid -c "\dt"

# the buckets exist (expect: raw, reports, tariff)
docker compose run --rm --no-deps --entrypoint /bin/sh storage-init -c "aws s3 ls --endpoint-url $S3_ENDPOINT"
```

Then open the object-storage UI at **http://localhost:8888** and browse into
`buckets/` — you should see the three empty buckets. That's the most
demo-friendly proof that Phase 0 works.

### Tear down

```powershell
docker compose down          # stop containers, keep data
docker compose down -v       # also wipe Postgres + MinIO data (full reset)
```

> Editing `infra/postgres/init/01_schema.sql` only takes effect on a **fresh**
> Postgres volume — `docker-entrypoint-initdb.d` runs once. Re-run with
> `docker compose down -v` after changing the schema.

---

## Verifying Phase 1, step by step

Phase 1 adds the two simulated sources. Phase 0's stack must be up first.

You need **three terminals**, each with the environment activated
(`conda activate bigdata`) and sitting in the project root.

### Terminal 1 — the meter stream

```powershell
python -m sources.stream_producer --log-format console
```

**Expect** a `starting` line stating the simulated clock, then `producer_ready`,
then a `progress` line every ~20 seconds:

```
[info] producer_ready   bootstrap=localhost:29092 meters=40 solar_meters=22 topic=meter.readings
[info] progress         events_sent=400 late_events=17 events_per_sec=22.2
                        sim_time=2026-01-01T18:58:23+05:30 sim_day_progress_pct=79.1
```

40 meters × one reading every 2 s ≈ **20 events/second**. `late_events` should be
roughly 3% of the total — those are deliberately backdated (see below). Leave it
running; Ctrl+C stops it cleanly.

Useful flags: `--sim-days 2` to stop after two simulated days, `--cloud-cover 0.9`
to suppress solar and force the low-renewable alert (used in Phase 3),
`--log-format json` for the machine-readable form.

### Terminal 2 — the daily tariff file

```powershell
python -m sources.tariff_generator --log-format console
```

**Expect** it to write the current simulated day immediately, then wait and write
one file per sim-day boundary — a new file every 5 real minutes:

```
[info] tariff_file_written              key=tariff_20260101.csv households=40 subsidised=12
[info] watching_for_sim_day_boundaries  next_boundary_in_real_seconds=5.0
[info] tariff_file_written              key=tariff_20260102.csv households=40 subsidised=12
```

`--once` writes today's file and exits; `--sim-day 20260103` regenerates a
specific day, which reproduces that day's file byte for byte.

### Terminal 3 — the checkpoint

```powershell
python scripts\check_phase1.py
```

**Expect** `PHASE 1 CHECKPOINT: PASSED`, with a per-zone table:

```
  sim day 2026-01-02 (35% elapsed), sim time 2026-01-02T08:17:13+05:30

  Meter stream         (sampling up to 120 events)
      + 120 events validated against the event contract

      zone             events  meters  kWh used  kWh solar  renew %
      Colombo-North        36      12     6.189      1.702    27.5%
      Colombo-South        30      10     5.586      0.943    16.9%
      Gampaha              27       9     3.874      0.641    16.5%
      Kandy                27       9     3.396      0.472    13.9%

      + daylight in simulated time, so solar generation is expected
      + 5 of 120 events arrived out of event-time order -- this is what the
        speed layer's 30-simulated-minute watermark absorbs

  Daily tariff file
      + 2 tariff file(s) in 'tariff': tariff_20260101.csv, tariff_20260102.csv
      + tariff_20260102.csv: header correct, 40 household rows
        H-101,30.73,domestic-1,false
```

**Reading the output.** `renew %` swings with simulated time and that is the
point: 0% overnight, climbing through the morning, peaking near midday, back to
0% after sunset. **`0.000 kWh solar` at night is correct, not a fault** — the
script says which it is, so you can tell a quiet night from a broken producer.
Over a whole simulated day each zone lands at 14–24% renewable.

### Seeing it yourself, without the script

```powershell
# raw events straight off the topic
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 --topic meter.readings --max-messages 5

# tariff files in object storage
docker compose run --rm --no-deps --entrypoint /bin/sh storage-init -c "aws s3 ls s3://tariff --endpoint-url $S3_ENDPOINT"

# read one tariff file
docker compose run --rm --no-deps --entrypoint /bin/sh storage-init -c "aws s3 cp s3://tariff/tariff_20260101.csv - --endpoint-url $S3_ENDPOINT"
```

A raw event looks exactly like the documented contract:

```json
{"meter_id": "M-104", "household_id": "H-104", "grid_zone": "Colombo-North",
 "power_consumption_kwh": 0.0932, "solar_generation_kwh": 0.0,
 "timestamp": "2026-01-03T04:50:12.435173+05:30"}
```

You can also browse **http://localhost:8888/buckets/tariff/** to see the CSVs
accumulating, one per simulated day.

### What Phase 1 models, and why

| Decision | Reason |
|---|---|
| Messages keyed by `household_id` | Even spread over 3 partitions. Keying by `grid_zone` would put 12 of 40 meters on one partition. |
| `run_id` in a Kafka **header**, not the payload | It is metadata about the pipeline run, not part of the meter-reading contract. |
| ~3% of events deliberately backdated | Gives the speed layer's watermark a real job instead of a decorative one. Capped below the watermark, and `common/config.py` refuses to start if that relationship is violated. |
| Solar ownership by **quota**, not a coin flip per household | With 9–12 meters per zone, independent draws vary wildly — Kandy came out with 0 of 9 panels against a 0.30 target, which would have pinned its renewable share at 0% and made the alert fire forever. |
| Tier and subsidy hashed from `household_id` | Stable across days and identical in every process, with no coordination. H-101 is on the same tier in every file. |
| Tariff **rate** varies daily, seeded by (household, day) | A published daily tariff should move — but regenerating a past day must reproduce it exactly, so the batch layer can recompute. |
| Rooftop PV sized at 0.9 kW | A realistic 3 kW array exported so heavily at midday that zone `renewable_pct` hit 131% — valid physics, but it makes the metric unreadable and puts the 15% alert out of reach. At 0.9 kW a solar household's day lands near the plan's H-101 example (21.6 kWh used, 6.9 kWh solar). |

> **For the report:** quote the figures the system actually produces, not the
> plan's illustrative ones. H-101 really is a solar household, but it lands on
> `domestic-1` at ~30 LKR/kWh with no subsidy, where the plan's example assumed
> `domestic-2` at 45 with a subsidy.

---

## What Phase 0 stood up

| Service | Host endpoint | Purpose |
|---|---|---|
| Kafka (KRaft, single node) | `localhost:29092` | ingestion bus; topic `meter.readings`, 3 partitions |
| SeaweedFS S3 API | `localhost:8333` | object store (`smartgrid` / `smartgridsecret`) |
| SeaweedFS filer UI | `localhost:8888` | browse buckets and objects in a browser |
| PostgreSQL | `localhost:5432` | serving layer (`smartgrid` / `grid` / `gridpass`) |

Buckets: `raw` (immutable Parquet master dataset), `tariff` (daily CSV drop),
`reports` (generated billing reports).
Tables: `zone_metrics`, `daily_bill`, `alerts`, `pipeline_runs`.

Three deliberate choices worth knowing:

- **Object storage is SeaweedFS, not MinIO.** MinIO has withdrawn its public
  container images, so `minio/minio` and `quay.io/minio/minio` can no longer be
  pulled. SeaweedFS serves the same S3 API. Because the code speaks S3 rather
  than a vendor SDK, the swap was a compose-file change and nothing else —
  which is why the environment variables are named `S3_*`.
  Full reasoning in [docs/02-tech-stack.md](docs/02-tech-stack.md).
- **Kafka is not persisted to a volume.** In Lambda, Kafka is a transport buffer
  and Parquet is the system of record, so retention is 24 h and a broker restart
  losing buffered events is acceptable. This also avoids the volume-ownership
  problems the Kafka image has on Windows hosts.
- **The storage service has no healthcheck.** The SeaweedFS image ships little
  beyond the `weed` binary, so any in-image HTTP probe would be guesswork.
  Readiness is gated by `storage-init`'s own retry loop instead.

---

## Repository layout

```
.
├─ docker-compose.yml          # the whole stack, one command
├─ .env.example                # image tags, credentials, sim-clock knobs
├─ config/config.yaml          # single source of truth for every tunable
├─ infra/postgres/init/        # serving schema, applied on first boot
├─ infra/seaweedfs/s3.json     # S3 identity, so credentials are enforced
├─ common/                     # config, JSON logging, simulated clock, S3 client
├─ docs/                       # ADR, diagrams, tech-stack justification
├─ scripts/
│  ├─ stack_status.py          # Phase 0 checkpoint verifier
│  └─ check_phase1.py          # Phase 1 checkpoint verifier
├─ tests/                      # 65 tests, no Docker required
├─ sources/
│  ├─ meter_model.py           # meter population + reading physics (pure, tested)
│  ├─ stream_producer.py       # meter events -> Kafka
│  └─ tariff_generator.py      # daily tariff CSV -> object storage
├─ processing/                 # Phase 2-4: common/ speed/ batch/
├─ orchestration/dags/         # Phase 4: Airflow billing DAG
├─ serving/api/                # Phase 3: FastAPI app
├─ observability/              # Phase 6: prometheus + grafana config
└─ reports/                    # generated billing report samples
```

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `failed to connect to the docker API` | Docker Desktop isn't running. Start it and wait for the whale icon to settle. |
| `kafka` stuck `starting` | First boot formats the KRaft log; give it ~30 s and re-check. |
| `sg-kafka exited (1)` on startup | Usually an invalid `KAFKA_CLUSTER_ID`. KRaft needs a base64-encoded 16-byte UUID (22 chars), not a descriptive name. Check `docker logs sg-kafka`. |
| Port already allocated | Something else holds 8333/8888/5432/29092. Change the left-hand side of the `ports:` mapping. |
| Postgres tables missing | The volume predates the schema file. `docker compose down -v; docker compose up -d`. |
| `storage-init` exited non-zero | The S3 gateway wasn't ready yet. `docker compose up -d storage-init` retries. |
| `pull access denied for minio/minio` | An old `.env` still pins a MinIO image. Re-copy `.env.example`. |
| `ModuleNotFoundError: kafka.vendor.six.moves` | `kafka-python` 2.0.2 is broken on Python 3.12. `requirements.txt` pins 3.0.11. |
| Components disagree about the sim-day | They share an anchor in `data/.sim_anchor`. Delete it to restart at sim-day 0, or set `SG_REAL_ANCHOR` to pin it. |
| `check_phase1.py` says "no events arrived" | The producer isn't running. Start `python -m sources.stream_producer` in another terminal first. |
| `ModuleNotFoundError: No module named 'common'` | Run from the project root with the environment activated. Scripts in `scripts/` add the root to `sys.path` themselves; `python -m sources.x` needs the root as your working directory. |
| All zones show 0% renewable | Check the simulated hour in the header line. Overnight that is correct — `check_phase1.py` states whether it is day or night. |

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
| **0+** | Shared foundation: config, JSON logging, simulated clock | `python -m pytest` → 106 passing | ✅ |
| **1** | Simulated sources: meter stream → Kafka, tariff CSV → object storage | `scripts/check_phase1.py` prints a green checkpoint | ✅ |
| **2** | Raw master store: Spark Structured Streaming → partitioned Parquet | `scripts/check_phase2.py` reconciles Parquet against Kafka | ✅ |
| **3** | Speed layer (windowed zone metrics + alerts) + FastAPI real-time API | `scripts/check_phase3.py` prints a green checkpoint; `--cloud-cover` fires an alert | ✅ |
| **4** | Batch billing (Spark) + Airflow DAG | `scripts/check_phase4.py` re-derives a bill from Parquet and matches `daily_bill` | ✅ |
| **5** | Grafana dashboards: live zone monitoring + daily billing | `scripts/check_phase5.py` runs every panel's query through Grafana | ✅ |
| **6** | Observability: Prometheus metrics + alert rules, Pipeline Health dashboard | stop the producer → `check_phase6.py --expect-firing NoMeterData` passes | ✅ |
| 7 | Report, README, demo | clean-clone reproduce | ⏳ |

---

## Verifying everything, phase 0 → 4

Each phase has its **own** checkpoint script, and running a later one does not
verify the earlier phases — the speed layer reads Kafka directly, so it can be
perfectly healthy while the Parquet master store is empty or stalled, and the
batch layer reads that same store, so *it* can be broken while the speed layer
looks fine. To check the platform end to end, run all five in order.

### Terminals

Only the long-running processes need a terminal of their own. The checkpoint
scripts are one-shot and can share one.

| Terminal | Command | Lifetime |
|---|---|---|
| 1 | `python -m sources.stream_producer --log-format console` | leave running throughout |
| 2 | `python -m sources.tariff_generator --log-format console` | leave running throughout |
| 3 | the checkpoint scripts below, one after another | returns each time |
| 4 *(optional)* | `docker compose logs -f speed-layer` | blocks until Ctrl+C |

The batch layer needs no terminal of its own: Airflow runs it inside the stack,
on a schedule. Its UI is at **http://localhost:8080** (`admin` / `admin`).
The dashboards are at **http://localhost:3000** (`admin` / `admin`), and
Prometheus with its alert rules at **http://localhost:9090/alerts**.

Every terminal needs the environment activated (`conda activate bigdata`) and the
project root as its working directory.

### The sequence

```powershell
# --- once, in any terminal: bring the stack up ---------------------------
docker compose up -d --build        # --build only on the first run or after a Dockerfile change

# --- terminal 1 ----------------------------------------------------------
python -m sources.stream_producer --log-format console

# --- terminal 2 ----------------------------------------------------------
python -m sources.tariff_generator --log-format console

# --- terminal 3, in this order -------------------------------------------
python -m pytest                    # 106 passing       (no Docker needed)
python scripts\stack_status.py      # PHASE 0 CHECKPOINT: PASSED
python scripts\check_phase1.py      # PHASE 1 CHECKPOINT: PASSED
python scripts\check_phase2.py      # PHASE 2 CHECKPOINT: PASSED
python scripts\check_phase3.py      # PHASE 3 CHECKPOINT: PASSED
python scripts\check_phase4.py      # PHASE 4 CHECKPOINT: PASSED
python scripts\check_phase5.py      # PHASE 5 CHECKPOINT: PASSED
python scripts\check_phase6.py      # PHASE 6 CHECKPOINT: PASSED
```

### What each one proves, and how long to wait first

| Step | Proves | Wait before running |
|---|---|---|
| `pytest` | The pure logic — simulated clock, meter physics, tariff generation, billing arithmetic, alert rules, config validation. No services touched. | none |
| `stack_status.py` | Containers up, Kafka topic with 3 partitions, the four serving tables, both buckets. Waits for the `-init` jobs itself. | none |
| `check_phase1.py` | Events on the topic match the event contract; a tariff CSV exists with 40 household rows. | ~20 s of producer, and one tariff file written |
| `check_phase2.py` | Parquet row count reconciles against Kafka's offsets; partition layout is `sim_day/grid_zone`; duplicates are accounted for. | ~30 s (the raw sink commits every 15 s) |
| `check_phase3.py` | Every zone is reporting fresh windows; window arithmetic is right; the API agrees with Postgres. | ~15 s (the first window closes after ~12.5 s) |
| `check_phase4.py` | A simulated day has been billed; one household's bill, recomputed from Parquet on the host, matches `daily_bill` to the cent. | one **closed** simulated day — 5 real minutes of producer, plus a DAG run |

Run them in order: a Phase 2 failure explains a Phase 3 *and* a Phase 4 failure,
but not the other way round, and `check_phase3.py` will happily report a healthy
speed layer while the master dataset the batch layer depends on is broken.

Phase 4 is the one that needs patience rather than seconds: nothing can be billed
until a simulated day has *closed*, which is five real minutes after the producer
starts. Until then `check_phase4.py` reports an empty `daily_bill` and tells you
how to bill a past day instead — see the Phase 4 walkthrough below.

### The alert demo, separately

The Phase 3 checkpoint reports the alert log but does not require a breach,
because in normal operation there is nothing to breach. To demonstrate one:

```powershell
# terminal 1: Ctrl+C, then
python -m sources.stream_producer --cloud-cover 0.95 --log-format console

# terminal 3, after a window inside 09:00-16:00 simulated (<= ~90 real seconds)
python scripts\check_phase3.py --expect-alert low_renewable_contribution
```

### Shutting down

```powershell
docker compose down        # stops everything, keeps the data
docker compose down -v     # also wipes Postgres, the buckets and the checkpoints
```

`./data/raw` is a host directory, so `down -v` does **not** remove the Parquet
master store. Delete it by hand if you want a clean slate — and delete
`data/.sim_anchor` too, which restarts the simulated clock at sim-day 0.

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

## Verifying Phase 2, step by step

Phase 2 adds **Spark**. A Structured Streaming job reads `meter.readings` and
appends Parquet to `./data/raw/meter_readings/`, partitioned by
`sim_day/grid_zone`. This is the immutable master dataset the batch layer
recomputes billing from — without it, the batch layer has no claim to be
authoritative.

> **The master store is a mounted volume, not S3.** The plan specifies S3, and
> that was built first — but Spark cannot commit a write to SeaweedFS. Three
> separate attempts each failed on a different defect, all in the commit path.
> The volume is the fallback the plan explicitly offers, and nothing about the
> architecture changes: still append-only Parquet, still partitioned by
> `sim_day/grid_zone`. Object storage keeps the tariff drop and the reports, and
> Spark still *reads* the tariff over `s3a://`. Full reasoning, including the
> exact errors, is in [docs/02-tech-stack.md](docs/02-tech-stack.md).

It runs as a compose service, so `docker compose up -d` starts it with
everything else. The first run builds the Spark image (~1 GB, several minutes)
because the connector jars are baked in.

### Step 1 — bring the stack up, including the sink

```powershell
docker compose up -d
docker compose ps
```

**Expect** four running services (`kafka`, `postgres`, `seaweedfs`, `raw-sink`)
and the two `-init` jobs `exited (0)`.

### Step 2 — start the producer, so there is something to store

```powershell
python -m sources.stream_producer --log-format console
```

The sink needs a stream. Leave this running in its own terminal.

### Step 3 — watch the sink commit micro-batches

```powershell
docker compose logs -f raw-sink
```

**Expect** a `starting_query` line, then one `batch_written` line per trigger
(every 15 s):

```json
{"component": "raw_sink", "source_topic": "meter.readings",
 "output_path": "/data/raw/meter_readings", "checkpoint_path": "/checkpoints/raw_sink",
 "partition_by": ["sim_day", "grid_zone"], "trigger_seconds": 15, "event": "starting_query"}
{"component": "raw_sink", "batch_id": 0, "event": "batch_written"}
{"component": "raw_sink", "batch_id": 1, "event": "batch_written"}
```

Spark's own `INFO` lines are suppressed to `WARN` so these stay readable.
The Spark UI is at **http://localhost:4040** — the *Structured Streaming* tab
shows input rate, batch duration and offsets per micro-batch.

### Step 4 — reconcile Parquet against Kafka

```powershell
python scripts\check_phase2.py
```

This is the plan's checkpoint — "row count in Parquet ≈ events produced" — made
precise by comparing two independent sources of truth.

```
  Raw master store   data/raw/meter_readings
      + Kafka holds       2,480 events
      + Parquet stores    2,200 rows in 38 file(s)
      + distinct readings 2,200  (meter_id + timestamp)
      + coverage 88.7%; lag 280 events (~14s of production at 20/s)
        budget is 900 events = 3 x the 15s trigger interval
      + all 6 data columns present in the files
      + partition columns encoded in the path: ['sim_day', 'grid_zone']

      sim_day       grid_zone           rows
      2025-12-31    Colombo-North          1
      2026-01-01    Colombo-North        659
      2026-01-01    Colombo-South        549
      ...
      PHASE 2 CHECKPOINT: PASSED
```

**Reading the output.** Coverage under 100% is normal and is *not* judged as a
percentage. The sink commits every 15 s, so it always trails Kafka by about one
interval's worth of events; on a small topic that is a big percentage and on a
large one it is a small one. The check instead compares the **absolute lag**
against a budget derived from config — 40 meters ÷ 2 s = 20 events/s × 15 s
trigger × 3 intervals = 900 events. Exceeding that means the sink has stalled.

The per-partition row counts are the strongest evidence the clock and the
partitioning are both right: a complete simulated day holds
**meters × 150 readings**, so Colombo-North's 12 meters give exactly 1800 rows.
Partial days at either end of a run are expected.

Uses pyarrow reading local files — no JVM, no credentials, no network.

### Step 5 — query the store with Spark

```powershell
docker compose run --rm --no-deps raw-sink `
  /opt/spark/bin/spark-submit /app/processing/inspect_raw.py
```

This is the checkpoint's second half — `spark.read.parquet(...)` and query a
partition — and a rehearsal for Phase 4, which reads the same data the same way.

**Expect** the schema, a dedup count, a partition table, and a per-zone
aggregate for one simulated day:

```
rows as stored:     2,520
rows after dedup:   2,520   (natural key: meter_id + timestamp)
duplicate rows:     0

=== one partition queried: sim_day = 2026-01-01 ===
|grid_zone    |consumption_kwh|solar_kwh|active_meters|renewable_pct|
|Colombo-North|93.392         |16.768   |12           |18.0         |
|Colombo-South|87.532         |11.155   |10           |12.7         |
|Gampaha      |60.446         |7.457    |9            |12.3         |
|Kandy        |53.965         |5.653    |9            |10.5         |
```

Add `--sim-day 2026-01-02` to query a specific day. `renewable_pct` depends on
how much of the simulated day the partition covers — a partial day ending mid
morning is lower than a full day's 14–24%.

### Seeing the files yourself

The store is a plain directory, so just look:

```powershell
Get-ChildItem data\raw\meter_readings -Recurse -Filter *.parquet | Select-Object -First 5 FullName
Get-ChildItem data\raw\meter_readings -Directory
```

Or open `data\raw\meter_readings` in Explorer and click down through
`sim_day=…\grid_zone=…\`. A `_temporary` directory appearing mid-write is
normal; the verification script skips `_`-prefixed paths.

### Restarting cleanly

The checkpoint holds the Kafka offsets already consumed, so a restart resumes
rather than re-reading the topic. To replay from the beginning you must clear
**both** the data and the checkpoint, or you will get a partial reprocess:

```powershell
docker compose rm -sf raw-sink
docker volume rm smartgrid_spark-checkpoints
Remove-Item data\raw\meter_readings -Recurse -Force
docker compose up -d raw-sink
```

### What Phase 2 decides, and why

| Decision | Reason |
|---|---|
| Connector jars **baked into the image**, versions pinned as build args | `--packages` re-resolves from Maven on every start. The plan names jar mismatches as a top pitfall; `infra/spark/Dockerfile` documents the compatibility chain (Spark 3.5.3 → Scala 2.12, hadoop-aws **must** be 3.3.4 to match the bundled hadoop-client, kafka-clients 3.4.1). |
| Master store on a **mounted volume**, not S3 | Spark could not commit a write to SeaweedFS by any of three routes — the file sink's `_spark_metadata` log, rename-based commit, and the magic committer. A local filesystem has real atomic rename, so the default committer just works. Errors and reasoning in [docs/02-tech-stack.md](docs/02-tech-stack.md). |
| `foreachBatch` instead of `.format("parquet")` | Kept from the S3 attempt, and still the right choice: it gives one explicit, readable write per micro-batch, and `repartition` before writing controls the file count. |
| Delivery is **at-least-once**, deduplicated on read | The cost of not using the file sink's commit log. If a batch's files land and the driver dies before offsets commit, the retry rewrites those rows. `dedupe_readings()` collapses them on the natural key `(meter_id, timestamp)`, and every reader goes through it — which matters directly for money, since a replayed batch would otherwise double-bill. |
| `repartition` by the partition columns before writing | Without it each of the topic's 3 partitions writes its own file per zone, so one batch produces ~12 files instead of ~4. Every extra small file is another object to open on a read. |
| Checkpoints on a **docker volume**, separate from the data | Checkpointing needs atomic rename and read-after-write consistency, and keeping offsets separate from data means the store can be wiped and replayed independently. |
| `sim_day` first in the partition path | The billing job filters by day, so Spark prunes whole days instead of opening every zone's files. |
| Session time zone pinned to `Asia/Colombo` | `to_date` resolves the event timestamp's `+05:30` offset in this zone. Left at UTC, a reading at 00:02 Colombo time would be filed under the *previous* simulated day and both days' bills would be wrong. |
| 15 s trigger | One file per partition per micro-batch. The raw store has a completeness requirement, not a latency one, so a slower trigger buys larger files. |
| `local[2]`, 1 GB driver | A single container as its own driver and executor. The marks are for the architecture, not for running a Spark cluster on a laptop. |

> **A partition you should expect to see:** a handful of rows filed one
> simulated day *earlier* than the rest. Those are the deliberately backdated
> late events crossing midnight — a reading backdated 20 simulated minutes from
> 00:05 genuinely belongs to the previous day. It is correct, and it is why
> Phase 4 should bill a day only after the watermark has passed.

---

## Verifying Phase 3, step by step

Phase 3 adds the **speed layer** and the **real-time API**. A second Spark
Structured Streaming job reads `meter.readings`, aggregates it into event-time
tumbling windows per grid zone, and upserts each window into `zone_metrics`;
threshold rules append breaches to `alerts`. FastAPI serves both over HTTP.

Two things are deliberately *not* here: no billing (that is the batch layer,
Phase 4) and no analytics inside the API. The API only reads what the speed layer
wrote — an API that recomputed the aggregation would be a second implementation
free to disagree with the first.

> **Latency, not exactness.** This is Lambda's speed layer, so it is allowed to
> be approximate: meter counts use HyperLogLog because Spark rejects an exact
> `count(distinct)` inside a streaming aggregation, and windows are published
> while still open and corrected in place afterwards. Nothing here is
> authoritative. Money is recomputed from Parquet in Phase 4.

### Step 0 — one-time: the serving port may collide

If you already run PostgreSQL locally, it owns `localhost:5432` — and on Windows
Docker *also* binds 5432 without complaining, so host tools silently authenticate
against your local server instead of the stack's. Set the published port aside in
`.env` before starting:

```
POSTGRES_EXTERNAL_PORT=5433
```

Check with `netstat -ano | findstr ":5432"`: two LISTENING lines means a
collision.

### Step 1 — build and start everything

```powershell
docker compose up -d --build
docker compose ps
```

**Expect** six running services — `kafka`, `postgres`, `seaweedfs`, `raw-sink`,
`speed-layer`, `api` (the last shown `healthy`) — plus `kafka-init` and
`storage-init` as `exited (0)`.

The first build adds `psycopg` to the Spark image and builds the API image. The
`psycopg` layer sits *below* the connector jars in `infra/spark/Dockerfile` on
purpose: a layer above them would invalidate the cache and re-download ~250 MB.

### Step 2 — start the producer

```powershell
python -m sources.stream_producer --log-format console
```

Leave it running in its own terminal. Without it there is no stream, and the
speed layer will sit idle producing no windows at all.

### Step 3 — watch windows being committed

```powershell
docker compose logs -f speed-layer
```

**Expect** one `starting_query` line stating every threshold in force, then one
`micro_batch` line per trigger (every 5 real seconds):

```json
{"component": "speed_layer", "window_minutes": 60, "watermark_minutes": 30,
 "trigger_seconds": 5, "sim_minutes_per_real_second": 4.8,
 "low_renewable_pct": 15.0, "renewable_check_hours": [9, 16],
 "zone_overload_kw_per_meter": 2.2, "event": "starting_query"}
{"component": "speed_layer", "batch_id": 149, "windows_upserted": 4,
 "alerts_raised": 0, "newest_window": "2026-01-23T13:00:00+05:30",
 "zones": ["Colombo-North", "Colombo-South", "Gampaha", "Kandy"],
 "event": "micro_batch"}
```

Two things worth reading in that output:

- **`windows_upserted` alternates between 4 and 8.** Four is the open window
  being refreshed for four zones. Eight is a trigger that crossed a window
  boundary, so the closing window and the new one are both written. That is
  `update` output mode working as intended.
- **`newest_window` carries `+05:30` and lands on the hour.** Spark aligns
  event-time windows to midnight *UTC*, which in Asia/Colombo would put every
  boundary at `HH:30`. `window_alignment_minutes()` in `common/sim_clock.py`
  shifts the grid back onto the local hour — without it the alert rules, written
  in local hours, would silently test 09:30–16:30 instead of 09:00–16:00.

The Spark UI for this job is **http://localhost:4041** (`raw-sink` still has
4040). Its *Structured Streaming* tab shows the watermark advancing and the
number of state rows retained.

### Step 4 — query the serving tables directly

```powershell
docker compose exec postgres psql -U grid -d smartgrid -c `
  "SELECT window_start, grid_zone, total_consumption_kwh, renewable_pct, active_meters FROM zone_metrics ORDER BY window_start DESC LIMIT 8;"
```

**Expect** four rows per window, one per zone, newest first. Overnight windows
show `renewable_pct = 0` — correct, not a fault. Around simulated midday the
high-penetration zones reach 60–90%.

### Step 5 — the API

```powershell
curl http://localhost:8000/health
curl http://localhost:8000/zones
curl "http://localhost:8000/zones/Colombo-North/load?windows=5"
curl "http://localhost:8000/alerts?limit=5"
curl http://localhost:8000/metrics
```

Or open **http://localhost:8000/docs** for the generated OpenAPI page, which is
the easiest thing to show in a demo.

`/health` separates two questions that are usually collapsed into one:

```json
{"status": "ok", "database": "reachable", "data_fresh": true,
 "seconds_since_last_write": 5.3, "freshness_threshold_seconds": 30.0,
 "windows_stored": 240, "zones_reporting": 4,
 "newest_window_start": "2026-01-23T11:00:00+05:30", "zones_configured": 4,
 "run_id": "api-20260929T033506-ec4674"}
```

Stop the producer and `data_fresh` flips to `false` while the status code stays
200 — the API is healthy and correctly reporting that its upstream has stopped.
Only an unreachable database is a 503. Phase 6 turns that flag into a firing
health-check alert.

`/zones` returns the newest **complete** window per zone:

```json
{"window_complete": true, "zone_count": 4,
 "grid_total_consumption_kwh": 29.5278, "grid_total_solar_kwh": 0.8809,
 "grid_renewable_pct": 2.98,
 "zones": [{"window_start": "2026-01-23T13:00:00+05:30",
            "window_end": "2026-01-23T14:00:00+05:30",
            "grid_zone": "Colombo-North", "total_consumption_kwh": 8.8107,
            "total_solar_kwh": 0.3501, "renewable_pct": 3.97,
            "active_meters": 12, "avg_load_kw": 8.811,
            "updated_at": "2026-09-29T09:12:16.564851+05:30"}]}
```

The window in progress is excluded because the speed layer rewrites it on every
micro-batch, so serving it raw would show consumption climbing from zero each
time a window opened. One window of latency is 12.5 real seconds at 288x. Pass
`?include_open=true` to see it anyway.

Note which timestamps are which: `window_start`/`window_end` are **simulated**
instants, `updated_at` is **real** time. It is the only real clock in the row,
which is why the freshness check reads it.

### Step 6 — run the checkpoint

```powershell
python scripts\check_phase3.py
```

This is the plan's checkpoint. It reads Postgres and the API *independently* and
compares them, because the failure worth catching is not a dead API — it is a
live-looking dashboard served from a stale or wrongly-joined query.

```
  Serving tables       [PASS]
      + zone_metrics holds 236 window row(s)
      + alerts holds 8 row(s)
      + pipeline_runs has 4 speed_layer run(s) registered

  Zone coverage        [PASS]
      grid_zone         renewable%    meters   age(s)  newest window (simulated)
      + Colombo-North          3.6        12      3.3  2026-01-23 10:00
      + Colombo-South          2.7        10      3.3  2026-01-23 10:00
      + Gampaha                2.6         9      3.3  2026-01-23 10:00
      + Kandy                  2.2         9      3.3  2026-01-23 10:00
      + all 4 configured zone(s) present
      + rows written within 45s (no_data_real_seconds + 15s slack)

  Window arithmetic    [PASS]
      + 200 window(s) checked, all 60 simulated minutes long
      + renewable_pct matches solar/consumption in every row
      + active_meters never exceeds the 40 configured meters

  API endpoints        [PASS]
      + /health   status=ok data_fresh=True last_write=3.4s ago
      + /zones    4 zone(s), grid load 32.72 kWh/window, renewable 2.4%
      + /zones agrees with the newest complete window in Postgres
      + /zones/Colombo-North/load returned 5 window(s), newest first
      + unknown zone: 404 as expected

  PHASE 3 CHECKPOINT: PASSED
```

**Window arithmetic** is the check that would catch a genuinely wrong
aggregation. That every window is exactly 60 simulated minutes proves the job
grouped on event time rather than processing time; recomputing `renewable_pct`
from the stored totals proves the served percentage came from the served numbers.

### Step 7 — make an alert fire

The plan's checkpoint asks for this explicitly: *forcing low solar in the
producer makes a low-renewable alert appear.* Restart the producer with heavy
cloud cover:

```powershell
# Ctrl+C the running producer first
python -m sources.stream_producer --cloud-cover 0.95 --log-format console
```

Then wait for a window inside **09:00–16:00 simulated** — at most ~90 real
seconds — and watch:

```powershell
docker compose logs -f speed-layer
```

**Expect** one `alert` line per zone per window:

```json
{"component": "speed_layer", "rule": "low_renewable_contribution",
 "grid_zone": "Kandy", "level": "warning",
 "message": "Low renewable contribution, Kandy: 2.2% (floor 15%) at 2026-01-23 14:00 simulated",
 "window_start": "2026-01-23T14:00:00+05:30", "event": "alert"}
```

and then confirm it end to end:

```powershell
python scripts\check_phase3.py --expect-alert low_renewable_contribution
curl "http://localhost:8000/alerts?rule=low_renewable_contribution&limit=5"
```

**One alert per zone per window, not one per micro-batch.** An open window is
re-emitted every 5 seconds, so the same breach is evaluated a dozen times before
the window closes. `Alert.dedupe_key` — `(rule, zone, window_start)` — is the
identity of the *condition*, and the sink keeps the keys it has already written.

Stop the cloud cover and the alerts stop with the next window: solar recovers to
60–90% around midday and the rule falls silent.

### Restarting cleanly

The checkpoint holds both the Kafka offsets and the open windows' state, so a
plain restart resumes mid-window. Changing the **window size or the watermark**
changes the query plan, which Structured Streaming cannot resume across — clear
the checkpoint or the job will fail to start:

```powershell
docker compose stop speed-layer
docker compose run --rm --no-deps --entrypoint sh speed-layer -c "rm -rf /checkpoints/speed_layer"
docker compose exec postgres psql -U grid -d smartgrid -c "TRUNCATE zone_metrics, alerts;"
docker compose up -d speed-layer
```

Only `speed_layer`'s checkpoint is removed — `raw-sink`'s lives beside it and the
master dataset is untouched.

### What Phase 3 decides, and why

| Decision | Reason |
|---|---|
| Aggregation lives in `processing/shared/transforms.py` | The concrete answer to Lambda's standard criticism. The speed layer groups by (window, zone) and the batch layer will group by (sim_day, household) — different questions, one piece of arithmetic, imported by both. |
| `update` output mode, not `append` | `append` emits a window once and only after the watermark has passed it, so a dashboard shows nothing about the window in progress. `update` re-emits changed windows, which is only safe because `zone_metrics` is keyed on `(window_start, grid_zone)` and the sink upserts. |
| Approximate meter counts in the speed layer | Spark rejects an exact `count(distinct)` in a streaming aggregation — exactness would mean retaining every meter id in each open window's state. So the speed layer is approximate and the batch layer is exact, which is the accuracy split the architecture decision argues for, showing up in the code. At 9–12 meters per zone HyperLogLog is exact anyway. |
| Writes go through **psycopg**, not Spark's JDBC writer | Every write has to be an idempotent upsert, and JDBC cannot express `ON CONFLICT DO UPDATE`: `append` hits the primary key, `overwrite` drops the table. The sink collects each micro-batch — 4–12 rows — and upserts it. Honest limit: this puts every row through the driver, so a production volume would need a staging-table merge instead. |
| A fresh connection per micro-batch | ~2 ms against a local Postgres, against a long-lived connection that would have to survive every Postgres restart and idle timeout for hours. Validated by accident when the Postgres container was recreated mid-run and the job simply carried on. |
| Windows shifted onto local hour boundaries | Spark aligns event-time windows to midnight UTC, so in +05:30 an "hourly" window runs `23:30–00:30`. Every reported boundary would be half an hour off the clock, and the alert rules — written in local hours — would test 09:30–16:30. |
| Alert rules are **pure functions** over one row | `processing/shared/alert_rules.py` imports neither Spark nor psycopg, so the logic that decides whether an operator is woken up is unit-tested in `tests/test_alert_rules.py` without Kafka, a JVM or a database. |
| The low-renewable rule checks **09:00–16:00**, not all daylight | A 06:00–07:00 window genuinely sits near 0% renewable because the sun has just risen. Checking the full 06:00–18:00 span fires the rule at dawn and dusk every simulated day, which is a calendar, not an alert. |
| The overload ceiling is **2.2 kW/meter**, per meter rather than per zone | Per-meter because zones differ in size (12 meters against 9), so one absolute kW figure is unreachable for the small zones. The value is calibrated against the simulator's own demand curve: 1.25 kW base × 1.50 evening peak × ~10% residual jitter ≈ 2.06, which is exactly the highest window measured across five simulated days. The first draft used 1.6, derived from base load alone, and fired for the two largest zones every simulated evening — caught by running it, and now pinned by a test. |
| A new run supersedes the previous `RUNNING` row | `docker compose stop` signals PID 1, which for a PySpark job is `spark-submit`'s JVM; the JVM kills the Python driver rather than forwarding the signal, so a container stop can never close its own row. Since a new run proves the old one has ended, `register_run` marks it `FAILED` with a note and logs how many it superseded. |
| 503 and 500 are kept apart in the API | An unreachable database is 503: the dependency is down and restarting the API would achieve nothing. A query Postgres refuses is 500: the API's own code is wrong. Collapsing them is how a malformed cast gets diagnosed as a dead database — which happened here, once. |

---

## Verifying Phase 4, step by step

Phase 4 adds the **batch layer** and the **scheduler**. A Spark batch job
recomputes one simulated day's household bills from the Parquet master store,
joins that day's tariff file, and upserts the result into `daily_bill`; an Airflow
DAG runs it once per simulated day and checks the result before calling the run
green.

> **Exactness, not latency.** This is the half of Lambda that is allowed to be
> slow and has to be right. It reads nothing from the speed layer and trusts
> nothing the speed layer wrote: the input is the immutable Parquet store, the
> meter count is exact, and every figure is reproducible by re-running the job.
> What the two layers share is their arithmetic —
> `processing/shared/transforms.py` and `processing/shared/billing.py` — and
> nothing else, which is what makes agreement between them evidence rather than
> tautology.

### Step 1 — build and start everything

The stack gains three things: `airflow`, a one-shot `airflow-db-init` that creates
Airflow's metadata database inside the existing Postgres, and `billing-batch`,
which is a job rather than a service and so does not start with the others.

```powershell
docker compose up -d --build
docker compose ps
```

The first build downloads a JRE and PySpark into the Airflow image — roughly 1 GB,
several minutes. Afterwards:

```
SERVICE       STATUS
airflow       Up 6 minutes (healthy)
api           Up About an hour (healthy)
kafka         Up 3 minutes (healthy)
postgres      Up 3 minutes (healthy)
raw-sink      Up 3 minutes
seaweedfs     Up 3 minutes
speed-layer   Up 45 seconds
```

`docker compose ps -a` additionally shows `airflow-db-init  Exited (0)`, which is
correct — it creates the `airflow` database if it is not there and gets out of the
way. It is a separate container rather than a script in `infra/postgres/init/`
because those only run when the Postgres volume is *created*, and by Phase 4 your
stack already has one.

Airflow takes two to three minutes to become healthy on a first start: it runs its
migrations, creates the admin user and boots three components. The UI is at
**http://localhost:8080**, username `admin`, password `admin` (both set in `.env`).

### Step 2 — choose a simulated day that has finished

The batch layer bills **closed** days only. Billing a day still being written
produces a total that is simply too low — with no error, and with a primary key
that then blocks the correct figure from being inserted later. So the job refuses:

```powershell
docker compose run --rm billing-batch --sim-day 20260321   # today, in simulated time
```

```
refusing to bill 2026-03-21: the simulated day is still in progress (it is now
2026-03-21 04:34:56+05:30 simulated). Bill the previous day, or pass --allow-open-day.
```

To see which days the master store actually holds:

```powershell
dir data\raw\meter_readings
```

A complete simulated day holds `40 meters × 150 readings = 6,000` rows. Days the
producer only partly covered are still billable — they bill what was measured — but
the checkpoint script will say so rather than let it pass unnoticed.

Every billed day also needs its tariff file. The generator reproduces any past day
exactly, because the rate is seeded by (household, day) rather than rolled:

```powershell
python -m sources.tariff_generator --sim-day 20260121
```

```
[info] tariff_file_written  bucket=tariff key=tariff_20260121.csv households=40
                            sim_day=2026-01-21 subsidised=12
```

Run it twice and the file is byte-identical. That property is what makes the batch
layer's claim to be re-runnable real rather than nominal.

### Step 3 — run the batch job by hand

Before involving a scheduler, run the job itself. `billing-batch` is a compose
service under the `manual` profile, so it never starts with `up`, and its entry
point is `spark-submit` — anything after the service name is an argument to the job:

```powershell
docker compose run --rm billing-batch --sim-day 20260121 --log-format console
```

```
[info] starting          component=billing_batch compression_factor=288.0 total_meters=40
[info] billing_day       sim_day=2026-01-21 currency=LKR net_metering=import_only
                         subsidy_pct=0.1
[info] tariff_loaded     source=s3://tariff/tariff_20260121.csv
[info] day_read          sim_day=2026-01-21 readings=6000 expected_readings=6000
                         completeness_pct=100.0
[info] report_published  bucket=reports key=billing_20260121.csv rows=40
[info] billing_complete  sim_day=2026-01-21 rows_written=40 households=40
                         total_consumption_kwh=817.1813 total_solar_kwh=151.1249
                         net_kwh=666.0564 gross_amount=28896.67 subsidy_amount=833.21
                         net_amount=28063.46 subsidised_households=12 currency=LKR
                         report=s3://reports/billing_20260121.csv
```

Four lines carry the whole argument:

| Log line | What it settles |
|---|---|
| `tariff_loaded` | The day's commercial terms came from the file drop, not from a default |
| `day_read` | 6,000 readings after deduplication against 6,000 expected — a complete day |
| `billing_complete` | 40 households billed; 817 kWh drawn, 151 kWh generated, 666 kWh billable |
| `report_published` | The artefact a billing department would be sent is in object storage |

If the tariff file is missing, the job stops **before** building a Spark session —
input validation is cheaper than JVM startup, so this fails in about a second
rather than after twenty — and tells you the one command that fixes it:

```
[error] billing_failed  sim_day=2026-01-19
        error=no tariff file for 2026-01-19 at s3://tariff/tariff_20260119.csv
        (NoSuchKey). The generator can reproduce any past day exactly:
            python -m sources.tariff_generator --sim-day 20260119
```

The run is still recorded in `pipeline_runs` as `FAILED`, so a day that was never
billed is visible as an attempt rather than as silence.

### Step 4 — look at what it wrote

**The serving table.** One row per household per simulated day:

```powershell
docker compose exec postgres psql -U grid -d smartgrid -c "SELECT household_id, grid_zone, billing_tier, net_kwh, tariff_rate, subsidy_flag, net_amount FROM daily_bill WHERE sim_day = '2026-01-21' ORDER BY household_id LIMIT 5;"
```

**The published report.** One CSV per billed day in the `reports` bucket, named
`billing_<YYYYMMDD>.csv` and overwritten on a re-run — the report is a view of
`daily_bill`, and two files for one day would leave it ambiguous which one is the
bill.

```powershell
python -c "from common.config import load_config; from common.storage import s3_client, list_keys; cfg = load_config(); print(list_keys(s3_client(cfg), cfg.bucket('reports')))"
```

**The API.** Phase 4 adds two read-only endpoints, so the batch layer's output is
visible over HTTP alongside the speed layer's — which is what makes Postgres a
Lambda *serving* layer rather than two unrelated tables:

```powershell
curl http://localhost:8000/bills
curl http://localhost:8000/bills/2026-01-21
```

`/bills` lists the days that have been billed and what each came to; `/bills/{sim_day}`
returns that day's forty bills with the day's totals. An unbilled day is a 404, and a
malformed date is a 400 rather than a 500.

### Step 5 — the same job, on a schedule

One simulated day is five real minutes, so the DAG is scheduled `*/5 * * * *` — the
compressed equivalent of a nightly billing run. Its four tasks are:

```
resolve_sim_day  ->  wait_for_tariff  ->  bill_day  ->  verify_bills
```

- **resolve_sim_day** asks the shared simulated clock which day has just closed and
  pushes it to XCom, so every later task works on the same day. Airflow's own
  logical date is real time and would name a date the simulation never reaches.
- **wait_for_tariff** is a sensor over object storage. Keeping "the input arrived"
  separate from "compute the money" means a missing tariff shows up as a task still
  waiting, not as a Spark job that crashed.
- **bill_day** is the `spark-submit` from Step 3.
- **verify_bills** re-reads `daily_bill` and re-derives every amount from the stored
  usage and rate. A green Spark task can still have written a wrong day.

**The DAG ships paused.** Unpausing it starts a Spark JVM every five real minutes
for as long as the stack is up, which on an 8 GB laptop is enough to get the
Airflow webserver OOM-killed mid-billing. So billing is started deliberately —
from the toggle in the UI, or:

```powershell
docker compose exec airflow airflow dags unpause daily_billing
```

Watch it in the UI, or from the command line:

```powershell
docker compose exec airflow airflow dags list-runs -d daily_billing --no-backfill
docker compose exec airflow airflow tasks states-for-dag-run daily_billing "<run_id>"
```

A scheduled run, billing the simulated day that had just closed:

```
task_id          state    start_date                        end_date
resolve_sim_day  success  2026-09-29T06:21:21.497124+00:00  2026-09-29T06:21:22.425273+00:00
wait_for_tariff  success  2026-09-29T06:21:44.496853+00:00  2026-09-29T06:22:06.717064+00:00
bill_day         success  2026-09-29T08:14:52.590736+00:00  2026-09-29T08:17:49.729934+00:00
verify_bills     success  2026-09-29T08:17:57.358389+00:00  2026-09-29T08:17:58.951796+00:00
```

To bill one specific past day instead — which is what a real backfill does, and what
the checkpoint below uses:

```powershell
docker compose exec airflow airflow dags trigger daily_billing --conf '{\"sim_day\": \"20260120\"}'
```

```
task_id          state    start_date                        end_date
resolve_sim_day  success  2026-09-29T06:01:01.682620+00:00  2026-09-29T06:01:06.709047+00:00
wait_for_tariff  success  2026-09-29T06:01:09.681580+00:00  2026-09-29T06:01:47.789581+00:00
bill_day         success  2026-09-29T06:01:54.149497+00:00  2026-09-29T06:09:55.732591+00:00
verify_bills     success  2026-09-29T06:10:06.118439+00:00  2026-09-29T06:10:07.983750+00:00
```

Two things to know before unpausing:

- **A manual trigger only runs once the DAG is unpaused.** Airflow's scheduler
  skips paused DAGs entirely, including runs you triggered by hand — they sit in
  `queued` until you unpause.
- **Scheduled runs need the producer and the tariff generator running.** Without
  them no simulated day is being filled, so `wait_for_tariff` polls for three real
  minutes and then fails — the sensor doing its job, not a broken DAG. Start both
  (terminals 1 and 2 above) and the next scheduled run goes green on its own.

`max_active_runs=1`, so a manual trigger waits while a scheduled run is still
polling. That is why the sensor's timeout is three minutes rather than ten: a run
that cannot find its input has to fail before the next one is scheduled.

When you are done demonstrating it, pause it again — `airflow dags pause
daily_billing` — and the machine goes back to running only the two streaming jobs.

### Step 6 — run the checkpoint

```powershell
python scripts\check_phase4.py
python scripts\check_phase4.py --sim-day 20260121 --household H-105
```

The script re-reads the household's readings **from the Parquet files with pyarrow**,
sums them in plain Python, applies the same billing rules, and compares the result
column by column against what Spark stored. The two paths share the rules and nothing
else, so agreement means the numbers are right rather than that one implementation ran
twice.

```
  Smart Grid Lambda Platform -- Phase 4 batch billing check
  ----------------------------------------------------------

  Serving DB   localhost:5433/smartgrid
  Master store data/raw/meter_readings
  Billed day   2026-01-21   (2026-01-21 to 2026-01-22, simulated)

  Batch run            [PASS]
      + newest run billing_batch-20260929T054713-0f05e8 finished SUCCESS
        wrote 40 row(s) in 66.9s
      + every row for the day carries one run_id
      + daily_bill.run_id matches the newest run, so the rows trace back to its logs

  Bill coverage        [PASS]

      grid_zone         bills      net kWh subsidised   payable LKR
      Colombo-North        12      195.857          3       9028.46
      Colombo-South        10      202.006          3       7685.69
      Gampaha               9      139.330          3       5630.41
      Kandy                 9      128.864          3       5718.90

      + 40 of 40 configured households billed
      + every configured zone appears
      + no bill is negative or charges more units than were used

  Hand calculation     [PASS]

      H-105 on 2026-01-21 -- recomputed from 150 reading(s) in the master store:

                                            recomputed        stored
      + consumption                   kWh       21.6811       21.6811
      + solar generation              kWh        7.0044        7.0044
      + net = max(used - solar, 0)    kWh       14.6767       14.6767
      + gross = net x 46.25           LKR         678.8         678.8
      + subsidy @  10%                 LKR         67.88         67.88
      + payable                       LKR        610.92        610.92

      + tier domestic-2 on the 2026-01-21 tariff file, subsidised

  Published report     [PASS]
      + s3://reports/billing_20260121.csv published
      + 40 data row(s), matching daily_bill's 40
        first row: 2026-01-21,H-101,Colombo-North,21.4214,6.8579,14.5635,domestic-1,30.85,False,449.28,0.0,449.28

  Serving API          [PASS]
      + /bills lists 1 billed day(s)
      + /bills/2026-01-21 serves 40 bills totalling 28063.46 LKR, agreeing with Postgres
      + an unbilled day: 404 as expected

  Lambda cross-check   [INFO]

      grid_zone            batch kWh     speed kWh   windows  difference
      Colombo-North           257.67        255.89        24        0.7%
      Colombo-South           243.29        241.65        24        0.7%
      Gampaha                 166.82        165.66        24        0.7%
      Kandy                   149.39        148.34        24        0.7%

      The batch figure is the authoritative one. The speed layer
      only covers the windows it was running for, so a difference
      here is coverage, not disagreement about arithmetic.

  ----------------------------------------------------------
  PHASE 4 CHECKPOINT: PASSED
```

The last block is the one worth pausing on. Both layers measured the same simulated
day: the batch layer read 6,000 Parquet rows, the speed layer summed the 24 windows
it had published live, and they land within 0.7% of each other. The deficit is the
speed layer's, and it is coverage rather than arithmetic — a window left partial when
the streaming job restarted stays partial, because nothing goes back to correct it.
That is precisely the trade the architecture decision argues for, measured rather than
asserted.

### Step 7 — prove it is re-runnable

Trigger the same day again. The job recomputes from immutable Parquet and upserts on
`(sim_day, household_id)`, so the second run rewrites the same forty rows:

```powershell
docker compose exec airflow airflow dags trigger daily_billing --conf '{\"sim_day\": \"20260120\"}'
python scripts\check_phase4.py --sim-day 20260120
```

```
  Batch run            [PASS]
      + newest run sg-billing-20260929T060002-1 finished SUCCESS
        wrote 40 row(s) in 326.8s
      + billed 2 times in total; the upsert kept one row per household
      + every row for the day carries one run_id
      + daily_bill.run_id matches the newest run, so the rows trace back to its logs
```

Two runs, forty rows, one `run_id` — and that id is `sg-billing-<timestamp>-<try>`,
minted by Airflow and adopted by the job through `SG_RUN_ID`. A suspicious bill
therefore names the DAG run that produced it, which joins to `pipeline_runs` and to
that task's JSON logs.

### Memory: the one real constraint on a laptop

The batch job starts a JVM inside the Airflow container while two streaming Spark
drivers are already running. On an 8 GB machine Docker Desktop takes 3.7 GB by
default, and that is not quite enough for all of it at once. The failure is not
subtle, but it is easy to misread — it appears in the `bill_day` task log as:

```
OSError: [Errno 12] Cannot allocate memory: '/app/processing'
```

The same shortage shows up on the Airflow side as a dead UI while the scheduler
keeps working — `Worker (pid:…) was sent SIGKILL! Perhaps out of memory?` followed
by `No response from gunicorn master`.

That is a capacity failure, not a bug. Three things hold it in check: the DAG ships
paused, so a Spark JVM starts only when you ask for one; both streaming jobs run
with `--driver-memory 512m` (measured working set ~450 MB); and the billing job runs
with 512m and the Spark UI disabled. If you still hit it:

- give Docker Desktop more memory (Settings → Resources) if the host has it to spare;
- or stop the producer while a billing run is in flight — the raw sink resumes from its
  checkpoint afterwards and loses nothing;
- or run the job with `docker compose run --rm billing-batch`, which is the same work
  without Airflow's scheduler and webserver resident alongside it.

With nothing else competing the job takes about a minute; under a full stack with the
producer running, it took five and a half.

### Re-billing from scratch

```powershell
docker compose exec postgres psql -U grid -d smartgrid -c "DELETE FROM daily_bill WHERE sim_day = '2026-01-21';"
docker compose run --rm billing-batch --sim-day 20260121
```

Nothing else needs clearing: the batch layer holds no checkpoint and no state. Its
inputs are a directory of Parquet files and a CSV, both immutable, so "delete the
output and run it again" is a complete reset. That is the property the speed layer
cannot offer, and the reason the batch layer is the authoritative one.

### What Phase 4 decides, and why

| Decision | Reason |
|---|---|
| The batch layer reads **Parquet, never Kafka or `zone_metrics`** | This is what "authoritative" means. Recomputing from the immutable master store means a bug fixed today can be replayed over history; recomputing from the speed layer's output would only reproduce its approximations, and Kafka's 24-hour retention cannot answer for last week at all. |
| Deduplicate on `(meter_id, timestamp)` before billing | The raw sink is at-least-once, so a replayed micro-batch is physically in the files. Without the dedupe those households are billed twice — the one defect in this phase that would look like a perfectly plausible number. |
| **Exact** meter counts here, approximate in the speed layer | Spark rejects an exact `count(distinct)` in a streaming aggregation; a batch job has no such limit. Same shared function, one flag — so the accuracy split the architecture decision argues for shows up in the code rather than only in prose. |
| Billing rules are **pure Python**, not Spark expressions | Money must be verifiable by hand. `processing/shared/billing.py` imports nothing, so the plan's worked example (H-101: 18 kWh used, 6 generated, rate 45, subsidised → 486) is a unit test, and `scripts/check_phase4.py` can recompute a real bill on the host with no JVM. Spark still does the big-data work: scan, dedupe, aggregate, join. |
| Round the money **once**, then subtract | `net_amount = gross - subsidy` exactly, because both were rounded before the subtraction. Rounding each column independently lets the three disagree by a cent, and a bill whose parts do not add up is indefensible however small the gap. |
| Upsert on `(sim_day, household_id)` | What makes a retry safe. Airflow retries `bill_day` twice by default; without the upsert the second attempt would either double-bill or hit the primary key and fail. |
| Refuse to bill an **open** simulated day | A partial day bills too little, silently, and then owns the primary key. `--allow-open-day` exists for a demo and says what it is doing. |
| **Left** join to the tariff, then fail on unpriced households | An inner join would quietly drop a household with no tariff row. Billing part of a zone produces a plausible, wrong total — so the job writes nothing and names the households. |
| The tariff is read with **boto3**, not Spark over S3A | Forty rows. Reading it through S3A would mean shipping a 250 MB AWS SDK into the Airflow image to fetch a 2 KB file. Spark's contribution to that join is the join. |
| Spark runs **inside** the Airflow image, not via `DockerOperator` | The textbook arrangement needs the Docker socket mounted, the container running as root to use it, and the DAG naming *host* paths for every mount — three environment-specific dependencies for a job that runs in local mode either way. A real deployment submits to a cluster instead, which is one operator swap, not a redesign. |
| Airflow's metadata lives in a **second database in the same Postgres** | One less container on a machine already running seven. The isolation that matters — Airflow's tables never mixing with the serving tables — is what a separate database gives; a separate server would only add operational surface. SQLite was the alternative, and it forces the SequentialExecutor. |
| The simulated day comes from the **simulated clock**, not Airflow's logical date | The logical date is real time. The simulation is somewhere in January 2026 regardless of today's date, so scheduling on the logical date would bill a day that does not exist. |
| `verify_bills` is a task, not a comment | A data-quality gate that fails the run: every household billed, every amount re-derived from its own stored usage and rate, and one run owning the whole day. The last check catches a day half-written by one run and half by another. |
| `SG_RUN_ID` is set by the DAG and adopted by the job | One correlation id spans the Airflow run, the `pipeline_runs` row, every `daily_bill` row and the job's JSON logs — so a suspicious bill leads back to the exact execution that wrote it. |

---

## Verifying Phase 5, step by step

Phase 5 adds **Grafana** over the serving layer: one dashboard per Lambda layer,
both reading Postgres directly. Nothing is configured by hand — the datasource and
both dashboards are provisioned from `observability/grafana/`, so a fresh clone
gets them on `docker compose up`.

| Dashboard | Reads | Shows |
|---|---|---|
| **Live Zone Monitoring** (`/d/sg-live`) | `zone_metrics`, `alerts` | renewable % and load per zone, simulated clock, freshness, active meters, alerts |
| **Daily Billing** (`/d/sg-billing`) | `daily_bill` | day totals, zone consumption vs solar, zone solar share, top 10 bills, lowest-revenue households, revenue by day, every bill |

```powershell
docker compose up -d grafana
python scripts\check_phase5.py      # PHASE 5 CHECKPOINT: PASSED
```

Open **http://localhost:3000** (`admin` / `admin`); the home page is the live
dashboard, refreshing every 10 s. The billing dashboard has a *Simulated day*
selector that defaults to the newest billed day.

The checkpoint script sends every panel's SQL back through Grafana's own query API
(`/api/ds/query`), the path the browser uses, so it proves the datasource, its
credentials, Grafana's macros and each query — not just that Grafana is up. The
`Recent alerts` panels may legitimately be empty; every other panel must return rows.

### What Phase 5 decides, and why

| Decision | Reason |
|---|---|
| Live charts plot each window at **`updated_at`**, not `window_start` | `window_start` is simulated time (January 2026); Grafana's time picker is real time, so those points would fall outside every range and the panels would say "No data". `updated_at` is the real time the speed layer last wrote the window — processing time — so "last 30 minutes" means what it says: six simulated days. The simulated window is shown as text in the *Simulated clock* tile. |
| Stat tiles use the **latest complete** window | The same rule as the API's `/zones`: the open window is still filling, and showing it would make the numbers sag every ~12 s. |
| Renewable tiles turn red **below 15%** | The speed layer's `low_renewable_pct` alert threshold, so the colour and the alert log never disagree. |
| The billing dashboard is keyed on a **`sim_day` variable**, not the time picker | A bill belongs to a simulated date; the variable lists exactly the days `daily_bill` holds. |
| **"Lowest-revenue"** rather than "loss-making" households | Under import-only net metering nobody is paid for exports, so no bill is negative. What the utility actually forgoes is solar offset plus subsidy, and that panel shows both. |
| Provisioned, read-only dashboards (`allowUiUpdates: false`) | The JSON in the repo is the source of truth. A UI edit that is not saved back to the file would vanish on the next `docker compose up` anyway. |

---

## Verifying Phase 6, step by step

Phase 6 makes the pipeline **observable**: structured logs with correlation ids
(in place since Phase 0), Prometheus metrics, operational alert rules — including
the **"no meter data" health check** — and a Grafana *Pipeline Health* dashboard.

| Signal | Where it comes from | Metric / location |
|---|---|---|
| Events/s | the producer, `http://localhost:9101/metrics` | `smartgrid_producer_events_total`, `..._late_events_total` |
| End-to-end lag | the API, from `zone_metrics.updated_at` | `smartgrid_serving_lag_seconds` |
| Batch duration | the API, from `pipeline_runs` | `smartgrid_batch_last_duration_seconds` |
| Error counts | producer delivery callback, API, `pipeline_runs` | `..._producer_send_errors_total`, `smartgrid_api_errors_total`, `smartgrid_pipeline_runs{status="FAILED"}` |
| Correlation ids | every JSON log line, `pipeline_runs`, `daily_bill.run_id`, `alerts.run_id` | *Recent pipeline runs* table |

Alert rules (`observability/prometheus/alert_rules.yml`):

| Alert | Fires when | Severity |
|---|---|---|
| **NoMeterData** | no zone window has reached Postgres for > 30 s (10 s `for`) | critical |
| ProducerDown | Prometheus cannot scrape the producer for 20 s | warning |
| ProducerSendErrors | Kafka rejected any reading in the last 5 min | warning |
| ApiDown | the API's `/metrics` is unreachable for 20 s | critical |
| BillingRunFailed | a `billing_batch` run was recorded `FAILED` in the last 15 min | warning |

```powershell
docker compose up -d prometheus
docker compose restart api grafana               # new API metrics, new Grafana datasource
python -m sources.stream_producer --log-format console   # restart: the metrics endpoint is new
python scripts\check_phase6.py                   # healthy: PHASE 6 CHECKPOINT: PASSED
# now stop the producer (Ctrl+C), then:
python scripts\check_phase6.py --expect-firing NoMeterData
```

The second run polls Prometheus until `NoMeterData` reaches `firing` — about 40–50 s
after the producer stops (30 s threshold, 10 s `for`, one scrape interval). The same
alert is visible at http://localhost:9090/alerts and on http://localhost:3000/d/sg-ops.
Restart the producer and it resolves within one scrape.

### What Phase 6 decides, and why

| Decision | Reason |
|---|---|
| The health check measures **the end of the pipeline** | `NoMeterData` watches when a zone window last reached Postgres, so it fires whichever stage stopped — producer, Kafka or the speed layer. `ProducerDown` then says *which*. |
| Health checks are **Prometheus rules**, not rows in `alerts` | They fire on the *absence* of data, which a streaming job cannot observe: with no input, it runs no micro-batch and has nothing to evaluate. |
| No Alertmanager | Firing state is visible in Prometheus and Grafana, which is the rubric's "working alert". Routing to email or Slack is one more container and a receiver config — the documented next step. |
| The API computes lag and run metrics from Postgres at scrape time | The API stays stateless; the serving database is already the one place both layers write to. |
| The producer exposes its own `/metrics` on the host | It is the only component outside Docker, and its throughput is the one number nothing downstream can measure honestly: a Kafka backlog hides a slow producer. |

---

## What Phase 0 stood up

| Service | Host endpoint | Purpose |
|---|---|---|
| Kafka (KRaft, single node) | `localhost:29092` | ingestion bus; topic `meter.readings`, 3 partitions |
| SeaweedFS S3 API | `localhost:8333` | object store (`smartgrid` / `smartgridsecret`) |
| SeaweedFS filer UI | `localhost:8888` | browse buckets and objects in a browser |
| PostgreSQL | `localhost:5432` | serving layer (`smartgrid` / `grid` / `gridpass`) |

Buckets: `tariff` (daily CSV drop), `reports` (generated billing reports). There
is no `raw` bucket — the Parquet master store is the `./data/raw` volume, for the
reasons in [docs/02-tech-stack.md](docs/02-tech-stack.md).
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
├─ infra/spark/Dockerfile      # Spark + pinned Kafka/S3A/committer jars
├─ infra/api/Dockerfile        # the API's own slim image
├─ infra/airflow/Dockerfile    # Airflow + a JRE + PySpark, for the billing DAG
├─ common/                     # config, JSON logging, simulated clock, S3 client
├─ docs/                       # ADR, diagrams, tech-stack justification
├─ scripts/
│  ├─ stack_status.py          # Phase 0 checkpoint verifier
│  ├─ check_phase1.py          # Phase 1 checkpoint verifier
│  ├─ check_phase2.py          # Phase 2 checkpoint verifier
│  ├─ check_phase3.py          # Phase 3 checkpoint verifier
│  ├─ check_phase4.py          # Phase 4 checkpoint verifier
│  ├─ check_phase5.py          # Phase 5 checkpoint verifier
│  └─ check_phase6.py          # Phase 6 checkpoint verifier
├─ tests/                      # 106 tests, no Docker or JVM required
├─ sources/
│  ├─ meter_model.py           # meter population + reading physics (pure, tested)
│  ├─ stream_producer.py       # meter events -> Kafka
│  └─ tariff_generator.py      # daily tariff CSV -> object storage
├─ processing/
│  ├─ shared/
│  │  ├─ schemas.py            # event contract + sim_day, used by both layers
│  │  ├─ transforms.py         # the energy aggregation, called by both layers
│  │  ├─ alert_rules.py        # threshold rules as pure, testable functions
│  │  └─ pg.py                 # idempotent writes into the serving layer
│  │  └─ billing.py            # net metering, subsidy, tariff parsing (pure)
│  ├─ raw_sink.py              # Phase 2: Kafka -> partitioned Parquet
│  ├─ inspect_raw.py           # Phase 2: read the store back with Spark
│  ├─ speed_layer.py           # Phase 3: windowed zone metrics + alerts
│  └─ billing_batch.py         # Phase 4: Parquet + tariff -> daily_bill
├─ orchestration/dags/
│  └─ daily_billing_dag.py     # Phase 4: the scheduled batch run
├─ serving/api/
│  ├─ main.py                  # Phase 3-4: FastAPI endpoints
│  └─ db.py                    # read-side queries (reads only, never writes)
├─ observability/
│  ├─ grafana/provisioning/    # Phase 5-6: Postgres + Prometheus datasources, dashboard loader
│  ├─ grafana/dashboards/      # live zone monitoring, daily billing, pipeline health (JSON)
│  └─ prometheus/              # Phase 6: scrape config + alert rules
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
| `raw-sink` restart-looping | Read the traceback: `docker compose logs --tail 40 raw-sink`. Jobs are mounted read-only from the host, so a syntax error needs only `docker compose restart raw-sink`. |
| `No module named 'common.config'` inside Spark | `spark-submit` puts the submitted script's directory first on `sys.path`. That is why the shared package is `processing/shared/` and not `processing/common/`, which would shadow the top-level `common/`. |
| Phase 2 coverage stuck below 90% | The sink has stalled. Check its logs, then restart it; the checkpoint means it resumes where it left off. |
| Master store has rows but `check_phase2` can't read it | pyarrow discovery skips `_`-prefixed paths. If a stray non-Parquet file was written under the prefix, remove it. |
| `password authentication failed for user "grid"` from the host | A PostgreSQL installed on your machine already owns 5432 and Docker binds alongside it, so you reached the wrong server. Set `POSTGRES_EXTERNAL_PORT=5433` in `.env` and `docker compose up -d postgres`. |
| `/zones` returns `"zones": []` with a note | No window has closed yet. The first one closes ~12 real seconds after the speed layer starts, and only if the producer is running. |
| API returns 503 | The serving database is unreachable — check `docker compose ps postgres` and the port note above. The API itself is fine; restarting it will not help. |
| API returns 500 | A query the API sends is wrong, not the database. The reason is in `docker compose logs api` as a `query_failed` line with the SQL error. |
| `speed-layer` exits with a `StreamingQueryException` about the query plan | The window size or watermark changed, which Structured Streaming cannot resume across. Clear the checkpoint — see *Restarting cleanly* above. |
| Alerts repeat for the same window | Only possible after a restart: the dedupe guard is in memory. A repeat is visible as a second `run_id` on the alert row. |
| `pipeline_runs` shows old rows as `RUNNING` | They were killed rather than stopped. The next run of the same component marks them `FAILED` with a note saying so. |
| No alerts ever fire | Expected in normal operation. The renewable rule only applies 09:00–16:00 simulated; force it with `--cloud-cover 0.95`. The overload ceiling sits above the simulated evening peak by design. |
| `bill_day` fails with `Cannot allocate memory` | The Docker VM ran out while a Spark JVM was starting inside Airflow. Not a bug — see *Memory: the one real constraint on a laptop* above. Stop the producer, or raise Docker Desktop's memory. |
| Airflow UI never comes up, but the scheduler runs the DAG | The webserver logged `No response from gunicorn master within 120 seconds` and shut itself down. Both webserver timeouts are raised to 300 s in `docker-compose.yml`; a slower machine may need more. |
| Airflow goes `unhealthy` after a billing run | A gunicorn worker was OOM-killed while the Spark JVM held memory (`Perhaps out of memory?` in the logs). `docker compose restart airflow` brings the UI back; keep the DAG paused between demos so it is not billing every five minutes. |
| A triggered DAG run never starts | The DAG is paused — it ships that way. `airflow dags unpause daily_billing`, and the queued run begins. |
| Phase 0 says `airflow running/unhealthy` | The webserver died; see above. The scheduler and the billing job are unaffected, so Phase 4 can still pass while Phase 0 fails. |
| `wait_for_tariff` fails after three minutes | No tariff file for the day being billed. Either the producer and tariff generator are not running, or you are billing a past day whose file was never written — `python -m sources.tariff_generator --sim-day YYYYMMDD` reproduces it exactly. |
| A manual DAG trigger sits in `queued` | `max_active_runs=1`, and a scheduled run is holding the slot while its sensor polls. It releases within three minutes, or clear that run in the UI. |
| `check_phase4.py` says `daily_bill` is empty | No simulated day has been billed yet. A day must *close* first — five real minutes of producer — or bill a past day with `--conf '{"sim_day": "YYYYMMDD"}'`. |
| `refusing to bill …: the simulated day is still in progress` | Working as intended: bill the previous day, or pass `--allow-open-day` to accept a partial total. |
| `N household(s) have no row in the … tariff file` | The tariff file predates a change to `grid.zones`. Regenerate it for that day; the job writes nothing rather than billing part of a zone. |
| Hand calculation disagrees by more than a cent | Not rounding. The script and the batch job read different data or different rules — check that `config.yaml`'s `billing` section has not changed since the day was billed. |
| `check_phase6.py`: producer `down (... connection refused)` | The producer is not running, or was started before Phase 6 and has no metrics endpoint. Restart it; the log shows `metrics_endpoint url=http://localhost:9101/metrics`. |
| Producer target down with a timeout rather than `connection refused` | Windows Firewall is blocking Docker from reaching Python on port 9101. Allow `python.exe` on private networks when prompted, or add an inbound rule for TCP 9101. |
| `OSError: address already in use` from the producer | Another producer is already running and owns port 9101. Only one should run; stop the other. |
| A Grafana panel says "No data" | Run `python scripts\check_phase5.py`: it names the panel and the query error. Live charts are empty if the speed layer has not written in the selected time range; billing panels are empty until a day is billed. |
| Grafana logs `level=error` about `provisioning/plugins`, `provisioning/alerting` or `xychart` at startup | Harmless: those provisioning folders are optional, and the `xychart` line is Grafana 11.3 noise. The lines that matter are `inserting datasource` and `finished to provision dashboards`. |
| The Lambda cross-check shows `no windows` | The speed layer was not running during that simulated day. It is informational only; the batch figure is the authoritative one either way. |

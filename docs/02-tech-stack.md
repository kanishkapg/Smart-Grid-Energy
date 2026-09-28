# Technology stack justification

Every component below is either mandated by the brief or earns its place by
doing a job UC3 actually requires. Nothing is included to look impressive.

| Layer | Choice | Job it does here | Why this and not the alternative |
|---|---|---|---|
| Streaming source | **Python** producer | Emits meter readings every 2 real seconds, keyed by `household_id` | Brief mandates simulated Python sources. `kafka-python` over `confluent-kafka` because it is pure Python — no C toolchain needed on the Windows dev machines this is built on. |
| Batch source | **Python** generator | Writes one tariff CSV per simulated day into `tariff/` | Brief mandates a once-per-day file drop. Keeping it a file is the whole basis of ADR-001. |
| Ingestion | **Apache Kafka, KRaft mode** | Buffered, partitioned, replayable event intake | Required by the brief. Decouples producer cadence from Spark's processing rate, so a slow consumer applies back-pressure instead of dropping data. KRaft removes the Zookeeper container — one less moving part to explain and to keep healthy. |
| Master store | **SeaweedFS (S3 API) + Parquet** | Append-only raw events, partitioned `sim_day / grid_zone` | Lambda's batch layer is defined by recomputing from immutable raw data, so this is load-bearing, not storage for its own sake. Parquet because the batch job reads whole columns over a day partition — columnar, with predicate pushdown on the partition keys. SeaweedFS because it serves the S3 API locally, so the code is identical against real S3. See the note below on why not MinIO. |
| Stream processing | **Spark Structured Streaming** | Event-time tumbling windows, zone aggregation, alert rules | Required. Crucially it gives **event-time** windowing with watermarks: readings arrive out of order under a compressed clock, and processing-time windows would smear them across the wrong buckets. |
| Batch processing | **Spark (batch)** | Daily `consumption ⨝ tariff` → billing | Reusing Spark is what makes the shared `processing/common/` library possible — the same aggregation function runs in both layers rather than being written twice in two dialects. |
| Orchestration | **Apache Airflow** | Schedules, sensors, retries and monitors the billing DAG | Required. Gives the batch layer real orchestration: a `FileSensor` on the tariff file (robust against clock drift, unlike a bare cron), retries, and a run history that makes the idempotent upsert design meaningful. |
| Serving DB | **PostgreSQL** | `zone_metrics`, `daily_bill`, `alerts`, `pipeline_runs` | Both layers need a single queryable serving contract. Relational because billing is relational and needs exact numeric aggregation; also a native Grafana data source, and `ON CONFLICT` upserts give us idempotency in the schema rather than in application code. |
| API | **FastAPI** | `/zones`, `/zones/{zone}/load`, `/alerts`, `/health`, `/metrics` | Brief explicitly asks for a metrics API. Async (the endpoints are IO-bound on Postgres), and auto-generated OpenAPI docs are a free demo asset. |
| Dashboards + observability | **Grafana** (+ **Prometheus**) | Business dashboards *and* ops metrics/alerting | One visualisation tool covering the live monitoring dashboard, the billing view **and** the observability dashboard keeps the component count down. Prometheus for pull-based metrics (events/s, consumer lag, batch duration, error counts) and Grafana alerting for the health-check rule. |
| Logging | **structlog** | JSON log lines carrying a `run_id` correlation id | Rubric wants structured logging and traceability. The same `run_id` is written into `pipeline_runs` and onto `daily_bill` rows, so any serving-layer number can be traced to the exact pipeline execution that produced it. |
| Packaging | **Docker Compose** | One-command reproducible stack | Brief strongly recommends it, and a clean-clone `docker compose up` is what the Code Quality criterion rewards. Image tags are pinned in `.env.example` so the stack is reproducible rather than "whatever `latest` was that day". |

## Why not MinIO — and why that turned out not to matter

The project plan names MinIO for object storage, and it is the usual choice. It
is no longer usable here: MinIO has withdrawn its public container images, so
neither `minio/minio` on Docker Hub (the repository returns *object not found*)
nor `quay.io/minio/minio` (401 Unauthorized) can be pulled without a licence.
`bitnami/minio` is gone for the same commercial reason.

Replacement candidates, and why SeaweedFS won:

| Candidate | Verdict |
|---|---|
| **SeaweedFS** | **Chosen.** A real distributed object store with an S3 gateway, used in production for exactly this kind of workload. `weed server -s3 -filer` runs the whole thing in one container, and it ships a web UI for browsing buckets. |
| LocalStack | Works, but it is an AWS *emulator* aimed at testing, and ~1 GB for the one service we need. |
| adobe/s3mock | Works, but it is explicitly a mock. Describing a mock as the master dataset store of a data platform would be dishonest. |
| A plain mounted volume | The plan's sanctioned fallback, and the simplest option — but it drops the S3 API the brief asks for, and with it the claim that this code would run unchanged against real S3. |

The useful point for the report: **swapping the storage provider was a change to
`docker-compose.yml` and one set of environment variable names.** No pipeline
code changed, because the code speaks the S3 API rather than a vendor's SDK.
That is the practical payoff of coding to a standard interface, and the
environment variables are named `S3_*` rather than `MINIO_*` to keep it that way.

One consequence worth knowing: SeaweedFS's S3 gateway accepts *any* access key
unless it is given an identity config, which would have made the credentials in
`.env` decorative. `infra/seaweedfs/s3.json` defines a real identity, so the
producers and Spark jobs authenticate the same way they would against S3.

## Minimal-viable fallback

If RAM or time run out, the following degradations keep every rubric criterion
satisfied, and the trade-off is documented in the report rather than hidden:

| Drop | Replace with | Criterion still met because |
|---|---|---|
| SeaweedFS | mounted `./data/raw` volume, same Parquet layout | the master dataset is still immutable and partitioned; only the access protocol changes |
| Prometheus + Grafana | JSON logs + FastAPI `/health` and `/metrics` + one in-code alert rule | Observability asks for logs, metrics and one working alert — not a full SRE stack |
| Airflow | a loop script invoking the same batch job | weakest substitution; loses sensors, retries and run history, so this is the last thing to cut |

## Notable version pins and why

| Pin | Reason |
|---|---|
| `apache/kafka:3.9.0` | Official image with first-class KRaft support via `KAFKA_*` env vars. |
| `postgres:16-alpine` | Small image; 16 is current-stable and Grafana-compatible. |
| Spark ↔ Kafka ↔ Postgres jars | Pinned together in the Spark image (Phase 2): mismatched `spark-sql-kafka` / Scala / JDBC versions are the single most common failure in this kind of stack. |
| `kafka-python` (pure Python) | Avoids needing a C compiler for `librdkafka` on Windows. |

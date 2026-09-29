# Architecture diagrams

## 1. System architecture (Lambda)

```mermaid
flowchart LR
  subgraph SRC["Simulated sources (Python)"]
    S["stream_producer.py<br/>meter event every 2s"]
    B["tariff_generator.py<br/>one CSV per sim-day"]
  end

  S -->|"produce, key=household_id"| K[["Kafka<br/>meter.readings (3 partitions)"]]
  B -->|"drop file"| M[("Object storage<br/>tariff/")]

  subgraph SPEED["SPEED LAYER — seconds, approximate"]
    K --> SS["Spark Structured Streaming<br/>event-time tumbling window 60 sim-min<br/>watermark 30 sim-min<br/>zone aggregation + alert rules"]
  end

  subgraph BATCH["BATCH LAYER — daily, exact, re-runnable"]
    K -->|"raw sink"| RAW[("Mounted volume / Parquet<br/>data/raw/meter_readings/<br/>sim_day=../grid_zone=..<br/>IMMUTABLE MASTER DATASET")]
    AF["Airflow DAG<br/>FileSensor on tariff file"] --> BJ
    RAW --> BJ["Spark batch<br/>consumption ⨝ tariff<br/>net metering + subsidy"]
    M --> BJ
  end

  SS --> PG[("PostgreSQL<br/>SERVING LAYER")]
  BJ --> PG
  BJ --> RPT[("Object storage<br/>reports/")]

  PG --> API["FastAPI<br/>/zones /alerts /health /metrics"]
  PG --> G["Grafana<br/>live monitoring + billing dashboards"]
  SS -. "metrics" .-> P["Prometheus"]
  API -. "metrics" .-> P
  P --> G

  classDef speed fill:#fff4e6,stroke:#e8891c
  classDef batch fill:#e8f4fd,stroke:#1c7ed6
  classDef serve fill:#e9f7ef,stroke:#2b8a3e
  class SS speed
  class RAW,BJ,AF batch
  class PG,API,G serve
```

**Pipeline stages:** generate → ingest (Kafka + file drop) → process (stream + daily batch)
→ store (Parquet master + Postgres serving) → analyse (windowed metrics + billing
reconciliation) → serve (API + Grafana) → observe (Prometheus/Grafana + JSON logs).

## 2. Where the two layers diverge — and where they don't

```mermaid
flowchart TB
  CM["processing/shared/<br/>schemas.py · spark_session.py · aggregations.py · billing.py<br/><b>ONE implementation of the business rules</b>"]
  CM --> RS["raw_sink.py<br/>trigger: micro-batch every 15s<br/>source: Kafka<br/>sink: Parquet (append)"]
  CM --> SP["speed/zone_metrics_job.py<br/>trigger: micro-batch every 5s<br/>source: Kafka<br/>sink: zone_metrics (upsert)"]
  CM --> BT["batch/billing_job.py<br/>trigger: Airflow, per sim-day<br/>source: Parquet + tariff CSV<br/>sink: daily_bill (upsert)"]
  classDef shared fill:#f3f0ff,stroke:#7048e8,stroke-width:2px
  class CM shared
```

> Named `shared`, not `common`: `spark-submit` puts the submitted script's own
> directory first on `sys.path`, so a `processing/common/` package would shadow
> the project's top-level `common/` and every job would fail to import its config.

This is the concrete answer to Lambda's standard criticism. The duplicated part
is only the **trigger and the sink** — which *should* differ between a
low-latency approximate path and an exact daily recompute. The
transformation logic itself is imported, not re-implemented.

## 3. Simulated clock mapping

One simulated day is compressed into 5 real minutes, a factor of **288×**.

| Event | Real time | Simulated time |
|---|---|---|
| Producer starts | 0 s | 00:00 |
| Each meter emits | every 2 s | every 9.6 min |
| One speed-layer window closes | every 12.5 s | every 60 min |
| Sim-day boundary: tariff file drops, billing DAG fires | every 300 s | every 24 h |

The 9.6-simulated-minute reading gap is why the window is 60 simulated minutes
rather than 1: a 1-minute window would be narrower than the gap between two
readings and would mostly be empty. At 60 minutes each window holds
60 / 9.6 ≈ 6 readings per meter — enough to be a genuine aggregate.

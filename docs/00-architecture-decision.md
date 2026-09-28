# ADR-001 — Lambda over Kappa for Smart Grid Monitoring & Billing

**Status:** Accepted · **Date:** 2026-09-28 · **Use case:** UC3 — Smart Grid Energy Monitoring & Billing

---

## 1. Context

UC3 asks one system to answer two questions that behave nothing alike:

- *Right now* — what is grid load and rooftop-solar contribution **per zone**?
- *Once per day* — what does **each household** owe, after that day's published tariff is applied to its net consumption?

The two paths disagree on every axis that matters when choosing an architecture:

| Requirement | Real-time monitoring | Daily billing |
|---|---|---|
| Latency budget | Seconds | Once per simulated day |
| Accuracy | Approximate is fine — a dropped reading barely moves a zone average | Exact, complete, reproducible — it is money |
| Input shape | High-velocity meter event stream | A slow reference **file** (one tariff CSV per day) |
| Consistency model | Eventual | Strong, auditable, re-runnable |
| Cost of being wrong | A dashboard number is briefly off | A customer is mis-billed |

A single processing model would have to serve both, and whichever one we picked
would be the wrong tool for the other half of the workload.

## 2. Decision

**Adopt the Lambda architecture**, with Spark as the shared compute engine on
both branches.

| Layer | Implementation | Responsibility |
|---|---|---|
| **Speed layer** | Spark Structured Streaming ← Kafka | Event-time windowed zone metrics + threshold alerts, seconds of latency, at-least-once |
| **Batch layer** | Spark batch, orchestrated by Airflow | Recompute the day's billing from the **immutable raw Parquet store** joined to that day's tariff file; exact and idempotent |
| **Serving layer** | PostgreSQL → FastAPI + Grafana | One queryable surface for both branches |

The master dataset is append-only Parquet in S3-compatible object storage, partitioned by
`sim_day / grid_zone`. Kafka is treated as a *transport buffer*, not the system
of record — which is why it is not persisted to a volume in `docker-compose.yml`.

## 3. Why this fits UC3 specifically

1. **The tariff genuinely is a file.** The brief specifies a once-per-day tariff
   drop. Lambda lets it stay a file, landed in object storage and read by the
   batch job. Forcing a daily 40-row CSV onto a Kafka topic would be
   architecture-driven-by-tooling.
2. **Billing has a natural batch boundary and strong-accuracy needs.**
   Recomputing a completed day from immutable raw data is the textbook case for
   a batch layer: it is deterministic, auditable, and re-runnable after a bug
   fix without replaying anything.
3. **Airflow gets real work.** Orchestrating the nightly billing DAG —
   sensing the tariff file, running the Spark job, upserting the serving table —
   is a legitimate orchestration problem, not a decorative one.
4. **Idempotency is designed in, not bolted on.** `daily_bill` is keyed on
   `(sim_day, household_id)`, so a retried DAG run upserts instead of
   double-billing. That is only clean because the batch layer owns billing
   outright.

## 4. Rejected alternative — Kappa

Kappa treats *everything* as a stream and reprocesses history by replaying the
Kafka log, so there is only one codebase and one processing model. That is a
genuinely strong design, and it is the better choice when all sources are
streams and reprocessing is frequent.

We rejected it here because:

- **(a)** The tariff is a clean daily file; publishing it to Kafka purely to
  satisfy the model adds a hop and a failure mode for no benefit.
- **(b)** Billing wants a recompute over a *closed* day with strong accuracy —
  natural batch semantics. Expressing "exactly this day's readings, joined to
  exactly this day's tariff, exactly once" as a stream needs careful state and
  trigger work to reach what a batch job gets for free.
- **(c)** Replay-based reprocessing means retaining the full event log in Kafka.
  Parquet in object storage is cheaper and directly queryable.
- **(d)** Airflow would shrink to triggering a stream restart, which does not
  represent orchestration honestly.

## 5. Accepted cost, and how it is mitigated

Lambda's known weakness is **two code paths to maintain**, with the risk that
streaming and batch logic drift and produce different numbers from the same
input.

Mitigation, implemented rather than asserted: the consumption/solar aggregation
and the schema definitions live once in `processing/common/`, and are imported
by **both** the streaming job and the batch job. Because Spark serves both
layers, the shared code is genuine shared code — the same functions, not two
translations of one rule. What differs between the layers is only the
*trigger and the sink*, which is exactly the part that should differ.

Residual risk accepted: the speed layer may briefly show figures that the batch
layer later restates. This is correct Lambda behaviour — the batch layer is
authoritative — and is surfaced to the user rather than hidden: the dashboard
labels live figures as provisional.

## 6. Consequences

- Two pipelines, one engine (Spark) and one shared transformation library.
- Object storage becomes load-bearing: the batch layer cannot recompute without it.
- Postgres is the single serving contract; API and dashboards never read Kafka
  or Parquet directly.
- Kafka retention can stay short (24 h), because durability is Parquet's job.

---

*Diagram: see `docs/01-architecture-diagram.md`. Component-by-component tech
stack justification: see `docs/02-tech-stack.md`.*

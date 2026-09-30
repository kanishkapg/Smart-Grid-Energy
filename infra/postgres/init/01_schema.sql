-- ===========================================================================
--  Serving-layer schema (PostgreSQL)
--
--  Written by both Lambda layers:
--    speed layer (Spark Structured Streaming) -> zone_metrics, alerts
--    batch layer (Spark batch, Airflow-driven) -> daily_bill
--
--  Read by: FastAPI (real-time API) and Grafana (dashboards).
--  Applied automatically on the first `docker compose up` of the postgres
--  service via /docker-entrypoint-initdb.d.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Speed layer output: one row per (event-time window, grid zone).
--
-- The primary key is the window + zone so that a re-delivered micro-batch
-- upserts rather than duplicating. Spark's streaming sink is at-least-once,
-- so idempotency has to live in the schema.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS zone_metrics (
    window_start          TIMESTAMPTZ      NOT NULL,
    window_end            TIMESTAMPTZ      NOT NULL,
    grid_zone             TEXT             NOT NULL,
    total_consumption_kwh DOUBLE PRECISION NOT NULL,
    total_solar_kwh       DOUBLE PRECISION NOT NULL,
    renewable_pct         DOUBLE PRECISION NOT NULL,
    active_meters         INTEGER          NOT NULL,
    updated_at            TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CONSTRAINT pk_zone_metrics PRIMARY KEY (window_start, grid_zone)
);

-- Grafana and the API both query "latest window, all zones" and
-- "one zone over time" -- this index serves both.
CREATE INDEX IF NOT EXISTS ix_zone_metrics_zone_time
    ON zone_metrics (grid_zone, window_start DESC);

-- ---------------------------------------------------------------------------
-- Batch layer output: one authoritative bill per household per simulated day.
--
-- PK (sim_day, household_id) is what makes the nightly billing DAG safe to
-- re-run: a retry upserts the same day's rows instead of double-billing.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_bill (
    sim_day               DATE             NOT NULL,
    household_id          TEXT             NOT NULL,
    grid_zone             TEXT             NOT NULL,
    total_consumption_kwh DOUBLE PRECISION NOT NULL,
    total_solar_kwh       DOUBLE PRECISION NOT NULL,
    net_kwh               DOUBLE PRECISION NOT NULL,
    tariff_rate           DOUBLE PRECISION NOT NULL,
    billing_tier          TEXT             NOT NULL,
    subsidy_flag          BOOLEAN          NOT NULL,
    gross_amount          DOUBLE PRECISION NOT NULL,
    subsidy_amount        DOUBLE PRECISION NOT NULL,
    net_amount            DOUBLE PRECISION NOT NULL,
    run_id                TEXT,            -- correlation id of the batch run that wrote this
    generated_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CONSTRAINT pk_daily_bill PRIMARY KEY (sim_day, household_id)
);

CREATE INDEX IF NOT EXISTS ix_daily_bill_day_zone
    ON daily_bill (sim_day DESC, grid_zone);

-- ---------------------------------------------------------------------------
-- Business alerts raised by the speed layer (threshold breaches). The Phase 6
-- health checks ("no meter data" etc.) are Prometheus rules instead: they fire
-- on the *absence* of data, which no stream job can observe from inside.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alerts (
    alert_id   BIGSERIAL   PRIMARY KEY,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    level      TEXT        NOT NULL CHECK (level IN ('INFO', 'WARN', 'CRITICAL')),
    grid_zone  TEXT,
    rule       TEXT        NOT NULL,
    message    TEXT        NOT NULL,
    run_id     TEXT
);

CREATE INDEX IF NOT EXISTS ix_alerts_ts ON alerts (ts DESC);

-- ---------------------------------------------------------------------------
-- Observability / traceability (rubric: structured logging + correlation ids).
-- Every producer, streaming job and batch run registers itself here with the
-- same run_id it stamps onto its JSON log lines, so a row in daily_bill can be
-- traced back to the exact pipeline execution that produced it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       TEXT        PRIMARY KEY,
    component    TEXT        NOT NULL,   -- e.g. stream_producer, speed_layer, billing_batch
    status       TEXT        NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    sim_day      DATE,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    rows_written BIGINT,
    notes        TEXT
);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_started ON pipeline_runs (started_at DESC);

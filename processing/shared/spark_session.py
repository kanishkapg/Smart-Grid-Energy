"""One place that knows how to build a SparkSession for this project.

Both layers need the same S3A wiring and the same session time zone, and getting
either wrong produces wrong numbers rather than an error -- so it is configured
once here instead of being copied into each job.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark_session(cfg, app_name: str, shuffle_partitions: int = 4) -> SparkSession:
    """A SparkSession configured for this project's object storage and clock."""
    access_key, secret_key = cfg.s3_credentials

    builder = (
        SparkSession.builder.appName(app_name)

        # --- the simulated clock ------------------------------------------
        # Every date and window boundary is resolved in the simulation's zone.
        # Left at UTC, `to_date` would file a reading taken at 02:00 Colombo time
        # under the previous simulated day and both days' bills would be wrong.
        .config("spark.sql.session.timeZone", cfg.simulation["timezone"])

        # --- S3A: talking to SeaweedFS as if it were S3 --------------------
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        # Virtual-host style would resolve `raw.seaweedfs`, which does not exist.
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # SeaweedFS has no IAM/EC2 metadata service; without pinning the provider
        # S3A spends its startup timeout probing for one before falling back.
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Spark only ever READS from S3 in this project -- the daily tariff CSV.
        # The Parquet master store is written to a mounted volume, because every
        # way of committing a Spark write to SeaweedFS failed: three separate
        # defects, all in the commit path, documented in docs/02-tech-stack.md.
        # Reads involve no commit protocol, so they work without special config.

        # --- sized for a laptop -------------------------------------------
        # The default 200 shuffle partitions would emit 200 tiny files per batch.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.streaming.metricsEnabled", "true")
    )

    spark = builder.getOrCreate()
    # WARN keeps the container logs readable during a demo; the job's own
    # structured JSON logs carry the information that matters.
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kafka_source_options(cfg, starting_offsets: str = "earliest") -> dict:
    """Reader options shared by the raw sink and the speed layer."""
    return {
        "kafka.bootstrap.servers": cfg.kafka_bootstrap,
        "subscribe": cfg.kafka_topic,
        "startingOffsets": starting_offsets,
        # Without this, a topic whose retention has expired mid-run kills the
        # query instead of skipping to the earliest surviving offset.
        "failOnDataLoss": "false",
    }

# ===========================================================================
#  Airflow image for this project: the official image plus a JRE and PySpark,
#  so the billing DAG can run the batch job itself.
#
#  Why Spark lives *in* the Airflow image rather than being launched in the
#  smartgrid/spark container:
#
#    The textbook arrangement is DockerOperator -- Airflow asks the Docker daemon
#    to start the Spark image. That needs the daemon's socket mounted into this
#    container, the container running as root to use it, and the DAG to name
#    *host* paths for every mount, because the daemon resolves them on the host
#    and not in here. Three environment-specific dependencies for a job that runs
#    in local mode either way.
#
#    So the batch job runs as a spark-submit subprocess of the Airflow task, over
#    exactly the same mounted code and data the other containers see. What is
#    being demonstrated -- a scheduled, retryable, idempotent batch layer -- is
#    unchanged, and `docker compose logs airflow` shows the Spark job's own
#    structured logs. A real deployment would submit to a cluster instead, which
#    is one operator swap, not a redesign.
#
#  Python 3.11, not the default 3.8 or the newest 3.12: PySpark 3.5.x is not
#  supported on 3.12, and 3.8 lacks `zoneinfo`, which the simulated clock needs.
# ===========================================================================
FROM apache/airflow:2.10.3-python3.11

USER root

# Spark needs a JVM; Spark 3.5 supports Java 8, 11 and 17. `procps` is for
# spark-submit's own process handling, and is absent from the slim base.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Same versions as the Spark container and requirements.txt: this image runs the
# same job files, so a version split here would mean the DAG tests something
# other than what `docker compose run` does.
#   pyspark  -- the batch job, in local mode
#   psycopg  -- serving-layer writes (processing/shared/pg.py) and the DAG's
#               verification query
#   structlog, PyYAML -- the project's logging and config
#   boto3    -- the tariff sensor and the report drop
RUN pip install --no-cache-dir \
      pyspark==3.5.3 \
      "psycopg[binary]==3.2.3" \
      structlog==24.4.0 \
      PyYAML==6.0.2 \
      boto3==1.35.36

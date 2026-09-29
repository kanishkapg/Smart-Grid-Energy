# ===========================================================================
#  Spark image for this project: the base image plus the connector jars and the
#  two Python packages the jobs import.
#
#  Why bake the jars in rather than use `spark-submit --packages`:
#    * --packages resolves from Maven on every container start, so the stack
#      needs internet and the first run of each job stalls for minutes.
#    * The project plan calls out "Spark <-> Kafka/Postgres jar version
#      mismatches" as a top pitfall. Pinning them here, as build arguments,
#      makes the versions explicit and the image reproducible.
#
#  Version compatibility, which is the whole point of this file:
#    Spark 3.5.3 is built against Scala 2.12       -> _2.12 artifacts
#    Spark 3.5.3 bundles hadoop-client 3.3.4       -> hadoop-aws MUST be 3.3.4
#    hadoop-aws 3.3.4 is built against AWS SDK v1  -> aws-java-sdk-bundle 1.12.x
#    Spark 3.5.3's Kafka connector needs           -> kafka-clients 3.4.1
#                                                     + commons-pool2 (absent
#                                                       from the base image)
# ===========================================================================
FROM apache/spark:3.5.3

USER root

ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

# The base image ships Python 3.8, so nothing here may use newer syntax.
# Pinned to the same versions as the host's requirements.txt.
RUN pip3 install --no-cache-dir PyYAML==6.0.2 structlog==24.4.0

ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eu; cd /opt/spark/jars; \
    for url in \
      "$MAVEN/org/apache/spark/spark-sql-kafka-0-10_$SCALA_VERSION/$SPARK_VERSION/spark-sql-kafka-0-10_$SCALA_VERSION-$SPARK_VERSION.jar" \
      "$MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_$SCALA_VERSION/$SPARK_VERSION/spark-token-provider-kafka-0-10_$SCALA_VERSION-$SPARK_VERSION.jar" \
      "$MAVEN/org/apache/kafka/kafka-clients/$KAFKA_CLIENTS_VERSION/kafka-clients-$KAFKA_CLIENTS_VERSION.jar" \
      "$MAVEN/org/apache/commons/commons-pool2/$COMMONS_POOL2_VERSION/commons-pool2-$COMMONS_POOL2_VERSION.jar" \
      "$MAVEN/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VERSION/hadoop-aws-$HADOOP_AWS_VERSION.jar" \
      "$MAVEN/com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_VERSION/aws-java-sdk-bundle-$AWS_SDK_VERSION.jar" \
    ; do echo "fetching $(basename "$url")"; curl -fsSLO "$url"; done

# The S3A "magic" committer, which commits by completing a multipart upload
# instead of renaming from a _temporary directory. Object stores have no rename,
# and SeaweedFS's copy-based emulation of it fails outright on the partitioned
# paths this job writes. In its own layer so adding it does not re-download the
# large AWS bundle above.
RUN cd /opt/spark/jars && \
    curl -fsSLO "$MAVEN/org/apache/spark/spark-hadoop-cloud_$SCALA_VERSION/$SPARK_VERSION/spark-hadoop-cloud_$SCALA_VERSION-$SPARK_VERSION.jar"

# Phase 3: both Spark layers write to the serving database through psycopg
# rather than through Spark's JDBC writer, because every write has to be an
# idempotent upsert and JDBC cannot express one -- reasoning in
# processing/shared/pg.py. The binary wheel bundles libpq, so no apt packages are
# needed.
#
# Deliberately below the jars: a layer added above them would invalidate the
# cache and re-download ~250 MB on every rebuild.
# boto3 is Phase 4: the billing job fetches the day's tariff CSV and drops its
# report through the S3 API, the same client the producers use. Only 40 rows, so
# it is read with boto3 rather than by Spark over S3A -- which would mean a
# 250 MB AWS SDK on the read path for a 2 KB file.
RUN pip3 install --no-cache-dir \
      "psycopg[binary]==3.2.3" \
      "backports.zoneinfo==0.2.1" \
      "boto3==1.35.36"

# Structured Streaming checkpoints live on a docker volume, not in object
# storage: checkpointing needs atomic rename, which S3 only emulates by copying.
# Creating the directory here with the spark user's ownership is what lets the
# non-root container write to a freshly created named volume.
RUN mkdir -p /checkpoints && chown -R 185:185 /checkpoints

USER 185

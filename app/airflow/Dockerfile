# Stage 1: use Spark image as a source for Spark binaries and Java. Airflow needs spark submit
FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu AS spark-base

# Stage 2: final Airflow image
FROM apache/airflow:3.0.0

# Switch to root to copy system files
USER root

# Copy Spark distribution (includes spark-submit binary)
COPY --from=spark-base /opt/spark /opt/spark

# Copy Java runtime (required by spark-submit)
COPY --from=spark-base /opt/java /opt/java

# Kafka connector and its dependencies
RUN curl -fLo /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar && \
    curl -fLo /opt/spark/jars/kafka-clients-3.9.0.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar && \
    curl -fLo /opt/spark/jars/commons-pool2-2.12.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar && \
    curl -fLo /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar && \
    # PostgreSQL JDBC driver
    curl -fLo /opt/spark/jars/postgresql-42.7.4.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar && \
    chmod 644 /opt/spark/jars/*.jar

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk

# Make spark-submit and java available in PATH
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

# Create checkpoints directory with write permissions for airflow user
RUN mkdir -p /opt/spark/checkpoints && \
    chown -R 50000:50000 /opt/spark/checkpoints && \
    chmod 755 /opt/spark/checkpoints

# Switch back to Airflow user (required for pip install)
USER airflow

# Install Airflow providers
# common-compat: compatibility layer for older provider APIs
# apache-spark: provides SparkSubmitOperator and SparkSubmitHook
RUN pip install --no-cache-dir --no-deps \
    "apache-airflow-providers-common-compat==1.12.0" \
    "apache-airflow-providers-apache-spark==6.3.1"


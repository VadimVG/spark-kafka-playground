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

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk

# Make spark-submit and java available in PATH
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

# Switch back to Airflow user (required for pip install)
USER airflow

# Install Airflow providers
# common-compat: compatibility layer for older provider APIs
# apache-spark: provides SparkSubmitOperator and SparkSubmitHook
RUN pip install --no-cache-dir --no-deps \
    "apache-airflow-providers-common-compat==1.12.0" \
    "apache-airflow-providers-apache-spark==6.3.1"


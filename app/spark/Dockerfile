FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu

USER root

# Kafka connector and its dependencies
RUN wget -P /opt/spark/jars \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar && \
    wget -P /opt/spark/jars \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar && \
    wget -P /opt/spark/jars \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar && \
    wget -P /opt/spark/jars \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar && \
    # PostgreSQL JDBC driver
    wget -P /opt/spark/jars \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar && \
    chmod 644 /opt/spark/jars/*.jar

RUN pip install --no-cache-dir psycopg2-binary==2.9.11

# Create checkpoints directory with write permissions for airflow user
RUN mkdir -p /opt/spark/checkpoints && \
    chown -R spark:spark /opt/spark/checkpoints && \
    chmod 755 /opt/spark/checkpoints

USER spark
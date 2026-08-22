# SPARK-KAFKA-PLAYGROUND

## Description
End-to-end data pipeline that simulates an e-commerce event stream, processes it in near real-time with Spark, and orchestrates everything with Airflow.

The project mimics a production setup:
- **event-simulator** generates fake but realistic events (sales, orders, logs) and sends them to Kafka.
- **Kafka** is a message broker. It receives events from producers and stores them until consumers are ready to read. If a consumer is down or slow, messages just wait in Kafka; nothing is lost.
- **Spark** consumes streams from Kafka, processes them, and writes results to PostgreSQL.
- **Airflow** orchestrates Spark jobs — schedules and monitors their execution.
- **PostgreSQL** stores processed data for analytics (external service).

## Quick Start

```bash
# Create new network for external services
docker network create data-platform-net

# Build and start all services
docker compose up --build -d
```

## Services

### 1. Event simulator
Generates fake e-commerce events (sales, orders, logs) and sends them to Kafka.

See [event-simulator/README.md](event-simulator/README.md) for details.

### 2. Zookeeper
Cluster coordinator for Kafka. Manages broker metadata, leader election, and health checks.
- Image: confluentinc/cp-zookeeper:7.8.0
- Port: 2181
- Note: Required by Kafka for cluster management. Kafka is gradually moving away from Zookeeper (KRaft mode), but it remains the standard in most production setups.

### 3. Kafka
Message broker that receives events from the simulator and makes them available for downstream consumers (Spark, Airflow).
- Image: confluentinc/cp-kafka:7.8.0
- Port: 9092 (internal Docker network)
- Auto-create topics: enabled — topics are created automatically on first message

#### Topics
- sales - Completed purchases
- orders - Order lifecycle events
- logs - User activity (page actions)

#### Useful commands
```bash
# List all topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume from a single topic
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sales --from-beginning

# Consume from all topics
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --whitelist "sales|orders|logs" --from-beginning
```

### 4. Airflow (standalone)
Orchestrator for Spark jobs. Manages DAGs, schedules, and monitors execution.

- **Image:** `apache/airflow:3.0.0`
- **Port:** `8080` (Web UI)
- **Executor:** `LocalExecutor`
- **Example DAGs:** disabled

#### Credentials
On first startup, Airflow standalone generates a random admin password. Retrieve it with:

```bash
docker compose logs airflow | grep -i "password\|user\|admin"
```

#### Access
Open http://localhost:8080, login with username admin and the password from the command above.

#### DAGs
Place DAG files in app/airflow/dags/. They are mounted into the container and picked up automatically.

#### Useful commands
```bash
# Show all airflow providers
docker compose exec airflow airflow providers list

# Spark submit version
docker compose exec airflow spark-submit --version
```


### 5. Spark
Distributed data processing engine. Handles streaming from Kafka and batch transformations.

- **Image:** `apache/spark:4.0.1-scala2.13-java17-python3-ubuntu`
- **Master UI:** `http://localhost:8081`
- **Worker UI:** `http://localhost:8082`
- **Master port:** `7077` (for job submission)

#### Job Structure

Spark jobs are mounted from `app/spark/jobs/` into both master and worker containers at `/opt/spark/jobs/`.

#### Useful Commands

```bash
# Test Spark with a sample job
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/test_job.py

# Open Spark shell (interactive PySpark)
docker compose exec -it spark-master /opt/spark/bin/pyspark
```



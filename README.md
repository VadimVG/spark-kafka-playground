# SPARK-KAFKA-PLAYGROUND

## Description


## Quick Start

```bash
# Build and start all services
docker compose up --build -d

# Create new network for external services
docker network create data-platform-net
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



# SPARK-KAFKA-PLAYGROUND
 
## 📖 Description
 
This project is an end-to-end data pipeline. It works like a small e-commerce system. The pipeline simulates events (like sales and orders), processes them in near real-time with Spark, and uses Airflow to manage everything.
 
The project copies a real production setup:
- **Event-simulator** creates fake but realistic events (sales, orders, logs) and sends them to Kafka.
- **Kafka** is a message broker. It gets events from the simulator and stores them until a consumer is ready to read them. If a consumer is slow or down, the messages just wait in Kafka. Nothing is lost.
- **Spark** reads the streams from Kafka, processes them, and saves the results in PostgreSQL.
- **Airflow** manages the Spark jobs. It schedules them and checks that they run correctly.
- **PostgreSQL** stores the processed data for analytics (an external service).
## 🛠️ Technologies
 
- **Kafka** – message broker between the event simulator and Spark
- **Spark** – processes the event streams and writes results to the database
- **Airflow** – schedules and monitors the Spark jobs
- **PostgreSQL** – stores the processed data
- **Docker** – runs all services in containers
## ⚙️ Installation
 
```bash
# Create a network for external services
docker network create data-platform-net
 
# Build and start all services
docker compose up --build -d
```
 
## 🧩 Components / Services
 
### 1. Event simulator
Creates fake e-commerce events (sales, orders, logs) and sends them to Kafka.
 
See [event-simulator/README.md](event-simulator/README.md) for more details.
 
### 2. Zookeeper
Manages the Kafka cluster: broker info, leader election, and health checks.
- **Image:** `confluentinc/cp-zookeeper:7.8.0`
- **Port:** `2181`
- **Note:** Kafka still needs Zookeeper for cluster management in this setup. Newer Kafka versions can run without it (KRaft mode), but Zookeeper is still common in production.
### 3. Kafka
Gets events from the simulator and makes them ready for Spark and Airflow to read.
- **Image:** `confluentinc/cp-kafka:7.8.0`
- **Port:** `9092` (inside the Docker network)
- **Auto-create topics:** on — topics appear on their own after the first message
**Topics:**
- `sales` – completed purchases
- `orders` – order status updates
- `logs` – user activity (page views, clicks)
**Useful commands:**
```bash
# List all topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
 
# Read messages from one topic
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sales --from-beginning
 
# Read messages from all topics
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --whitelist "sales|orders|logs" --from-beginning
```
 
### 4. Airflow (standalone)
Manages the Spark jobs. Runs the DAGs on a schedule and checks their status.
- **Image:** `apache/airflow:3.0.0-python3.10`
- **Port:** `8080` (Web UI)
- **Executor:** `LocalExecutor`
- **Example DAGs:** off
**Login:**
On the first start, Airflow makes a random admin password. Get it with:
```bash
docker compose logs airflow | grep -i "password\|user\|admin"
```
Open http://localhost:8080 and log in as `admin` with this password.
 
**DAGs:**
Put your DAG files in `app/airflow/dags/`. Airflow finds them automatically — no extra setup needed.
 
**Useful commands:**
```bash
# Show all Airflow providers
docker compose exec airflow airflow providers list
 
# Check the Spark version
docker compose exec airflow spark-submit --version
```
 
### 5. Spark
Processes the data streams from Kafka and runs batch jobs.
- **Image:** `apache/spark:4.0.1-scala2.13-java17-python3-ubuntu`
- **Master UI:** `http://localhost:8081`
- **Worker UI:** `http://localhost:8082`
- **Master port:** `7077` (for job submission)
**Job files:**
Spark jobs live in `app/spark/jobs/`. This folder is shared with both the master and worker containers, at `/opt/app/spark/jobs/`.
 
**Useful commands:**
```bash
# Run a test job
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/app/spark/jobs/test_job.py
 
# Open an interactive PySpark shell
docker compose exec -it spark-master /opt/spark/bin/pyspark
```
 
## 🚀 Launch and testing
 
1. Open the Airflow UI at http://localhost:8080 and check that your DAGs are there.
2. Trigger a `test_spark_dag` DAG run.
3. Open the Spark UI at http://localhost:8081 and check that the job is running.
4. Check PostgreSQL for the new processed data.

## 🔄 How it works
 
<!-- ![task_flow](readme_images/task_flow.png) -->
 
The flow in simple words:
1. The event simulator sends fake events to Kafka (`sales`, `orders`, `logs`).
2. Kafka stores these events and waits for a reader.
3. Spark reads the events, processes them, and saves the results to PostgreSQL.
4. Airflow schedules and watches the Spark jobs, so everything runs on time.
---
 
Happy streaming! 🚀

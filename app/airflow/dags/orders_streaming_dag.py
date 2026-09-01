from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="orders_streaming",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["streaming", "spark", "kafka"],
    description="Streaming job: Kafka orders topic -> Spark -> PostgreSQL",
)
def orders_streaming():
    submit_orders_stream = SparkSubmitOperator(
        task_id="submit_orders_stream",
        application="/opt/spark/jobs/orders_to_postgres.py",
        conn_id="spark_default",
        name="orders-to-postgres",
        verbose=True,
    )

    submit_orders_stream

orders_streaming()
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="logs_metrics_streaming",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["streaming", "spark", "kafka"],
    description="Streaming job: Kafka logs topic -> Spark aggregation -> PostgreSQL metrics",
)
def logs_metrics_streaming():

    submit_logs_metrics = SparkSubmitOperator(
        task_id="submit_logs_metrics",
        application="/opt/spark/jobs/logs_metrics_to_postgres.py",
        conn_id="spark_default",
        name="logs-metrics-to-postgres",
        verbose=True,
    )

    submit_logs_metrics


logs_metrics_streaming()
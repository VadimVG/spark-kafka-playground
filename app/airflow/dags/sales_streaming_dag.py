from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="sales_streaming",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["streaming", "spark", "kafka"],
    description="Streaming job: Kafka sales topic → Spark → PostgreSQL",
)
def sales_streaming():

    submit_sales_stream = SparkSubmitOperator(
        task_id="submit_sales_stream",
        application="/opt/spark/jobs/sales_to_postgres.py",
        conn_id="spark_default",
        name="sales-to-postgres",
        verbose=True,
    )

    submit_sales_stream


sales_streaming()
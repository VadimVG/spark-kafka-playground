from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="test_spark",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False, # launch skipped dag's
    tags=["test", "spark"],
    description="Test DAG to verify Airflow can submit Spark jobs",
)
def test_spark():

    submit_spark_job = SparkSubmitOperator(
        task_id="submit_test_job",
        application="/opt/spark/jobs/test_job.py",
        conn_id="spark_default",
        name="test-spark-job",
        verbose=True,
    )

    @task
    def check_result():
        print("Spark job finished successfully!")

    submit_spark_job >> check_result()


test_spark()
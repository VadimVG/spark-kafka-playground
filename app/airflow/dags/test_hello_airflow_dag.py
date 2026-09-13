from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="test_hello_airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual trigger only
    catchup=False,
    tags=["test"],
    description="Test DAG to verify Airflow is working",
)
def test_hello_airflow():

    @task
    def say_hello() -> str:
        print("Hello from Airflow!")
        return "done"

    @task
    def print_time(status: str) -> None:
        print(f"Status: {status}, current time: {datetime.now()}")

    print_time(say_hello())


test_hello_airflow()
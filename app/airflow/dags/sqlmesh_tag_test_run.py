import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    "sqlmesh_tag_test_run",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
):
    DockerOperator(
        task_id="sqlmesh_tag_test_run",
        image="sqlmesh:local",
        command="sqlmesh run --select-model tag:test",
        docker_url="unix://var/run/docker.sock",
        network_mode="data-platform-net",
        auto_remove="success",
        mount_tmp_dir=False,
        user=os.environ.get("UID_GID"),
        mounts=[
            Mount(
                source=f"{os.environ['HOST_PROJECT_DIR']}/app/sqlmesh",
                target="/opt/app/sqlmesh",
                type="bind",
            )
        ],
        environment={
            k: os.environ[k]
            for k in (
                "POSTGRES_HOST",
                "POSTGRES_PORT",
                "POSTGRES_USER",
                "POSTGRES_PASSWORD",
                "POSTGRES_DB",
            )
        },
    )
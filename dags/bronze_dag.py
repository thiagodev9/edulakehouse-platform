"""
DAG independente da camada Bronze.
Executado em 01:00 UTC, pode ser ativado separadamente do pipeline completo.
"""

import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)


def run_bronze(**context):
    from pipelines.bronze.ibge_pipeline import IBGEBronzePipeline
    IBGEBronzePipeline().run()


def on_task_failure(context):
    print(
        f"[BRONZE FALHA] task={context['task_instance'].task_id} | "
        f"data={context['execution_date']}"
    )


default_args = {
    "owner": "edulakehouse",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": on_task_failure,
}

with DAG(
    dag_id="ibge_bronze",
    default_args=default_args,
    description="Ingestão Bronze: JSON landing → Parquet Bronze",
    schedule_interval="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ibge", "bronze"],
) as dag:

    PythonOperator(
        task_id="run_bronze",
        python_callable=run_bronze,
    )

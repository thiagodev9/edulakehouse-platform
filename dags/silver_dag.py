"""
DAG independente da camada Silver.
Aguarda a conclusão do Bronze (ExternalTaskSensor) antes de executar.
Agendado para 02:00 UTC.
"""

import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)


def run_silver(**context):
    from pipelines.silver.ibge_silver_pipeline import IBGESilverPipeline
    IBGESilverPipeline().run()


def on_task_failure(context):
    print(
        f"[SILVER FALHA] task={context['task_instance'].task_id} | "
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
    dag_id="ibge_silver",
    default_args=default_args,
    description="Transformação Silver: Bronze Parquet → Silver Delta",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ibge", "silver"],
) as dag:

    # Aguarda o Bronze ter concluído na mesma data de execução
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="ibge_bronze",
        external_task_id="run_bronze",
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="poke",
    )

    run_silver_task = PythonOperator(
        task_id="run_silver",
        python_callable=run_silver,
    )

    wait_for_bronze >> run_silver_task

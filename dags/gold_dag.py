"""
DAG independente da camada Gold.
Aguarda a conclusão do Silver (ExternalTaskSensor) antes de executar.
Agendado para 03:00 UTC.
"""

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

log = logging.getLogger(__name__)


def run_gold(**context):
    from pipelines.gold.ibge_gold_pipeline import IBGEGoldPipeline
    IBGEGoldPipeline().run()


def on_task_failure(context):
    ti = context["task_instance"]
    log.error(
        "Task falhou | dag=%s | task=%s | execution_date=%s",
        ti.dag_id, ti.task_id, context["execution_date"],
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
    dag_id="ibge_gold",
    default_args=default_args,
    description="Agregação Gold: Silver Delta → Gold Delta (BI-ready)",
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ibge", "gold"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="ibge_silver",
        external_task_id="run_silver",
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="poke",
    )

    run_gold_task = PythonOperator(
        task_id="run_gold",
        python_callable=run_gold,
    )

    wait_for_silver >> run_gold_task

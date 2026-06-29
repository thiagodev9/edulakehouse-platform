"""
DAG principal: orquestra Bronze → Silver → Gold em sequência.
Schedulado diariamente às 00:00 UTC.
"""

import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# Adiciona a raiz do projeto ao sys.path para que os módulos
# framework/ e pipelines/ sejam localizáveis pelo Airflow
sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)


###############################################################
# CALLBACKS DE MONITORAMENTO
###############################################################

def on_task_failure(context):
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    execution_date = context["execution_date"]
    print(
        f"[FALHA] DAG={dag_id} | Task={task_id} | "
        f"execution_date={execution_date}"
    )


def on_task_success(context):
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    print(f"[OK] DAG={dag_id} | Task={task_id}")


###############################################################
# CALLABLES
###############################################################

def run_bronze(**context):
    from pipelines.bronze.ibge_pipeline import IBGEBronzePipeline
    IBGEBronzePipeline().run()


def run_silver(**context):
    from pipelines.silver.ibge_silver_pipeline import IBGESilverPipeline
    IBGESilverPipeline().run()


def run_gold(**context):
    from pipelines.gold.ibge_gold_pipeline import IBGEGoldPipeline
    IBGEGoldPipeline().run()


###############################################################
# DEFAULT ARGS
###############################################################

default_args = {
    "owner": "edulakehouse",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": on_task_failure,
    "on_success_callback": on_task_success,
}


###############################################################
# DAG
###############################################################

with DAG(
    dag_id="ibge_lakehouse",
    default_args=default_args,
    description="Pipeline IBGE: Bronze → Silver → Gold",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ibge", "lakehouse", "bronze", "silver", "gold"],
    doc_md="""
    # IBGE Lakehouse Pipeline

    Orquestração completa das camadas Medallion.

    | Camada | Fonte        | Destino             | Partição  |
    |--------|-------------|---------------------|-----------|
    | Bronze | landing/JSON | data/bronze/ibge    | year/month|
    | Silver | Bronze       | data/silver/ibge    | uf_sigla  |
    | Gold   | Silver       | data/gold/ibge_dashboard | regiao_nome |

    ## Agendamento
    Execução diária à meia-noite UTC.

    ## Retries
    3 tentativas com intervalo de 5 minutos.
    """,
) as dag:

    with TaskGroup(
        "bronze_layer",
        tooltip="Camada Bronze — Ingestão"
    ) as bronze_group:

        bronze_task = PythonOperator(
            task_id="run_bronze",
            python_callable=run_bronze,
        )

    with TaskGroup(
        "silver_layer",
        tooltip="Camada Silver — Transformação"
    ) as silver_group:

        silver_task = PythonOperator(
            task_id="run_silver",
            python_callable=run_silver,
        )

    with TaskGroup(
        "gold_layer",
        tooltip="Camada Gold — Agregação para BI"
    ) as gold_group:

        gold_task = PythonOperator(
            task_id="run_gold",
            python_callable=run_gold,
        )

    bronze_group >> silver_group >> gold_group

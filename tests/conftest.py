"""
Fixtures compartilhadas entre todos os testes.
O SparkSession é criado uma única vez por sessão de teste (scope=session).
"""

import os
import sys

import pytest

# Adiciona a raiz do projeto ao path para que os módulos sejam importáveis
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("EduLakehouse-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture(scope="session")
def bronze_schema():
    """Schema completo da camada Bronze (nested structs do IBGE)."""
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType([
        StructField("id", IntegerType()),
        StructField("nome", StringType()),
        StructField("microrregiao", StructType([
            StructField("id", IntegerType()),
            StructField("nome", StringType()),
            StructField("mesorregiao", StructType([
                StructField("id", IntegerType()),
                StructField("nome", StringType()),
                StructField("UF", StructType([
                    StructField("nome", StringType()),
                    StructField("sigla", StringType()),
                    StructField("regiao", StructType([
                        StructField("nome", StringType()),
                    ])),
                ])),
            ])),
        ])),
        StructField("regiao-imediata", StringType()),
        StructField("ingestion_timestamp", TimestampType()),
        StructField("source_system", StringType()),
        StructField("file_name", StringType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
    ])


def _pipeline(cls, spark, **attrs):
    """
    Instancia uma pipeline sem rodar BasePipeline.__init__, injetando a
    SparkSession de teste no lugar de abrir uma nova via SparkManager.
    """
    from framework.logger import LoggerManager

    pipeline = object.__new__(cls)
    pipeline.spark = spark
    pipeline.logger = LoggerManager().get_logger()
    for key, value in attrs.items():
        setattr(pipeline, key, value)
    return pipeline


@pytest.fixture(scope="session")
def bronze_pipeline(spark):
    from pipelines.bronze.ibge_pipeline import IBGEBronzePipeline

    return _pipeline(
        IBGEBronzePipeline, spark,
        input_path="data/landing/ibge/municipios.json",
        output_path="data/bronze/ibge",
    )


@pytest.fixture(scope="session")
def silver_pipeline(spark):
    from pipelines.silver.ibge_silver_pipeline import IBGESilverPipeline

    return _pipeline(
        IBGESilverPipeline, spark,
        input_path="data/bronze/ibge",
        output_path="data/silver/ibge",
    )


@pytest.fixture(scope="session")
def gold_pipeline(spark):
    from pipelines.gold.ibge_gold_pipeline import IBGEGoldPipeline

    return _pipeline(
        IBGEGoldPipeline, spark,
        input_path="data/silver/ibge",
        output_path="data/gold/ibge_dashboard",
    )


@pytest.fixture(scope="session")
def sample_bronze_data():
    """Dados mínimos representando dois municípios de UFs diferentes."""
    return [
        # SP
        (
            3550308, "São Paulo",
            (1004, "São Paulo", (35014, "Metrópole SP", ("São Paulo", "SP", ("Sudeste",)))),
            None, None, "IBGE", "municipios.json", 2025, 1
        ),
        # MG
        (
            3106200, "Belo Horizonte",
            (1501, "Belo Horizonte", (31010, "Metropolitana BH", ("Minas Gerais", "MG", ("Sudeste",)))),
            None, None, "IBGE", "municipios.json", 2025, 1
        ),
    ]

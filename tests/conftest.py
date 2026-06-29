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

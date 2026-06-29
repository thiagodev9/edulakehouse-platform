"""
Testes da camada Gold.
Verifica a agregação por UF, contagens e colunas de metadados.
"""

import pytest


SILVER_COLUMNS = [
    "municipio_id", "municipio_nome", "microrregiao_id", "microrregiao_nome",
    "mesorregiao_id", "mesorregiao_nome", "uf_nome", "uf_sigla", "regiao_nome",
    "ingestion_timestamp", "source_system",
]

GOLD_EXPECTED_COLUMNS = {
    "uf_sigla", "uf_nome", "regiao_nome",
    "total_municipios", "data_processamento", "run_id",
}


@pytest.fixture
def silver_df(spark):
    """DataFrame simulando 3 municípios de SP e 2 de MG."""
    data = [
        (3550308, "São Paulo",      1, "Grande SP", 35, "Metrópole SP", "São Paulo",    "SP", "Sudeste", None, "IBGE"),
        (3550100, "Campinas",       2, "Campinas",  35, "Metrópole SP", "São Paulo",    "SP", "Sudeste", None, "IBGE"),
        (3503208, "Araçatuba",      3, "Araçatuba", 35, "Araçatuba",    "São Paulo",    "SP", "Sudeste", None, "IBGE"),
        (3106200, "Belo Horizonte", 4, "BH",        31, "Metro BH",     "Minas Gerais", "MG", "Sudeste", None, "IBGE"),
        (3118601, "Contagem",       5, "BH",        31, "Metro BH",     "Minas Gerais", "MG", "Sudeste", None, "IBGE"),
    ]
    return spark.createDataFrame(data, SILVER_COLUMNS)


def test_transform_output_columns(spark, silver_df, gold_pipeline):
    result = gold_pipeline.transform(silver_df, "test001")

    assert set(result.columns) == GOLD_EXPECTED_COLUMNS


def test_transform_aggregation_count(spark, silver_df, gold_pipeline):
    result = gold_pipeline.transform(silver_df, "test001")

    # 2 UFs distintas: SP e MG
    assert result.count() == 2


def test_transform_municipios_por_uf(spark, silver_df, gold_pipeline):
    result = gold_pipeline.transform(silver_df, "test001")

    sp = result.filter("uf_sigla = 'SP'").first()
    mg = result.filter("uf_sigla = 'MG'").first()

    assert sp.total_municipios == 3
    assert mg.total_municipios == 2


def test_transform_run_id_propagated(spark, silver_df, gold_pipeline):
    run_id = "abc123"
    result = gold_pipeline.transform(silver_df, run_id)

    run_ids = {row.run_id for row in result.collect()}
    assert run_ids == {run_id}


def test_transform_data_processamento_not_null(spark, silver_df, gold_pipeline):
    from pyspark.sql import functions as F

    result = gold_pipeline.transform(silver_df, "test001")

    null_count = result.filter(F.col("data_processamento").isNull()).count()
    assert null_count == 0


def test_transform_total_municipios_positive(spark, silver_df, gold_pipeline):
    result = gold_pipeline.transform(silver_df, "test001")

    non_positive = result.filter("total_municipios <= 0").count()
    assert non_positive == 0

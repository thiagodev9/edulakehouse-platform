"""
Testes da camada Silver.
Verifica o flatten dos nested structs e a renomeação das colunas.
"""

EXPECTED_COLUMNS = {
    "municipio_id",
    "municipio_nome",
    "microrregiao_id",
    "microrregiao_nome",
    "mesorregiao_id",
    "mesorregiao_nome",
    "uf_nome",
    "uf_sigla",
    "regiao_nome",
    "ingestion_timestamp",
    "source_system",
}


def test_transform_output_columns(spark, bronze_schema, sample_bronze_data, silver_pipeline):
    df = spark.createDataFrame(sample_bronze_data, bronze_schema)
    result = silver_pipeline.transform(df)

    assert set(result.columns) == EXPECTED_COLUMNS


def test_transform_preserves_record_count(spark, bronze_schema, sample_bronze_data, silver_pipeline):
    df = spark.createDataFrame(sample_bronze_data, bronze_schema)
    result = silver_pipeline.transform(df)

    assert result.count() == len(sample_bronze_data)


def test_transform_municipio_id_correct(spark, bronze_schema, sample_bronze_data, silver_pipeline):
    df = spark.createDataFrame(sample_bronze_data, bronze_schema)
    result = silver_pipeline.transform(df)

    ids = {row.municipio_id for row in result.collect()}
    assert 3550308 in ids  # São Paulo
    assert 3106200 in ids  # Belo Horizonte


def test_transform_uf_sigla_correct(spark, bronze_schema, sample_bronze_data, silver_pipeline):
    df = spark.createDataFrame(sample_bronze_data, bronze_schema)
    result = silver_pipeline.transform(df)

    siglas = {row.uf_sigla for row in result.collect()}
    assert "SP" in siglas
    assert "MG" in siglas


def test_transform_regiao_nome_correct(spark, bronze_schema, sample_bronze_data, silver_pipeline):
    df = spark.createDataFrame(sample_bronze_data, bronze_schema)
    result = silver_pipeline.transform(df)

    regioes = {row.regiao_nome for row in result.collect()}
    assert "Sudeste" in regioes


def test_transform_no_nulls_in_key_columns(spark, bronze_schema, sample_bronze_data, silver_pipeline):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(sample_bronze_data, bronze_schema)
    result = silver_pipeline.transform(df)

    for col in ["municipio_id", "municipio_nome", "uf_sigla", "regiao_nome"]:
        null_count = result.filter(F.col(col).isNull()).count()
        assert null_count == 0, f"Coluna {col} contém {null_count} nulls inesperados"

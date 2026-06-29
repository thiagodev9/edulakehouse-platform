"""
Testes da camada Bronze.
Verifica que o transform() adiciona as colunas de metadados corretamente.
"""


def test_transform_adds_source_system(spark):
    from pyspark.sql import functions as F
    from pipelines.bronze.ibge_pipeline import IBGEBronzePipeline

    input_data = [("São Paulo", 3550308)]
    df = spark.createDataFrame(input_data, ["nome", "id"])

    pipeline = IBGEBronzePipeline()
    result = pipeline.transform(df)

    assert "source_system" in result.columns
    assert result.first().source_system == "IBGE"


def test_transform_adds_year_month(spark):
    from pipelines.bronze.ibge_pipeline import IBGEBronzePipeline

    df = spark.createDataFrame([("São Paulo",)], ["nome"])

    pipeline = IBGEBronzePipeline()
    result = pipeline.transform(df)

    assert "year" in result.columns
    assert "month" in result.columns
    row = result.first()
    assert row.year is not None
    assert row.month is not None


def test_transform_adds_ingestion_timestamp(spark):
    from pipelines.bronze.ibge_pipeline import IBGEBronzePipeline

    df = spark.createDataFrame([("São Paulo",)], ["nome"])

    pipeline = IBGEBronzePipeline()
    result = pipeline.transform(df)

    assert "ingestion_timestamp" in result.columns
    assert result.first().ingestion_timestamp is not None


def test_transform_preserves_record_count(spark):
    from pipelines.bronze.ibge_pipeline import IBGEBronzePipeline

    data = [("São Paulo",), ("Belo Horizonte",), ("Curitiba",)]
    df = spark.createDataFrame(data, ["nome"])

    pipeline = IBGEBronzePipeline()
    result = pipeline.transform(df)

    assert result.count() == 3


def test_transform_adds_file_name(spark):
    from pipelines.bronze.ibge_pipeline import IBGEBronzePipeline

    df = spark.createDataFrame([("São Paulo",)], ["nome"])

    pipeline = IBGEBronzePipeline()
    result = pipeline.transform(df)

    assert "file_name" in result.columns

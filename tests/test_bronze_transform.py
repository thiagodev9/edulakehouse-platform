"""
Testes da camada Bronze.
Verifica que o transform() adiciona as colunas de metadados corretamente.
"""


def test_transform_adds_source_system(spark, bronze_pipeline):
    input_data = [("São Paulo", 3550308)]
    df = spark.createDataFrame(input_data, ["nome", "id"])

    result = bronze_pipeline.transform(df)

    assert "source_system" in result.columns
    assert result.first().source_system == "IBGE"


def test_transform_adds_year_month(spark, bronze_pipeline):
    df = spark.createDataFrame([("São Paulo",)], ["nome"])

    result = bronze_pipeline.transform(df)

    assert "year" in result.columns
    assert "month" in result.columns
    row = result.first()
    assert row.year is not None
    assert row.month is not None


def test_transform_adds_ingestion_timestamp(spark, bronze_pipeline):
    df = spark.createDataFrame([("São Paulo",)], ["nome"])

    result = bronze_pipeline.transform(df)

    assert "ingestion_timestamp" in result.columns
    assert result.first().ingestion_timestamp is not None


def test_transform_preserves_record_count(spark, bronze_pipeline):
    data = [("São Paulo",), ("Belo Horizonte",), ("Curitiba",)]
    df = spark.createDataFrame(data, ["nome"])

    result = bronze_pipeline.transform(df)

    assert result.count() == 3


def test_transform_adds_file_name(spark, bronze_pipeline):
    df = spark.createDataFrame([("São Paulo",)], ["nome"])

    result = bronze_pipeline.transform(df)

    assert "file_name" in result.columns

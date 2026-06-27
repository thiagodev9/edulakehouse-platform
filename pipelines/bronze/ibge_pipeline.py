from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
    year,
    month
)

from framework.spark import SparkManager


class IBGEBronzePipeline:

    def __init__(self):

        self.spark = SparkManager().get_session()

        self.input_path = "data/landing/ibge/municipios.json"

        self.output_path = "data/bronze/ibge"

    def extract(self):

        print("Lendo arquivo JSON...")

        return self.spark.read.json(self.input_path)

    def transform(self, df):

        print("Adicionando metadados...")

        return (
            df
            .withColumn(
                "ingestion_timestamp",
                current_timestamp()
            )
            .withColumn(
                "source_system",
                lit("IBGE")
            )
            .withColumn(
                "file_name",
                input_file_name()
            )
            .withColumn(
                "year",
                year("ingestion_timestamp")
            )
            .withColumn(
                "month",
                month("ingestion_timestamp")
            )
        )

    def load(self, df):

        print("Gravando Bronze...")

        (
            df.write
            .mode("overwrite")
            .partitionBy("year", "month")
            .parquet(self.output_path)
        )

    def run(self):

        df = self.extract()

        # Contagem de registros
        record_count = df.count()

        print(f"Total de registros: {record_count}")

        # Validação
        if record_count == 0:
            raise ValueError("Nenhum registro encontrado.")

        print("=" * 80)
        print("SCHEMA")
        print("=" * 80)

        df.printSchema()

        print("=" * 80)
        print("AMOSTRA")
        print("=" * 80)

        df.show(10, truncate=False)

        df = self.transform(df)

        self.load(df)

        print(f"Dados gravados em: {self.output_path}")

        print()

        print("Pipeline Bronze finalizada com sucesso.")


if __name__ == "__main__":

    pipeline = IBGEBronzePipeline()

    pipeline.run()
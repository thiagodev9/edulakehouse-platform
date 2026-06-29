from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
    year,
    month,
)

from framework.base_pipeline import BasePipeline


class IBGEBronzePipeline(BasePipeline):

    def __init__(self):
        super().__init__()
        self.input_path = "data/landing/ibge/municipios.json"
        self.output_path = "data/bronze/ibge"

    def extract(self):
        self.logger.info("Lendo arquivo JSON...")
        return self.spark.read.json(self.input_path)

    def transform(self, df):
        self.logger.info("Adicionando metadados...")
        return (
            df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("IBGE"))
            .withColumn("file_name", input_file_name())
            .withColumn("year", year("ingestion_timestamp"))
            .withColumn("month", month("ingestion_timestamp"))
        )

    def load(self, df):
        self.logger.info("Gravando Bronze...")
        (
            df.write
            .mode("overwrite")
            .partitionBy("year", "month")
            .parquet(self.output_path)
        )

    def run(self):
        df = self.extract()

        record_count = df.count()
        self.logger.info(f"Bronze: {record_count} registros")

        if record_count == 0:
            raise ValueError("Nenhum registro encontrado.")

        if self.spark.conf.get("spark.ui.enabled", "true") != "false":
            self.logger.info("Schema Bronze:")
            df.printSchema()
            self.logger.info("Amostra Bronze (10 registros):")
            df.show(10, truncate=False)

        df = self.transform(df)
        self.load(df)

        self.logger.success(
            f"Pipeline Bronze finalizada | {record_count} registros"
            f" | destino: {self.output_path}"
        )


if __name__ == "__main__":

    pipeline = IBGEBronzePipeline()
    pipeline.run()

from framework.spark import SparkManager
from framework.quality import DataQuality
from framework.schema_validator import SchemaValidator
import time

from framework.monitoring import PipelineMonitor


class IBGESilverPipeline:

    def __init__(self):

        self.spark = SparkManager().get_session()

        self.input_path = "data/bronze/ibge"

        self.output_path = "data/silver/ibge"

    def extract(self):

        print("Lendo Bronze...")

        return self.spark.read.parquet(
            self.input_path
        )

    def transform(self, df):

        print("Flatten dos dados...")

        return (
            df.select(

                df.id.alias("municipio_id"),

                df.nome.alias("municipio_nome"),

                df["microrregiao.id"].alias("microrregiao_id"),

                df["microrregiao.nome"].alias("microrregiao_nome"),

                df["microrregiao.mesorregiao.id"].alias("mesorregiao_id"),

                df["microrregiao.mesorregiao.nome"].alias("mesorregiao_nome"),

                df["microrregiao.mesorregiao.UF.nome"].alias("uf_nome"),

                df["microrregiao.mesorregiao.UF.sigla"].alias("uf_sigla"),

                df["microrregiao.mesorregiao.UF.regiao.nome"].alias("regiao_nome"),

                df.ingestion_timestamp.alias("ingestion_timestamp"),

                df.source_system.alias("source_system")

            )
        )

    def load(self, df):

        print("Gravando Silver...")

        (
            df.write
            .mode("overwrite")
            .parquet(
                self.output_path
            )
        )

    def run(self):
        start = time.time()

        df = self.extract()

        expected_columns = [

            "id",

            "nome",

            "microrregiao",

            "regiao-imediata",

            "ingestion_timestamp",

            "source_system",

            "file_name",

            "year",

            "month"

        ]

        SchemaValidator.validate(
            df,
            expected_columns
        )

        record_count = df.count()

        print(f"Total de registros: {record_count}")

        if record_count == 0:

            raise ValueError(
                "Nenhum registro encontrado na Bronze."
            )

        print()

        print("=" * 80)
        print("SCHEMA BRONZE")
        print("=" * 80)

        df.printSchema()

        df = self.transform(df)

        print()

        print("=" * 80)
        print("REGISTROS COM CAMPOS NULOS")
        print("=" * 80)

        df.filter(

            df.microrregiao_id.isNull()

        ).show(truncate=False)

        DataQuality.generate_report(

            df,

            "municipio_id"

        )

        print()

        print("=" * 80)
        print("SCHEMA SILVER")
        print("=" * 80)

        df.printSchema()

        print()

        print("=" * 80)
        print("AMOSTRA SILVER")
        print("=" * 80)

        df.show(
            10,
            truncate=False
        )

        self.load(df)

        print()

        print(
            f"Dados gravados em: {self.output_path}"
        )

        print()

        print(
            "Pipeline Silver finalizada com sucesso."
        )


if __name__ == "__main__":

    pipeline = IBGESilverPipeline()

    pipeline.run()
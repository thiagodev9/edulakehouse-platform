import time
import uuid
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from framework.audit import AuditManager
from framework.config import (
    PIPELINE_VERSION,
    REPARTITIONS,
    DEBUG,
    SHOW_SAMPLE,
    SHOW_SCHEMA,
    SAVE_AUDIT,
    SAVE_QUALITY,
    SAVE_METRICS
)
from framework.delta_utils import DeltaUtils
from framework.logger import LoggerManager
from framework.quality import DataQuality
from framework.schema_validator import SchemaValidator
from framework.spark import SparkManager
from framework.monitoring import PipelineMonitor


class IBGESilverPipeline:

    def __init__(self):

        self.spark = SparkManager().get_session()

        self.logger = LoggerManager().get_logger()

        self.input_path = "data/bronze/ibge"

        self.output_path = "data/silver/ibge"

    ###########################################################
    # EXTRACT
    ###########################################################

    def extract(self):

        self.logger.info("Lendo Bronze...")

        return self.spark.read.parquet(
            self.input_path
        )

    ###########################################################
    # TRANSFORM
    ###########################################################

    def transform(self, df):

        self.logger.info("Flatten dos dados...")

        # Municípios sem UF na fonte IBGE (ex: Boa Esperança do Norte)
        # são descartados na Silver — dados incompletos não têm valor analítico.
        # _uf_valid evita double count: uma única passagem marca e depois filtra.
        return (
            df.select(

                df.id.alias("municipio_id"),

                df.nome.alias("municipio_nome"),

                df["microrregiao.id"].alias(
                    "microrregiao_id"
                ),

                df["microrregiao.nome"].alias(
                    "microrregiao_nome"
                ),

                df["microrregiao.mesorregiao.id"].alias(
                    "mesorregiao_id"
                ),

                df["microrregiao.mesorregiao.nome"].alias(
                    "mesorregiao_nome"
                ),

                df["microrregiao.mesorregiao.UF.nome"].alias(
                    "uf_nome"
                ),

                df["microrregiao.mesorregiao.UF.sigla"].alias(
                    "uf_sigla"
                ),

                df["microrregiao.mesorregiao.UF.regiao.nome"].alias(
                    "regiao_nome"
                ),

                df.ingestion_timestamp,

                df.source_system,

                F.col("microrregiao.mesorregiao.UF.sigla").isNotNull().alias(
                    "_uf_valid"
                ),

            )
            .filter(F.col("_uf_valid"))
            .drop("_uf_valid")
        )

    ###########################################################
    # LOAD
    ###########################################################

    def load(self, df):

        self.logger.info("Gravando Silver — Delta Lake (particionado por uf_sigla)...")

        DeltaUtils.write_or_merge(
            self.spark,
            df.repartition("uf_sigla"),
            self.output_path,
            merge_condition="target.municipio_id = source.municipio_id",
            partition_by=["uf_sigla"],
        )

        self.logger.info("Executando OPTIMIZE...")
        DeltaUtils.optimize(self.spark, self.output_path)

        self.logger.info("Executando VACUUM (retenção 7 dias)...")
        DeltaUtils.vacuum(self.spark, self.output_path, retention_hours=168)

    ###########################################################
    # RUN
    ###########################################################

    def run(self):

        run_id = uuid.uuid4().hex[:8]

        self.logger.info(f"Iniciando pipeline IBGE Silver | RUN ID: {run_id}")

        pipeline_start = time.perf_counter()

        df_raw = None
        df = None

        try:

            ###################################################
            # EXTRACT
            ###################################################

            extract_start = time.perf_counter()

            df_raw = self.extract()

            # Repartition to parallelize execution over configured Spark partitions
            df_raw = df_raw.repartition(REPARTITIONS)

            # Persist raw partition to prevent repeating shuffles and files scan (Memory and Disk)
            df_raw.persist(StorageLevel.MEMORY_AND_DISK)

            extract_time = (
                time.perf_counter() - extract_start
            )

            self.logger.info(
                f"Extract finalizado em {extract_time:.2f}s"
            )

            self.logger.info(
                f"Partições: {df_raw.rdd.getNumPartitions()}"
            )

            ###################################################
            # SCHEMA VALIDATION
            ###################################################

            validation_start = time.perf_counter()

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
                df_raw,
                expected_columns
            )

            validation_time = (
                time.perf_counter() - validation_start
            )

            ###################################################
            # RECORD COUNT BRONZE
            ###################################################

            count_start = time.perf_counter()

            bronze_count = df_raw.count()

            count_time = (
                time.perf_counter() - count_start
            )

            self.logger.info(
                f"Record Count finalizado em {count_time:.2f}s"
            )

            print(f"Total de registros Bronze: {bronze_count}")

            if bronze_count == 0:

                raise ValueError(
                    "Nenhum registro encontrado na Bronze."
                )

            ###################################################
            # SCHEMA BRONZE
            ###################################################

            if SHOW_SCHEMA:
                print()
                print("=" * 80)
                print("SCHEMA BRONZE")
                print("=" * 80)

                df_raw.printSchema()

            ###################################################
            # TRANSFORM
            ###################################################

            transform_start = time.perf_counter()

            df = self.transform(df_raw)

            # Persist transformed dataframe to speed up multiple quality and load actions (Memory and Disk)
            df.persist(StorageLevel.MEMORY_AND_DISK)

            record_count = df.count()

            dropped_count = bronze_count - record_count
            if dropped_count > 0:
                self.logger.warning(
                    f"{dropped_count} município(s) descartado(s) por uf_sigla nula"
                )

            transform_time = (
                time.perf_counter() - transform_start
            )

            self.logger.info(
                f"Transform finalizado em {transform_time:.2f}s | Silver: {record_count} registros"
            )

            ###################################################
            # REGISTROS INVÁLIDOS
            ###################################################

            if DEBUG:
                print()
                print("=" * 80)
                print("REGISTROS COM CAMPOS NULOS")
                print("=" * 80)

                (
                    df.filter(

                        df.municipio_id.isNull()

                        |

                        df.microrregiao_id.isNull()

                        |

                        df.uf_nome.isNull()

                    )

                    .show(truncate=False)

                )

            ###################################################
            # DATA QUALITY
            ###################################################

            quality_start = time.perf_counter()

            quality_report = DataQuality.generate_report(
                df,
                "municipio_id"
            )

            quality_path = (
                DataQuality.save(quality_report) if SAVE_QUALITY else None
            )

            quality_time = (
                time.perf_counter() - quality_start
            )

            self.logger.info(
                f"Data Quality executada em {quality_time:.2f}s"
            )

            ###################################################
            # SCHEMA SILVER
            ###################################################

            if SHOW_SCHEMA:
                print()
                print("=" * 80)
                print("SCHEMA SILVER")
                print("=" * 80)

                df.printSchema()

            ###################################################
            # SAMPLE
            ###################################################

            if SHOW_SAMPLE:
                print()
                print("=" * 80)
                print("AMOSTRA SILVER")
                print("=" * 80)

                df.show(
                    10,
                    truncate=False
                )

            ###################################################
            # LOAD
            ###################################################

            load_start = time.perf_counter()

            self.load(df)

            load_time = (
                time.perf_counter() - load_start
            )

            # Conta apenas arquivos de dados, excluindo _delta_log (checkpoints)
            written_files = len([
                f for f in Path(self.output_path).rglob("*.parquet")
                if "_delta_log" not in f.parts
            ])

            self.logger.info(
                f"Load finalizado em {load_time:.2f}s | Arquivos Parquet: {written_files}"
            )

            # Unpersist raw and transformed dataframe caches to free memory resources
            df.unpersist()
            df_raw.unpersist()

            ###################################################
            # PIPELINE SUMMARY
            ###################################################

            pipeline_time = (
                time.perf_counter() - pipeline_start
            )

            print()
            print(f"Dados gravados em: {self.output_path}")
            print()

            self.logger.success(
                "Pipeline Silver finalizada com sucesso."
            )

            ###################################################
            # AUDIT
            ###################################################

            audit_start = time.perf_counter()

            audit = AuditManager.generate(
                pipeline_name="IBGE Silver",
                status=quality_report["status"],
                records=record_count,
                quality=quality_report["quality"],
                duration=pipeline_time,
                version=PIPELINE_VERSION
            )

            AuditManager.print(audit)

            audit_path = (
                AuditManager.save(audit) if SAVE_AUDIT else None
            )

            audit_time = (
                time.perf_counter() - audit_start
            )

            # Extract metrics dictionary
            metrics = {
                "extract": extract_time,
                "transform": transform_time,
                "quality": quality_time,
                "load": load_time,
                "total": pipeline_time
            }

            metrics_path = (
                PipelineMonitor.save_metrics(metrics) if SAVE_METRICS else None
            )

            # Summary print
            print()
            print("=" * 60)
            print("PIPELINE SUMMARY")
            print("=" * 60)

            print(f"Extract............. {extract_time:.2f}s")
            print(f"Schema Validation.. {validation_time:.2f}s")
            print(f"Transform.......... {transform_time:.2f}s")
            print(f"Quality............ {quality_time:.2f}s")
            print(f"Load............... {load_time:.2f}s")
            print(f"Audit.............. {audit_time:.2f}s")

            print("-" * 60)

            print(f"TOTAL.............. {pipeline_time:.2f}s")
            print(f"REGISTROS.......... {record_count}")
            print(
                f"QUALIDADE.......... {quality_report['quality']}%"
            )
            print(f"PIPELINE RESULT.... SUCCESS")
            print(
                f"QUALITY STATUS..... {quality_report['status']}"
            )
            print(f"ARQUIVOS ESCRITOS.. {written_files}")
            print(f"RUN ID............. {run_id}")

            print()
            print("=" * 60)
            print("OUTPUTS")
            print("=" * 60)
            print(f"Silver.............. {self.output_path}")
            if audit_path:
                print(f"Audit............... {audit_path}")
            if metrics_path:
                print(f"Metrics............. {metrics_path}")
            if quality_path:
                print(f"Quality............. {quality_path}")
            print(f"Run ID.............. {run_id}")

            self.logger.info(
                f"Pipeline executada em {pipeline_time:.2f}s | RUN ID: {run_id}"
            )

        except Exception as e:

            # Make sure we unpersist caches in case of errors
            if df is not None:
                try:
                    df.unpersist()
                except Exception:
                    pass
            if df_raw is not None:
                try:
                    df_raw.unpersist()
                except Exception:
                    pass

            self.logger.exception(
                f"Erro durante execução da pipeline: {e}"
            )

            raise


if __name__ == "__main__":

    pipeline = IBGESilverPipeline()

    pipeline.run()
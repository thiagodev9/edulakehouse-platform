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
from framework.monitoring import PipelineMonitor
from framework.quality import DataQuality
from framework.schema_validator import SchemaValidator
from framework.spark import SparkManager


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

        return self.spark.read.parquet(self.input_path)

    ###########################################################
    # TRANSFORM
    ###########################################################

    def transform(self, df):

        self.logger.info("Flatten dos dados...")

        # Municípios sem UF na fonte IBGE (ex: Boa Esperança do Norte)
        # são descartados na Silver — dados incompletos não têm valor analítico.
        # _uf_valid marca em uma única passagem para evitar double Spark action.
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
                df.ingestion_timestamp,
                df.source_system,
                F.col("microrregiao.mesorregiao.UF.sigla").isNotNull().alias("_uf_valid"),
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
    # SUMMARY
    ###########################################################

    def _log_summary(self, *, run_id, timings, record_count, quality_report,
                     written_files, audit_path, metrics_path, quality_path):

        t = timings
        self.logger.info(
            "\n"
            + "=" * 60 + "\n"
            + "PIPELINE SUMMARY\n"
            + "=" * 60 + "\n"
            + f"Extract............. {t['extract']:.2f}s\n"
            + f"Schema Validation.. {t['validation']:.2f}s\n"
            + f"Transform.......... {t['transform']:.2f}s\n"
            + f"Quality............ {t['quality']:.2f}s\n"
            + f"Load............... {t['load']:.2f}s\n"
            + f"Audit.............. {t['audit']:.2f}s\n"
            + "-" * 60 + "\n"
            + f"TOTAL.............. {t['pipeline']:.2f}s\n"
            + f"REGISTROS.......... {record_count}\n"
            + f"QUALIDADE.......... {quality_report['quality']}%\n"
            + f"PIPELINE RESULT.... SUCCESS\n"
            + f"QUALITY STATUS..... {quality_report['status']}\n"
            + f"ARQUIVOS ESCRITOS.. {written_files}\n"
            + f"RUN ID............. {run_id}\n"
            + "\n"
            + "=" * 60 + "\n"
            + "OUTPUTS\n"
            + "=" * 60 + "\n"
            + f"Silver.............. {self.output_path}\n"
            + (f"Audit............... {audit_path}\n" if audit_path else "")
            + (f"Metrics............. {metrics_path}\n" if metrics_path else "")
            + (f"Quality............. {quality_path}\n" if quality_path else "")
            + f"Run ID.............. {run_id}"
        )

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
            df_raw = df_raw.repartition(REPARTITIONS)
            df_raw.persist(StorageLevel.MEMORY_AND_DISK)
            extract_time = time.perf_counter() - extract_start

            self.logger.info(
                f"Extract: {extract_time:.2f}s"
                f" | partições: {df_raw.rdd.getNumPartitions()}"
            )

            ###################################################
            # SCHEMA VALIDATION
            ###################################################

            validation_start = time.perf_counter()

            SchemaValidator.validate(df_raw, [
                "id", "nome", "microrregiao", "regiao-imediata",
                "ingestion_timestamp", "source_system", "file_name",
                "year", "month",
            ])

            validation_time = time.perf_counter() - validation_start

            ###################################################
            # RECORD COUNT BRONZE
            ###################################################

            bronze_count = df_raw.count()
            self.logger.info(f"Bronze: {bronze_count} registros")

            if bronze_count == 0:
                raise ValueError("Nenhum registro encontrado na Bronze.")

            ###################################################
            # SCHEMA BRONZE
            ###################################################

            if SHOW_SCHEMA:
                self.logger.info("Schema Bronze:")
                df_raw.printSchema()

            ###################################################
            # TRANSFORM
            ###################################################

            transform_start = time.perf_counter()
            df = self.transform(df_raw)
            df.persist(StorageLevel.MEMORY_AND_DISK)
            transform_time = time.perf_counter() - transform_start

            self.logger.info(f"Transform: {transform_time:.2f}s")

            ###################################################
            # REGISTROS INVÁLIDOS (debug)
            ###################################################

            if DEBUG:
                self.logger.info("Registros com campos nulos após transform:")
                df.filter(
                    df.municipio_id.isNull()
                    | df.microrregiao_id.isNull()
                    | df.uf_nome.isNull()
                ).show(truncate=False)

            ###################################################
            # DATA QUALITY
            ###################################################

            quality_start = time.perf_counter()

            quality_report = DataQuality.generate_report(df, "municipio_id")
            quality_path = DataQuality.save(quality_report) if SAVE_QUALITY else None

            quality_time = time.perf_counter() - quality_start

            # record_count vem do quality_report — sem Spark action extra
            record_count = quality_report["records"]
            dropped_count = bronze_count - record_count
            if dropped_count > 0:
                self.logger.warning(
                    f"{dropped_count} município(s) descartado(s) por uf_sigla nula"
                )

            self.logger.info(
                f"Quality: {quality_time:.2f}s"
                f" | {record_count} registros"
                f" | {quality_report['quality']}%"
                f" | {quality_report['status']}"
            )

            ###################################################
            # SCHEMA SILVER
            ###################################################

            if SHOW_SCHEMA:
                self.logger.info("Schema Silver:")
                df.printSchema()

            ###################################################
            # SAMPLE
            ###################################################

            if SHOW_SAMPLE:
                self.logger.info("Amostra Silver (10 registros):")
                df.show(10, truncate=False)

            ###################################################
            # LOAD
            ###################################################

            load_start = time.perf_counter()
            self.load(df)
            load_time = time.perf_counter() - load_start

            written_files = len([
                f for f in Path(self.output_path).rglob("*.parquet")
                if "_delta_log" not in f.parts
            ])

            self.logger.info(
                f"Load: {load_time:.2f}s | {written_files} arquivos Parquet"
            )

            df.unpersist()
            df_raw.unpersist()

            pipeline_time = time.perf_counter() - pipeline_start

            self.logger.success("Pipeline Silver finalizada com sucesso.")

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
                version=PIPELINE_VERSION,
            )

            AuditManager.print(audit)
            audit_path = AuditManager.save(audit) if SAVE_AUDIT else None
            audit_time = time.perf_counter() - audit_start

            ###################################################
            # METRICS
            ###################################################

            metrics_path = PipelineMonitor.save_metrics({
                "extract": extract_time,
                "transform": transform_time,
                "quality": quality_time,
                "load": load_time,
                "total": pipeline_time,
            }) if SAVE_METRICS else None

            ###################################################
            # SUMMARY
            ###################################################

            self._log_summary(
                run_id=run_id,
                timings={
                    "extract": extract_time,
                    "validation": validation_time,
                    "transform": transform_time,
                    "quality": quality_time,
                    "load": load_time,
                    "audit": audit_time,
                    "pipeline": pipeline_time,
                },
                record_count=record_count,
                quality_report=quality_report,
                written_files=written_files,
                audit_path=audit_path,
                metrics_path=metrics_path,
                quality_path=quality_path,
            )

            self.logger.info(
                f"Pipeline executada em {pipeline_time:.2f}s | RUN ID: {run_id}"
            )

        except Exception as e:

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

            self.logger.exception(f"Erro durante execução da pipeline: {e}")
            raise


if __name__ == "__main__":

    pipeline = IBGESilverPipeline()
    pipeline.run()
